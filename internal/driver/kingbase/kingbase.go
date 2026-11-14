// 金仓驱动:Kingbase ES V8 走 PostgreSQL 协议直连,用 pgx 原生连接与 pgconn 低层接口收割多结果集
// 不依赖厂商 JDBC 驱动,这是 Go 版能支持金仓的关键
package kingbase

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/fengfffffff/database-mcp-go/internal/driver"
	"github.com/fengfffffff/database-mcp-go/internal/driver/sqlsplit"
	"github.com/fengfffffff/database-mcp-go/internal/model"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
)

// init 把金仓工厂注册进驱动注册表
func init() {

	driver.Register(model.DatabaseTypeKingbase8, NewPool)
}

// pool 金仓连接池
type pool struct {
	p *pgxpool.Pool
}

// NewPool 由实例连接配置构造连接池
func NewPool(cfg *model.DataSourceConfig) (driver.Pool, error) {

	minIdle, maxOpen, connTimeout, idleTimeout, lifeTime := cfg.EffectivePool()
	u := &url.URL{
		Scheme: "postgres",
		User:   url.UserPassword(cfg.Username, cfg.Password),
		Host:   net.JoinHostPort(cfg.Host, strconv.Itoa(cfg.Port)),
		Path:   "/" + cfg.DatabaseName,
	}
	props := driver.ParseProperties(cfg.ConnectionProperties)
	params := url.Values{}
	params.Set("sslmode", "disable")
	applyConnectionProperties(params, props)
	u.RawQuery = params.Encode()

	poolCfg, err := pgxpool.ParseConfig(u.String())
	if err != nil {
		return nil, fmt.Errorf("解析金仓连接串失败 - 实例别名: %s, 错误: %w", cfg.Alias, err)
	}
	// 金仓对扩展协议的兼容度有限,统一走简单协议,与 spike 验证过的路径一致
	poolCfg.ConnConfig.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol
	// 连接属性里写了连接超时就用属性值, 没写才用连接池参数
	if !driver.HasAny(props, "connect_timeout", "connecttimeout") {
		poolCfg.ConnConfig.ConnectTimeout = time.Duration(connTimeout) * time.Millisecond
	}
	poolCfg.MaxConns = int32(maxOpen)
	poolCfg.MinConns = int32(minIdle)
	poolCfg.MaxConnIdleTime = time.Duration(idleTimeout) * time.Millisecond
	poolCfg.MaxConnLifetime = time.Duration(lifeTime) * time.Millisecond

	ctx, cancel := context.WithTimeout(context.Background(), driver.ProbeTimeout(connTimeout))
	defer cancel()
	p, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		return nil, fmt.Errorf("初始化金仓连接池失败 - 实例别名: %s, 错误: %w", cfg.Alias, err)
	}
	return &pool{p: p}, nil
}

// Acquire 取一个连接
func (p *pool) Acquire(ctx context.Context) (driver.Conn, error) {

	c, err := p.p.Acquire(ctx)
	if err != nil {
		return nil, fmt.Errorf("获取金仓连接失败: %w", err)
	}
	return &conn{c: c}, nil
}

// Close 关闭连接池
func (p *pool) Close() {

	p.p.Close()
}

// nativeParams 可直接透传给 pgx 的连接参数
var nativeParams = map[string]struct{}{
	"sslmode":              {},
	"connect_timeout":      {},
	"application_name":     {},
	"search_path":          {},
	"statement_timeout":    {},
	"options":              {},
	"target_session_attrs": {},
}

// applyConnectionProperties 把 JDBC 写法的连接属性映射成 pgx 可识别的参数
// useUnicode、serverTimezone 这类属性金仓服务端没有对应配置项,原样透传会导致连接失败
func applyConnectionProperties(params url.Values, props map[string]string) {

	for key, value := range props {
		switch key {
		case "useunicode", "characterencoding", "zerodatetimebehavior", "allowpublickeyretrieval",
			"servertimezone", "usessl":
			// 与金仓连接无关,忽略
		case "currentschema":
			// JDBC 的 currentSchema 对应 PostgreSQL 的 search_path
			params.Set("search_path", value)
		case "connecttimeout":
			// JDBC 是毫秒,pgx 的 connect_timeout 只接受整数秒
			params.Set("connect_timeout", strconv.FormatInt(parseMillisToSeconds(value), 10))
		default:
			if _, ok := nativeParams[key]; ok {
				params.Set(key, value)
			}
		}
	}
}

// parseMillisToSeconds 毫秒文本转整秒, 不足一秒按一秒算, 解析失败取默认 5 秒
func parseMillisToSeconds(value string) int64 {

	var ms int64
	_, err := fmt.Sscanf(strings.TrimSpace(value), "%d", &ms)
	if err != nil || ms <= 0 {
		return 5
	}
	seconds := ms / 1000
	if seconds < 1 {
		seconds = 1
	}
	return seconds
}

// conn 占用中的连接
type conn struct {
	c *pgxpool.Conn
}

// Ping 连通性检查
func (c *conn) Ping(ctx context.Context) error {

	return c.c.Ping(ctx)
}

// Close 归还连接
func (c *conn) Close() {

	c.c.Release()
}

// Execute 在一个事务内逐条执行 SQL,每条语句用 pgconn 低层接口收割全部结果集
func (c *conn) Execute(ctx context.Context, sqlText string, opts driver.ExecOptions) (*driver.Outcome, error) {

	stmts := sqlsplit.SplitPostgres(sqlText)
	if len(stmts) == 0 {
		return nil, fmt.Errorf("SQL 语句不能为空")
	}
	tx, err := c.c.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("开启事务失败: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback(context.Background())
		}
	}()

	typeMap := tx.Conn().TypeMap()
	pgConn := tx.Conn().PgConn()
	outcome := &driver.Outcome{
		Rows:       make([]model.OrderedRow, 0, 8),
		Statements: make([]driver.StatementOutcome, 0, len(stmts)),
	}
	for i, stmt := range stmts {
		stmtCtx := ctx
		var cancel context.CancelFunc
		if opts.QueryTimeout > 0 {
			stmtCtx, cancel = context.WithTimeout(ctx, opts.QueryTimeout)
		}
		stmtOutcome := &driver.StatementOutcome{Index: i + 1, Rows: make([]model.OrderedRow, 0, 4)}
		execErr := execStatement(stmtCtx, pgConn, typeMap, stmt, opts, outcome, stmtOutcome)
		if cancel != nil {
			cancel()
		}
		if execErr != nil {
			return nil, driver.NewStmtError(i+1, len(stmts), stmt, execErr, describeError(execErr, stmt))
		}
		outcome.Statements = append(outcome.Statements, *stmtOutcome)
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("提交事务失败: %w", err)
	}
	committed = true
	return outcome, nil
}

// execStatement 执行单条语句并把结果并入累计结果与单条明细
// 逐行读取而不是把整个结果集读进内存, max_rows 才能真正限制内存占用
func execStatement(ctx context.Context, pgConn *pgconn.PgConn, typeMap *pgtype.Map, stmt string, opts driver.ExecOptions, outcome *driver.Outcome, stmtOutcome *driver.StatementOutcome) error {

	multi := pgConn.Exec(ctx, stmt)
	for multi.NextResult() {
		reader := multi.ResultReader()
		fields := reader.FieldDescriptions()
		// 无字段描述说明是写操作,读完命令标签累计受影响行数
		if len(fields) == 0 {
			tag, err := reader.Close()
			if err != nil {
				_ = multi.Close()
				return err
			}
			affected := tag.RowsAffected()
			outcome.Affected += affected
			stmtOutcome.Affected += affected
			continue
		}
		columns := make([]string, len(fields))
		for i, fd := range fields {
			columns[i] = fd.Name
		}
		for reader.NextRow() {
			if opts.MaxRows > 0 && len(outcome.Rows) >= opts.MaxRows {
				outcome.Truncated = true
				stmtOutcome.Truncated = true
				break
			}
			raw := reader.Values()
			values := make([]any, len(raw))
			for i, cell := range raw {
				if cell == nil {
					values[i] = nil
					continue
				}
				var decoded any
				if scanErr := typeMap.Scan(fields[i].DataTypeOID, fields[i].Format, cell, &decoded); scanErr != nil {
					// 未知类型退化为原始文本,保证整批不因单列出错而失败
					values[i] = string(cell)
					continue
				}
				values[i] = decoded
			}
			row := driver.BuildRow(columns, values)
			outcome.Rows = append(outcome.Rows, row)
			stmtOutcome.Rows = append(stmtOutcome.Rows, row)
		}
		// Close 读完当前结果集的剩余数据并拿到命令标签, 行数据本身不保留
		if _, err := reader.Close(); err != nil {
			_ = multi.Close()
			return err
		}
		if outcome.Truncated {
			return multi.Close()
		}
	}
	return multi.Close()
}

// errorStates 金仓常见 SQLSTATE 的中文类别与修正建议
var errorStates = map[string]struct {
	category string
	advice   string
}{
	"3D000": {"数据库不存在", "核对数据库名, 可先调 get-datasources 查看该实例已配置的库"},
	"42501": {"权限不足", "核对实例用户名对该对象的权限"},
	"42601": {"语法错误", "按位置定位到出错片段后修改语句"},
	"42703": {"字段不存在", "核对列名, 可先查询 information_schema.columns 查看实际列"},
	"42883": {"函数不存在", "核对函数名与参数类型"},
	"42P01": {"表不存在", "核对表名与 search_path, 可先查询 information_schema.tables 查看实际表名"},
	"42P07": {"表已存在", "换一个表名或改用 IF NOT EXISTS"},
	"23502": {"非空约束", "该列不允许 NULL, 请补上字段值"},
	"23503": {"外键约束", "先处理主表与子表的引用关系"},
	"23505": {"唯一键冲突", "该值已存在, 换一个值或改用 ON CONFLICT 写法"},
	"22001": {"数据超长", "缩短写入内容或改用更长的列类型"},
	"22P02": {"值格式不正确", "核对写入值与列类型是否匹配"},
	"40001": {"序列化失败", "稍后重试"},
	"40P01": {"死锁", "调整语句顺序或缩短事务后重试"},
	"57014": {"查询被取消", "缩短查询或调整超时配置"},
}

// describeError 把金仓错误拆成结构化信息,非数据库返回的错误返回 nil
func describeError(err error, stmt string) *driver.ErrorInfo {

	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) {
		return nil
	}
	info := &driver.ErrorInfo{
		SQLState: pgErr.Code,
		Message:  pgErr.Message,
		Detail:   pgErr.Detail,
		Hint:     pgErr.Hint,
		Location: errorLocation(stmt, pgErr.Position),
	}
	if detail, ok := errorStates[pgErr.Code]; ok {
		info.Category = detail.category
		info.Advice = detail.advice
	}
	return info
}

// errorLocation 把服务端给的字符偏移换算成行列
func errorLocation(stmt string, position int32) string {

	if position <= 0 {
		return ""
	}
	runes := []rune(stmt)
	index := int(position) - 1
	if index > len(runes) {
		index = len(runes)
	}
	line, column := 1, 1
	for _, r := range runes[:index] {
		if r == '\n' {
			line++
			column = 1
			continue
		}
		column++
	}
	return fmt.Sprintf("第 %d 行第 %d 列", line, column)
}
