// SQLite 驱动:纯 Go 实现, 无需 cgo, 单个数据库文件即一个实例
// 实例地址填数据库文件的绝对路径, 库名固定用 main, 一次执行的多条语句同事务
package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/fengfffffff/database-mcp-go/internal/driver"
	"github.com/fengfffffff/database-mcp-go/internal/driver/sqlsplit"
	"github.com/fengfffffff/database-mcp-go/internal/model"
	sqlite3 "modernc.org/sqlite"
)

// init 把 SQLite 工厂注册进驱动注册表
func init() {

	driver.Register(model.DatabaseTypeSQLite, NewPool)
}

// pool SQLite 连接池
type pool struct {
	db *sql.DB
}

// NewPool 由实例连接配置构造连接池
func NewPool(cfg *model.DataSourceConfig) (driver.Pool, error) {

	path := strings.TrimSpace(cfg.Host)
	if path == "" {
		return nil, fmt.Errorf("初始化 SQLite 连接池失败 - 实例别名: %s, 实例地址须填数据库文件的绝对路径", cfg.Alias)
	}
	minIdle, maxOpen, connTimeout, idleTimeout, lifeTime := cfg.EffectivePool()
	// 单文件库写入要排队, 连接数压到 4 以内, 避免自己跟自己抢锁
	if maxOpen > 4 {
		maxOpen = 4
	}
	if minIdle > maxOpen {
		minIdle = maxOpen
	}
	db, err := sql.Open("sqlite", buildDSN(path))
	if err != nil {
		return nil, fmt.Errorf("初始化 SQLite 连接池失败 - 实例别名: %s, 错误: %w", cfg.Alias, err)
	}
	db.SetMaxOpenConns(maxOpen)
	db.SetMaxIdleConns(minIdle)
	db.SetConnMaxIdleTime(time.Duration(idleTimeout) * time.Millisecond)
	db.SetConnMaxLifetime(time.Duration(lifeTime) * time.Millisecond)
	// 建池时探一次, 文件打不开或路径写错要在建池阶段就暴露
	ctx, cancel := context.WithTimeout(context.Background(), driver.ProbeTimeout(connTimeout))
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("打开 SQLite 数据库文件失败 - 实例别名: %s, 路径: %s, 错误: %w", cfg.Alias, path, err)
	}
	return &pool{db: db}, nil
}

// buildDSN 拼装连接串, 开启 WAL 与忙等待, 减少并发读写时的锁冲突
func buildDSN(path string) string {

	normalized := escapePath(strings.ReplaceAll(path, "\\", "/"))
	params := url.Values{}
	params.Add("_pragma", "busy_timeout(5000)")
	params.Add("_pragma", "journal_mode(WAL)")
	params.Set("_foreign_keys", "1")
	return "file:" + normalized + "?" + params.Encode()
}

// escapePath 按路径段做转义, 保留分隔符, 让含空格与中文的路径在 URI 里可用
func escapePath(path string) string {

	segments := strings.Split(path, "/")
	for i, segment := range segments {
		segments[i] = url.PathEscape(segment)
	}
	return strings.Join(segments, "/")
}

// Acquire 取一个连接
func (p *pool) Acquire(ctx context.Context) (driver.Conn, error) {

	c, err := p.db.Conn(ctx)
	if err != nil {
		return nil, fmt.Errorf("获取 SQLite 连接失败: %w", err)
	}
	return &conn{c: c}, nil
}

// Close 关闭连接池
func (p *pool) Close() {

	_ = p.db.Close()
}

// conn 占用中的连接
type conn struct {
	c *sql.Conn
}

// Ping 连通性检查
func (c *conn) Ping(ctx context.Context) error {

	return c.c.PingContext(ctx)
}

// Close 归还连接
func (c *conn) Close() {

	_ = c.c.Close()
}

// Execute 在一个事务内逐条执行 SQL
func (c *conn) Execute(ctx context.Context, sqlText string, opts driver.ExecOptions) (*driver.Outcome, error) {

	stmts := sqlsplit.SplitSQLite(sqlText)
	if len(stmts) == 0 {
		return nil, fmt.Errorf("SQL 语句不能为空")
	}
	tx, err := c.c.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("开启事务失败: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()

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
		if isQueryStatement(stmt) {
			if err := readQuery(stmtCtx, tx, stmt, opts, outcome, stmtOutcome); err != nil {
				if cancel != nil {
					cancel()
				}
				return nil, driver.NewStmtError(i+1, len(stmts), stmt, err, describeError(err))
			}
		} else {
			res, execErr := tx.ExecContext(stmtCtx, stmt)
			if execErr != nil {
				if cancel != nil {
					cancel()
				}
				return nil, driver.NewStmtError(i+1, len(stmts), stmt, execErr, describeError(execErr))
			}
			if affected, aerr := res.RowsAffected(); aerr == nil {
				stmtOutcome.Affected = affected
				outcome.Affected += affected
			}
		}
		outcome.Statements = append(outcome.Statements, *stmtOutcome)
		if cancel != nil {
			cancel()
		}
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("提交事务失败: %w", err)
	}
	committed = true
	return outcome, nil
}

// readQuery 执行查询类语句并累计行数据
func readQuery(ctx context.Context, tx *sql.Tx, stmt string, opts driver.ExecOptions, outcome *driver.Outcome, stmtOutcome *driver.StatementOutcome) error {

	rows, err := tx.QueryContext(ctx, stmt)
	if err != nil {
		return err
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return fmt.Errorf("读取列信息失败: %w", err)
	}
	if len(columns) == 0 {
		return nil
	}
	for rows.Next() {
		if opts.MaxRows > 0 && len(outcome.Rows) >= opts.MaxRows {
			outcome.Truncated = true
			stmtOutcome.Truncated = true
			break
		}
		values := make([]any, len(columns))
		ptrs := make([]any, len(columns))
		for i := range values {
			ptrs[i] = &values[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return fmt.Errorf("读取行数据失败: %w", err)
		}
		row := driver.BuildRow(columns, values)
		outcome.Rows = append(outcome.Rows, row)
		stmtOutcome.Rows = append(stmtOutcome.Rows, row)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("遍历结果集失败: %w", err)
	}
	if outcome.Truncated {
		// 截断后主动关闭, 把数据库后续返回的错误暴露出来, 不能静默当成功
		return rows.Close()
	}
	return nil
}

// returningPattern 匹配 SQLite 3.35 起的 RETURNING 子句
var returningPattern = regexp.MustCompile(`(?i)\bRETURNING\b`)

// isQueryStatement 判断语句是否会返回结果集
// 写语句走 Exec 以便拿到准确的受影响行数, 带 RETURNING 的写语句会返回行数据, 必须走查询路径
func isQueryStatement(stmt string) bool {

	kw := firstKeyword(stmt)
	switch kw {
	case "SELECT", "WITH", "PRAGMA", "EXPLAIN", "VALUES":
		return true
	}
	switch kw {
	case "INSERT", "UPDATE", "DELETE":
		return returningPattern.MatchString(stmt)
	}
	return false
}

// firstKeyword 取语句的首个关键字,跳过前置注释与空白
func firstKeyword(stmt string) string {

	s := strings.TrimSpace(stmt)
	for {
		switch {
		case strings.HasPrefix(s, "/*"):
			if end := strings.Index(s, "*/"); end >= 0 {
				s = strings.TrimSpace(s[end+2:])
				continue
			}
			return ""
		case strings.HasPrefix(s, "--"):
			if idx := strings.IndexAny(s, "\r\n"); idx >= 0 {
				s = strings.TrimSpace(s[idx+1:])
				continue
			}
			return ""
		case strings.HasPrefix(s, "("):
			// 括号开头的通常是子查询包裹的 SELECT
			s = strings.TrimSpace(s[1:])
			continue
		}
		break
	}
	for i, r := range s {
		if r == ' ' || r == '\t' || r == '\n' || r == '\r' || r == '(' {
			return strings.ToUpper(s[:i])
		}
	}
	return strings.ToUpper(s)
}

// errorCodes SQLite 基础错误码的中文类别与修正建议, 扩展码按位与 0xFF 归到基础码
var errorCodes = map[int]struct {
	category string
	advice   string
}{
	1:  {"执行出错", "按原始信息定位, 常见为表或字段不存在、语法错误"},
	5:  {"数据库被占用", "写入被其它连接占用, 稍后重试或减少并发写"},
	6:  {"表被锁定", "表处于锁定状态, 稍后重试"},
	8:  {"数据库只读", "文件或所在目录不可写, 核对文件权限"},
	11: {"数据库文件损坏", "用 sqlite3 的完整性检查确认, 必要时从备份恢复"},
	13: {"磁盘空间不足", "清理磁盘或扩大所在分区"},
	14: {"无法打开数据库文件", "核对实例地址里的文件路径是否存在、是否可写"},
	19: {"约束冲突", "核对主键、唯一键、外键与非空约束"},
	20: {"类型不匹配", "核对写入值与列的实际类型"},
	21: {"使用方式错误", "核对语句写法是否符合 SQLite 语法"},
	26: {"不是数据库文件", "实例地址指向的文件不是 SQLite 数据库"},
}

// describeError 把 SQLite 错误拆成结构化信息,非数据库返回的错误返回 nil
func describeError(err error) *driver.ErrorInfo {

	var sqliteErr *sqlite3.Error
	if !errors.As(err, &sqliteErr) {
		return nil
	}
	code := sqliteErr.Code()
	info := &driver.ErrorInfo{
		Code:    strconv.Itoa(code),
		Message: strings.TrimSpace(sqliteErr.Error()),
	}
	base := code & 0xFF
	if detail, ok := errorCodes[base]; ok {
		info.Category = detail.category
		info.Advice = detail.advice
	}
	// 扩展码细分约束类型, 提示更具体
	switch code {
	case 1555:
		info.Category = "主键冲突"
		info.Advice = "该主键已存在, 换一个主键值或改用 INSERT OR REPLACE"
	case 2067:
		info.Category = "唯一键冲突"
		info.Advice = "该值已存在, 换一个值或改用 INSERT OR IGNORE / ON CONFLICT 写法"
	case 787:
		info.Category = "外键约束"
		info.Advice = "先处理主表与子表的引用关系"
	case 1299:
		info.Category = "非空约束"
		info.Advice = "该列不允许 NULL, 请补上字段值"
	case 275:
		info.Category = "检查约束不满足"
		info.Advice = "核对写入值是否满足表的检查约束"
	}
	// 基础码 1 的原始信息里带具体原因, 再细化一次类别
	if base == 1 {
		msg := strings.ToLower(info.Message)
		switch {
		case strings.Contains(msg, "no such table"):
			info.Category = "表不存在"
			info.Advice = "核对表名, 可先查询 sqlite_master 查看实际表名"
		case strings.Contains(msg, "no such column"):
			info.Category = "字段不存在"
			info.Advice = "核对列名, 可先执行 PRAGMA table_info(表名) 查看实际列"
		case strings.Contains(msg, "syntax error"):
			info.Category = "语法错误"
			info.Advice = "按原始信息里的 near 片段修改语句"
		}
	}
	return info
}
