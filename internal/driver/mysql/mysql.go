// MySQL 驱动:按实例别名与数据库建连接池,执行时整批放在一个事务里逐条执行
// 逐条执行是为了拿到每条写语句的受影响行数,go-sql-driver 在多语句模式下会丢弃中间结果
package mysql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/fengfffffff/database-mcp-go/internal/driver"
	"github.com/fengfffffff/database-mcp-go/internal/driver/sqlsplit"
	"github.com/fengfffffff/database-mcp-go/internal/model"
	gomysql "github.com/go-sql-driver/mysql"
)

// init 把 MySQL 工厂注册进驱动注册表
// MariaDB、TiDB、OceanBase 的 MySQL 模式共用同一套线协议, 直接复用本驱动
func init() {

	driver.Register(model.DatabaseTypeMySQL, NewPool)
	driver.Register(model.DatabaseTypeMariaDB, NewPool)
	driver.Register(model.DatabaseTypeTiDB, NewPool)
	driver.Register(model.DatabaseTypeOceanBase, NewPool)
}

// pool MySQL 协议族的连接池
type pool struct {
	db    *sql.DB
	label string
}

// NewPool 由实例连接配置构造连接池
func NewPool(cfg *model.DataSourceConfig) (driver.Pool, error) {

	label := model.DatabaseTypeDesc(cfg.DatabaseType)
	if label == "" {
		label = "MySQL"
	}
	minIdle, maxOpen, connTimeout, idleTimeout, lifeTime := cfg.EffectivePool()
	db, err := sql.Open("mysql", buildDSN(cfg, connTimeout))
	if err != nil {
		return nil, fmt.Errorf("初始化 %s 连接池失败 - 实例别名: %s, 错误: %w", label, cfg.Alias, err)
	}
	db.SetMaxOpenConns(maxOpen)
	db.SetMaxIdleConns(minIdle)
	db.SetConnMaxIdleTime(time.Duration(idleTimeout) * time.Millisecond)
	db.SetConnMaxLifetime(time.Duration(lifeTime) * time.Millisecond)
	return &pool{db: db, label: label}, nil
}

// buildDSN 拼装连接串,时区与编码按项目口径固定
// 不设固定读写超时: 语句中止由上下文超时负责, 驱动会关连接并返回上下文错误
// 需要 socket 级超时时用连接属性 socketTimeout, 它会被映射成 readTimeout 与 writeTimeout
func buildDSN(cfg *model.DataSourceConfig, connTimeoutMS int64) string {

	params := url.Values{}
	params.Set("parseTime", "true")
	params.Set("loc", "Asia/Shanghai")
	params.Set("charset", "utf8mb4")
	params.Set("interpolateParams", "false")
	params.Set("timeout", strconv.FormatInt(connTimeoutMS, 10)+"ms")
	applyConnectionProperties(params, cfg.ConnectionProperties)
	// 用户名与密码要按参数值转义, 空格须转成 %20, MySQL 驱动按路径语义解码, 加号不会被还原成空格
	user := escapeDSNValue(cfg.Username)
	pass := escapeDSNValue(cfg.Password)
	return fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?%s", user, pass, cfg.Host, cfg.Port, cfg.DatabaseName, params.Encode())
}

// escapeDSNValue 连接串取值转义, 覆盖空格与特殊字符
func escapeDSNValue(v string) string {

	return strings.ReplaceAll(url.QueryEscape(v), "+", "%20")
}

// nativeParams 可直接透传给 go-sql-driver 的参数
var nativeParams = map[string]string{
	"parsetime":         "parseTime",
	"timeout":           "timeout",
	"readtimeout":       "readTimeout",
	"writetimeout":      "writeTimeout",
	"tls":               "tls",
	"charset":           "charset",
	"collation":         "collation",
	"loc":               "loc",
	"interpolateparams": "interpolateParams",
	"maxallowedpacket":  "maxAllowedPacket",
	"clientfoundrows":   "clientFoundRows",
}

// applyConnectionProperties 把 JDBC 写法的连接属性映射成驱动原生参数
// 配置里的连接属性是给 JDBC 用的,原样透传会被当成未定义的系统变量报错,必须逐个映射
func applyConnectionProperties(params url.Values, raw string) {

	props := driver.ParseProperties(raw)
	for key, value := range props {
		switch key {
		case "useunicode", "characterencoding", "zerodatetimebehavior", "allowpublickeyretrieval", "servertimezone":
			// 由固定的 charset 与 loc 覆盖,无需透传
		case "usessl":
			if strings.EqualFold(value, "true") {
				params.Set("tls", "preferred")
			} else {
				params.Set("tls", "false")
			}
		case "sslmode":
			if strings.EqualFold(value, "disabled") {
				params.Set("tls", "false")
			} else {
				params.Set("tls", "skip-verify")
			}
		case "connecttimeout":
			params.Set("timeout", value+"ms")
		case "sockettimeout":
			params.Set("readTimeout", value+"ms")
			params.Set("writeTimeout", value+"ms")
		case "sessionvariables":
			for name, val := range driver.SplitSessionVariables(value) {
				// 系统变量须带引号写入,驱动会原样拼进 SET 语句
				params.Set(name, "'"+val+"'")
			}
		default:
			if canonical, ok := nativeParams[key]; ok {
				params.Set(canonical, value)
			}
		}
	}
}

// Acquire 取一个连接
func (p *pool) Acquire(ctx context.Context) (driver.Conn, error) {

	c, err := p.db.Conn(ctx)
	if err != nil {
		return nil, fmt.Errorf("获取 %s 连接失败: %w", p.label, err)
	}
	return &conn{c: c, label: p.label}, nil
}

// Close 关闭连接池
func (p *pool) Close() {

	_ = p.db.Close()
}

// conn 占用中的连接
type conn struct {
	c     *sql.Conn
	label string
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

	stmts := sqlsplit.Split(sqlText)
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

	for {
		columns, err := rows.Columns()
		if err != nil {
			return fmt.Errorf("读取列信息失败: %w", err)
		}
		if len(columns) > 0 {
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
		}
		if outcome.Truncated {
			// 截断后主动关闭, 把数据库后续返回的错误暴露出来, 不能静默当成功
			return rows.Close()
		}
		if !rows.NextResultSet() {
			return nil
		}
	}
}

// isQueryStatement 判断语句是否会返回结果集
// 只按首关键字分类,写语句一律走 Exec 以便拿到准确的受影响行数
func isQueryStatement(stmt string) bool {

	kw := firstKeyword(stmt)
	switch kw {
	case "SELECT", "WITH", "SHOW", "DESC", "DESCRIBE", "EXPLAIN", "CALL", "VALUES", "TABLE", "ANALYZE", "OPTIMIZE", "CHECK", "HANDLER":
		return true
	default:
		return false
	}
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
		case strings.HasPrefix(s, "--"), strings.HasPrefix(s, "#"):
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

// errorCodes MySQL 常见错误码的中文类别与修正建议
var errorCodes = map[uint16]struct {
	category string
	advice   string
}{
	1044: {"权限不足", "核对实例用户名对该库的权限"},
	1045: {"账号或密码错误", "核对实例用户名与密码"},
	1048: {"非空约束", "该列不允许 NULL, 请补上字段值"},
	1049: {"数据库不存在", "核对数据库名, 可先调 get-datasources 查看该实例已配置的库"},
	1050: {"表已存在", "换一个表名或改用 IF NOT EXISTS"},
	1054: {"字段不存在", "核对列名, 可先执行 DESC 表名 查看实际列"},
	1060: {"字段已存在", "该表已有同名列, 核对列名"},
	1062: {"唯一键冲突", "该值已存在, 换一个值或改用忽略冲突的写法"},
	1064: {"语法错误", "按原始信息里的 near 片段与行号修改语句"},
	1136: {"列数与值数不一致", "核对 INSERT 的列清单与 VALUES 的值个数"},
	1142: {"权限不足", "核对实例用户名对该表的权限"},
	1146: {"表不存在", "核对库名与表名, 可先执行 SHOW TABLES 查看该库实际表名"},
	1205: {"锁等待超时", "缩短事务或稍后重试"},
	1213: {"死锁", "调整语句顺序或缩短事务后重试"},
	1292: {"值格式不正确", "核对日期与数值的格式是否与列类型匹配"},
	1305: {"函数或存储过程不存在", "核对函数名与库名"},
	1366: {"值类型不匹配", "核对写入值与列类型是否一致"},
	1406: {"数据超长", "缩短写入内容或改用更长的列类型"},
	1451: {"外键约束", "先处理引用该记录的子表数据"},
	1452: {"外键约束", "先写入被引用的主表记录"},
}

// describeError 把 MySQL 错误拆成结构化信息,非数据库返回的错误返回 nil
func describeError(err error) *driver.ErrorInfo {

	var mysqlErr *gomysql.MySQLError
	if !errors.As(err, &mysqlErr) {
		return nil
	}
	info := &driver.ErrorInfo{
		Code:     strconv.Itoa(int(mysqlErr.Number)),
		SQLState: strings.TrimRight(string(mysqlErr.SQLState[:]), "\x00"),
		Message:  mysqlErr.Message,
	}
	if detail, ok := errorCodes[mysqlErr.Number]; ok {
		info.Category = detail.category
		info.Advice = detail.advice
	}
	return info
}
