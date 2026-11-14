// SQL Server 驱动:按实例别名与数据库建连接池,执行时整批放在一个事务里逐条执行
// 逐条执行是为了拿到每条写语句的受影响行数,并让报错定位到具体第几条语句
package sqlserver

import (
	"context"
	"database/sql"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/fengfffffff/database-mcp-go/internal/driver"
	"github.com/fengfffffff/database-mcp-go/internal/driver/sqlsplit"
	"github.com/fengfffffff/database-mcp-go/internal/model"
	mssql "github.com/microsoft/go-mssqldb"
)

// init 把 SQL Server 工厂注册进驱动注册表
func init() {

	driver.Register(model.DatabaseTypeSQLServer, NewPool)
}

// pool SQL Server 连接池
type pool struct {
	db *sql.DB
}

// NewPool 由实例连接配置构造连接池
func NewPool(cfg *model.DataSourceConfig) (driver.Pool, error) {

	minIdle, maxOpen, connTimeout, idleTimeout, lifeTime := cfg.EffectivePool()
	db, err := sql.Open("sqlserver", buildDSN(cfg, connTimeout))
	if err != nil {
		return nil, fmt.Errorf("初始化 SQLServer 连接池失败 - 实例别名: %s, 错误: %w", cfg.Alias, err)
	}
	db.SetMaxOpenConns(maxOpen)
	db.SetMaxIdleConns(minIdle)
	db.SetConnMaxIdleTime(time.Duration(idleTimeout) * time.Millisecond)
	db.SetConnMaxLifetime(time.Duration(lifeTime) * time.Millisecond)
	return &pool{db: db}, nil
}

// buildDSN 拼装连接串
// 库名走 database 参数; encrypt 默认关闭以适配内网实例, 云上或强制加密的实例用连接属性覆盖
func buildDSN(cfg *model.DataSourceConfig, connTimeoutMS int64) string {

	u := &url.URL{
		Scheme: "sqlserver",
		User:   url.UserPassword(cfg.Username, cfg.Password),
		Host:   net.JoinHostPort(cfg.Host, strconv.Itoa(cfg.Port)),
	}
	params := url.Values{}
	params.Set("database", cfg.DatabaseName)
	params.Set("encrypt", "disable")
	params.Set("app name", "database-mcp-go")
	timeoutSeconds := strconv.FormatInt(effectiveTimeoutSeconds(connTimeoutMS), 10)
	params.Set("connection timeout", timeoutSeconds)
	params.Set("dial timeout", timeoutSeconds)
	applyConnectionProperties(params, cfg.ConnectionProperties)
	u.RawQuery = params.Encode()
	return u.String()
}

// effectiveTimeoutSeconds 毫秒转秒,不足一秒按一秒算,0 与负数取驱动默认
func effectiveTimeoutSeconds(connTimeoutMS int64) int64 {

	if connTimeoutMS <= 0 {
		return 15
	}
	seconds := connTimeoutMS / 1000
	if seconds < 1 {
		return 1
	}
	return seconds
}

// nativeParams 可直接透传给 go-mssqldb 的参数
var nativeParams = map[string]string{
	"encrypt":                "encrypt",
	"trustservercertificate": "trustservercertificate",
	"appname":                "app name",
	"applicationname":        "app name",
	"packetsize":             "packet size",
	"multisubnetfailover":    "multisubnetfailover",
	"applicationintent":      "applicationintent",
	"workstationid":          "workstation id",
}

// applyConnectionProperties 把 JDBC 写法的连接属性映射成驱动原生参数
// JDBC 专有属性在 Go 驱动里没有对应物, 直接忽略, 避免被当成未知参数导致连接失败
func applyConnectionProperties(params url.Values, raw string) {

	props := driver.ParseProperties(raw)
	for key, value := range props {
		switch key {
		case "databasename":
			params.Set("database", value)
		case "connecttimeout":
			// JDBC 是毫秒, go-mssqldb 是秒; 取值非法时保留连接池算出的默认值
			if ms := parseMillis(value); ms > 0 {
				params.Set("connection timeout", strconv.FormatInt(effectiveTimeoutSeconds(ms), 10))
			}
		case "logintimeout":
			params.Set("dial timeout", value)
		case "selectmethod", "responsebuffering", "sendstringparametersasunicode", "sockettimeout",
			"locktimeout", "querytimeout", "instancename", "servername", "guidconversion", "guid conversion":
			// JDBC 专有属性, 命名实例请直接填实例的监听端口
			// guid conversion 由本驱动按服务端原始字节序自行还原, 不能交给驱动再转一次
		default:
			if canonical, ok := nativeParams[key]; ok {
				params.Set(canonical, value)
			}
		}
	}
}

// parseMillis 毫秒文本转数值,解析失败返回 0
func parseMillis(value string) int64 {

	var ms int64
	_, err := fmt.Sscanf(strings.TrimSpace(value), "%d", &ms)
	if err != nil {
		return 0
	}
	return ms
}

// Acquire 取一个连接
func (p *pool) Acquire(ctx context.Context) (driver.Conn, error) {

	c, err := p.db.Conn(ctx)
	if err != nil {
		return nil, fmt.Errorf("获取 SQLServer 连接失败: %w", err)
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

	stmts := sqlsplit.SplitTSQL(sqlText)
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
			guidColumns := guidColumnIndexes(rows, len(columns))
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
				formatGUIDColumns(values, guidColumns)
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

// guidColumnIndexes 找出结果集里 uniqueidentifier 类型的列
// 驱动把该类型读成 16 字节, 直接序列化会变成 Base64, 这里转成标准 GUID 文本
func guidColumnIndexes(rows *sql.Rows, count int) []bool {

	types, err := rows.ColumnTypes()
	if err != nil || len(types) < count {
		return nil
	}
	indexes := make([]bool, count)
	found := false
	for i := 0; i < count; i++ {
		if strings.EqualFold(types[i].DatabaseTypeName(), "uniqueidentifier") {
			indexes[i] = true
			found = true
		}
	}
	if !found {
		return nil
	}
	return indexes
}

// formatGUIDColumns 把 GUID 列的 16 字节按 SQL Server 的文本形式输出
func formatGUIDColumns(values []any, guidColumns []bool) {

	if guidColumns == nil {
		return
	}
	for i := range values {
		if i >= len(guidColumns) || !guidColumns[i] {
			continue
		}
		if raw, ok := values[i].([]byte); ok && len(raw) == 16 {
			values[i] = formatGUID(raw)
		}
	}
}

// formatGUID 按 SQL Server 的存储字节序还原成 8-4-4-4-12 文本
// 前三段在服务端是小端存放, 后 8 字节原样输出
func formatGUID(b []byte) string {

	return fmt.Sprintf("%08X-%04X-%04X-%02X%02X-%02X%02X%02X%02X%02X%02X",
		binary.LittleEndian.Uint32(b[0:4]),
		binary.LittleEndian.Uint16(b[4:6]),
		binary.LittleEndian.Uint16(b[6:8]),
		b[8], b[9], b[10], b[11], b[12], b[13], b[14], b[15])
}

// outputPattern 匹配 T-SQL 的 OUTPUT 子句
var outputPattern = regexp.MustCompile(`(?i)\bOUTPUT\b`)

// isQueryStatement 判断语句是否会返回结果集
// 写语句走 Exec 以便拿到准确的受影响行数, 但带 OUTPUT 子句的写语句会返回行数据, 必须走查询路径
func isQueryStatement(stmt string) bool {

	kw := firstKeyword(stmt)
	switch kw {
	case "SELECT", "WITH", "EXEC", "EXECUTE", "DBCC":
		return true
	}
	if strings.HasPrefix(kw, "SP_") || strings.HasPrefix(kw, "XP_") {
		return true
	}
	switch kw {
	case "INSERT", "UPDATE", "DELETE", "MERGE":
		return outputPattern.MatchString(stmt)
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

// errorCodes SQL Server 常见错误码的中文类别与修正建议
var errorCodes = map[int32]struct {
	category string
	advice   string
}{
	102:   {"语法错误", "按原始信息里的 near 片段与行号修改语句"},
	105:   {"语法错误", "核对引号是否成对闭合"},
	156:   {"语法错误", "核对关键字附近是否有保留字未转义, 保留字可用方括号包裹"},
	207:   {"字段不存在", "核对列名, 可先查询 INFORMATION_SCHEMA.COLUMNS 查看实际列"},
	208:   {"表不存在", "核对库名与表名, 可先查询 INFORMATION_SCHEMA.TABLES 查看实际表名"},
	209:   {"列名歧义", "多表关联时给同名列加上表别名前缀"},
	223:   {"对象依赖冲突", "先处理引用该对象的约束或索引"},
	229:   {"权限不足", "核对实例用户名对该对象的 SELECT 或 EXECUTE 权限"},
	245:   {"类型转换失败", "核对比较或赋值两侧的数据类型是否一致"},
	262:   {"权限不足", "核对实例用户名是否有 CREATE 权限"},
	515:   {"非空约束", "该列不允许 NULL, 请补上字段值"},
	547:   {"外键或检查约束", "先处理主表与子表的引用关系, 或核对值是否满足检查约束"},
	1205:  {"死锁", "调整语句顺序或缩短事务后重试"},
	1222:  {"锁请求超时", "缩短事务或稍后重试"},
	2601:  {"唯一索引冲突", "该值已存在, 换一个值或改用 MERGE 写法"},
	2627:  {"唯一约束冲突", "该值已存在, 换一个值或改用 MERGE 写法"},
	2628:  {"数据截断", "缩短写入内容, 注意 NVARCHAR 与 VARCHAR 的长度差异"},
	2714:  {"对象已存在", "换一个名称或先判断 OBJECT_ID 是否存在"},
	2812:  {"存储过程不存在", "核对存储过程名与架构名前缀"},
	3701:  {"对象不存在", "核对要删除的对象名, 可先判断 OBJECT_ID 是否存在"},
	3906:  {"数据库只读", "数据库处于只读状态, 需先解除只读"},
	3960:  {"快照隔离冲突", "稍后重试, 或改用读已提交快照外的隔离级别"},
	4060:  {"无法打开数据库", "核对数据库名与实例用户名对该库的访问权限"},
	4064:  {"无法打开默认数据库", "为该登录名指定一个可用的默认数据库"},
	8114:  {"类型转换失败", "核对写入值能否转换为列类型"},
	8134:  {"除数为零", "在除数位置加 NULLIF 或 CASE 判断"},
	8152:  {"数据超长", "缩短写入内容或改用 MAX 长度类型"},
	8645:  {"内存授予超时", "缩小排序或哈希的数据量, 或稍后重试"},
	9002:  {"事务日志已满", "备份事务日志或扩大日志文件"},
	1105:  {"磁盘空间不足", "清理实例所在磁盘或扩大数据文件"},
	18456: {"登录失败", "核对实例用户名与密码, 以及该登录名是否允许连接"},
	50000: {"用户自定义错误", "按原始信息里的提示处理, 通常来自 RAISERROR 或存储过程校验"},
}

// describeError 把 SQL Server 错误拆成结构化信息,非数据库返回的错误返回 nil
func describeError(err error) *driver.ErrorInfo {

	var mssqlErr mssql.Error
	if !errors.As(err, &mssqlErr) {
		return nil
	}
	info := &driver.ErrorInfo{
		Code:    strconv.Itoa(int(mssqlErr.Number)),
		Message: mssqlErr.Message,
	}
	if mssqlErr.ProcName != "" {
		info.Detail = "存储过程: " + mssqlErr.ProcName
	}
	if mssqlErr.LineNo > 0 {
		info.Location = fmt.Sprintf("第 %d 行", mssqlErr.LineNo)
	}
	if detail, ok := errorCodes[mssqlErr.Number]; ok {
		info.Category = detail.category
		info.Advice = detail.advice
	}
	return info
}
