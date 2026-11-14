// MongoDB 驱动:官方 mongo-driver v2, 纯 Go 无 cgo
// sql 字段传 runCommand 语法的 JSON 命令文档, 单个对象或对象数组
// 不做命令分类: 每条命令原样经 RunCommand 下发, 回复文档按顶层字段平铺成一行
package mongodb

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/fengfffffff/database-mcp-go/internal/driver"
	"github.com/fengfffffff/database-mcp-go/internal/model"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/readpref"
)

// init 把 MongoDB 工厂注册进驱动注册表
func init() {

	driver.Register(model.DatabaseTypeMongoDB, NewPool)
}

// pool MongoDB 连接池, 客户端自带连接管理, 按库各建一个
type pool struct {
	client *mongo.Client
	db     *mongo.Database
}

// NewPool 由实例连接配置构造连接池
func NewPool(cfg *model.DataSourceConfig) (driver.Pool, error) {

	minIdle, maxOpen, connTimeout, idleTimeout, _ := cfg.EffectivePool()
	// 文件模式的数据源不走保存校验, 这里再兜一次, 避免密码被静默丢掉
	if cfg.Password != "" && cfg.Username == "" {
		return nil, fmt.Errorf("MongoDB 填了密码就必须同时填实例用户名 - 实例别名: %s", cfg.Alias)
	}
	u := &url.URL{
		Scheme: "mongodb",
		Host:   net.JoinHostPort(cfg.Host, strconv.Itoa(cfg.Port)),
	}
	if cfg.Username != "" {
		u.User = url.UserPassword(cfg.Username, cfg.Password)
	}
	props := driver.ParseProperties(cfg.ConnectionProperties)
	applyConnectionProperties(u, props, connTimeout)

	clientOpts := options.Client().
		ApplyURI(u.String()).
		SetMaxPoolSize(uint64(maxOpen)).
		SetMinPoolSize(uint64(minIdle)).
		SetMaxConnIdleTime(time.Duration(idleTimeout) * time.Millisecond)
	client, err := mongo.Connect(clientOpts)
	if err != nil {
		return nil, fmt.Errorf("初始化 MongoDB 客户端失败 - 实例别名: %s, 错误: %w", cfg.Alias, err)
	}
	// Connect 不建连, 建池时探一次让地址与账号问题当场暴露
	ctx, cancel := context.WithTimeout(context.Background(), driver.ProbeTimeout(connTimeout))
	defer cancel()
	if err := client.Ping(ctx, readpref.Primary()); err != nil {
		_ = client.Disconnect(context.Background())
		return nil, fmt.Errorf("连接 MongoDB 失败 - 实例别名: %s, 地址: %s, 错误: %w", cfg.Alias, u.Host, err)
	}
	return &pool{client: client, db: client.Database(cfg.DatabaseName)}, nil
}

// applyConnectionProperties 把连接属性里的常用项映射到 URI 参数
// authSource、replicaSet、tls 与 connectTimeoutMS 是部署 MongoDB 时最常要配的四项
// 连接超时以连接属性为准, 属性没写才用连接池参数兜底
func applyConnectionProperties(u *url.URL, props map[string]string, connTimeoutMS int64) {

	params := url.Values{}
	if v := props["authsource"]; v != "" {
		params.Set("authSource", v)
	}
	if v := props["replicaset"]; v != "" {
		params.Set("replicaSet", v)
	}
	if v, ok := props["tls"]; ok && (strings.EqualFold(v, "true") || v == "1") {
		params.Set("tls", "true")
	}
	if v := props["connecttimeoutms"]; v != "" {
		params.Set("connectTimeoutMS", v)
	} else if v := props["connecttimeout"]; v != "" {
		// 连接属性与其它驱动保持一致, 按毫秒理解, 不做单位猜测
		params.Set("connectTimeoutMS", v)
	} else if connTimeoutMS > 0 {
		// 属性没配时用连接池参数, 连不上地址时才能按预期超时
		params.Set("connectTimeoutMS", strconv.FormatInt(connTimeoutMS, 10))
	}
	// 服务选择超时没有对应的 JDBC 属性, 统一跟连接池参数走
	if connTimeoutMS > 0 {
		params.Set("serverSelectionTimeoutMS", strconv.FormatInt(connTimeoutMS, 10))
	}
	if len(params) > 0 {
		u.RawQuery = params.Encode()
	}
}

// Acquire 取一个连接
func (p *pool) Acquire(ctx context.Context) (driver.Conn, error) {

	return &conn{db: p.db}, nil
}

// Close 关闭连接池
func (p *pool) Close() {

	_ = p.client.Disconnect(context.Background())
}

// conn 占用中的连接, MongoDB 无连接概念, 命令直接下发
type conn struct {
	db *mongo.Database
}

// Ping 连通性检查
func (c *conn) Ping(ctx context.Context) error {

	return c.db.Client().Ping(ctx, readpref.Primary())
}

// Close 归还连接
func (c *conn) Close() {

}

// Execute 解析 JSON 命令文档并逐条原样下发
// 回复文档按顶层字段平铺成一行, 命令是否合法交给服务端判断
func (c *conn) Execute(ctx context.Context, sqlText string, opts driver.ExecOptions) (*driver.Outcome, error) {

	cmds, err := parseCommands(sqlText)
	if err != nil {
		return nil, err
	}
	outcome := &driver.Outcome{
		Rows:       make([]model.OrderedRow, 0, 8),
		Statements: make([]driver.StatementOutcome, 0, len(cmds)),
	}
	for i, cmd := range cmds {
		stmtCtx := ctx
		var cancel context.CancelFunc
		if opts.QueryTimeout > 0 {
			stmtCtx, cancel = context.WithTimeout(ctx, opts.QueryTimeout)
		}
		stmtOutcome := &driver.StatementOutcome{Index: i + 1, Rows: make([]model.OrderedRow, 0, 1)}
		var reply bson.M
		execErr := c.db.RunCommand(stmtCtx, cmd).Decode(&reply)
		if cancel != nil {
			cancel()
		}
		if execErr != nil {
			return nil, driver.NewStmtError(i+1, len(cmds), commandText(cmd), execErr, describeError(execErr))
		}
		if opts.MaxRows <= 0 || len(outcome.Rows) < opts.MaxRows {
			row := driver.BuildRow(docColumns(reply), docValues(reply))
			outcome.Rows = append(outcome.Rows, row)
			stmtOutcome.Rows = append(stmtOutcome.Rows, row)
		} else {
			outcome.Truncated = true
			stmtOutcome.Truncated = true
		}
		outcome.Statements = append(outcome.Statements, *stmtOutcome)
	}
	return outcome, nil
}

// parseCommands 把 sql 字段解析成有序命令文档列表, 单个对象或对象数组
// 用扩展 JSON 解析器而不是 encoding/json, 后者解析到 map 会丢失键序
func parseCommands(sqlText string) ([]bson.D, error) {

	trimmed := strings.TrimSpace(sqlText)
	if trimmed == "" {
		return nil, fmt.Errorf("命令不能为空, MongoDB 传 runCommand 语法的 JSON 命令文档")
	}
	switch trimmed[0] {
	case '[':
		// 扩展 JSON 解析器会忽略结尾的多余内容, 这里按首尾字符兜一层
		if trimmed[len(trimmed)-1] != ']' {
			return nil, fmt.Errorf("命令解析失败 - JSON 数组后有多余内容")
		}
		var arr bson.A
		if err := bson.UnmarshalExtJSON([]byte(trimmed), false, &arr); err != nil {
			return nil, fmt.Errorf("命令解析失败 - 须为合法的 JSON 或扩展 JSON: %v", err)
		}
		if len(arr) == 0 {
			return nil, fmt.Errorf("命令解析失败 - 命令数组不能为空")
		}
		cmds := make([]bson.D, 0, len(arr))
		for i, item := range arr {
			d, ok := item.(bson.D)
			if !ok || len(d) == 0 {
				return nil, fmt.Errorf("命令解析失败 - 数组第 %d 项须为非空 JSON 对象", i+1)
			}
			cmds = append(cmds, normalizeIDHex(d))
		}
		return cmds, nil
	case '{':
		if trimmed[len(trimmed)-1] != '}' {
			return nil, fmt.Errorf("命令解析失败 - JSON 对象后有多余内容")
		}
		var d bson.D
		if err := bson.UnmarshalExtJSON([]byte(trimmed), false, &d); err != nil {
			return nil, fmt.Errorf("命令解析失败 - 须为合法的 JSON 或扩展 JSON: %v", err)
		}
		if len(d) == 0 {
			return nil, fmt.Errorf("命令解析失败 - 命令文档不能为空")
		}
		return []bson.D{normalizeIDHex(d)}, nil
	default:
		return nil, fmt.Errorf("命令解析失败 - 顶层须为 JSON 对象或对象数组")
	}
}

// normalizeIDHex 把 _id 上的 24 位十六进制字符串转成 ObjectId
// 输出侧把 ObjectId 出成十六进制文本, 这样复制查询结果里的 _id 就能直接查回
func normalizeIDHex(d bson.D) bson.D {

	out := make(bson.D, len(d))
	for i, e := range d {
		out[i] = bson.E{Key: e.Key, Value: normalizeIDValue(e.Key, e.Value)}
	}
	return out
}

// normalizeIDValue 递归处理嵌套文档与数组, 只对 _id 键上的字符串生效
func normalizeIDValue(key string, value any) any {

	if key == "_id" {
		if s, ok := value.(string); ok && isHex24(s) {
			if oid, err := bson.ObjectIDFromHex(s); err == nil {
				return oid
			}
		}
	}
	switch x := value.(type) {
	case bson.D:
		return normalizeIDHex(x)
	case bson.A:
		out := make(bson.A, len(x))
		for i, item := range x {
			out[i] = normalizeIDValue("", item)
		}
		return out
	}
	return value
}

// docColumns 回复文档的顶层字段, _id 永远排第一, 其余按键名排序保证输出稳定
func docColumns(doc bson.M) []string {

	if _, ok := doc["_id"]; ok {
		cols := make([]string, 0, len(doc))
		cols = append(cols, "_id")
		rest := make([]string, 0, len(doc)-1)
		for key := range doc {
			if key != "_id" {
				rest = append(rest, key)
			}
		}
		sort.Strings(rest)
		return append(cols, rest...)
	}
	keys := make([]string, 0, len(doc))
	for key := range doc {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

// docValues 与 docColumns 同序取值并转成可序列化的普通值
func docValues(doc bson.M) []any {

	cols := docColumns(doc)
	values := make([]any, 0, len(cols))
	for _, key := range cols {
		values = append(values, bsonToPlain(doc[key]))
	}
	return values
}

// bsonToPlain 把驱动返回的 BSON 值转成可序列化的普通值
// ObjectId 出十六进制文本, 时间转本机时区, 嵌套文档与数组转普通值后由归一化层输出 JSON 文本
func bsonToPlain(v any) any {

	switch x := v.(type) {
	case nil, bool, string, int, int32, int64, float64, time.Time:
		return x
	case bson.ObjectID:
		return x.Hex()
	case bson.DateTime:
		// 服务端按 UTC 存时间, 输出转成本机时区, 与其它驱动的本地时间口径一致
		return x.Time().Local()
	case bson.Decimal128:
		return x.String()
	case bson.M:
		out := make(map[string]any, len(x))
		for key, value := range x {
			out[key] = bsonToPlain(value)
		}
		return out
	case bson.D:
		out := make(map[string]any, len(x))
		for _, e := range x {
			out[e.Key] = bsonToPlain(e.Value)
		}
		return out
	case bson.A:
		out := make([]any, 0, len(x))
		for _, item := range x {
			out = append(out, bsonToPlain(item))
		}
		return out
	case []any:
		out := make([]any, 0, len(x))
		for _, item := range x {
			out = append(out, bsonToPlain(item))
		}
		return out
	case bson.Binary:
		// 二进制内容交给归一化层, 合法 UTF-8 出文本否则 Base64
		return x.Data
	default:
		return fmt.Sprintf("%v", x)
	}
}

// isHex24 判断是否为 24 位十六进制字符串, ObjectId 的标准文本长度
func isHex24(s string) bool {

	if len(s) != 24 {
		return false
	}
	for _, r := range s {
		if !strings.ContainsRune("0123456789abcdefABCDEF", r) {
			return false
		}
	}
	return true
}

// commandText 命令压成扩展 JSON 文本, 用于报错时附上原文
func commandText(cmd bson.D) string {

	raw, err := bson.MarshalExtJSON(cmd, false, true)
	if err != nil {
		return fmt.Sprintf("%v", cmd)
	}
	return string(raw)
}

// errorCodes MongoDB 常见错误码的中文类别与修正建议
var errorCodes = map[int32]struct {
	category string
	advice   string
}{
	2:     {"参数值非法", "核对命令里各字段的取值"},
	9:     {"命令解析失败", "核对命令文档的写法与字段名"},
	13:    {"权限不足", "核对实例用户名对该库或集合的权限"},
	14:    {"类型不匹配", "核对字段取值与实际类型是否一致"},
	18:    {"账号或密码错误", "核对实例用户名与密码, 以及 authSource 认证库"},
	26:    {"命名空间不存在", "核对集合名, 可先用 listCollections 命令查看实际集合"},
	48:    {"命名空间已存在", "换一个集合名或先 drop"},
	50:    {"查询超时或被中止", "缩小查询范围或调整超时配置"},
	59:    {"命令不存在", "核对命令名是否为 MongoDB 支持的命令"},
	73:    {"名称非法", "核对库名与集合名的写法"},
	121:   {"文档校验失败", "核对写入文档是否满足集合的 validator 规则"},
	125:   {"文件打开失败", "核对数据文件路径"},
	211:   {"分片键缺失", "核对分片集合的写入是否带分片键"},
	11000: {"唯一键冲突", "该值已存在, 换一个值或在写入里加 upsert 语义"},
}

// describeError 把 MongoDB 错误拆成结构化信息,非数据库返回的错误返回 nil
func describeError(err error) *driver.ErrorInfo {

	var cmdErr mongo.CommandError
	if errors.As(err, &cmdErr) {
		info := &driver.ErrorInfo{
			Code:    strconv.Itoa(int(cmdErr.Code)),
			Message: strings.TrimSpace(cmdErr.Message),
		}
		if detail, ok := errorCodes[cmdErr.Code]; ok {
			info.Category = detail.category
			info.Advice = detail.advice
		}
		return info
	}
	var writeErr mongo.WriteException
	if errors.As(err, &writeErr) {
		for _, we := range writeErr.WriteErrors {
			info := &driver.ErrorInfo{
				Code:    strconv.Itoa(we.Code),
				Message: strings.TrimSpace(we.Message),
			}
			if detail, ok := errorCodes[int32(we.Code)]; ok {
				info.Category = detail.category
				info.Advice = detail.advice
			}
			return info
		}
	}
	return nil
}
