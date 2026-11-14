// Redis 驱动:官方 go-redis v9, 纯 Go 无 cgo
// sql 字段每行一条 Redis 命令, 参数空格分隔, 含空格的参数用双引号或单引号包裹
// 标量回复占一行一列 result, 数组回复每个元素一行, 嵌套结构输出 JSON 文本
package redis

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/fengfffffff/database-mcp-go/internal/driver"
	"github.com/fengfffffff/database-mcp-go/internal/model"
	goredis "github.com/redis/go-redis/v9"
)

// init 把 Redis 工厂注册进驱动注册表
func init() {

	driver.Register(model.DatabaseTypeRedis, NewPool)
}

// pool Redis 连接池
type pool struct {
	client *goredis.Client
}

// NewPool 由实例连接配置构造连接池
// 库名填 Redis 的逻辑库编号, 0 至 15, 常用 0
func NewPool(cfg *model.DataSourceConfig) (driver.Pool, error) {

	dbIndex, err := strconv.Atoi(strings.TrimSpace(cfg.DatabaseName))
	if err != nil || dbIndex < 0 || dbIndex > 65535 {
		return nil, fmt.Errorf("Redis 的库名须为逻辑库编号 0 至 65535 - 实例别名: %s, 库名: %s", cfg.Alias, cfg.DatabaseName)
	}
	minIdle, maxOpen, connTimeout, idleTimeout, lifeTime := cfg.EffectivePool()
	// go-redis 默认走 RESP3, 映射类命令的回复是 map, 归一化层已按 JSON 文本输出
	// 阻塞类命令的等待不受上下文约束, 只能靠连接级读取超时兜底, 默认与语句超时对齐的 30 秒
	ioTimeout := readTimeout(cfg.ConnectionProperties)
	client := goredis.NewClient(&goredis.Options{
		Addr:            net.JoinHostPort(cfg.Host, strconv.Itoa(cfg.Port)),
		Username:        cfg.Username,
		Password:        cfg.Password,
		DB:              dbIndex,
		PoolSize:        maxOpen,
		MinIdleConns:    minIdle,
		ConnMaxIdleTime: time.Duration(idleTimeout) * time.Millisecond,
		ConnMaxLifetime: time.Duration(lifeTime) * time.Millisecond,
		DialTimeout:     time.Duration(connTimeout) * time.Millisecond,
		ReadTimeout:     ioTimeout,
		WriteTimeout:    ioTimeout,
		// 关掉驱动重试, 否则超时会被重试次数成倍放大, 阻塞类命令尤其明显
		MaxRetries: -1,
	})
	// NewClient 不建连, 建池时探一次让地址与密码问题当场暴露
	ctx, cancel := context.WithTimeout(context.Background(), driver.ProbeTimeout(connTimeout))
	defer cancel()
	if err := client.Ping(ctx).Err(); err != nil {
		_ = client.Close()
		return nil, fmt.Errorf("连接 Redis 失败 - 实例别名: %s, 地址: %s, 库: %d, 错误: %w", cfg.Alias, cfg.Host+":"+strconv.Itoa(cfg.Port), dbIndex, err)
	}
	return &pool{client: client}, nil
}

// readTimeout 读取超时, 连接属性 readtimeout 按毫秒覆盖, 缺省 30 秒
// 取值非法或非正数时回落默认值
func readTimeout(raw string) time.Duration {

	const defaultMS = 30000
	ms := defaultMS
	if v := driver.ParseProperties(raw)["readtimeout"]; v != "" {
		if parsed, err := strconv.Atoi(strings.TrimSpace(v)); err == nil && parsed > 0 {
			ms = parsed
		}
	}
	return time.Duration(ms) * time.Millisecond
}

// Acquire 取一个连接, Redis 客户端自带连接池, 这里直接复用
func (p *pool) Acquire(ctx context.Context) (driver.Conn, error) {

	return &conn{client: p.client}, nil
}

// Close 关闭连接池
func (p *pool) Close() {

	_ = p.client.Close()
}

// conn 占用中的连接
type conn struct {
	client *goredis.Client
}

// Ping 连通性检查
func (c *conn) Ping(ctx context.Context) error {

	return c.client.Ping(ctx).Err()
}

// Close 归还连接
func (c *conn) Close() {

}

// blockedCommands 只拦会让连接进入订阅态、接管连接、改掉连接状态或依赖同一条连接的协议级命令
// 不做业务语义判断, 业务上能执行什么由服务端 ACL 决定
// 事务命令必须落在同一条连接上, 而本服务每条命令独立取连接, 放行只会得到随机行为
var blockedCommands = map[string]string{
	"SUBSCRIBE":  "订阅会独占连接, 本服务不提供长连接订阅",
	"PSUBSCRIBE": "订阅会独占连接, 本服务不提供长连接订阅",
	"MONITOR":    "MONITOR 会独占连接, 本服务不提供实时监控流",
	"SYNC":       "主从同步命令由运维使用, 不通过本服务执行",
	"PSYNC":      "主从同步命令由运维使用, 不通过本服务执行",
	"MULTI":      "事务命令需要同一条连接, 本服务每条命令独立取连接, 请改用 Lua 脚本",
	"EXEC":       "事务命令需要同一条连接, 本服务每条命令独立取连接, 请改用 Lua 脚本",
	"DISCARD":    "事务命令需要同一条连接, 本服务每条命令独立取连接, 请改用 Lua 脚本",
	"WATCH":      "事务命令需要同一条连接, 本服务每条命令独立取连接, 请改用 Lua 脚本",
	"UNWATCH":    "事务命令需要同一条连接, 本服务每条命令独立取连接, 请改用 Lua 脚本",
	"SELECT":     "会改掉本条连接的逻辑库, 其它请求复用这条连接会读写错库, 换库请改数据源配置",
	"HELLO":      "会切换连接的协议版本, 驱动的回复解析会跟着错乱",
}

// blockedClientSubcommands CLIENT 命令里会改连接状态或波及其它连接的子命令
// CLIENT LIST、CLIENT INFO、CLIENT ID 这类只读诊断照旧放行
var blockedClientSubcommands = map[string]string{
	"REPLY":    "REPLY OFF 或 SKIP 会让连接不再回包, 后续请求只能挂到读取超时",
	"KILL":     "KILL 会断开连接, 可能把自己这条一起杀掉",
	"PAUSE":    "PAUSE 会暂停全部客户端的读写, 影响整个实例",
	"TRACKING": "TRACKING 会开启客户端缓存并推送失效消息, 打乱当前连接的回复流",
}

// Execute 按行解析命令并逐条执行
func (c *conn) Execute(ctx context.Context, sqlText string, opts driver.ExecOptions) (*driver.Outcome, error) {

	cmds, err := parseCommands(sqlText)
	if err != nil {
		return nil, err
	}
	outcome := &driver.Outcome{
		Rows:       make([]model.OrderedRow, 0, 8),
		Statements: make([]driver.StatementOutcome, 0, len(cmds)),
	}
	for i, args := range cmds {
		stmtCtx := ctx
		var cancel context.CancelFunc
		if opts.QueryTimeout > 0 {
			stmtCtx, cancel = context.WithTimeout(ctx, opts.QueryTimeout)
		}
		stmtOutcome := &driver.StatementOutcome{Index: i + 1, Rows: make([]model.OrderedRow, 0, 4)}
		execErr := c.execCommand(stmtCtx, args, opts, outcome, stmtOutcome)
		if cancel != nil {
			cancel()
		}
		if execErr != nil {
			return nil, driver.NewStmtError(i+1, len(cmds), strings.Join(args, " "), execErr, describeError(execErr))
		}
		// Redis 无受影响行数概念, 用成功执行的命令条数代替, 让调用方对批量有个数
		stmtOutcome.Affected = 1
		outcome.Affected++
		outcome.Statements = append(outcome.Statements, *stmtOutcome)
	}
	return outcome, nil
}

// execCommand 执行单条命令并把回复转成行
func (c *conn) execCommand(ctx context.Context, args []string, opts driver.ExecOptions, outcome *driver.Outcome, stmtOutcome *driver.StatementOutcome) error {

	name := strings.ToUpper(args[0])
	if advice, blocked := blockedCommands[name]; blocked {
		return fmt.Errorf("命令 %s 不可执行 - %s", name, advice)
	}
	if name == "CLIENT" && len(args) > 1 {
		sub := strings.ToUpper(args[1])
		if advice, blocked := blockedClientSubcommands[sub]; blocked {
			return fmt.Errorf("命令 CLIENT %s 不可执行 - %s", sub, advice)
		}
	}
	callArgs := make([]any, 0, len(args))
	for _, arg := range args {
		callArgs = append(callArgs, arg)
	}
	reply, err := c.client.Do(ctx, callArgs...).Result()
	if err != nil {
		// Redis 的 nil 回复是 redis.Nil, 按空值处理不视为错误
		if errors.Is(err, goredis.Nil) {
			return appendRow(outcome, stmtOutcome, []string{"result"}, []any{nil}, opts)
		}
		return err
	}
	return appendReply(reply, opts, outcome, stmtOutcome)
}

// appendReply 把回复值转成行
// 数组按元素逐行展开, 标量单行, 映射转成普通 map 后由归一化层输出 JSON 文本
func appendReply(reply any, opts driver.ExecOptions, outcome *driver.Outcome, stmtOutcome *driver.StatementOutcome) error {

	switch x := reply.(type) {
	case []any:
		if len(x) == 0 {
			return nil
		}
		for _, item := range x {
			if err := appendRow(outcome, stmtOutcome, []string{"result"}, []any{plainValue(item)}, opts); err != nil {
				return err
			}
		}
		return nil
	case map[any]any:
		converted := make(map[string]any, len(x))
		for key, value := range x {
			converted[fmt.Sprintf("%v", key)] = plainValue(value)
		}
		return appendRow(outcome, stmtOutcome, []string{"result"}, []any{converted}, opts)
	case map[string]any:
		converted := make(map[string]any, len(x))
		for key, value := range x {
			converted[key] = plainValue(value)
		}
		return appendRow(outcome, stmtOutcome, []string{"result"}, []any{converted}, opts)
	default:
		return appendRow(outcome, stmtOutcome, []string{"result"}, []any{plainValue(reply)}, opts)
	}
}

// appendRow 追加一行, 受 max_rows 限制
func appendRow(outcome *driver.Outcome, stmtOutcome *driver.StatementOutcome, columns []string, values []any, opts driver.ExecOptions) error {

	if opts.MaxRows > 0 && len(outcome.Rows) >= opts.MaxRows {
		outcome.Truncated = true
		stmtOutcome.Truncated = true
		return nil
	}
	row := driver.BuildRow(columns, values)
	outcome.Rows = append(outcome.Rows, row)
	stmtOutcome.Rows = append(stmtOutcome.Rows, row)
	return nil
}

// plainValue 把回复里的元素转成可序列化的普通值, 嵌套结构交由归一化层输出 JSON
func plainValue(v any) any {

	switch x := v.(type) {
	case nil, bool, int64, float64, time.Time:
		return x
	case string:
		// 二进制值会以非法 UTF-8 的字符串回来, 转成字节让归一化层走 Base64
		if !utf8.ValidString(x) {
			return []byte(x)
		}
		return x
	case int:
		return int64(x)
	case []any:
		out := make([]any, 0, len(x))
		for _, item := range x {
			out = append(out, plainValue(item))
		}
		return out
	case map[any]any:
		out := make(map[string]any, len(x))
		for key, value := range x {
			out[fmt.Sprintf("%v", key)] = plainValue(value)
		}
		return out
	case map[string]any:
		out := make(map[string]any, len(x))
		for key, value := range x {
			out[key] = plainValue(value)
		}
		return out
	default:
		return fmt.Sprintf("%v", x)
	}
}

// parseCommands 按行切命令, 空行与井号开头的注释行跳过
func parseCommands(sqlText string) ([][]string, error) {

	cmds := make([][]string, 0, 4)
	for lineNo, rawLine := range strings.Split(sqlText, "\n") {
		line := strings.TrimSpace(strings.TrimSuffix(rawLine, "\r"))
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		args, err := splitArgs(line)
		if err != nil {
			return nil, fmt.Errorf("第 %d 行命令解析失败 - %v", lineNo+1, err)
		}
		if len(args) == 0 {
			continue
		}
		cmds = append(cmds, args)
	}
	if len(cmds) == 0 {
		return nil, fmt.Errorf("命令不能为空, Redis 每行写一条命令, 参数空格分隔")
	}
	return cmds, nil
}

// splitArgs 按空格切参数, 支持双引号与单引号包裹含空格的参数
// 双引号内识别 \n \r \t \" \\ \xHH 转义, 单引号内只识别 \' 与 \\, 与 redis-cli 的口径一致
func splitArgs(line string) ([]string, error) {

	args := make([]string, 0, 4)
	var current strings.Builder
	inQuote := rune(0)
	hasToken := false
	runes := []rune(line)

	for i := 0; i < len(runes); i++ {
		ch := runes[i]
		switch {
		case inQuote == 0 && (ch == ' ' || ch == '\t'):
			if hasToken {
				args = append(args, current.String())
				current.Reset()
				hasToken = false
			}
		case inQuote == 0 && (ch == '"' || ch == '\''):
			inQuote = ch
			hasToken = true
		case inQuote != 0 && ch == inQuote:
			inQuote = 0
		case ch == '\\' && i+1 < len(runes):
			next := runes[i+1]
			if inQuote == '\'' {
				// 单引号内只有 \' 与 \\ 是转义, 其余原样保留
				if next == '\'' || next == '\\' {
					current.WriteRune(next)
					i++
					continue
				}
				current.WriteRune(ch)
				continue
			}
			switch next {
			case 'n':
				current.WriteRune('\n')
			case 'r':
				current.WriteRune('\r')
			case 't':
				current.WriteRune('\t')
			case '"', '\'', '\\':
				current.WriteRune(next)
			case 'x':
				if i+3 < len(runes) {
					if code, err := strconv.ParseUint(string(runes[i+2:i+4]), 16, 8); err == nil {
						current.WriteByte(byte(code))
						// 这里提前 continue, 必须自己把参数标记打上, 否则该参数会被整体丢掉
						hasToken = true
						i += 3
						continue
					}
				}
				// 转义写法不合法时原样保留两个字符, 不能悄悄吞掉
				current.WriteRune(ch)
				current.WriteRune(next)
			default:
				current.WriteRune(ch)
				current.WriteRune(next)
			}
			i++
			hasToken = true
		default:
			current.WriteRune(ch)
			hasToken = true
		}
	}
	if inQuote != 0 {
		return nil, fmt.Errorf("引号未闭合")
	}
	if hasToken {
		args = append(args, current.String())
	}
	return args, nil
}

// describeError 把 Redis 错误说明转成中文类别与修正建议
// go-redis 的错误是字符串, 按前缀关键字归类
func describeError(err error) *driver.ErrorInfo {

	if err == nil {
		return nil
	}
	message := strings.TrimSpace(err.Error())
	upper := strings.ToUpper(message)
	lower := strings.ToLower(message)
	info := &driver.ErrorInfo{Message: message}
	switch {
	case strings.HasPrefix(upper, "WRONGPASS"):
		info.Category = "账号或密码错误"
		info.Advice = "核对实例用户名与密码, Redis 6 以上才支持用户名"
	case strings.HasPrefix(upper, "NOAUTH"):
		info.Category = "未认证"
		info.Advice = "该实例要求密码, 请在数据源配置里填密码"
	case strings.HasPrefix(upper, "NOPERM"):
		info.Category = "权限不足"
		info.Advice = "核对实例账号的 ACL 权限, 该命令可能未对该账号开放"
	case strings.HasPrefix(upper, "WRONGTYPE"):
		info.Category = "类型不匹配"
		info.Advice = "该键存的类型与命令不符, 可先用 TYPE 命令查看键类型"
	case strings.HasPrefix(upper, "NOGROUP"):
		info.Category = "消费者组不存在"
		info.Advice = "先用 XGROUP CREATE 创建消费者组"
	case strings.HasPrefix(upper, "BUSYGROUP"):
		info.Category = "消费者组已存在"
		info.Advice = "换一个组名或直接使用已有组"
	case strings.HasPrefix(upper, "OOM"):
		info.Category = "内存不足"
		info.Advice = "实例达到 maxmemory 上限, 清理键或调整内存策略"
	case strings.HasPrefix(upper, "READONLY"):
		info.Category = "只读实例"
		info.Advice = "当前连的是只读副本, 写操作请连主节点"
	case strings.HasPrefix(upper, "MOVED"), strings.HasPrefix(upper, "ASK"):
		info.Category = "集群重定向"
		info.Advice = "当前实例是集群模式, 本服务按单机接入, 请改连集群入口或单机实例"
	case strings.HasPrefix(upper, "ERR UNKNOWN COMMAND"):
		info.Category = "命令不存在"
		info.Advice = "核对命令名拼写, 可用 COMMAND COUNT 查看服务端支持的命令数量"
	case strings.HasPrefix(upper, "ERR WRONG NUMBER OF ARGUMENTS"):
		info.Category = "参数个数不对"
		info.Advice = "核对命令的参数个数, 含空格的参数要用引号包裹"
	case strings.Contains(upper, "DB INDEX IS OUT OF RANGE"):
		info.Category = "逻辑库编号超出范围"
		info.Advice = "服务端 databases 配置的库数量有限, 换一个更小的库号"
	case strings.Contains(lower, "read tcp") && strings.Contains(lower, "i/o timeout"):
		info.Category = "读取超时"
		info.Advice = "命令在读取超时时间内未返回; 长阻塞命令请调大连接属性 readtimeout, 单位毫秒"
	case strings.HasPrefix(upper, "ERR SYNTAX ERROR"):
		info.Category = "语法错误"
		info.Advice = "核对命令参数顺序, 可参考 COMMAND DOCS 命令名 查看用法"
	default:
		if isConnectError(message) {
			info.Category = "连接失败"
			info.Advice = "核对实例地址、端口与服务端是否在运行"
		}
	}
	return info
}

// isConnectError 识别网络层错误, 这些错误本身不带 Redis 错误前缀
func isConnectError(message string) bool {

	lower := strings.ToLower(message)
	return strings.Contains(lower, "connection refused") ||
		strings.Contains(lower, "i/o timeout") ||
		strings.Contains(lower, "no such host") ||
		strings.Contains(lower, "connection reset")
}
