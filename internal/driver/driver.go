// 驱动层契约:实例类型到连接池的注册表,以及执行结果的统一形状
package driver

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/fengfffffff/database-mcp-go/internal/model"
)

// ExecOptions 单次执行的可选参数
type ExecOptions struct {
	// QueryTimeout 单条语句超时时间
	QueryTimeout time.Duration
	// MaxRows 累计返回的最大行数,超出截断
	MaxRows int
}

// StatementOutcome 单条语句的执行结果
type StatementOutcome struct {
	// Index 语句序号,从 1 开始
	Index int
	// Rows 该语句返回的行数据
	Rows []model.OrderedRow
	// Affected 该语句写操作的受影响行数
	Affected int64
	// Truncated 该语句的结果是否因超出 MaxRows 被截断
	Truncated bool
}

// Outcome 执行结果,批次内汇总与逐条明细都保留
type Outcome struct {
	// Rows 合并后的行数据,保持每次查询的列顺序
	Rows []model.OrderedRow
	// Affected 写操作受影响行数累计
	Affected int64
	// Truncated 是否因超出 MaxRows 被截断
	Truncated bool
	// Statements 按语句顺序记录每条语句的结果
	Statements []StatementOutcome
}

// ErrorInfo 数据库错误的结构化描述,由各驱动按自身错误类型填充
type ErrorInfo struct {
	// Category 中文错误类别,如 表不存在
	Category string
	// Code 数据库错误码, MySQL 与 SQL Server 为数字码, 金仓与 PostgreSQL 走 SQLSTATE
	Code string
	// SQLState SQL 标准状态码
	SQLState string
	// Message 数据库返回的原始信息
	Message string
	// Detail 服务端补充的详情
	Detail string
	// Hint 服务端给出的提示
	Hint string
	// Location 错误在语句中的位置,形如 第 1 行第 15 列
	Location string
	// Advice 建议的修正动作
	Advice string
}

// StmtError 单条语句执行失败,带语句序号与原文,由上层拼装中文提示
type StmtError struct {
	// Index 失败语句序号,从 1 开始
	Index int
	// Total 本次执行的语句总数
	Total int
	// Statement 失败语句原文
	Statement string
	// Timeout 是否因超过单条语句超时时间被中止
	Timeout bool
	// Info 数据库错误的分类信息,非数据库返回的错误为 nil
	Info *ErrorInfo
	// Cause 驱动返回的原始错误
	Cause error
}

// Error 返回原始错误文本
func (e *StmtError) Error() string {

	return e.Cause.Error()
}

// Unwrap 支持 errors.Is 与 errors.As 继续向底层错误判断
func (e *StmtError) Unwrap() error {

	return e.Cause
}

// NewStmtError 包装单条语句错误,自动识别超时
func NewStmtError(index, total int, statement string, cause error, info *ErrorInfo) *StmtError {

	return &StmtError{
		Index:     index,
		Total:     total,
		Statement: statement,
		Timeout:   errors.Is(cause, context.DeadlineExceeded),
		Info:      info,
		Cause:     cause,
	}
}

// Conn 占用中的连接,一次 Execute 对应一个完整事务
type Conn interface {
	// Execute 在一个事务内执行 SQL,全部成功才提交,任一失败整体回滚
	Execute(ctx context.Context, sql string, opts ExecOptions) (*Outcome, error)
	// Ping 连通性检查
	Ping(ctx context.Context) error
	// Close 归还连接到池
	Close()
}

// Pool 按实例别名与数据库维度建立的连接池
type Pool interface {
	// Acquire 取一个连接
	Acquire(ctx context.Context) (Conn, error)
	// Close 关闭整个池
	Close()
}

// Factory 由实例连接配置构造连接池
type Factory func(cfg *model.DataSourceConfig) (Pool, error)

// Registry 实例类型到驱动工厂的注册表
type Registry struct {
	factories map[int]Factory
}

// NewRegistry 创建注册表
func NewRegistry() *Registry {

	return &Registry{factories: make(map[int]Factory)}
}

// Register 注册驱动工厂
func (r *Registry) Register(databaseType int, f Factory) {

	r.factories[databaseType] = f
}

// NewPool 按配置构造连接池,未注册的类型直接报错
func (r *Registry) NewPool(cfg *model.DataSourceConfig) (Pool, error) {

	f, ok := r.factories[cfg.DatabaseType]
	if !ok {
		return nil, fmt.Errorf("未注册的实例类型 - 实例类型: %d, 已注册类型: %v", cfg.DatabaseType, r.Supported())
	}
	return f(cfg)
}

// defaultRegistry 全局注册表,各驱动包在 init 中自行注册
var defaultRegistry = NewRegistry()

// Register 向全局注册表注册驱动工厂
func Register(databaseType int, f Factory) {

	defaultRegistry.Register(databaseType, f)
}

// Default 返回全局注册表
func Default() *Registry {

	return defaultRegistry
}

// Supported 已注册的实例类型列表
func (r *Registry) Supported() []int {

	types := make([]int, 0, len(r.factories))
	for t := range r.factories {
		types = append(types, t)
	}
	sort.Ints(types)
	return types
}

// Supports 判断实例类型是否已注册驱动
func (r *Registry) Supports(databaseType int) bool {

	_, ok := r.factories[databaseType]
	return ok
}

// ProbeTimeout 返回建池探活用的超时
// 连接超时配成 0 表示不设上限, 但建池阶段的探活不能无限等, 统一给一个兜底值
func ProbeTimeout(connTimeoutMS int64) time.Duration {

	if connTimeoutMS <= 0 {
		return 10 * time.Second
	}
	return time.Duration(connTimeoutMS) * time.Millisecond
}
