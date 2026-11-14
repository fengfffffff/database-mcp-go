// 领域模型:数据源配置、执行结果与统一响应体
package model

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

// 实例类型取值,对应配置表 sys_mcp_database_config.database_type
// 3 号位保留不用, 其余取值与驱动注册表一一对应
const (
	DatabaseTypeMySQL      = 0
	DatabaseTypeKingbase8  = 1
	DatabaseTypePostgreSQL = 2
	DatabaseTypeSQLServer  = 4
	DatabaseTypeMariaDB    = 5
	DatabaseTypeTiDB       = 6
	DatabaseTypeOceanBase  = 7
	DatabaseTypeSQLite     = 8
	DatabaseTypeMongoDB    = 9
	DatabaseTypeRedis      = 10
)

// DatabaseTypeDesc 返回实例类型的展示名
func DatabaseTypeDesc(t int) string {

	switch t {
	case DatabaseTypeMySQL:
		return "MySQL"
	case DatabaseTypeKingbase8:
		return "Kingbase8"
	case DatabaseTypePostgreSQL:
		return "PostgreSQL"
	case DatabaseTypeSQLServer:
		return "SQLServer"
	case DatabaseTypeMariaDB:
		return "MariaDB"
	case DatabaseTypeTiDB:
		return "TiDB"
	case DatabaseTypeOceanBase:
		return "OceanBase"
	case DatabaseTypeSQLite:
		return "SQLite"
	case DatabaseTypeMongoDB:
		return "MongoDB"
	case DatabaseTypeRedis:
		return "Redis"
	default:
		return ""
	}
}

// 审计身份取值,写入配置表的创建人与修改人列
const (
	OperatorTypeAdmin  = "后台用户"
	OperatorTypeSystem = "系统用户"
	OperatorNameMCP    = "MCP服务"
	OperatorNameAdmin  = "管理员"
)

// Operator 操作者身份, 随新增与修改请求传入, 用于写入审计列
// 服务没有用户体系, ID 统一为 0, 只靠名称与类型区分入口
type Operator struct {
	ID   int64
	Name string
	Type string
}

// DataSourceConfig 实例连接配置,对应 sys_mcp_database_config 的业务列与连接池列
type DataSourceConfig struct {
	ID                   int64  `json:"id"`
	Alias                string `json:"alias"`
	DatabaseName         string `json:"databaseName"`
	DatabaseType         int    `json:"databaseType"`
	DatabaseVersion      string `json:"databaseVersion"`
	Host                 string `json:"host"`
	Port                 int    `json:"port"`
	Username             string `json:"username"`
	Password             string `json:"password"`
	DriverClass          string `json:"driverClass"`
	ConnectionProperties string `json:"connectionProperties"`
	PoolMinimumIdle      int    `json:"poolMinimumIdle"`
	PoolMaximumPoolSize  int    `json:"poolMaximumPoolSize"`
	PoolConnectionTimeMS int64  `json:"poolConnectionTimeout"`
	PoolIdleTimeoutMS    int64  `json:"poolIdleTimeout"`
	PoolMaxLifetimeMS    int64  `json:"poolMaxLifetime"`
	PoolValidationTimeMS int64  `json:"poolValidationTimeout"`
	Status               int    `json:"status"`
	Remark               string `json:"remark"`
	// 审计列,只读:查询时回填,仅管理页详情返回,列表视图不对外
	// 写库不经过这些字段,新增与修改由 Operator 传入的身份落库
	CreateByName string    `json:"-"`
	CreateByType string    `json:"-"`
	CreateTime   time.Time `json:"-"`
	UpdateByName string    `json:"-"`
	UpdateByType string    `json:"-"`
	UpdateTime   time.Time `json:"-"`
	Weight       float64   `json:"-"`
	// PasswordSet 实例密码是否已设置,列表与详情只关心有无密码,不便于为此解密
	PasswordSet bool `json:"-"`
}

// Databases 拆分所辖数据库名列表
func (c *DataSourceConfig) Databases() []string {

	return SplitDatabaseNames(c.DatabaseName)
}

// UnmarshalJSON 解析配置里的数据源时补齐未填写的默认值
// 手写配置通常只写连接信息, 不补 status 会把配置误判成停用, 不补连接池参数会退化成 0
// 先置默认值再解析, 显式写了 0 的项会被原值覆盖, 所以 0 仍然算合法值
func (c *DataSourceConfig) UnmarshalJSON(data []byte) error {

	type rawConfig DataSourceConfig
	tmp := rawConfig{
		Status:               1,
		PoolMinimumIdle:      2,
		PoolMaximumPoolSize:  10,
		PoolConnectionTimeMS: 5000,
		PoolIdleTimeoutMS:    600000,
		PoolMaxLifetimeMS:    1800000,
		PoolValidationTimeMS: 3000,
	}
	if err := json.Unmarshal(data, &tmp); err != nil {
		return err
	}
	*c = DataSourceConfig(tmp)
	return nil
}

// SplitDatabaseNames 按英文逗号拆分数据库名,去空白与重复,保持顺序
func SplitDatabaseNames(raw string) []string {

	seen := make(map[string]struct{})
	out := make([]string, 0, 4)
	for _, part := range strings.Split(raw, ",") {
		name := strings.TrimSpace(part)
		if name == "" {
			continue
		}
		if _, ok := seen[name]; ok {
			continue
		}
		seen[name] = struct{}{}
		out = append(out, name)
	}
	return out
}

// PoolSignature 返回影响连接本身的配置指纹, 指纹变了连接池就得重建
// 备注、状态、版本号与审计列不参与, 只改这些不会让连接池白重建
// 字符串用 %q 引起来, 免得取值里带分隔符时两组不同配置拼出同一个指纹
func (c *DataSourceConfig) PoolSignature() string {

	return fmt.Sprintf("%d|%q|%d|%q|%q|%q|%q|%d|%d|%d|%d|%d",
		c.DatabaseType, c.Host, c.Port, c.Username, c.Password, c.DriverClass,
		c.ConnectionProperties, c.PoolMinimumIdle, c.PoolMaximumPoolSize,
		c.PoolConnectionTimeMS, c.PoolIdleTimeoutMS, c.PoolMaxLifetimeMS)
}

// EffectivePool 返回连接池参数, 缺省值由新增入口与文件解析补齐, 这里只把负数的脏值兜回合法区间
// 0 是合法值: 最小空闲 0 表示不保留空闲连接, 超时 0 表示不设上限, 不能当成未配置
func (c *DataSourceConfig) EffectivePool() (minIdle, maxOpen int, connTimeoutMS, idleTimeoutMS, maxLifetimeMS int64) {

	minIdle, maxOpen = c.PoolMinimumIdle, c.PoolMaximumPoolSize
	connTimeoutMS, idleTimeoutMS, maxLifetimeMS = c.PoolConnectionTimeMS, c.PoolIdleTimeoutMS, c.PoolMaxLifetimeMS
	if maxOpen <= 0 {
		maxOpen = 10
	}
	if minIdle < 0 {
		minIdle = 0
	}
	if minIdle > maxOpen {
		minIdle = maxOpen
	}
	if connTimeoutMS < 0 {
		connTimeoutMS = 5000
	}
	if idleTimeoutMS < 0 {
		idleTimeoutMS = 600000
	}
	if maxLifetimeMS < 0 {
		maxLifetimeMS = 1800000
	}
	return
}

// DatasourceView 对外展示的数据源视图,不含实例密码
type DatasourceView struct {
	ConfigID             int64    `json:"configId"`
	Alias                string   `json:"alias"`
	Databases            []string `json:"databases"`
	DatabaseType         int      `json:"databaseType"`
	DatabaseTypeDesc     string   `json:"databaseTypeDesc"`
	DatabaseVersion      string   `json:"databaseVersion"`
	Host                 string   `json:"host"`
	Port                 int      `json:"port"`
	Username             string   `json:"username"`
	DriverClass          string   `json:"driverClass"`
	ConnectionProperties string   `json:"connectionProperties"`
	PoolMinimumIdle      int      `json:"poolMinimumIdle"`
	PoolMaximumPoolSize  int      `json:"poolMaximumPoolSize"`
	Status               int      `json:"status"`
	Remark               string   `json:"remark"`
}

// View 由配置生成对外视图
func View(c *DataSourceConfig) DatasourceView {

	return DatasourceView{
		ConfigID:             c.ID,
		Alias:                c.Alias,
		Databases:            c.Databases(),
		DatabaseType:         c.DatabaseType,
		DatabaseTypeDesc:     DatabaseTypeDesc(c.DatabaseType),
		DatabaseVersion:      c.DatabaseVersion,
		Host:                 c.Host,
		Port:                 c.Port,
		Username:             c.Username,
		DriverClass:          c.DriverClass,
		ConnectionProperties: c.ConnectionProperties,
		PoolMinimumIdle:      c.PoolMinimumIdle,
		PoolMaximumPoolSize:  c.PoolMaximumPoolSize,
		Status:               c.Status,
		Remark:               c.Remark,
	}
}

// DetailView 详情视图,含连接池全部参数与密码是否已设置标记,密码本身不返回
type DetailView struct {
	DatasourceView
	PasswordSet          bool   `json:"passwordSet"`
	PoolConnectionTimeMS int64  `json:"poolConnectionTimeout"`
	PoolIdleTimeoutMS    int64  `json:"poolIdleTimeout"`
	PoolMaxLifetimeMS    int64  `json:"poolMaxLifetime"`
	PoolValidationTimeMS int64  `json:"poolValidationTimeout"`
	CreateByName         string `json:"createByName"`
	CreateByType         string `json:"createByType"`
	CreateTime           string `json:"createTime"`
	UpdateByName         string `json:"updateByName"`
	UpdateByType         string `json:"updateByType"`
	UpdateTime           string `json:"updateTime"`
}

// timeLayout 审计信息展示用的时间格式, 统一到年月日时分秒
const timeLayout = "2006-01-02 15:04:05"

// Detail 由配置生成详情视图
func Detail(c *DataSourceConfig) DetailView {

	return DetailView{
		DatasourceView:       View(c),
		PasswordSet:          c.PasswordSet,
		PoolConnectionTimeMS: c.PoolConnectionTimeMS,
		PoolIdleTimeoutMS:    c.PoolIdleTimeoutMS,
		PoolMaxLifetimeMS:    c.PoolMaxLifetimeMS,
		PoolValidationTimeMS: c.PoolValidationTimeMS,
		CreateByName:         c.CreateByName,
		CreateByType:         c.CreateByType,
		CreateTime:           formatTime(c.CreateTime),
		UpdateByName:         c.UpdateByName,
		UpdateByType:         c.UpdateByType,
		UpdateTime:           formatTime(c.UpdateTime),
	}
}

// formatTime 把审计时间格式化成接口展示用字符串,零值返回空串
func formatTime(t time.Time) string {

	if t.IsZero() {
		return ""
	}
	return t.Format(timeLayout)
}

// ListQuery 列表查询条件, 别名与库名走模糊匹配, 类型与状态走精确匹配
type ListQuery struct {
	Alias        string `json:"alias"`
	DatabaseName string `json:"databaseName"`
	DatabaseType *int   `json:"databaseType"`
	Status       *int   `json:"status"`
	PageNum      int    `json:"pageNum"`
	PageSize     int    `json:"pageSize"`
}

// Normalize 补齐分页默认值并限制上限
func (q *ListQuery) Normalize() {

	if q.PageNum <= 0 {
		q.PageNum = 1
	}
	if q.PageSize <= 0 {
		q.PageSize = 10
	}
	if q.PageSize > 200 {
		q.PageSize = 200
	}
}

// Offset 计算偏移量
func (q *ListQuery) Offset() int {

	return (q.PageNum - 1) * q.PageSize
}

// PageResult 分页结果
type PageResult struct {
	List     []DatasourceView `json:"list"`
	Total    int64            `json:"total"`
	PageNum  int              `json:"pageNum"`
	PageSize int              `json:"pageSize"`
}

// TypeCountItem 单个实例类型的配置数量
type TypeCountItem struct {
	DatabaseType int   `json:"databaseType"`
	ConfigCount  int64 `json:"configCount"`
}

// TypeCount 实例类型数量统计,口径为全部未删除配置
type TypeCount struct {
	TotalCount int64           `json:"totalCount"`
	Items      []TypeCountItem `json:"items"`
}

// OrderedRow 一行数据,保留列顺序
// 用切片而非 map,避免 encoding/json 对 map 按键排序丢失列顺序
type OrderedRow struct {
	Columns []string
	Values  []any
}

// MarshalJSON 按列顺序输出对象
func (r OrderedRow) MarshalJSON() ([]byte, error) {

	var buf bytes.Buffer
	buf.WriteByte('{')
	for i, col := range r.Columns {
		if i > 0 {
			buf.WriteByte(',')
		}
		key, err := json.Marshal(col)
		if err != nil {
			return nil, err
		}
		buf.Write(key)
		buf.WriteByte(':')
		var val []byte
		if i < len(r.Values) {
			val, err = json.Marshal(r.Values[i])
			if err != nil {
				return nil, err
			}
		} else {
			val = []byte("null")
		}
		buf.Write(val)
	}
	buf.WriteByte('}')
	return buf.Bytes(), nil
}

// StatementResult 单条语句的执行结果,多条语句时按条返回
type StatementResult struct {
	// StatementIndex 语句序号,从 1 开始,与 SQL 里的分号顺序一致
	StatementIndex int          `json:"statementIndex"`
	Rows           []OrderedRow `json:"rows"`
	RowCount       int          `json:"rowCount"`
	AffectedRows   int64        `json:"affectedRows"`
	Truncated      bool         `json:"truncated,omitempty"`
}

// QueryResult 执行结果
// 单条语句时行数据平铺在 rows; 多条语句时 rows 为空, 按语句放在 results 里, 避免各行混在一起分不清来源
type QueryResult struct {
	Rows           []OrderedRow      `json:"rows"`
	RowCount       int               `json:"rowCount"`
	AffectedRows   int64             `json:"affectedRows"`
	ExecutionTime  int64             `json:"executionTime"`
	Truncated      bool              `json:"truncated,omitempty"`
	StatementCount int               `json:"statementCount,omitempty"`
	Results        []StatementResult `json:"results,omitempty"`
}

// R 统一响应体,含状态码、提示信息与响应数据
type R struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Data    any    `json:"data"`
}

// OK 成功并携带数据
func OK(data any) R {

	return R{Code: 200, Message: "操作成功", Data: data}
}

// OKMsg 成功并自定义提示
func OKMsg(message string) R {

	return R{Code: 200, Message: message, Data: nil}
}

// Error 失败响应
func Error(code int, message string) R {

	return R{Code: code, Message: message, Data: nil}
}
