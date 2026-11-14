// MCP 工具层:暴露 6 个工具, 工具名与参数名固定, 客户端提示词按这套契约编写
// 参数解析与类型校验在本层完成, 失败提示统一为中文, 不把 SDK 的英文校验原文抛给模型
package tool

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"

	"github.com/fengfffffff/database-mcp-go/internal/model"
	"github.com/fengfffffff/database-mcp-go/internal/service"
	"github.com/google/jsonschema-go/jsonschema"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// Register 注册全部 MCP 工具
func Register(server *mcp.Server, proxy *service.Proxy) {

	registerExecuteSQL(server, proxy)
	registerAddConfig(server, proxy)
	registerUpdateConfig(server, proxy)
	registerDeleteConfig(server, proxy)
	registerGetDatasources(server, proxy)
	registerRefreshConfig(server, proxy)
}

// toolFunc 工具处理函数的统一形状, 错误由本层包成工具错误结果
type toolFunc[In any] func(ctx context.Context, req *mcp.CallToolRequest, in In) (*mcp.CallToolResult, error)

// mcpOperator MCP 工具写库时使用的固定审计身份, 与平台上原有系统写入口径一致
func mcpOperator() model.Operator {

	return model.Operator{Name: model.OperatorNameMCP, Type: model.OperatorTypeSystem}
}

// addTool 注册工具:入参结构由泛型推导, 参数解析与校验走本层, 校验失败返回中文提示
func addTool[In any](server *mcp.Server, t *mcp.Tool, handler toolFunc[In]) {

	if t.InputSchema == nil {
		schema, err := jsonschema.For[In](nil)
		if err != nil {
			panic(fmt.Sprintf("工具 %s 生成入参结构失败: %v", t.Name, err))
		}
		t.InputSchema = schema
	}
	server.AddTool(t, func(ctx context.Context, req *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		var args json.RawMessage
		if req.Params != nil {
			args = req.Params.Arguments
		}
		in, err := bindArguments[In](args)
		if err != nil {
			return errorResult(err), nil
		}
		res, err := handler(ctx, req, in)
		if err != nil {
			return errorResult(err), nil
		}
		return res, nil
	})
}

// bindArguments 解析工具入参,缺少必填、类型不符或传了未知参数时给出中文提示
func bindArguments[In any](args json.RawMessage) (In, error) {

	var in In
	trimmed := bytes.TrimSpace(args)
	if len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null")) {
		trimmed = []byte("{}")
	}
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(trimmed, &raw); err != nil {
		return in, fmt.Errorf("参数校验失败 - 参数须为 JSON 对象")
	}
	decoder := json.NewDecoder(bytes.NewReader(trimmed))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&in); err != nil {
		return in, describeArgumentError[In](err, raw)
	}
	for _, name := range requiredArguments[In]() {
		value, ok := rawArgument(raw, name)
		if !ok || bytes.Equal(bytes.TrimSpace(value), []byte("null")) {
			return in, fmt.Errorf("参数校验失败 - 缺少必填参数: %s", name)
		}
	}
	return in, nil
}

// describeArgumentError 把 encoding/json 的英文错误翻成中文
func describeArgumentError[In any](err error, raw map[string]json.RawMessage) error {

	var typeErr *json.UnmarshalTypeError
	if errors.As(err, &typeErr) {
		field := typeErr.Field
		if field == "" {
			return fmt.Errorf("参数校验失败 - 参数类型不正确")
		}
		want := describeGoType(typeErr.Type)
		value, _ := rawArgument(raw, field)
		got := describeJSONValue(value)
		if want == "整数" && strings.HasPrefix(got, "整数 ") {
			return fmt.Errorf("参数校验失败 - %s 超出整数取值范围, 收到%s", field, got)
		}
		return fmt.Errorf("参数校验失败 - %s 须为%s, 收到%s", field, want, got)
	}
	const unknownPrefix = "json: unknown field "
	if msg := err.Error(); strings.HasPrefix(msg, unknownPrefix) {
		name := strings.Trim(strings.TrimPrefix(msg, unknownPrefix), `"`)
		names := argumentNames[In]()
		if len(names) == 0 {
			return fmt.Errorf("参数校验失败 - 未知参数: %s", name)
		}
		return fmt.Errorf("参数校验失败 - 未知参数: %s, 可用参数: %s", name, strings.Join(names, ", "))
	}
	return fmt.Errorf("参数校验失败 - %s", err.Error())
}

// rawArgument 按 JSON 字段名取原始值, encoding/json 匹配字段不区分大小写, 这里保持一致
func rawArgument(raw map[string]json.RawMessage, field string) (json.RawMessage, bool) {

	if value, ok := raw[field]; ok {
		return value, true
	}
	for name, value := range raw {
		if strings.EqualFold(name, field) {
			return value, true
		}
	}
	return nil, false
}

// describeJSONValue 用中文描述收到的 JSON 值
func describeJSONValue(raw json.RawMessage) string {

	text := strings.TrimSpace(string(raw))
	if text == "" {
		return "空值"
	}
	switch {
	case text == "null":
		return "null"
	case strings.HasPrefix(text, `"`):
		var value string
		if json.Unmarshal(raw, &value) == nil {
			return fmt.Sprintf("字符串 %q", truncateText(value, 80))
		}
		return "字符串"
	case text == "true" || text == "false":
		return "布尔值 " + text
	case strings.HasPrefix(text, "["):
		return "数组"
	case strings.HasPrefix(text, "{"):
		return "对象"
	case strings.ContainsAny(text, ".eE"):
		return "小数 " + truncateText(text, 80)
	default:
		return "整数 " + truncateText(text, 80)
	}
}

// describeGoType 用中文描述入参字段应有的类型
func describeGoType(t reflect.Type) string {

	for t != nil && t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	if t == nil {
		return "正确类型"
	}
	switch t.Kind() {
	case reflect.String:
		return "字符串"
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return "整数"
	case reflect.Bool:
		return "布尔值"
	case reflect.Float32, reflect.Float64:
		return "小数"
	case reflect.Slice, reflect.Array:
		return "数组"
	case reflect.Map, reflect.Struct:
		return "对象"
	default:
		return t.String()
	}
}

// argumentField 入参字段的 JSON 名称与是否必填
type argumentField struct {
	Name     string
	Required bool
}

// argumentFields 反射出入参结构体的字段信息, 必填口径与生成 schema 的 required 一致
func argumentFields[In any]() []argumentField {

	t := reflect.TypeFor[In]()
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	fields := make([]argumentField, 0, t.NumField())
	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		if field.PkgPath != "" {
			continue
		}
		parts := strings.Split(field.Tag.Get("json"), ",")
		name := parts[0]
		if name == "" || name == "-" {
			continue
		}
		required := true
		for _, option := range parts[1:] {
			if option == "omitempty" || option == "omitzero" {
				required = false
				break
			}
		}
		fields = append(fields, argumentField{Name: name, Required: required})
	}
	return fields
}

// argumentNames 工具入参的 JSON 字段名列表,用于未知参数提示
func argumentNames[In any]() []string {

	fields := argumentFields[In]()
	names := make([]string, 0, len(fields))
	for _, field := range fields {
		names = append(names, field.Name)
	}
	return names
}

// requiredArguments 必填参数名列表
func requiredArguments[In any]() []string {

	fields := argumentFields[In]()
	names := make([]string, 0, len(fields))
	for _, field := range fields {
		if field.Required {
			names = append(names, field.Name)
		}
	}
	return names
}

// truncateText 超长取值限长,避免提示被长文本撑爆
func truncateText(text string, max int) string {

	runes := []rune(text)
	if len(runes) <= max {
		return text
	}
	return string(runes[:max]) + "…"
}

// errorResult 把错误包成工具错误结果,文本即给模型看的提示
func errorResult(err error) *mcp.CallToolResult {

	return &mcp.CallToolResult{
		IsError: true,
		Content: []mcp.Content{&mcp.TextContent{Text: err.Error()}},
	}
}

// jsonResult 把统一响应体序列化为文本内容返回
func jsonResult(payload any) (*mcp.CallToolResult, error) {

	raw, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	return &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{Text: string(raw)}}}, nil
}

// ========================== execute-sql ==========================

// executeSQLInput 执行 SQL 入参
type executeSQLInput struct {
	Alias        string `json:"alias" jsonschema:"实例别名, 先用 get-datasources 查询"`
	DatabaseName string `json:"databaseName" jsonschema:"数据库名, 须是该实例所辖库之一, 先用 get-datasources 查询"`
	SQL          string `json:"sql" jsonschema:"要执行的 SQL 或命令; SQL 可多条, 用英文分号分隔; MongoDB 传 runCommand 语法的 JSON 命令文档, 单个对象或对象数组; Redis 每行一条命令, 参数空格分隔, 含空格的参数用双引号包裹"`
}

func registerExecuteSQL(server *mcp.Server, proxy *service.Proxy) {

	addTool(server, &mcp.Tool{
		Name: "execute-sql",
		Description: "执行 SQL 或原生命令, 原样透传数据库执行, 返回结果集或受影响行数; 调用前先用 get-datasources 确认 alias 与 databaseName\n" +
			"SQL 型实例: 可多条, 用英文分号分隔, 同一事务执行, 全部成功才提交, 任一失败整体回滚; 请勿自带 BEGIN/COMMIT; DDL 与 DML 不要混在一批, 改表等 DDL 请单独调用\n" +
			"MongoDB: sql 传 runCommand 语法的 JSON 命令文档, 单个对象或对象数组; 命令按顺序执行, 无事务回滚, 回复文档按顶层字段平铺为一行; find 后续结果用 getMore 继续取\n" +
			"Redis: sql 每行一条命令, 参数空格分隔, 含空格的参数用双引号包裹; 命令按顺序执行, 无事务回滚\n" +
			"返回: 单条时 rows 与 affectedRows; 多条时 results 内每项含该语句的结果, 并带 statementIndex 与 affectedRows; 默认最多 1000 行, 超出截断并标记 truncated, 截断只限返回体积, 数据库仍会跑完全量, 大表查询必须自带 LIMIT; 单条语句超时默认 30 秒, 超时即中止本批",
	}, func(ctx context.Context, _ *mcp.CallToolRequest, in executeSQLInput) (*mcp.CallToolResult, error) {

		result, err := proxy.ExecuteSQL(ctx, in.Alias, in.DatabaseName, in.SQL)
		if err != nil {
			return nil, err
		}
		return jsonResult(model.OK(result))
	})
}

// ========================== add-database-config ==========================

// addConfigInput 新增实例连接配置入参
type addConfigInput struct {
	Alias                string  `json:"alias" jsonschema:"实例别名(唯一标识,不可与已有配置重复)"`
	DatabaseName         string  `json:"databaseName" jsonschema:"数据库名,多个用英文逗号分隔"`
	DatabaseType         int     `json:"databaseType" jsonschema:"实例类型(0-MySQL 1-Kingbase8 2-PostgreSQL 4-SQLServer 5-MariaDB 6-TiDB 7-OceanBase 8-SQLite 9-MongoDB 10-Redis)"`
	Host                 string  `json:"host" jsonschema:"实例地址(SQLite 填数据库文件的绝对路径, 其端口与用户名不用填)"`
	Port                 int     `json:"port" jsonschema:"实例端口,1至65535(SQLite 填 0)"`
	Username             string  `json:"username" jsonschema:"实例用户名(SQLite、MongoDB 与 Redis 可传空串)"`
	DriverClass          string  `json:"driverClass" jsonschema:"实例驱动类全限定名(SQLite、MongoDB 与 Redis 可传空串)"`
	Password             *string `json:"password,omitempty" jsonschema:"实例密码,不传时存为空"`
	DatabaseVersion      *string `json:"databaseVersion,omitempty" jsonschema:"实例版本,仅展示不参与连接"`
	ConnectionProperties *string `json:"connectionProperties,omitempty" jsonschema:"额外连接属性"`
	PoolMinimumIdle      *int    `json:"poolMinimumIdle,omitempty" jsonschema:"最小空闲连接数,默认2"`
	PoolMaximumPoolSize  *int    `json:"poolMaximumPoolSize,omitempty" jsonschema:"最大连接数,默认10"`
	PoolConnectionTimeMS *int64  `json:"poolConnectionTimeout,omitempty" jsonschema:"连接超时时间(毫秒),默认5000"`
	PoolIdleTimeoutMS    *int64  `json:"poolIdleTimeout,omitempty" jsonschema:"空闲连接超时时间(毫秒),默认600000"`
	PoolMaxLifetimeMS    *int64  `json:"poolMaxLifetime,omitempty" jsonschema:"连接最大存活时间(毫秒),默认1800000"`
	PoolValidationTimeMS *int64  `json:"poolValidationTimeout,omitempty" jsonschema:"连接有效性校验超时时间(毫秒),默认3000"`
	Status               *int    `json:"status,omitempty" jsonschema:"状态(0-禁用;1-启用),默认1"`
	Remark               *string `json:"remark,omitempty" jsonschema:"备注"`
}

func registerAddConfig(server *mcp.Server, proxy *service.Proxy) {

	addTool(server, &mcp.Tool{
		Name: "add-database-config",
		Description: "新增一条实例连接配置, 成功后配置缓存自动失效并返回配置ID, 可在 get-datasources 中立即看到\n" +
			"alias 是唯一标识, 不可与已有配置重复; databaseName 可填多个库, 用英文逗号分隔\n" +
			"连接信息按 databaseType 填写: SQLite 的 host 填数据库文件绝对路径, port 填 0; SQLite、MongoDB 与 Redis 的 username 与 driverClass 可传空串\n" +
			"连接池参数与 status 不传时取默认值",
	}, func(ctx context.Context, _ *mcp.CallToolRequest, in addConfigInput) (*mcp.CallToolResult, error) {

		patch := model.ConfigPatch{
			Alias:                &in.Alias,
			DatabaseName:         &in.DatabaseName,
			DatabaseType:         &in.DatabaseType,
			Host:                 &in.Host,
			Port:                 &in.Port,
			Username:             &in.Username,
			DriverClass:          &in.DriverClass,
			Password:             in.Password,
			DatabaseVersion:      in.DatabaseVersion,
			ConnectionProperties: in.ConnectionProperties,
			PoolMinimumIdle:      in.PoolMinimumIdle,
			PoolMaximumPoolSize:  in.PoolMaximumPoolSize,
			PoolConnectionTimeMS: in.PoolConnectionTimeMS,
			PoolIdleTimeoutMS:    in.PoolIdleTimeoutMS,
			PoolMaxLifetimeMS:    in.PoolMaxLifetimeMS,
			PoolValidationTimeMS: in.PoolValidationTimeMS,
			Status:               in.Status,
			Remark:               in.Remark,
		}
		id, err := proxy.Create(ctx, patch, mcpOperator())
		if err != nil {
			return nil, err
		}
		return jsonResult(model.R{Code: 200, Message: "新增成功", Data: id})
	})
}

// ========================== update-database-config ==========================

// updateConfigInput 修改实例连接配置入参
type updateConfigInput struct {
	ConfigID             int64   `json:"configId" jsonschema:"配置ID,可先用 get-datasources 查询"`
	Alias                *string `json:"alias,omitempty" jsonschema:"实例别名"`
	DatabaseName         *string `json:"databaseName,omitempty" jsonschema:"数据库名,多个用英文逗号分隔,传入时整体替换"`
	DatabaseType         *int    `json:"databaseType,omitempty" jsonschema:"实例类型(0-MySQL 1-Kingbase8 2-PostgreSQL 4-SQLServer 5-MariaDB 6-TiDB 7-OceanBase 8-SQLite 9-MongoDB 10-Redis)"`
	Host                 *string `json:"host,omitempty" jsonschema:"实例地址(SQLite 填数据库文件的绝对路径)"`
	Port                 *int    `json:"port,omitempty" jsonschema:"实例端口,1至65535(SQLite 填 0)"`
	Username             *string `json:"username,omitempty" jsonschema:"实例用户名(SQLite、MongoDB 与 Redis 可传空串)"`
	DriverClass          *string `json:"driverClass,omitempty" jsonschema:"实例驱动类全限定名(SQLite、MongoDB 与 Redis 可传空串)"`
	Password             *string `json:"password,omitempty" jsonschema:"实例密码,非空时视为新密码更新,空则保留原值"`
	DatabaseVersion      *string `json:"databaseVersion,omitempty" jsonschema:"实例版本,仅展示不参与连接"`
	ConnectionProperties *string `json:"connectionProperties,omitempty" jsonschema:"额外连接属性"`
	PoolMinimumIdle      *int    `json:"poolMinimumIdle,omitempty" jsonschema:"最小空闲连接数"`
	PoolMaximumPoolSize  *int    `json:"poolMaximumPoolSize,omitempty" jsonschema:"最大连接数"`
	PoolConnectionTimeMS *int64  `json:"poolConnectionTimeout,omitempty" jsonschema:"连接超时时间(毫秒)"`
	PoolIdleTimeoutMS    *int64  `json:"poolIdleTimeout,omitempty" jsonschema:"空闲连接超时时间(毫秒)"`
	PoolMaxLifetimeMS    *int64  `json:"poolMaxLifetime,omitempty" jsonschema:"连接最大存活时间(毫秒)"`
	PoolValidationTimeMS *int64  `json:"poolValidationTimeout,omitempty" jsonschema:"连接有效性校验超时时间(毫秒)"`
	Status               *int    `json:"status,omitempty" jsonschema:"状态(0-禁用;1-启用)"`
	Remark               *string `json:"remark,omitempty" jsonschema:"备注"`
}

func registerUpdateConfig(server *mcp.Server, proxy *service.Proxy) {

	addTool(server, &mcp.Tool{
		Name: "update-database-config",
		Description: "按 configId 修改一条实例连接配置, 成功后配置缓存自动失效; 除 configId 外参数全部可选, 只传要改的字段\n" +
			"不传的字段保留原值; alias、databaseName、host、username、password、driverClass 传空串也保留原值\n" +
			"databaseVersion、connectionProperties、remark 传空串即清空\n" +
			"databaseName 传入时整体替换",
	}, func(ctx context.Context, _ *mcp.CallToolRequest, in updateConfigInput) (*mcp.CallToolResult, error) {

		patch := model.ConfigPatch{
			Alias:                in.Alias,
			DatabaseName:         in.DatabaseName,
			DatabaseType:         in.DatabaseType,
			Host:                 in.Host,
			Port:                 in.Port,
			Username:             in.Username,
			DriverClass:          in.DriverClass,
			Password:             in.Password,
			DatabaseVersion:      in.DatabaseVersion,
			ConnectionProperties: in.ConnectionProperties,
			PoolMinimumIdle:      in.PoolMinimumIdle,
			PoolMaximumPoolSize:  in.PoolMaximumPoolSize,
			PoolConnectionTimeMS: in.PoolConnectionTimeMS,
			PoolIdleTimeoutMS:    in.PoolIdleTimeoutMS,
			PoolMaxLifetimeMS:    in.PoolMaxLifetimeMS,
			PoolValidationTimeMS: in.PoolValidationTimeMS,
			Status:               in.Status,
			Remark:               in.Remark,
		}
		if err := proxy.Update(ctx, in.ConfigID, patch, mcpOperator()); err != nil {
			return nil, err
		}
		return jsonResult(model.OKMsg("修改成功"))
	})
}

// ========================== delete-database-config ==========================

// deleteConfigInput 删除实例连接配置入参
type deleteConfigInput struct {
	ConfigID int64 `json:"configId" jsonschema:"配置ID,可先用 get-datasources 查询"`
}

func registerDeleteConfig(server *mcp.Server, proxy *service.Proxy) {

	addTool(server, &mcp.Tool{
		Name:        "delete-database-config",
		Description: "按 configId 删除一条实例连接配置, 成功后配置缓存自动失效; configId 可先用 get-datasources 查询",
	}, func(ctx context.Context, _ *mcp.CallToolRequest, in deleteConfigInput) (*mcp.CallToolResult, error) {

		if err := proxy.Delete(ctx, in.ConfigID); err != nil {
			return nil, err
		}
		return jsonResult(model.OKMsg("删除成功"))
	})
}

// ========================== get-datasources ==========================

// getDatasourcesInput 查询数据源入参
type getDatasourcesInput struct {
	Alias *string `json:"alias,omitempty" jsonschema:"实例别名过滤关键字(模糊匹配,不传返回全部)"`
}

func registerGetDatasources(server *mcp.Server, proxy *service.Proxy) {

	addTool(server, &mcp.Tool{
		Name: "get-datasources",
		Description: "列出全部启用状态的实例连接配置, 含配置ID、实例地址、实例端口、实例用户名、实例类型与所辖数据库列表\n" +
			"执行 SQL 或增删改配置前, 先用它确认 alias 与 databaseName; 也可用于查询 configId\n" +
			"alias 为模糊过滤, 不传返回全部",
	}, func(ctx context.Context, _ *mcp.CallToolRequest, in getDatasourcesInput) (*mcp.CallToolResult, error) {

		filter := ""
		if in.Alias != nil {
			filter = *in.Alias
		}
		views, err := proxy.List(ctx, filter)
		if err != nil {
			return nil, err
		}
		return jsonResult(model.OK(views))
	})
}

// ========================== refresh-config ==========================

func registerRefreshConfig(server *mcp.Server, proxy *service.Proxy) {

	addTool(server, &mcp.Tool{
		Name: "refresh-config",
		Description: "立即从配置库重新读取全部连接配置, 返回启用状态的实例数量\n" +
			"一般无需调用: 配置的增删改成功后缓存会自动失效, 下一次调用自动回源\n" +
			"仅在配置被本服务之外改动过, 或用户明确要求刷新时调用",
	}, func(ctx context.Context, _ *mcp.CallToolRequest, _ struct{}) (*mcp.CallToolResult, error) {

		count, err := proxy.Reload(ctx)
		if err != nil {
			return nil, err
		}
		return jsonResult(model.OK(count))
	})
}
