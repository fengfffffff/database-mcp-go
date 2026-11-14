// 配置加载:主配置为 YAML,支持 ${ENV} 占位符从环境变量取值
// 数据源不在主配置里, 由 source.type 决定走库表还是走独立的 JSON 文件
package config

import (
	"fmt"
	"os"
	"regexp"
	"strings"

	"gopkg.in/yaml.v3"
)

// Config 应用总配置
type Config struct {
	Server   ServerConfig   `yaml:"server"`
	MCP      MCPConfig      `yaml:"mcp"`
	Database DatabaseConfig `yaml:"database"`
	Source   SourceConfig   `yaml:"source"`
	Log      LogConfig      `yaml:"log"`
	Admin    AdminConfig    `yaml:"admin"`
}

// ServerConfig HTTP 服务配置
type ServerConfig struct {
	Port int `yaml:"port"`
}

// MCPConfig MCP 协议层配置
type MCPConfig struct {
	Endpoint  string `yaml:"endpoint"`
	AuthToken string `yaml:"auth_token"`
	// SessionTimeoutSeconds 空闲会话超时,避免断开的流长期驻留
	SessionTimeoutSeconds int `yaml:"session_timeout_seconds"`
	// MaxRequestBodyBytes 请求体上限,0 表示用 SDK 默认值
	MaxRequestBodyBytes int64 `yaml:"max_request_body_bytes"`
	// DisableLocalhostProtection 反向代理场景下必须关闭,否则 Host 头与回环地址不一致会被拒
	DisableLocalhostProtection bool `yaml:"disable_localhost_protection"`
	// Stateless 为 true 时不维持会话,每个请求独立; 为 false 时维持会话
	Stateless bool `yaml:"stateless"`
}

// DatabaseConfig 通用数据库代理行为配置
type DatabaseConfig struct {
	QueryTimeoutSeconds  int `yaml:"query_timeout_seconds"`
	SlowQueryThresholdMs int `yaml:"slow_query_threshold_ms"`
	// MaxRows 单次执行累计返回的最大行数,超出即截断并标记
	MaxRows int `yaml:"max_rows"`
}

// SourceConfig 数据源存储方式
type SourceConfig struct {
	// Type 存储方式: file 走独立 JSON 文件, mysql 走配置表
	Type  string            `yaml:"type"`
	File  FileSourceConfig  `yaml:"file"`
	MySQL MySQLSourceConfig `yaml:"mysql"`
}

// FileSourceConfig JSON 文件存储
type FileSourceConfig struct {
	// Path 数据源清单文件路径
	Path string `yaml:"path"`
}

// MySQLSourceConfig 库表存储
type MySQLSourceConfig struct {
	DSN    string `yaml:"dsn"`
	AESKey string `yaml:"aes_key"`
	Table  string `yaml:"table"`
}

// LogConfig 日志配置
type LogConfig struct {
	Dir        string `yaml:"dir"`
	Level      string `yaml:"level"`
	RetainDays int    `yaml:"retain_days"`
	Console    bool   `yaml:"console"`
	// SQL 是否把执行的语句记入日志,默认开启
	SQL bool `yaml:"sql"`
}

// AdminConfig 内嵌管理页配置
type AdminConfig struct {
	Enabled  bool   `yaml:"enabled"`
	Username string `yaml:"username"`
	Password string `yaml:"password"`
}

var envPattern = regexp.MustCompile(`\$\{([A-Za-z_][A-Za-z0-9_]*)\}`)

// Load 读取主配置并展开环境变量占位符
func Load(path string) (*Config, error) {

	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("读取配置文件失败: %w", err)
	}
	expanded := envPattern.ReplaceAllStringFunc(string(raw), func(token string) string {
		name := envPattern.FindStringSubmatch(token)[1]
		if v, ok := os.LookupEnv(name); ok {
			return v
		}
		return ""
	})

	// 日志默认记录 SQL,先置默认值,yaml 里没写 sql 时保持开启
	cfg := &Config{Log: LogConfig{SQL: true}}
	if err := yaml.Unmarshal([]byte(expanded), cfg); err != nil {
		return nil, fmt.Errorf("解析配置文件失败: %w", err)
	}
	cfg.applyDefaults()
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	return cfg, nil
}

// applyDefaults 补默认值,保证零配置也能起来
func (c *Config) applyDefaults() {

	if c.Server.Port == 0 {
		c.Server.Port = 47319
	}
	if c.MCP.Endpoint == "" {
		c.MCP.Endpoint = "/mcp"
	}
	// 去掉结尾多余的斜杠, 根路径保持不动; 否则配成 /mcp/ 时注册不出 /mcp 这个精确路径
	if len(c.MCP.Endpoint) > 1 {
		if trimmed := strings.TrimRight(c.MCP.Endpoint, "/"); trimmed != "" {
			c.MCP.Endpoint = trimmed
		}
	}
	if c.MCP.SessionTimeoutSeconds == 0 {
		c.MCP.SessionTimeoutSeconds = 600
	}
	// 0 与负数都兜回默认值: 负数会让每条语句一开始就过期, 比不设超时更难排查
	if c.Database.QueryTimeoutSeconds <= 0 {
		c.Database.QueryTimeoutSeconds = 30
	}
	if c.Database.SlowQueryThresholdMs == 0 {
		c.Database.SlowQueryThresholdMs = 1000
	}
	if c.Database.MaxRows == 0 {
		c.Database.MaxRows = 1000
	}
	if c.Source.Type == "" {
		c.Source.Type = "file"
	}
	if c.Source.File.Path == "" {
		c.Source.File.Path = "datasources.json"
	}
	if c.Source.MySQL.Table == "" {
		c.Source.MySQL.Table = "sys_mcp_database_config"
	}
	if c.Log.Dir == "" {
		c.Log.Dir = "logs"
	}
	if c.Log.Level == "" {
		c.Log.Level = "info"
	}
	if c.Log.RetainDays == 0 {
		c.Log.RetainDays = 14
	}
}

// validate 校验关键项,配置错误启动即失败,不留到运行期
func (c *Config) validate() error {

	switch c.Source.Type {
	case "file":
		if c.Source.File.Path == "" {
			return fmt.Errorf("source.type=file 时 source.file.path 必填")
		}
	case "mysql":
		if c.Source.MySQL.DSN == "" {
			return fmt.Errorf("source.type=mysql 时 source.mysql.dsn 必填")
		}
		if c.Source.MySQL.AESKey == "" {
			return fmt.Errorf("source.type=mysql 时 source.mysql.aes_key 必填")
		}
	default:
		return fmt.Errorf("不支持的 source.type: %s, 仅支持 file 或 mysql", c.Source.Type)
	}
	if !strings.HasPrefix(c.MCP.Endpoint, "/") {
		return fmt.Errorf("mcp.endpoint 必须以 / 开头")
	}
	// 根路径会让 MCP 处理器吃掉所有未匹配的请求, 鉴权判断也失去可比的路径前缀, 直接禁止
	if c.MCP.Endpoint == "/" {
		return fmt.Errorf("mcp.endpoint 不能是根路径, 请填具体路径, 例如 /mcp")
	}
	return nil
}
