// HTTP 传输层:暴露 MCP 端点与内嵌管理页
package transport

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/fengfffffff/database-mcp-go/internal/admin"
	"github.com/fengfffffff/database-mcp-go/internal/config"
	"github.com/fengfffffff/database-mcp-go/internal/model"
	"github.com/fengfffffff/database-mcp-go/internal/service"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// NewHandler 组装全部路由
func NewHandler(cfg *config.Config, server *mcp.Server, proxy *service.Proxy, logger *slog.Logger) http.Handler {

	mux := http.NewServeMux()

	// MCP 端点:Streamable HTTP 传输
	mcpHandler := mcp.NewStreamableHTTPHandler(func(r *http.Request) *mcp.Server {
		return server
	}, &mcp.StreamableHTTPOptions{
		SessionTimeout:             time.Duration(cfg.MCP.SessionTimeoutSeconds) * time.Second,
		MaxRequestBodyBytes:        cfg.MCP.MaxRequestBodyBytes,
		DisableLocalhostProtection: cfg.MCP.DisableLocalhostProtection,
		Stateless:                  cfg.MCP.Stateless,
	})
	authMCP := tokenAuth(cfg.MCP, cfg.MCP.Endpoint, mcpHandler)
	mux.Handle(cfg.MCP.Endpoint, authMCP)
	// 带尾斜杠的地址同样可用: mux 的路径模式不带尾斜杠时是精确匹配, 客户端多写一个斜杠会直接 404
	if !strings.HasSuffix(cfg.MCP.Endpoint, "/") {
		mux.Handle(cfg.MCP.Endpoint+"/", authMCP)
	}

	// 内嵌管理页
	if cfg.Admin.Enabled {
		admin.Register(mux, proxy, cfg.Admin, logger)
	}

	// 健康检查
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, model.OKMsg("ok"))
	})

	return accessLog(logger, mux)
}

// tokenAuth 对 MCP 端点做 Bearer Token 校验,token 为空时不校验
func tokenAuth(cfg config.MCPConfig, endpoint string, next http.Handler) http.Handler {

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if cfg.AuthToken == "" {
			next.ServeHTTP(w, r)
			return
		}
		// 只保护 MCP 端点及其子路径
		if r.URL.Path != endpoint && !strings.HasPrefix(r.URL.Path, endpoint+"/") {
			next.ServeHTTP(w, r)
			return
		}
		expected := "Bearer " + cfg.AuthToken
		if r.Header.Get("Authorization") == expected {
			next.ServeHTTP(w, r)
			return
		}
		writeJSON(w, http.StatusUnauthorized, model.Error(http.StatusUnauthorized, "MCP鉴权未通过"))
	})
}

// accessLog 记录访问日志
func accessLog(logger *slog.Logger, next http.Handler) http.Handler {

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(rec, r)
		logger.Info("HTTP 请求",
			"method", r.Method,
			"path", r.URL.Path,
			"status", rec.status,
			"elapsed_ms", time.Since(start).Milliseconds(),
			"remote", r.RemoteAddr,
			"x_real_ip", r.Header.Get("X-Real-IP"),
		)
	})
}

// statusRecorder 记录响应状态码
type statusRecorder struct {
	http.ResponseWriter
	status int
}

// WriteHeader 记录状态码后透传
func (r *statusRecorder) WriteHeader(status int) {

	r.status = status
	r.ResponseWriter.WriteHeader(status)
}

// Flush 透传刷新,保证流式响应不被缓冲
func (r *statusRecorder) Flush() {

	if f, ok := r.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

// Unwrap 暴露底层写入器,让 http.ResponseController 能正确控制刷新与超时
func (r *statusRecorder) Unwrap() http.ResponseWriter {

	return r.ResponseWriter
}

// writeJSON 输出统一响应体
func writeJSON(w http.ResponseWriter, status int, payload any) {

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}
