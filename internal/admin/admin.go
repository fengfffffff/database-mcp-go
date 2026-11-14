// 内嵌管理页:Vue3 加 Element Plus 的构建产物随二进制分发,页面与接口都从这里提供
package admin

import (
	"embed"
	"encoding/json"
	"io/fs"
	"log/slog"
	"net/http"
	"strconv"

	"github.com/fengfffffff/database-mcp-go/internal/config"
	"github.com/fengfffffff/database-mcp-go/internal/model"
	"github.com/fengfffffff/database-mcp-go/internal/service"
)

//go:embed static
var staticFS embed.FS

// Register 注册管理页与后台接口路由
func Register(mux *http.ServeMux, proxy *service.Proxy, cfg config.AdminConfig, logger *slog.Logger) {

	auth := basicAuth(cfg)
	sub, err := fs.Sub(staticFS, "static")
	if err != nil {
		logger.Error("加载管理页静态资源失败", "err", err)
		return
	}

	// 静态资源:JS 与 CSS
	mux.Handle("/admin/assets/", auth(http.StripPrefix("/admin/assets/", http.FileServer(http.FS(sub)))))

	// 页面入口
	index := func(w http.ResponseWriter, r *http.Request) {
		raw, readErr := fs.ReadFile(sub, "index.html")
		if readErr != nil {
			http.Error(w, "管理页资源缺失", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		// 页面不缓存,避免改了前端后浏览器还拿旧页面
		w.Header().Set("Cache-Control", "no-store")
		_, _ = w.Write(raw)
	}
	mux.Handle("/admin", auth(http.HandlerFunc(index)))
	mux.Handle("/admin/", auth(http.HandlerFunc(index)))

	api := &apiHandler{proxy: proxy, logger: logger}
	mux.Handle("/admin/api/list", auth(http.HandlerFunc(api.list)))
	mux.Handle("/admin/api/detail", auth(http.HandlerFunc(api.detail)))
	mux.Handle("/admin/api/create", auth(http.HandlerFunc(api.create)))
	mux.Handle("/admin/api/update", auth(http.HandlerFunc(api.update)))
	mux.Handle("/admin/api/status", auth(http.HandlerFunc(api.status)))
	mux.Handle("/admin/api/batch-delete", auth(http.HandlerFunc(api.batchDelete)))
	mux.Handle("/admin/api/type-count", auth(http.HandlerFunc(api.typeCount)))
	mux.Handle("/admin/api/reload", auth(http.HandlerFunc(api.reload)))
	mux.Handle("/admin/api/meta", auth(http.HandlerFunc(api.meta)))
	// 未匹配到的接口路径直接 404,避免被页面入口兜底成 HTML 让前端解析失败
	mux.Handle("/admin/api/", auth(http.NotFoundHandler()))
}

// apiHandler 管理页接口
type apiHandler struct {
	proxy  *service.Proxy
	logger *slog.Logger
}

// list 分页查询
func (h *apiHandler) list(w http.ResponseWriter, r *http.Request) {

	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, model.Error(http.StatusMethodNotAllowed, "仅支持 GET"))
		return
	}
	q := &model.ListQuery{
		Alias:        r.URL.Query().Get("alias"),
		DatabaseName: r.URL.Query().Get("databaseName"),
	}
	if v := r.URL.Query().Get("databaseType"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			q.DatabaseType = &n
		}
	}
	if v := r.URL.Query().Get("status"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			q.Status = &n
		}
	}
	q.PageNum, _ = strconv.Atoi(r.URL.Query().Get("pageNum"))
	q.PageSize, _ = strconv.Atoi(r.URL.Query().Get("pageSize"))
	q.Normalize()

	result, err := h.proxy.ListPage(r.Context(), q)
	if err != nil {
		writeError(w, h.logger, "查询配置列表失败", err)
		return
	}
	writeJSON(w, http.StatusOK, model.OK(result))
}

// detail 详情查询
func (h *apiHandler) detail(w http.ResponseWriter, r *http.Request) {

	id, err := strconv.ParseInt(r.URL.Query().Get("id"), 10, 64)
	if err != nil || id <= 0 {
		writeJSON(w, http.StatusBadRequest, model.Error(http.StatusBadRequest, "配置ID无效"))
		return
	}
	detail, err := h.proxy.Detail(r.Context(), id)
	if err != nil {
		writeError(w, h.logger, "查询配置详情失败", err)
		return
	}
	writeJSON(w, http.StatusOK, model.OK(detail))
}

// create 新增
func (h *apiHandler) create(w http.ResponseWriter, r *http.Request) {

	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, model.Error(http.StatusMethodNotAllowed, "仅支持 POST"))
		return
	}
	patch := model.ConfigPatch{}
	if err := json.NewDecoder(r.Body).Decode(&patch); err != nil {
		writeJSON(w, http.StatusBadRequest, model.Error(http.StatusBadRequest, "请求体解析失败: "+err.Error()))
		return
	}
	id, err := h.proxy.Create(r.Context(), patch, operatorOf(r))
	if err != nil {
		writeError(w, h.logger, "新增配置失败", err)
		return
	}
	writeJSON(w, http.StatusOK, model.R{Code: 200, Message: "新增成功", Data: id})
}

// updateRequest 修改请求体
type updateRequest struct {
	ConfigID int64 `json:"configId"`
	model.ConfigPatch
}

// update 修改
func (h *apiHandler) update(w http.ResponseWriter, r *http.Request) {

	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, model.Error(http.StatusMethodNotAllowed, "仅支持 POST"))
		return
	}
	req := updateRequest{}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, model.Error(http.StatusBadRequest, "请求体解析失败: "+err.Error()))
		return
	}
	if err := h.proxy.Update(r.Context(), req.ConfigID, req.ConfigPatch, operatorOf(r)); err != nil {
		writeError(w, h.logger, "修改配置失败", err)
		return
	}
	writeJSON(w, http.StatusOK, model.OKMsg("修改成功"))
}

// statusRequest 状态切换请求体
type statusRequest struct {
	ConfigID int64 `json:"configId"`
	Status   int   `json:"status"`
}

// status 启用停用
func (h *apiHandler) status(w http.ResponseWriter, r *http.Request) {

	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, model.Error(http.StatusMethodNotAllowed, "仅支持 POST"))
		return
	}
	req := statusRequest{}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, model.Error(http.StatusBadRequest, "请求体解析失败: "+err.Error()))
		return
	}
	if err := h.proxy.SetStatus(r.Context(), req.ConfigID, req.Status, operatorOf(r)); err != nil {
		writeError(w, h.logger, "修改状态失败", err)
		return
	}
	if req.Status == 1 {
		writeJSON(w, http.StatusOK, model.OKMsg("已启用"))
		return
	}
	writeJSON(w, http.StatusOK, model.OKMsg("已禁用"))
}

// deleteRequest 批量删除请求体
type deleteRequest struct {
	ConfigIDs []int64 `json:"configIds"`
}

// batchDelete 批量删除
func (h *apiHandler) batchDelete(w http.ResponseWriter, r *http.Request) {

	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, model.Error(http.StatusMethodNotAllowed, "仅支持 POST"))
		return
	}
	req := deleteRequest{}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, model.Error(http.StatusBadRequest, "请求体解析失败: "+err.Error()))
		return
	}
	if err := h.proxy.BatchDelete(r.Context(), req.ConfigIDs); err != nil {
		writeError(w, h.logger, "删除配置失败", err)
		return
	}
	writeJSON(w, http.StatusOK, model.OKMsg("删除成功"))
}

// typeCount 类型数量统计
func (h *apiHandler) typeCount(w http.ResponseWriter, r *http.Request) {

	result, err := h.proxy.TypeCount(r.Context())
	if err != nil {
		writeError(w, h.logger, "统计类型数量失败", err)
		return
	}
	writeJSON(w, http.StatusOK, model.OK(result))
}

// reload 立即回源刷新配置缓存
func (h *apiHandler) reload(w http.ResponseWriter, r *http.Request) {

	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, model.Error(http.StatusMethodNotAllowed, "仅支持 POST"))
		return
	}
	count, err := h.proxy.Reload(r.Context())
	if err != nil {
		writeError(w, h.logger, "重载配置失败", err)
		return
	}
	writeJSON(w, http.StatusOK, model.OK(count))
}

// meta 页面元信息,当前配置来源类型
func (h *apiHandler) meta(w http.ResponseWriter, r *http.Request) {

	writeJSON(w, http.StatusOK, model.OK(map[string]string{"sourceKind": h.proxy.SourceKind()}))
}

// basicAuth 配置了账号密码时启用基础认证
func basicAuth(cfg config.AdminConfig) func(http.Handler) http.Handler {

	return func(next http.Handler) http.Handler {
		if cfg.Username == "" {
			return next
		}
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			user, pass, ok := r.BasicAuth()
			if !ok || user != cfg.Username || pass != cfg.Password {
				w.Header().Set("WWW-Authenticate", `Basic realm="database-mcp-go admin"`)
				http.Error(w, "未授权", http.StatusUnauthorized)
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}

// operatorOf 从 basic auth 取管理页操作者身份, 未启用认证时按管理员记录
// 用户名按列宽截断, 避免超长账号名把审计列写超长导致新增失败
func operatorOf(r *http.Request) model.Operator {

	username, _, _ := r.BasicAuth()
	if username == "" {
		username = model.OperatorNameAdmin
	}
	return model.Operator{Name: truncateRunes(username, 128), Type: model.OperatorTypeAdmin}
}

// truncateRunes 按字符数截断
// 审计名列是 varchar(128), 按字符计, 不能按字节截断, 否则中文名会被截半
func truncateRunes(s string, limit int) string {

	runes := []rune(s)
	if len(runes) <= limit {
		return s
	}
	return string(runes[:limit])
}

// writeError 记录日志并输出业务失败响应
func writeError(w http.ResponseWriter, logger *slog.Logger, action string, err error) {

	logger.Error(action, "err", err)
	writeJSON(w, http.StatusOK, model.Error(500, err.Error()))
}

// writeJSON 输出统一响应体
func writeJSON(w http.ResponseWriter, status int, payload any) {

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}
