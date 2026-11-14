// 业务编排:SQL 执行、配置增删改查, 以及配置缓存的失效与回源
package service

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/fengfffffff/database-mcp-go/internal/config"
	"github.com/fengfffffff/database-mcp-go/internal/driver"
	"github.com/fengfffffff/database-mcp-go/internal/model"
	"github.com/fengfffffff/database-mcp-go/internal/pool"
	"github.com/fengfffffff/database-mcp-go/internal/repository"
)

// Proxy 数据库代理业务服务
// 配置在内存里是缓存: 增删改成功后只让它失效, 下一次访问再回源, 不回源的路径不碰配置库
type Proxy struct {
	source repository.Source
	pools  *pool.Manager
	logger *slog.Logger
	cfg    config.DatabaseConfig
	logSQL bool

	mu      sync.RWMutex
	loadMu  sync.Mutex
	byAlias map[string]*model.DataSourceConfig
	// loaded 缓存是否可用; 失效后置 false, 下次取配置时回源
	loaded bool
	// gen 配置版本, 失效与装入都会自增, 用于丢弃回源途中的旧结果
	gen uint64
}

// NewProxy 创建业务服务
func NewProxy(source repository.Source, pools *pool.Manager, logger *slog.Logger, cfg config.DatabaseConfig, logSQL bool) *Proxy {

	return &Proxy{
		source:  source,
		pools:   pools,
		logger:  logger,
		cfg:     cfg,
		logSQL:  logSQL,
		byAlias: make(map[string]*model.DataSourceConfig),
	}
}

// Load 启动时加载配置
func (p *Proxy) Load(ctx context.Context) error {

	list, err := p.source.List(ctx)
	if err != nil {
		return err
	}
	p.install(list)
	if len(list) == 0 {
		p.logger.Warn("未加载到任何启用的存储配置, 请先配置数据源")
		return nil
	}
	for _, cfg := range list {
		p.logger.Info("已加载数据源配置", "alias", cfg.Alias, "databases", cfg.Databases(), "type", model.DatabaseTypeDesc(cfg.DatabaseType))
	}
	return nil
}

// Reload 立即回源刷新配置, 供管理页按钮与 refresh-config 工具在配置被服务外部改动后使用
// 连接池不整体逐出: 配置指纹没变的池继续用, 变了的在下一次取用时自动重建
func (p *Proxy) Reload(ctx context.Context) (int, error) {

	p.loadMu.Lock()
	defer p.loadMu.Unlock()

	count, err := p.loadAndInstall(ctx)
	if err != nil {
		p.logger.Error("重新加载配置失败", "err", err)
		return 0, err
	}
	p.logger.Info("配置刷新完成", "count", count)
	return count, nil
}

// ExecuteSQL 执行任意 SQL
func (p *Proxy) ExecuteSQL(ctx context.Context, alias, databaseName, sqlText string) (model.QueryResult, error) {

	if strings.TrimSpace(alias) == "" {
		return model.QueryResult{}, fmt.Errorf("实例别名不能为空")
	}
	if strings.TrimSpace(databaseName) == "" {
		return model.QueryResult{}, fmt.Errorf("数据库名不能为空")
	}
	if strings.TrimSpace(sqlText) == "" {
		return model.QueryResult{}, fmt.Errorf("SQL 语句不能为空")
	}
	cfg, err := p.getConfig(ctx, alias)
	if err != nil {
		return model.QueryResult{}, err
	}

	conn, err := p.acquireConn(ctx, cfg, databaseName)
	if err != nil {
		return model.QueryResult{}, err
	}
	defer conn.Close()

	start := time.Now()
	outcome, err := conn.Execute(ctx, sqlText, driver.ExecOptions{
		QueryTimeout: time.Duration(p.cfg.QueryTimeoutSeconds) * time.Second,
		MaxRows:      p.cfg.MaxRows,
	})
	if err != nil {
		p.logger.Error("SQL 执行异常", p.sqlLogAttrs(alias, databaseName, sqlText, "err", err)...)
		return model.QueryResult{}, formatExecuteError(err, time.Duration(p.cfg.QueryTimeoutSeconds)*time.Second)
	}
	elapsed := time.Since(start).Milliseconds()

	if elapsed > int64(p.cfg.SlowQueryThresholdMs) {
		p.logger.Warn("慢操作告警", p.sqlLogAttrs(alias, databaseName, sqlText,
			"rows", len(outcome.Rows), "affected", outcome.Affected, "elapsed_ms", elapsed)...)
	} else {
		p.logger.Info("SQL 执行成功", p.sqlLogAttrs(alias, databaseName, sqlText,
			"rows", len(outcome.Rows), "affected", outcome.Affected, "elapsed_ms", elapsed)...)
	}

	result := model.QueryResult{
		Rows:          []model.OrderedRow{},
		RowCount:      len(outcome.Rows),
		AffectedRows:  outcome.Affected,
		ExecutionTime: elapsed,
		Truncated:     outcome.Truncated,
	}
	if len(outcome.Statements) > 1 {
		// 多条语句按条返回, 避免不同语句的行混在一起分不清来源
		result.StatementCount = len(outcome.Statements)
		result.Results = make([]model.StatementResult, 0, len(outcome.Statements))
		for _, stmt := range outcome.Statements {
			result.Results = append(result.Results, model.StatementResult{
				StatementIndex: stmt.Index,
				Rows:           stmt.Rows,
				RowCount:       len(stmt.Rows),
				AffectedRows:   stmt.Affected,
				Truncated:      stmt.Truncated,
			})
		}
		return result, nil
	}
	result.Rows = outcome.Rows
	return result, nil
}

// acquireConn 取连接; 给连接获取加一道上限, 避免池被占满或实例不可达时无限等待
// 连接超时配成 0 表示不设上限, 这道兜底只在获取连接阶段生效, 不影响语句本身的超时配置
func (p *Proxy) acquireConn(ctx context.Context, cfg *model.DataSourceConfig, databaseName string) (driver.Conn, error) {

	timeout := time.Duration(p.cfg.QueryTimeoutSeconds) * time.Second
	if timeout <= 0 {
		return p.pools.Acquire(ctx, cfg, databaseName)
	}
	limit := timeout + 5*time.Second
	acquireCtx, cancel := context.WithTimeout(ctx, limit)
	defer cancel()
	conn, err := p.pools.Acquire(acquireCtx, cfg, databaseName)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			return nil, fmt.Errorf("获取数据库连接超时 - 超过 %d 秒仍未取到连接, 请检查实例地址是否可达与连接池上限是否偏小", int(limit.Seconds()))
		}
		return nil, err
	}
	return conn, nil
}

// sqlLogAttrs 组装执行日志字段,开启 SQL 日志时附上截断后的语句
func (p *Proxy) sqlLogAttrs(alias, databaseName, sqlText string, extra ...any) []any {

	attrs := make([]any, 0, 6+len(extra))
	attrs = append(attrs, "alias", alias, "database", databaseName)
	if p.logSQL {
		attrs = append(attrs, "sql", summarizeText(sqlText, 500))
	}
	attrs = append(attrs, extra...)
	return attrs
}

// List 返回数据源视图,支持别名模糊过滤
func (p *Proxy) List(ctx context.Context, aliasFilter string) ([]model.DatasourceView, error) {

	for attempt := 0; attempt < 3; attempt++ {
		if err := p.ensureLoaded(ctx); err != nil {
			return nil, err
		}
		views, ok := p.listLoaded(aliasFilter)
		if ok {
			return views, nil
		}
		// 取视图时缓存又被失效, 重来一次
	}
	return nil, fmt.Errorf("实例连接配置正在变更, 请稍后重试")
}

// listLoaded 在缓存可用时生成视图, 缓存刚好失效则返回 false
func (p *Proxy) listLoaded(aliasFilter string) ([]model.DatasourceView, bool) {

	p.mu.RLock()
	defer p.mu.RUnlock()
	if !p.loaded {
		return nil, false
	}

	configs := make([]*model.DataSourceConfig, 0, len(p.byAlias))
	for _, cfg := range p.byAlias {
		if aliasFilter != "" && !strings.Contains(cfg.Alias, aliasFilter) {
			continue
		}
		configs = append(configs, cfg)
	}
	sort.Slice(configs, func(i, j int) bool { return configs[i].ID < configs[j].ID })

	views := make([]model.DatasourceView, 0, len(configs))
	for _, cfg := range configs {
		views = append(views, model.View(cfg))
	}
	return views, true
}

// Create 新增实例连接配置, op 为操作者身份, 写入创建人列
func (p *Proxy) Create(ctx context.Context, patch model.ConfigPatch, op model.Operator) (int64, error) {

	cfg, err := p.buildForCreate(patch)
	if err != nil {
		return 0, err
	}
	id, err := p.source.Create(ctx, cfg, op)
	if err != nil {
		return 0, fmt.Errorf("实例连接配置新增失败 - %s", err.Error())
	}
	p.logger.Info("实例连接配置新增成功", "configId", id, "alias", cfg.Alias, "operator", op.Name)
	p.invalidateAfterMutation("实例连接配置新增")
	return id, nil
}

// Update 修改实例连接配置, op 为操作者身份, 写入修改人列
// 未传的字段保留原值;连接类字段传空也保留,实例版本、连接属性、备注传空即清空
func (p *Proxy) Update(ctx context.Context, id int64, patch model.ConfigPatch, op model.Operator) error {

	if id <= 0 {
		return fmt.Errorf("配置ID不能为空")
	}
	// 直接读库取基线,停用状态与刚被其它端改过的配置都能取到最新值
	current, err := p.source.Get(ctx, id)
	if err != nil {
		return err
	}
	merged := mergePatch(current, patch)
	if err := validateConfig(merged); err != nil {
		return err
	}
	if err := p.source.Update(ctx, merged, op); err != nil {
		return fmt.Errorf("实例连接配置修改失败 - %s", err.Error())
	}
	p.logger.Info("实例连接配置修改成功", "configId", merged.ID, "alias", merged.Alias, "operator", op.Name)
	p.invalidateAfterMutation("实例连接配置修改")
	return nil
}

// SetStatus 单独切换启用状态,供管理页列表开关调用
func (p *Proxy) SetStatus(ctx context.Context, id int64, status int, op model.Operator) error {

	if id <= 0 {
		return fmt.Errorf("配置ID不能为空")
	}
	if status != 0 && status != 1 {
		return fmt.Errorf("状态只能为 0 或 1")
	}
	return p.Update(ctx, id, model.ConfigPatch{Status: &status}, op)
}

// ListPage 管理页分页查询
func (p *Proxy) ListPage(ctx context.Context, q *model.ListQuery) (model.PageResult, error) {

	list, total, err := p.source.ListPage(ctx, q)
	if err != nil {
		return model.PageResult{}, err
	}
	views := make([]model.DatasourceView, 0, len(list))
	for _, cfg := range list {
		views = append(views, model.View(cfg))
	}
	return model.PageResult{List: views, Total: total, PageNum: q.PageNum, PageSize: q.PageSize}, nil
}

// Detail 管理页详情查询,不返回实例密码本身
func (p *Proxy) Detail(ctx context.Context, id int64) (model.DetailView, error) {

	cfg, err := p.source.Get(ctx, id)
	if err != nil {
		return model.DetailView{}, err
	}
	return model.Detail(cfg), nil
}

// TypeCount 实例类型数量统计
func (p *Proxy) TypeCount(ctx context.Context) (model.TypeCount, error) {

	return p.source.TypeCount(ctx)
}

// BatchDelete 批量删除配置
func (p *Proxy) BatchDelete(ctx context.Context, ids []int64) error {

	if len(ids) == 0 {
		return fmt.Errorf("请选择要删除的配置")
	}
	if err := p.source.BatchDelete(ctx, ids); err != nil {
		return fmt.Errorf("实例连接配置删除失败 - %s", err.Error())
	}
	p.logger.Info("实例连接配置批量删除成功", "count", len(ids))
	p.invalidateAfterMutation("实例连接配置删除")
	return nil
}

// Delete 删除实例连接配置
func (p *Proxy) Delete(ctx context.Context, id int64) error {

	if id <= 0 {
		return fmt.Errorf("配置ID不能为空")
	}
	if err := p.source.Delete(ctx, id); err != nil {
		return fmt.Errorf("实例连接配置删除失败 - %s", err.Error())
	}
	p.logger.Info("实例连接配置删除成功", "configId", id)
	p.invalidateAfterMutation("实例连接配置删除")
	return nil
}

// SourceKind 当前配置来源类型
func (p *Proxy) SourceKind() string {

	return p.source.Kind()
}

// invalidate 让配置缓存失效, 下一次取配置时回源
// 不在这里读库: 变更接口只保证写入成功, 回源失败不能反过来把已成功的变更报成失败
func (p *Proxy) invalidate() {

	p.mu.Lock()
	p.byAlias = nil
	p.loaded = false
	p.gen++
	p.mu.Unlock()
}

// invalidateAfterMutation 增删改成功后失效缓存, 连接池不用在这里逐个关
// 指纹变了的池会在下次取用时重建, 别名或库已不存在的池在回源后剪掉
func (p *Proxy) invalidateAfterMutation(operation string) {

	p.invalidate()
	p.logger.Info(operation + "后配置缓存已失效, 下次访问自动回源")
}

// install 装入最新配置索引并递增版本, 再剪掉键已不在配置里的连接池
func (p *Proxy) install(list []*model.DataSourceConfig) {

	p.mu.Lock()
	index := p.indexLocked(list)
	p.mu.Unlock()
	p.pools.PruneAsync(poolKeys(index))
}

// indexLocked 在持有写锁的情况下替换索引, 返回新索引供剪枝用
func (p *Proxy) indexLocked(list []*model.DataSourceConfig) map[string]*model.DataSourceConfig {

	index := make(map[string]*model.DataSourceConfig, len(list))
	for _, cfg := range list {
		if exist, ok := index[cfg.Alias]; ok {
			p.logger.Warn("实例别名重复, 后者覆盖前者", "alias", cfg.Alias, "覆盖配置ID", exist.ID, "新配置ID", cfg.ID)
		}
		index[cfg.Alias] = cfg
	}
	p.byAlias = index
	p.loaded = true
	p.gen++
	return index
}

// poolKeys 由配置索引生成全部合法的池键, 供剪枝比对
func poolKeys(index map[string]*model.DataSourceConfig) map[string]struct{} {

	keys := make(map[string]struct{}, len(index))
	for alias, cfg := range index {
		for _, database := range cfg.Databases() {
			keys[pool.Key(alias, database)] = struct{}{}
		}
	}
	return keys
}

// ensureLoaded 缓存里没有可用配置时回源, 并发请求只会有一个真正去查配置库
func (p *Proxy) ensureLoaded(ctx context.Context) error {

	p.mu.RLock()
	loaded := p.loaded
	p.mu.RUnlock()
	if loaded {
		return nil
	}

	p.loadMu.Lock()
	defer p.loadMu.Unlock()

	p.mu.RLock()
	loaded = p.loaded
	p.mu.RUnlock()
	if loaded {
		return nil
	}

	_, err := p.loadAndInstall(ctx)
	return err
}

// loadAndInstall 回源并装入索引, 回源期间配置被改过就丢弃这次结果重来, 最多三次
// 调用方须持有 loadMu, 保证同一时刻只有一个回源动作在跑
func (p *Proxy) loadAndInstall(ctx context.Context) (int, error) {

	for attempt := 0; attempt < 3; attempt++ {
		p.mu.RLock()
		gen := p.gen
		p.mu.RUnlock()

		list, err := p.source.List(ctx)
		if err != nil {
			return 0, fmt.Errorf("从配置库加载实例连接配置失败 - 错误信息: %s", err.Error())
		}

		p.mu.Lock()
		if p.gen != gen {
			p.mu.Unlock()
			continue
		}
		index := p.indexLocked(list)
		p.mu.Unlock()

		p.pools.PruneAsync(poolKeys(index))
		return len(list), nil
	}
	return 0, fmt.Errorf("实例连接配置正在变更, 请稍后重试")
}

// getConfig 按别名取配置, 缓存不可用时先回源
func (p *Proxy) getConfig(ctx context.Context, alias string) (*model.DataSourceConfig, error) {

	for attempt := 0; attempt < 3; attempt++ {
		if err := p.ensureLoaded(ctx); err != nil {
			return nil, err
		}
		p.mu.RLock()
		cfg, ok := p.byAlias[alias]
		loaded := p.loaded
		p.mu.RUnlock()
		if ok {
			return cfg, nil
		}
		// 有些名确实不在配置里就直接报错; 缓存中途又被失效时才重来
		if loaded {
			return nil, p.aliasNotFound(alias)
		}
	}
	return nil, fmt.Errorf("实例连接配置正在变更, 请稍后重试")
}

// aliasNotFound 组装未命中提示, 附上当前可用的别名
func (p *Proxy) aliasNotFound(alias string) error {

	p.mu.RLock()
	aliases := make([]string, 0, len(p.byAlias))
	for name := range p.byAlias {
		aliases = append(aliases, name)
	}
	p.mu.RUnlock()
	sort.Strings(aliases)
	return fmt.Errorf("未找到实例别名 - 实例别名: %s, 可用实例别名: %v", alias, aliases)
}

// buildForCreate 校验并组装新增配置
func (p *Proxy) buildForCreate(patch model.ConfigPatch) (*model.DataSourceConfig, error) {

	cfg := &model.DataSourceConfig{Status: 1}
	merged := mergePatch(cfg, patch)
	applyCreateDefaults(merged, patch)
	if err := validateConfig(merged); err != nil {
		return nil, err
	}
	return merged, nil
}

// applyCreateDefaults 给新增请求里没传的连接池参数补平台默认值
// 必须按补丁判断有没有传, 不能看合并结果: 显式传 0 的项要保留 0, 与平台新增口径一致
func applyCreateDefaults(cfg *model.DataSourceConfig, patch model.ConfigPatch) {

	if patch.PoolMinimumIdle == nil {
		cfg.PoolMinimumIdle = 2
	}
	if patch.PoolMaximumPoolSize == nil {
		cfg.PoolMaximumPoolSize = 10
	}
	if patch.PoolConnectionTimeMS == nil {
		cfg.PoolConnectionTimeMS = 5000
	}
	if patch.PoolIdleTimeoutMS == nil {
		cfg.PoolIdleTimeoutMS = 600000
	}
	if patch.PoolMaxLifetimeMS == nil {
		cfg.PoolMaxLifetimeMS = 1800000
	}
	if patch.PoolValidationTimeMS == nil {
		cfg.PoolValidationTimeMS = 3000
	}
}

// mergePatch 用补丁覆盖基线:连接类字符串空值视为保留原值,实例版本、连接属性、备注按传入值覆盖
func mergePatch(base *model.DataSourceConfig, patch model.ConfigPatch) *model.DataSourceConfig {

	out := *base
	if patch.Alias != nil && *patch.Alias != "" {
		out.Alias = *patch.Alias
	}
	if patch.DatabaseName != nil && *patch.DatabaseName != "" {
		out.DatabaseName = *patch.DatabaseName
	}
	if patch.DatabaseType != nil {
		out.DatabaseType = *patch.DatabaseType
	}
	if patch.DatabaseVersion != nil {
		out.DatabaseVersion = *patch.DatabaseVersion
	}
	if patch.Host != nil && *patch.Host != "" {
		out.Host = *patch.Host
	}
	if patch.Port != nil {
		out.Port = *patch.Port
	}
	if patch.Username != nil && *patch.Username != "" {
		out.Username = *patch.Username
	}
	if patch.Password != nil && *patch.Password != "" {
		out.Password = *patch.Password
	}
	if patch.DriverClass != nil && *patch.DriverClass != "" {
		out.DriverClass = *patch.DriverClass
	}
	if patch.ConnectionProperties != nil {
		out.ConnectionProperties = *patch.ConnectionProperties
	}
	if patch.PoolMinimumIdle != nil {
		out.PoolMinimumIdle = *patch.PoolMinimumIdle
	}
	if patch.PoolMaximumPoolSize != nil {
		out.PoolMaximumPoolSize = *patch.PoolMaximumPoolSize
	}
	if patch.PoolConnectionTimeMS != nil {
		out.PoolConnectionTimeMS = *patch.PoolConnectionTimeMS
	}
	if patch.PoolIdleTimeoutMS != nil {
		out.PoolIdleTimeoutMS = *patch.PoolIdleTimeoutMS
	}
	if patch.PoolMaxLifetimeMS != nil {
		out.PoolMaxLifetimeMS = *patch.PoolMaxLifetimeMS
	}
	if patch.PoolValidationTimeMS != nil {
		out.PoolValidationTimeMS = *patch.PoolValidationTimeMS
	}
	if patch.Status != nil {
		out.Status = *patch.Status
	}
	if patch.Remark != nil {
		out.Remark = *patch.Remark
	}
	return &out
}

// validateConfig 校验必填项与取值范围,新增与修改共用
func validateConfig(cfg *model.DataSourceConfig) error {

	switch {
	case cfg.Alias == "":
		return fmt.Errorf("实例别名不能为空")
	case cfg.DatabaseName == "":
		return fmt.Errorf("数据库名不能为空")
	case cfg.Host == "":
		return fmt.Errorf("实例地址不能为空")
	}
	if len(cfg.Databases()) == 0 {
		return fmt.Errorf("数据库名不能为空")
	}
	// 连接池参数按原值落库, 负数会撞 unsigned 列的越界错误, 保存时就挡住
	switch {
	case cfg.PoolMinimumIdle < 0:
		return fmt.Errorf("最小空闲连接数不能为负数")
	case cfg.PoolMaximumPoolSize < 0:
		return fmt.Errorf("最大连接数不能为负数")
	case cfg.PoolConnectionTimeMS < 0:
		return fmt.Errorf("连接超时时间不能为负数")
	case cfg.PoolIdleTimeoutMS < 0:
		return fmt.Errorf("空闲连接超时时间不能为负数")
	case cfg.PoolMaxLifetimeMS < 0:
		return fmt.Errorf("连接最大存活时间不能为负数")
	case cfg.PoolValidationTimeMS < 0:
		return fmt.Errorf("连接有效性校验超时时间不能为负数")
	}
	// Redis 的库名是逻辑库编号, 保存时就校验, 免得等到执行才暴露
	if cfg.DatabaseType == model.DatabaseTypeRedis {
		for _, name := range cfg.Databases() {
			dbIndex, err := strconv.Atoi(strings.TrimSpace(name))
			if err != nil || dbIndex < 0 || dbIndex > 65535 {
				return fmt.Errorf("Redis 的库名须为逻辑库编号 0 至 65535 - 当前: %s", name)
			}
		}
	}
	// SQLite 是单文件库, 实例地址填文件路径; MongoDB 与 Redis 允许无账号的本地实例
	// 这三类不要求用户名与驱动类, 端口只有 SQLite 可不填
	if cfg.DatabaseType != model.DatabaseTypeSQLite {
		switch {
		case cfg.Port == 0:
			return fmt.Errorf("实例端口不能为空")
		case cfg.Port < 1 || cfg.Port > 65535:
			return fmt.Errorf("实例端口须在 1 至 65535 之间")
		}
		noAccount := cfg.DatabaseType == model.DatabaseTypeMongoDB || cfg.DatabaseType == model.DatabaseTypeRedis
		if !noAccount {
			switch {
			case cfg.Username == "":
				return fmt.Errorf("实例用户名不能为空")
			case cfg.DriverClass == "":
				return fmt.Errorf("实例驱动类不能为空")
			}
		}
		// MongoDB 的认证必须有用户名, 只填密码会被静默丢掉
		if cfg.DatabaseType == model.DatabaseTypeMongoDB && cfg.Password != "" && cfg.Username == "" {
			return fmt.Errorf("MongoDB 填了密码就必须同时填实例用户名")
		}
	}
	// 类型必须有对应驱动, 否则配置能存下但第一次执行才暴露, 用户很难定位
	if !driver.Default().Supports(cfg.DatabaseType) {
		name := model.DatabaseTypeDesc(cfg.DatabaseType)
		if name == "" {
			name = "未知类型"
		}
		return fmt.Errorf("实例类型暂未接入驱动 - 实例类型: %d(%s), 已接入: %s",
			cfg.DatabaseType, name, supportedTypeNames())
	}
	return nil
}

// supportedTypeNames 已接入驱动的实例类型名称,用于错误提示
func supportedTypeNames() string {

	types := driver.Default().Supported()
	names := make([]string, 0, len(types))
	for _, t := range types {
		name := model.DatabaseTypeDesc(t)
		if name == "" {
			name = fmt.Sprintf("类型 %d", t)
		}
		names = append(names, name)
	}
	return strings.Join(names, "、")
}

// formatExecuteError 把驱动错误翻成给模型看的中文提示
// 明确区分超时与执行失败, 批量执行带语句序号, 超时与批量失败都附上语句原文
func formatExecuteError(err error, timeout time.Duration) error {

	var stmtErr *driver.StmtError
	if !errors.As(err, &stmtErr) {
		return fmt.Errorf("SQL 执行失败 - %s", err.Error())
	}
	statement := summarizeStatement(stmtErr.Statement)
	batch := stmtErr.Total > 1
	if stmtErr.Timeout {
		limit := ""
		if seconds := int(timeout.Seconds()); seconds > 0 {
			limit = fmt.Sprintf("超过 %d 秒未完成, ", seconds)
		}
		if batch {
			return fmt.Errorf("第 %d/%d 条语句执行超时 - %s已中止, 语句: %s", stmtErr.Index, stmtErr.Total, limit, statement)
		}
		return fmt.Errorf("SQL 执行超时 - %s已中止, 语句: %s", limit, statement)
	}
	if batch {
		return fmt.Errorf("第 %d/%d 条语句执行失败 - %s, 语句: %s", stmtErr.Index, stmtErr.Total, describeStmtError(stmtErr), statement)
	}
	return fmt.Errorf("SQL 执行失败 - %s", describeStmtError(stmtErr))
}

// describeStmtError 拼装数据库错误的明细,带类别、错误码、原始信息与修正建议
func describeStmtError(e *driver.StmtError) string {

	info := e.Info
	if info == nil {
		return e.Cause.Error()
	}
	parts := make([]string, 0, 8)
	if info.Category != "" {
		parts = append(parts, "错误类别: "+info.Category)
	}
	if info.Code != "" {
		parts = append(parts, "错误码: "+info.Code)
	}
	if info.SQLState != "" {
		parts = append(parts, "SQLSTATE: "+info.SQLState)
	}
	if info.Message != "" {
		parts = append(parts, "原始信息: "+summarizeText(info.Message, 500))
	}
	if info.Detail != "" {
		parts = append(parts, "详情: "+summarizeText(info.Detail, 500))
	}
	if info.Hint != "" {
		parts = append(parts, "服务端提示: "+summarizeText(info.Hint, 500))
	}
	if info.Location != "" {
		parts = append(parts, "位置: "+info.Location)
	}
	if info.Advice != "" {
		parts = append(parts, "建议: "+info.Advice)
	}
	if len(parts) == 0 {
		return e.Cause.Error()
	}
	return strings.Join(parts, ", ")
}

// summarizeStatement 把语句压成一行并限长,避免报错信息被长语句撑爆
func summarizeStatement(stmt string) string {

	return summarizeText(stmt, 300)
}

// summarizeText 把文本压成一行并限长
func summarizeText(text string, max int) string {

	compact := strings.Join(strings.Fields(text), " ")
	runes := []rune(compact)
	if len(runes) <= max {
		return compact
	}
	return string(runes[:max]) + "…"
}
