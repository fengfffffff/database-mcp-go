// 连接池管理:按「实例别名::数据库」维度懒创建并缓存连接池
// 池条目记下建池时的配置指纹, 配置变了下次取用就重建; 别名或库不存在后由剪枝关掉
package pool

import (
	"context"
	"fmt"
	"log/slog"
	"sync"

	"github.com/fengfffffff/database-mcp-go/internal/driver"
	"github.com/fengfffffff/database-mcp-go/internal/model"
)

// keySeparator 连接池缓存键分隔符
const keySeparator = "::"

// Key 拼接连接池缓存键, 业务层剪枝时要用同一个格式
func Key(alias, database string) string {

	return alias + keySeparator + database
}

// entry 连接池条目
type entry struct {
	// sig 建池时的配置指纹, 与当前配置不一致说明连接参数变了, 池要重建
	sig  string
	pool driver.Pool
}

// Manager 连接池管理器
type Manager struct {
	registry *driver.Registry
	logger   *slog.Logger

	mu    sync.Mutex
	pools map[string]entry
}

// NewManager 创建连接池管理器
func NewManager(registry *driver.Registry, logger *slog.Logger) *Manager {

	return &Manager{
		registry: registry,
		logger:   logger,
		pools:    make(map[string]entry),
	}
}

// Acquire 按实例配置与数据库名取连接
// 缓存命中且配置指纹一致才复用, 否则按当前配置重建, 旧池在后台关闭
func (m *Manager) Acquire(ctx context.Context, cfg *model.DataSourceConfig, databaseName string) (driver.Conn, error) {

	databases := cfg.Databases()
	found := false
	for _, name := range databases {
		if name == databaseName {
			found = true
			break
		}
	}
	if !found {
		return nil, fmt.Errorf("该实例别名下未配置此数据库 - 实例别名: %s, 数据库: %s, 已配置数据库: %v", cfg.Alias, databaseName, databases)
	}

	key := Key(cfg.Alias, databaseName)
	sig := cfg.PoolSignature()
	m.mu.Lock()
	if cached, ok := m.pools[key]; ok && cached.sig == sig {
		m.mu.Unlock()
		return cached.pool.Acquire(ctx)
	}
	m.mu.Unlock()

	// 建池要拨号与认证, Redis 与 MongoDB 还会在建池时 Ping, 不能放在锁里做,
	// 否则一个连不上的实例会把其它实例的取连接一起堵住
	single := *cfg
	single.DatabaseName = databaseName
	created, err := m.registry.NewPool(&single)
	if err != nil {
		return nil, err
	}

	m.mu.Lock()
	if current, ok := m.pools[key]; ok && current.sig == sig {
		// 别的请求已经建好同款池, 用它的, 自己这份关掉
		m.mu.Unlock()
		m.closeAsync(created)
		return current.pool.Acquire(ctx)
	}
	previous, hadPrevious := m.pools[key]
	m.pools[key] = entry{sig: sig, pool: created}
	m.mu.Unlock()

	if hadPrevious && previous.sig != sig {
		// 配置变了: 旧池在后台关, 在途查询可以用到归还为止, 不阻塞本次请求
		m.closeAsync(previous.pool)
		m.logger.Info("配置已变更, 重建连接池", "alias", cfg.Alias, "database", databaseName)
	} else {
		m.logger.Info("创建连接池成功", "alias", cfg.Alias, "database", databaseName)
	}
	return created.Acquire(ctx)
}

// PruneAsync 关掉键已不在当前配置里的连接池, 关闭动作在后台做
// 别名被删除、停用、改名, 或所辖库里删掉某个库后, 对应的池不会被再次取用, 只能靠这里回收
func (m *Manager) PruneAsync(validKeys map[string]struct{}) {

	m.mu.Lock()
	removed := make([]driver.Pool, 0, 2)
	for key, e := range m.pools {
		if _, ok := validKeys[key]; ok {
			continue
		}
		delete(m.pools, key)
		removed = append(removed, e.pool)
	}
	m.mu.Unlock()

	if len(removed) == 0 {
		return
	}
	m.logger.Info("回收已失效的连接池", "count", len(removed))
	for _, p := range removed {
		m.closeAsync(p)
	}
}

// Detach 摘下全部连接池并清空登记表, 返回的池由调用方关闭
func (m *Manager) Detach() []driver.Pool {

	m.mu.Lock()
	old := m.pools
	m.pools = make(map[string]entry)
	m.mu.Unlock()

	pools := make([]driver.Pool, 0, len(old))
	for _, e := range old {
		pools = append(pools, e.pool)
	}
	return pools
}

// Close 关闭全部连接池, 进程退出时用, 同步关闭保证连接都断干净
func (m *Manager) Close() {

	pools := m.Detach()
	m.logger.Info("关闭所有数据库连接池", "count", len(pools))
	for _, p := range pools {
		func() {
			defer func() {
				// 单个池关闭异常不影响其余池
				if r := recover(); r != nil {
					m.logger.Error("关闭连接池异常", "panic", r)
				}
			}()
			p.Close()
		}()
	}
}

// closeAsync 后台关闭单个连接池, pgx 的关闭会等在途查询归还连接, 不能挡在请求路径上
func (m *Manager) closeAsync(p driver.Pool) {

	go func() {
		defer func() {
			if r := recover(); r != nil {
				m.logger.Error("关闭连接池异常", "panic", r)
			}
		}()
		p.Close()
	}()
}
