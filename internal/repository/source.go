// 配置来源契约:文件来源与库表来源各自实现,业务层只依赖该接口
package repository

import (
	"context"
	"sort"

	"github.com/fengfffffff/database-mcp-go/internal/model"
)

// buildTypeCount 把类型计数映射整理成按类型升序的结果
func buildTypeCount(countMap map[int]int64, total int64) model.TypeCount {

	types := make([]int, 0, len(countMap))
	for t := range countMap {
		types = append(types, t)
	}
	sort.Ints(types)
	items := make([]model.TypeCountItem, 0, len(types))
	for _, t := range types {
		items = append(items, model.TypeCountItem{DatabaseType: t, ConfigCount: countMap[t]})
	}
	return model.TypeCount{TotalCount: total, Items: items}
}

// Source 配置来源
type Source interface {
	// Kind 来源类型标识,file 或 mysql
	Kind() string
	// List 返回启用状态的全部配置,密码为明文,供运行时连接池使用
	List(ctx context.Context) ([]*model.DataSourceConfig, error)
	// ListPage 按条件分页查询全部配置,含停用项,供管理页使用
	ListPage(ctx context.Context, q *model.ListQuery) ([]*model.DataSourceConfig, int64, error)
	// Get 按主键读取单条配置,不存在时返回错误
	Get(ctx context.Context, id int64) (*model.DataSourceConfig, error)
	// TypeCount 统计各实例类型的配置数量,口径为全部未删除配置
	TypeCount(ctx context.Context) (model.TypeCount, error)
	// Create 新增配置并返回配置ID, 操作者写入创建人列
	Create(ctx context.Context, cfg *model.DataSourceConfig, op model.Operator) (int64, error)
	// Update 按主键整体更新配置, 操作者写入修改人列
	Update(ctx context.Context, cfg *model.DataSourceConfig, op model.Operator) error
	// Delete 删除配置
	Delete(ctx context.Context, id int64) error
	// BatchDelete 批量删除配置
	BatchDelete(ctx context.Context, ids []int64) error
	// Close 释放资源
	Close()
}
