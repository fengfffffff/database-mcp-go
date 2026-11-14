// JSON 文件数据源存储:数据源清单存在独立 JSON 文件里, 增删改直接回写该文件
// source.type=file 时走这条路径, 与走配置表的 mysql 模式二选一
package repository

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"

	"github.com/fengfffffff/database-mcp-go/internal/model"
)

// fileDocument 数据源清单文件结构
type fileDocument struct {
	DataSources []*model.DataSourceConfig `json:"datasources"`
}

// fileSource JSON 文件存储实现
type fileSource struct {
	path string
	mu   sync.Mutex
}

// NewFileSource 创建 JSON 文件存储, 文件不存在时首次读取按空清单处理
func NewFileSource(path string) (Source, error) {

	if path == "" {
		return nil, fmt.Errorf("source.file.path 不能为空")
	}
	return &fileSource{path: path}, nil
}

// Kind 存储方式标识
func (s *fileSource) Kind() string {

	return "file"
}

// List 读取并返回启用状态的配置
func (s *fileSource) List(ctx context.Context) ([]*model.DataSourceConfig, error) {

	s.mu.Lock()
	defer s.mu.Unlock()
	all, err := s.read()
	if err != nil {
		return nil, err
	}
	enabled := make([]*model.DataSourceConfig, 0, len(all))
	for _, item := range all {
		if item.Status != 1 {
			continue
		}
		enabled = append(enabled, item)
	}
	return enabled, nil
}

// ListPage 按条件分页查询, 内存过滤, 按 ID 倒序
func (s *fileSource) ListPage(ctx context.Context, q *model.ListQuery) ([]*model.DataSourceConfig, int64, error) {

	s.mu.Lock()
	defer s.mu.Unlock()
	q.Normalize()
	all, err := s.read()
	if err != nil {
		return nil, 0, err
	}

	matched := make([]*model.DataSourceConfig, 0, len(all))
	for _, item := range all {
		if q.Alias != "" && !strings.Contains(item.Alias, q.Alias) {
			continue
		}
		if q.DatabaseName != "" && !strings.Contains(item.DatabaseName, q.DatabaseName) {
			continue
		}
		if q.DatabaseType != nil && item.DatabaseType != *q.DatabaseType {
			continue
		}
		if q.Status != nil && item.Status != *q.Status {
			continue
		}
		matched = append(matched, item)
	}

	sort.Slice(matched, func(i, j int) bool { return matched[i].ID > matched[j].ID })
	total := int64(len(matched))
	start := q.Offset()
	if start >= len(matched) {
		return []*model.DataSourceConfig{}, total, nil
	}
	end := start + q.PageSize
	if end > len(matched) {
		end = len(matched)
	}
	return matched[start:end], total, nil
}

// Get 按主键读取单条配置
func (s *fileSource) Get(ctx context.Context, id int64) (*model.DataSourceConfig, error) {

	s.mu.Lock()
	defer s.mu.Unlock()
	all, err := s.read()
	if err != nil {
		return nil, err
	}
	for _, item := range all {
		if item.ID == id {
			return item, nil
		}
	}
	return nil, fmt.Errorf("实例连接配置不存在 - 配置ID: %d", id)
}

// TypeCount 统计各实例类型数量
func (s *fileSource) TypeCount(ctx context.Context) (model.TypeCount, error) {

	s.mu.Lock()
	defer s.mu.Unlock()
	all, err := s.read()
	if err != nil {
		return model.TypeCount{}, err
	}
	countMap := make(map[int]int64)
	for _, item := range all {
		countMap[item.DatabaseType]++
	}
	return buildTypeCount(countMap, int64(len(all))), nil
}

// Create 新增配置并回写文件, 文件存储没有审计列, 操作者不落盘
func (s *fileSource) Create(ctx context.Context, cfg *model.DataSourceConfig, _ model.Operator) (int64, error) {

	s.mu.Lock()
	defer s.mu.Unlock()
	all, err := s.read()
	if err != nil {
		return 0, err
	}
	for _, exist := range all {
		if exist.Alias == cfg.Alias {
			return 0, fmt.Errorf("实例别名已存在: %s", cfg.Alias)
		}
	}
	var maxID int64
	for _, exist := range all {
		if exist.ID > maxID {
			maxID = exist.ID
		}
	}
	cfg.ID = maxID + 1
	all = append(all, cfg)
	if err := s.write(all); err != nil {
		return 0, err
	}
	return cfg.ID, nil
}

// Update 整条替换指定配置并回写文件, 文件存储没有审计列, 操作者不落盘
func (s *fileSource) Update(ctx context.Context, cfg *model.DataSourceConfig, _ model.Operator) error {

	s.mu.Lock()
	defer s.mu.Unlock()
	all, err := s.read()
	if err != nil {
		return err
	}
	for i, exist := range all {
		if exist.ID != cfg.ID {
			continue
		}
		for j, other := range all {
			if j != i && other.Alias == cfg.Alias {
				return fmt.Errorf("实例别名已存在: %s", cfg.Alias)
			}
		}
		all[i] = cfg
		return s.write(all)
	}
	return fmt.Errorf("实例连接配置不存在 - 配置ID: %d", cfg.ID)
}

// Delete 删除指定配置并回写文件
func (s *fileSource) Delete(ctx context.Context, id int64) error {

	return s.BatchDelete(ctx, []int64{id})
}

// BatchDelete 批量删除指定配置并回写文件
func (s *fileSource) BatchDelete(ctx context.Context, ids []int64) error {

	s.mu.Lock()
	defer s.mu.Unlock()
	if len(ids) == 0 {
		return fmt.Errorf("请选择要删除的配置")
	}
	all, err := s.read()
	if err != nil {
		return err
	}
	target := make(map[int64]struct{}, len(ids))
	for _, id := range ids {
		target[id] = struct{}{}
	}
	kept := make([]*model.DataSourceConfig, 0, len(all))
	removed := 0
	for _, item := range all {
		if _, ok := target[item.ID]; ok {
			removed++
			continue
		}
		kept = append(kept, item)
	}
	if removed == 0 {
		return fmt.Errorf("实例连接配置不存在 - 配置ID: %v", ids)
	}
	return s.write(kept)
}

// Close 文件存储无资源需要释放
func (s *fileSource) Close() {}

// read 读取数据源清单, 文件不存在时返回空清单
func (s *fileSource) read() ([]*model.DataSourceConfig, error) {

	raw, err := os.ReadFile(s.path)
	if err != nil {
		if os.IsNotExist(err) {
			return []*model.DataSourceConfig{}, nil
		}
		return nil, fmt.Errorf("读取数据源文件失败: %w", err)
	}
	if len(strings.TrimSpace(string(raw))) == 0 {
		return []*model.DataSourceConfig{}, nil
	}
	doc := &fileDocument{}
	if err := json.Unmarshal(raw, doc); err != nil {
		return nil, fmt.Errorf("解析数据源文件失败: %w", err)
	}
	if doc.DataSources == nil {
		doc.DataSources = []*model.DataSourceConfig{}
	}
	var maxID int64
	for _, item := range doc.DataSources {
		if item.ID > maxID {
			maxID = item.ID
		}
	}
	// 手写配置通常不带ID, 按顺序补齐, 保证管理页可编辑与删除
	for _, item := range doc.DataSources {
		if item.ID == 0 {
			maxID++
			item.ID = maxID
		}
		item.PasswordSet = item.Password != ""
	}
	return doc.DataSources, nil
}

// write 原子回写数据源清单
func (s *fileSource) write(all []*model.DataSourceConfig) error {

	doc := &fileDocument{DataSources: all}
	raw, err := json.MarshalIndent(doc, "", "  ")
	if err != nil {
		return fmt.Errorf("序列化数据源文件失败: %w", err)
	}
	raw = append(raw, '\n')
	dir := filepath.Dir(s.path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("创建数据源文件目录失败: %w", err)
	}
	tmp, err := os.CreateTemp(dir, ".datasources-*.json")
	if err != nil {
		return fmt.Errorf("创建临时文件失败: %w", err)
	}
	tmpName := tmp.Name()
	if _, err := tmp.Write(raw); err != nil {
		tmp.Close()
		os.Remove(tmpName)
		return fmt.Errorf("写入临时文件失败: %w", err)
	}
	if err := tmp.Close(); err != nil {
		os.Remove(tmpName)
		return fmt.Errorf("关闭临时文件失败: %w", err)
	}
	if err := os.Rename(tmpName, s.path); err != nil {
		os.Remove(tmpName)
		return fmt.Errorf("替换数据源文件失败: %w", err)
	}
	return nil
}
