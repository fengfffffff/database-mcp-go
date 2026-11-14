// 库表数据源存储:读取 sys_mcp_database_config, 增删改直接写配置表
package repository

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"net/url"
	"regexp"
	"strings"
	"time"

	"github.com/fengfffffff/database-mcp-go/internal/cryptox"
	"github.com/fengfffffff/database-mcp-go/internal/model"
	_ "github.com/go-sql-driver/mysql"
)

// identifierPattern 表名白名单校验,避免拼接出非法表名
var identifierPattern = regexp.MustCompile(`^[A-Za-z0-9_]+$`)

// selectColumns 查询列,与 scanConfig 的扫描顺序严格对应
const selectColumns = `id, alias, database_name, database_type, database_version, host, port, username, password,
	driver_class, connection_properties, pool_minimum_idle, pool_maximum_pool_size, pool_connection_timeout,
	pool_idle_timeout, pool_max_lifetime, pool_validation_timeout, status, remark,
	create_by_name, create_by_type, create_time, update_by_name, update_by_type, update_time`

// mysqlSource 库表配置来源实现
type mysqlSource struct {
	db     *sql.DB
	key    []byte
	table  string
	logger *slog.Logger
}

// NewMySQLSource 创建库表配置来源
func NewMySQLSource(dsn, aesKey, table string, logger *slog.Logger) (Source, error) {

	if !identifierPattern.MatchString(table) {
		return nil, fmt.Errorf("表名不合法: %s", table)
	}
	key, err := cryptox.NewKey(aesKey)
	if err != nil {
		return nil, err
	}
	db, err := sql.Open("mysql", normalizeDSN(dsn))
	if err != nil {
		return nil, fmt.Errorf("连接配置库失败: %w", err)
	}
	db.SetMaxOpenConns(5)
	db.SetMaxIdleConns(2)
	db.SetConnMaxLifetime(30 * time.Minute)
	return &mysqlSource{db: db, key: key, table: table, logger: logger}, nil
}

// Kind 来源类型
func (s *mysqlSource) Kind() string {

	return "mysql"
}

// List 读取启用状态的配置并解密实例密码
func (s *mysqlSource) List(ctx context.Context) ([]*model.DataSourceConfig, error) {

	query := fmt.Sprintf(`SELECT %s FROM %s WHERE status = 1 AND is_deleted = 0 ORDER BY id ASC`, selectColumns, s.table)

	rows, err := s.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("查询实例连接配置失败: %w", err)
	}
	defer rows.Close()

	list := make([]*model.DataSourceConfig, 0, 8)
	for rows.Next() {
		cfg, decErr := s.scanConfig(rows, true)
		if decErr != nil {
			return nil, decErr
		}
		if cfg == nil {
			continue
		}
		list = append(list, cfg)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("遍历实例连接配置失败: %w", err)
	}
	return list, nil
}

// ListPage 按条件分页查询,含停用项,按 ID 倒序
func (s *mysqlSource) ListPage(ctx context.Context, q *model.ListQuery) ([]*model.DataSourceConfig, int64, error) {

	q.Normalize()
	where := "is_deleted = 0"
	args := make([]any, 0, 4)
	if q.Alias != "" {
		where += " AND alias LIKE ?"
		args = append(args, "%"+q.Alias+"%")
	}
	if q.DatabaseName != "" {
		where += " AND database_name LIKE ?"
		args = append(args, "%"+q.DatabaseName+"%")
	}
	if q.DatabaseType != nil {
		where += " AND database_type = ?"
		args = append(args, *q.DatabaseType)
	}
	if q.Status != nil {
		where += " AND status = ?"
		args = append(args, *q.Status)
	}

	var total int64
	countQuery := fmt.Sprintf(`SELECT COUNT(*) FROM %s WHERE %s`, s.table, where)
	if err := s.db.QueryRowContext(ctx, countQuery, args...).Scan(&total); err != nil {
		return nil, 0, fmt.Errorf("统计实例连接配置数量失败: %w", err)
	}
	if total == 0 {
		return []*model.DataSourceConfig{}, 0, nil
	}

	listQuery := fmt.Sprintf(`SELECT %s FROM %s WHERE %s ORDER BY id DESC LIMIT ? OFFSET ?`, selectColumns, s.table, where)
	listArgs := append(append([]any{}, args...), q.PageSize, q.Offset())
	rows, err := s.db.QueryContext(ctx, listQuery, listArgs...)
	if err != nil {
		return nil, 0, fmt.Errorf("分页查询实例连接配置失败: %w", err)
	}
	defer rows.Close()

	list := make([]*model.DataSourceConfig, 0, q.PageSize)
	for rows.Next() {
		cfg, decErr := s.scanConfig(rows, false)
		if decErr != nil {
			return nil, 0, decErr
		}
		if cfg == nil {
			continue
		}
		list = append(list, cfg)
	}
	if err := rows.Err(); err != nil {
		return nil, 0, fmt.Errorf("遍历实例连接配置失败: %w", err)
	}
	return list, total, nil
}

// Get 按主键读取单条配置
func (s *mysqlSource) Get(ctx context.Context, id int64) (*model.DataSourceConfig, error) {

	query := fmt.Sprintf(`SELECT %s FROM %s WHERE id = ? AND is_deleted = 0`, selectColumns, s.table)
	rows, err := s.db.QueryContext(ctx, query, id)
	if err != nil {
		return nil, fmt.Errorf("查询实例连接配置失败: %w", err)
	}
	defer rows.Close()
	if !rows.Next() {
		return nil, fmt.Errorf("实例连接配置不存在 - 配置ID: %d", id)
	}
	cfg, err := s.scanConfig(rows, true)
	if err != nil {
		return nil, err
	}
	if cfg == nil {
		return nil, fmt.Errorf("实例密码解密失败 - 配置ID: %d", id)
	}
	return cfg, nil
}

// TypeCount 统计各实例类型数量,口径为全部未删除配置
func (s *mysqlSource) TypeCount(ctx context.Context) (model.TypeCount, error) {

	query := fmt.Sprintf(`SELECT database_type, COUNT(*) FROM %s WHERE is_deleted = 0 GROUP BY database_type`, s.table)
	rows, err := s.db.QueryContext(ctx, query)
	if err != nil {
		return model.TypeCount{}, fmt.Errorf("统计实例类型数量失败: %w", err)
	}
	defer rows.Close()

	countMap := make(map[int]int64)
	var total int64
	for rows.Next() {
		var databaseType int
		var count int64
		if err := rows.Scan(&databaseType, &count); err != nil {
			return model.TypeCount{}, fmt.Errorf("读取实例类型数量失败: %w", err)
		}
		countMap[databaseType] = count
		total += count
	}
	if err := rows.Err(); err != nil {
		return model.TypeCount{}, fmt.Errorf("遍历实例类型数量失败: %w", err)
	}
	return buildTypeCount(countMap, total), nil
}

// scanConfig 按固定列顺序读取一行配置
// decrypt=false 时只读密文是否存在,供管理页列表使用,避免一条坏密文导致整页查询失败
func (s *mysqlSource) scanConfig(rows *sql.Rows, decrypt bool) (*model.DataSourceConfig, error) {

	cfg := &model.DataSourceConfig{}
	var cipher string
	var databaseVersion, connectionProperties, remark sql.NullString
	var createByName, createByType, updateByName, updateByType sql.NullString
	var updateTime sql.NullTime
	err := rows.Scan(&cfg.ID, &cfg.Alias, &cfg.DatabaseName, &cfg.DatabaseType, &databaseVersion, &cfg.Host, &cfg.Port,
		&cfg.Username, &cipher, &cfg.DriverClass, &connectionProperties, &cfg.PoolMinimumIdle,
		&cfg.PoolMaximumPoolSize, &cfg.PoolConnectionTimeMS, &cfg.PoolIdleTimeoutMS, &cfg.PoolMaxLifetimeMS,
		&cfg.PoolValidationTimeMS, &cfg.Status, &remark,
		&createByName, &createByType, &cfg.CreateTime, &updateByName, &updateByType, &updateTime)
	if err != nil {
		return nil, fmt.Errorf("读取实例连接配置失败: %w", err)
	}
	cfg.DatabaseVersion = databaseVersion.String
	cfg.ConnectionProperties = connectionProperties.String
	cfg.Remark = remark.String
	cfg.CreateByName = createByName.String
	cfg.CreateByType = createByType.String
	cfg.UpdateByName = updateByName.String
	cfg.UpdateByType = updateByType.String
	if updateTime.Valid {
		cfg.UpdateTime = updateTime.Time
	}
	cfg.PasswordSet = cipher != ""
	if !decrypt {
		return cfg, nil
	}
	// 单条解密失败只跳过该条,避免一条坏数据导致整体加载失败
	plain, decErr := cryptox.Decrypt(cipher, s.key)
	if decErr != nil {
		s.logger.Error("实例密码解密失败, 跳过该配置", "alias", cfg.Alias, "err", decErr)
		return nil, nil
	}
	cfg.Password = plain
	return cfg, nil
}

// Create 新增配置
func (s *mysqlSource) Create(ctx context.Context, cfg *model.DataSourceConfig, op model.Operator) (int64, error) {

	// 直连库表时自己保证别名唯一,避免出现同别名多行导致运行期配置互相覆盖
	if dup, err := s.aliasExists(ctx, cfg.Alias, 0); err != nil {
		return 0, err
	} else if dup {
		return 0, fmt.Errorf("实例别名已存在: %s", cfg.Alias)
	}
	cipher, err := cryptox.Encrypt(cfg.Password, s.key)
	if err != nil {
		return 0, fmt.Errorf("实例密码加密失败: %w", err)
	}
	// 连接池参数按配置原值落库, 不写运行期的兜底值: 用户填 0 存的就是 0
	query := fmt.Sprintf(`INSERT INTO %s (alias, database_name, database_type, database_version, host, port, username, password,
		driver_class, connection_properties, pool_minimum_idle, pool_maximum_pool_size, pool_connection_timeout,
		pool_idle_timeout, pool_max_lifetime, pool_validation_timeout, status, remark,
		create_by_id, create_by_name, create_by_type)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`, s.table)
	res, err := s.db.ExecContext(ctx, query, cfg.Alias, cfg.DatabaseName, cfg.DatabaseType, cfg.DatabaseVersion, cfg.Host, cfg.Port,
		cfg.Username, cipher, cfg.DriverClass, cfg.ConnectionProperties, cfg.PoolMinimumIdle, cfg.PoolMaximumPoolSize,
		cfg.PoolConnectionTimeMS, cfg.PoolIdleTimeoutMS, cfg.PoolMaxLifetimeMS,
		cfg.PoolValidationTimeMS, cfg.Status, cfg.Remark, op.ID, op.Name, op.Type)
	if err != nil {
		return 0, fmt.Errorf("新增实例连接配置失败: %w", err)
	}
	id, err := res.LastInsertId()
	if err != nil {
		return 0, fmt.Errorf("获取新增配置ID失败: %w", err)
	}
	return id, nil
}

// Update 整条更新配置
func (s *mysqlSource) Update(ctx context.Context, cfg *model.DataSourceConfig, op model.Operator) error {

	if dup, err := s.aliasExists(ctx, cfg.Alias, cfg.ID); err != nil {
		return err
	} else if dup {
		return fmt.Errorf("实例别名已存在: %s", cfg.Alias)
	}
	cipher, err := cryptox.Encrypt(cfg.Password, s.key)
	if err != nil {
		return fmt.Errorf("实例密码加密失败: %w", err)
	}
	// 同新增: 连接池参数按配置原值落库, 不把运行期兜底值写回表
	query := fmt.Sprintf(`UPDATE %s SET alias = ?, database_name = ?, database_type = ?, database_version = ?, host = ?,
		port = ?, username = ?, password = ?, driver_class = ?, connection_properties = ?, pool_minimum_idle = ?,
		pool_maximum_pool_size = ?, pool_connection_timeout = ?, pool_idle_timeout = ?, pool_max_lifetime = ?,
		pool_validation_timeout = ?, status = ?, remark = ?,
		update_by_id = ?, update_by_name = ?, update_by_type = ?, update_time = NOW() WHERE id = ? AND is_deleted = 0`, s.table)
	res, err := s.db.ExecContext(ctx, query, cfg.Alias, cfg.DatabaseName, cfg.DatabaseType, cfg.DatabaseVersion, cfg.Host, cfg.Port,
		cfg.Username, cipher, cfg.DriverClass, cfg.ConnectionProperties, cfg.PoolMinimumIdle, cfg.PoolMaximumPoolSize,
		cfg.PoolConnectionTimeMS, cfg.PoolIdleTimeoutMS, cfg.PoolMaxLifetimeMS,
		cfg.PoolValidationTimeMS, cfg.Status, cfg.Remark, op.ID, op.Name, op.Type, cfg.ID)
	if err != nil {
		return fmt.Errorf("修改实例连接配置失败: %w", err)
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("修改实例连接配置失败: %w", err)
	}
	if affected == 0 {
		return fmt.Errorf("实例连接配置不存在或已删除 - 配置ID: %d", cfg.ID)
	}
	return nil
}

// Delete 软删除配置
func (s *mysqlSource) Delete(ctx context.Context, id int64) error {

	query := fmt.Sprintf(`UPDATE %s SET is_deleted = 1, update_time = NOW() WHERE id = ? AND is_deleted = 0`, s.table)
	res, err := s.db.ExecContext(ctx, query, id)
	if err != nil {
		return fmt.Errorf("删除实例连接配置失败: %w", err)
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("删除实例连接配置失败: %w", err)
	}
	if affected == 0 {
		return fmt.Errorf("实例连接配置不存在或已删除 - 配置ID: %d", id)
	}
	return nil
}

// BatchDelete 批量软删除配置
func (s *mysqlSource) BatchDelete(ctx context.Context, ids []int64) error {

	if len(ids) == 0 {
		return fmt.Errorf("请选择要删除的配置")
	}
	placeholders := strings.TrimSuffix(strings.Repeat("?,", len(ids)), ",")
	args := make([]any, 0, len(ids))
	for _, id := range ids {
		args = append(args, id)
	}
	query := fmt.Sprintf(`UPDATE %s SET is_deleted = 1, update_time = NOW() WHERE id IN (%s) AND is_deleted = 0`, s.table, placeholders)
	res, err := s.db.ExecContext(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("批量删除实例连接配置失败: %w", err)
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("批量删除实例连接配置失败: %w", err)
	}
	if affected == 0 {
		return fmt.Errorf("所选实例连接配置不存在或已删除")
	}
	return nil
}

// aliasExists 判断别名是否已被其它未删除配置占用,excludeID 用于修改时排除自身
func (s *mysqlSource) aliasExists(ctx context.Context, alias string, excludeID int64) (bool, error) {

	query := fmt.Sprintf(`SELECT COUNT(*) FROM %s WHERE alias = ? AND is_deleted = 0 AND id <> ?`, s.table)
	var count int64
	if err := s.db.QueryRowContext(ctx, query, alias, excludeID).Scan(&count); err != nil {
		return false, fmt.Errorf("校验实例别名失败: %w", err)
	}
	return count > 0, nil
}

// Close 关闭配置库连接
func (s *mysqlSource) Close() {

	_ = s.db.Close()
}

// normalizeDSN 规范化连接串参数
// go-sql-driver 要求参数值做 URL 编码, 直接写 loc=Asia/Shanghai 会因斜杠被判为非法, 这里统一转义
// 审计时间列按 time.Time 扫描, 依赖 parseTime=true, 这里强制置为 true, 免得换一份 DSN 就读不出配置表
func normalizeDSN(dsn string) string {

	base, rawQuery := dsn, ""
	if idx := strings.LastIndex(dsn, "?"); idx >= 0 {
		candidate := dsn[idx+1:]
		// 密码里可能自带问号,若问号之后不像参数串就整串当连接串,避免把密码截断
		if !strings.Contains(candidate, "@") && strings.Contains(candidate, "=") {
			base, rawQuery = dsn[:idx], candidate
		}
	}
	values, err := url.ParseQuery(rawQuery)
	if err != nil {
		return dsn
	}
	values.Set("parseTime", "true")
	return base + "?" + values.Encode()
}
