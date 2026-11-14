// 配置补丁:新增与修改共用的入参形状,指针用于区分未传与传空
package model

// ConfigPatch 实例连接配置补丁
type ConfigPatch struct {
	Alias                *string `json:"alias"`
	DatabaseName         *string `json:"databaseName"`
	DatabaseType         *int    `json:"databaseType"`
	DatabaseVersion      *string `json:"databaseVersion"`
	Host                 *string `json:"host"`
	Port                 *int    `json:"port"`
	Username             *string `json:"username"`
	Password             *string `json:"password"`
	DriverClass          *string `json:"driverClass"`
	ConnectionProperties *string `json:"connectionProperties"`
	PoolMinimumIdle      *int    `json:"poolMinimumIdle"`
	PoolMaximumPoolSize  *int    `json:"poolMaximumPoolSize"`
	PoolConnectionTimeMS *int64  `json:"poolConnectionTimeout"`
	PoolIdleTimeoutMS    *int64  `json:"poolIdleTimeout"`
	PoolMaxLifetimeMS    *int64  `json:"poolMaxLifetime"`
	PoolValidationTimeMS *int64  `json:"poolValidationTimeout"`
	Status               *int    `json:"status"`
	Remark               *string `json:"remark"`
}
