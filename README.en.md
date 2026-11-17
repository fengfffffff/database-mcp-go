# database-mcp-go

[中文](README.md) | English

> Expose MySQL, MariaDB, TiDB, OceanBase, KingbaseES, PostgreSQL, SQL Server, SQLite, MongoDB and Redis to AI clients as MCP tools. Connection credentials stay server-side; clients call with an instance alias plus a database name. Datasources live in a file or a database table. A single static binary ships with a built-in admin page.

---

## 1. Features

| Capability | Description |
|------|------|
| Ten databases | One interface covers ten database types, each with its own connection pool |
| Six MCP tools | Execute statements, CRUD datasources, refresh config |
| Centralized credentials | Clients only get aliases and database names |
| Transaction semantics | For SQL databases, multiple statements run in one transaction and roll back as a whole on failure |
| Dual storage | `file` is zero-dependency; `mysql` stores passwords AES-encrypted |
| Hot config reload | Cache invalidated after writes; the next access re-reads the source |
| Built-in admin page | `/admin` with search, pagination, batch delete, status switch, column settings, and a Chinese/English UI (defaults to Chinese) |
| Structured errors | Error category, error code, SQLSTATE and fix suggestion |
| Single-file delivery | Pure Go without cgo; copy the binary and config and it runs |
| Logs | Rotated daily, expired files cleaned automatically |

### 1.1 Supported databases

| databaseType | Database | Driver | Connection notes |
|------|------|------|------|
| 0 | MySQL | go-sql-driver/mysql | Host, port, account, database name |
| 1 | KingbaseES V8 | pgx, PostgreSQL protocol | Same as above |
| 2 | PostgreSQL | pgx | Same as above, default sslmode=prefer |
| 4 | SQL Server | microsoft/go-mssqldb | Named instances use the actual listening port, encrypt defaults to disable |
| 5 | MariaDB | Reuses the MySQL driver | Same as MySQL |
| 6 | TiDB | Reuses the MySQL driver | Same as MySQL |
| 7 | OceanBase MySQL mode | Reuses the MySQL driver | Same as MySQL |
| 8 | SQLite | modernc.org/sqlite | Host is the absolute file path, database name should be main |
| 9 | MongoDB | mongo-driver v2 | Host, port, database name, account optional |
| 10 | Redis | go-redis v9 | Host, port, database name is the logical DB index |

`driverClass` is display-only; the actual driver is chosen by `databaseType`. One instance can carry multiple databases, each with its own pool; `poolMaximumPoolSize` is the per-database cap.

## 2. Quick start

### 2.1 Build

Go 1.27 or newer:

```bash
go build -o database-mcp-go ./cmd/database-mcp-go

# Static single file
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -ldflags "-s -w" -o database-mcp-go ./cmd/database-mcp-go

# Inject a version string
CGO_ENABLED=0 go build -ldflags "-s -w -X main.version=v1.0.0" -o database-mcp-go ./cmd/database-mcp-go
```

There are only two command-line flags: `-config` for the config file, default config.yaml; `-version` prints the version. On Windows name the artifact `database-mcp-go.exe`.

### 2.2 Minimal config

```yaml
server:
  port: 47319

mcp:
  auth_token: "change-me"

admin:
  username: "admin"
  password: "${ADMIN_PASSWORD}"

source:
  type: file
  file:
    path: datasources.json
```

`admin.username` and `admin.password` are required; startup fails immediately when either is empty. The example injects the password through an environment variable, so set `ADMIN_PASSWORD` before starting.

### 2.3 Prepare datasources

In `file` mode edit `datasources.json`; the remaining fields are described in chapter 3, the `file` mode section:

```json
{
  "datasources": [
    {
      "id": 1,
      "alias": "local-db",
      "host": "127.0.0.1",
      "port": 3306,
      "username": "root",
      "password": "your-password",
      "databaseName": "app",
      "databaseType": 0,
      "driverClass": "com.mysql.cj.jdbc.Driver"
    }
  ]
}
```

A missing file is treated as an empty list and created automatically on the first insert.

### 2.4 Start and verify

```bash
./database-mcp-go -config config.yaml
curl http://127.0.0.1:47319/healthz
```

Fill in `admin.username` and `admin.password` in `config.yaml` before starting, or inject the password from an environment variable with `${ADMIN_PASSWORD}`; with either one missing the process exits immediately. The admin page is always on at `http://127.0.0.1:47319/admin`, supporting type stats, four-condition filtering, pagination, batch delete, status switches, detail, column settings and reload; the UI switches between Chinese and English in the top-right corner and defaults to Chinese.

### 2.5 Connect an MCP client

ZCode as an example; other clients with MCP Streamable HTTP work the same way:

```json
{
  "mcpServers": {
    "database-mcp-go": {
      "type": "http",
      "url": "http://127.0.0.1:47319/mcp",
      "headers": {
        "Authorization": "Bearer change-me"
      }
    }
  }
}
```

### 2.6 HTTP endpoints

| Endpoint | Description |
|------|------|
| `/mcp` | MCP endpoint, path configurable, requires Bearer Token when a token is configured |
| `/admin` | Built-in admin page, always on, requires basic auth |
| `/healthz` | Health check |

Config refresh is only needed when the config was changed outside this service; see chapter 3 (3).

## 3. Configuration guide

All configuration items of `config.yaml`. Items not listed take their defaults, and values support `${ENV_NAME}` environment variable injection. Everything in this chapter is startup configuration; restart after changes.

### 3.1 Server and MCP

| Item | Default | Description |
|------|------|------|
| `server.port` | 47319 | Service port |
| `mcp.endpoint` | `/mcp` | MCP endpoint path, must start with / and cannot be the root path; trailing slashes are trimmed |
| `mcp.auth_token` | empty | Bearer Token; empty means no check, loopback-only use |
| `mcp.session_timeout_seconds` | 600 | Idle session timeout in seconds |
| `mcp.max_request_body_bytes` | 0 | Request body size limit in bytes, 0 uses the SDK default |
| `mcp.disable_localhost_protection` | false | Set true when accessed through a tunnel or reverse proxy |
| `mcp.stateless` | false | When true, no sessions are kept and every request stands alone |

### 3.2 Execution and limits

| Item | Default | Description |
|------|------|------|
| `database.query_timeout_seconds` | 30 | Per-statement timeout in seconds; timing out aborts the whole batch; 0 or negative is treated as 30 |
| `database.slow_query_threshold_ms` | 1000 | Slow-operation alert threshold in milliseconds |
| `database.max_rows` | 1000 | Cumulative row cap for a single execution; rows beyond it are truncated |

### 3.3 Datasource storage

| Item | Default | Description |
|------|------|------|
| `source.type` | file | `file` or `mysql`, pick one |
| `source.file.path` | datasources.json | Datasource list file, effective in file mode |
| `source.mysql.dsn` | none | DSN of the config database, required in mysql mode |
| `source.mysql.aes_key` | none | AES-256-GCM key, Base64 of 32 bytes |
| `source.mysql.table` | sys_mcp_database_config | Config table name, letters digits and underscores only |

Configs are cached in-process: writes only invalidate the cache and the next access re-reads the source. Pools are built lazily per "alias::database" with a config fingerprint recorded, so they are rebuilt only when connection parameters change, and recycled on the next source read when an alias or database disappears. When the config was changed outside this service, use the reload button on the admin page or the MCP `refresh-config` tool.

### 3.4 Logs and admin page

| Item | Default | Description |
|------|------|------|
| `log.dir` | logs | Log directory |
| `log.level` | info | Log level |
| `log.retain_days` | 14 | Retention days; expired files are cleaned automatically |
| `log.console` | false | Also log to the console |
| `log.sql` | true | Log executed statements, truncated to 500 chars |
| `admin.username` | no default | Basic auth account, required, must not contain a colon |
| `admin.password` | no default | Basic auth password, required |

The old `admin.enabled` has been removed and is ignored even if present; the admin page is always on and access is controlled solely by the account and password.

### 3.5 Connection properties

`connectionProperties` follows JDBC syntax, `key=value&key=value`. Unrecognized keys are ignored.

| Database | Common properties | Description |
|------|------|------|
| MySQL family | `useSSL`, `connectTimeout`, `socketTimeout`, `sessionVariables` | useSSL maps to tls, timeouts in milliseconds |
| PostgreSQL and KingbaseES | `sslmode`, `currentSchema`, `connectTimeout` | currentSchema maps to search_path |
| SQL Server | `encrypt`, `TrustServerCertificate`, `databaseName`, `connectTimeout` | connectTimeout in milliseconds |
| SQLite | Not effective | Behavior is decided by built-in driver parameters |
| MongoDB | `authSource`, `replicaSet`, `tls`, `connectTimeoutMS` | connectTimeout in milliseconds |
| Redis | `readtimeout` | Milliseconds, default 30000, for blocking commands |

When the same timeout is set both in connection properties and in pool parameters, connection properties win.

### 3.6 file mode

Datasources live in a standalone JSON file and writes go straight back to that file. Passwords are plaintext; suitable for local and intranet trials.

| Field | Required | Description |
|------|------|------|
| `alias` | Yes | Instance alias, unique |
| `databaseType` | Yes | Type code, see the support matrix |
| `host` | Yes | Instance host, SQLite uses the absolute file path |
| `port` | Yes | Instance port, SQLite uses 0 |
| `databaseName` | Yes | Database names separated by commas; SQLite should use main, decided by the file path |
| `username` | SQL types yes | MongoDB and Redis may be empty, SQLite not needed |
| `password` | No | For MongoDB, a password requires a username; Redis may set only a password |
| `driverClass` | SQL types yes | Display only; empty for SQLite, MongoDB and Redis |
| `connectionProperties` | No | JDBC syntax, see the connection properties section |
| `poolMinimumIdle` and four more | No | Defaults 2, 10, 5000, 600000, 1800000 when unset; 0 keeps 0, a connect timeout of 0 means no cap |
| `poolValidationTimeout` | No | Kept for compatibility, not used by current drivers |
| `status` | No | 0 disabled, 1 enabled, defaults to 1 |
| `id` | No | Must be unique for admin page maintenance; recommended when editing by hand |

### 3.7 mysql mode

Datasources live in a MySQL config table with passwords stored as AES-256-GCM ciphertext.

```yaml
source:
  type: mysql
  mysql:
    dsn: "user:password@tcp(127.0.0.1:3306)/database_mcp?parseTime=true&loc=Asia/Shanghai"
    aes_key: "generate with openssl rand -base64 32 and paste here"
    table: sys_mcp_database_config
```

Generate a key:

```bash
openssl rand -base64 32
```

Create the table before first use, MySQL 8.0.13 or newer:

```sql
CREATE TABLE `sys_mcp_database_config` (
  `id`                      BIGINT UNSIGNED  NOT NULL AUTO_INCREMENT COMMENT 'primary key',
  `alias`                   VARCHAR(128)     NOT NULL DEFAULT '' COMMENT 'instance alias',
  `database_name`           VARCHAR(512)     NOT NULL DEFAULT '' COMMENT 'database names separated by commas',
  `database_type`           TINYINT UNSIGNED NOT NULL DEFAULT 0 COMMENT 'instance type, see the support matrix',
  `database_version`        VARCHAR(32)      NOT NULL DEFAULT '' COMMENT 'instance version, display only',
  `host`                    VARCHAR(128)     NOT NULL DEFAULT '' COMMENT 'instance host',
  `port`                    INT UNSIGNED     NOT NULL DEFAULT 3306 COMMENT 'instance port',
  `username`                VARCHAR(128)     NOT NULL DEFAULT '' COMMENT 'instance username',
  `password`                VARCHAR(256)     NOT NULL DEFAULT '' COMMENT 'instance password, AES-256-GCM ciphertext',
  `driver_class`            VARCHAR(256)     NOT NULL DEFAULT '' COMMENT 'driver class',
  `connection_properties`   VARCHAR(512)     NOT NULL DEFAULT '' COMMENT 'connection properties, JDBC syntax',
  `pool_minimum_idle`       INT UNSIGNED     NOT NULL DEFAULT 2 COMMENT 'minimum idle connections',
  `pool_maximum_pool_size`  INT UNSIGNED     NOT NULL DEFAULT 10 COMMENT 'maximum pool size',
  `pool_connection_timeout` BIGINT UNSIGNED  NOT NULL DEFAULT 5000 COMMENT 'connect timeout in milliseconds',
  `pool_idle_timeout`       BIGINT UNSIGNED  NOT NULL DEFAULT 600000 COMMENT 'idle timeout in milliseconds',
  `pool_max_lifetime`       BIGINT UNSIGNED  NOT NULL DEFAULT 1800000 COMMENT 'max connection lifetime in milliseconds',
  `pool_validation_timeout` BIGINT UNSIGNED  NOT NULL DEFAULT 3000 COMMENT 'validation timeout in milliseconds',
  `status`                  TINYINT UNSIGNED NOT NULL DEFAULT 1 COMMENT 'status 0-disabled 1-enabled',
  `create_by_id`            BIGINT UNSIGNED  NOT NULL DEFAULT 0 COMMENT 'creator ID',
  `create_by_name`          VARCHAR(128)     NOT NULL DEFAULT '' COMMENT 'creator name',
  `create_by_type`          VARCHAR(64)      NOT NULL DEFAULT '' COMMENT 'creator type',
  `create_time`             DATETIME         NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'creation time',
  `update_by_id`            BIGINT UNSIGNED  NOT NULL DEFAULT 0 COMMENT 'updater ID',
  `update_by_name`          VARCHAR(128)     NOT NULL DEFAULT '' COMMENT 'updater name',
  `update_by_type`          VARCHAR(64)      NOT NULL DEFAULT '' COMMENT 'updater type',
  `update_time`             DATETIME         DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP COMMENT 'update time',
  `is_deleted`              TINYINT UNSIGNED NOT NULL DEFAULT 0 COMMENT 'deleted 0-no 1-yes',
  `remark`                  VARCHAR(500)     NOT NULL DEFAULT '' COMMENT 'remark',
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_alias` ((CASE WHEN `is_deleted` = 0 THEN `alias` END))
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_bin COMMENT = 'MCP database proxy config table';
```

## 4. Tools and responses

### 4.1 Tool list

| Tool | Purpose |
|------|------|
| `execute-sql` | Execute statements; SQL statements separated by semicolons; MongoDB takes runCommand-style JSON; Redis takes one command per line |
| `get-datasources` | List enabled instances and their databases, with fuzzy alias filtering |
| `add-database-config` | Add an instance config; the config cache is invalidated on success |
| `update-database-config` | Update an instance config; fields not passed keep their values |
| `delete-database-config` | Delete an instance config |
| `refresh-config` | Re-read the config source immediately |

`get-datasources` only lists enabled instances; disabled ones can be neither queried nor executed; no tool ever returns instance passwords.

### 4.2 Three input formats

SQL databases:

```json
{"alias": "prod-db", "databaseName": "order", "sql": "SELECT id, status FROM t_order ORDER BY id DESC LIMIT 20"}
```

MongoDB takes runCommand-style JSON:

```json
{"alias": "doc-db", "databaseName": "app", "sql": "{\"find\": \"users\", \"filter\": {\"age\": {\"$gt\": 18}}, \"limit\": 5}"}
```

Redis takes one command per line:

```json
{"alias": "cache", "databaseName": "0", "sql": "SET greeting \"hello world\"\nGET greeting"}
```

### 4.3 Response structure

Single statement, the `message` field is Chinese in actual responses:

```json
{"code": 200, "message": "操作成功", "data": {"rows": [{"id": 1}], "rowCount": 1, "affectedRows": 0, "executionTime": 8}}
```

With multiple statements `rows` is empty and per-statement results go into `results`:

```json
{"code": 200, "data": {"rows": [], "rowCount": 2, "affectedRows": 1, "statementCount": 2, "results": [{"statementIndex": 1, "rows": [{"cnt": 3}], "rowCount": 1, "affectedRows": 0}, {"statementIndex": 2, "rows": [], "rowCount": 0, "affectedRows": 1}]}}
```

| Field | Description |
|------|------|
| `rows` | Rows of a single statement, column order matches the query; duplicate column names get `_2`, `_3` suffixes |
| `results` | Per-statement results for multiple statements, with `statementIndex` |
| `rowCount` | Rows returned this time, capped by `database.max_rows` |
| `affectedRows` | Affected rows for writes; `WITH` writes, writes with `RETURNING` or `OUTPUT`, and MongoDB commands count as 0; Redis counts successful commands |
| `truncated` | Whether output was truncated by the cap |
| `executionTime` | Milliseconds |

Value formatting: NULL prints as null; binary that is valid UTF-8 prints as text, otherwise Base64; times are normalized to `2006-01-02 15:04:05`; `NaN` and `Infinity` print as text; JSON and JSONB print as JSON text.

### 4.4 Error format

```text
SQL 执行失败 - 错误类别: 表不存在, 错误码: 1146, SQLSTATE: 42S02, 原始信息: Table 'db.t' doesn't exist, 建议: 核对库名与表名
第 2/3 条语句执行失败 - ... 语句: UPDATE t SET a = 1
```

Error message text is Chinese by design so AI clients can act on it directly. KingbaseES and PostgreSQL additionally carry server detail, hint and the failing row and column; SQL Server carries the line number; SQLite carries a constraint breakdown; Redis errors are classified by prefix; timeout messages include the statement text.

## 5. Per-database behavior

### 5.1 SQL databases

1. Statements are split on semicolons and executed one by one inside a single transaction, committed only when all succeed; each statement runs through its own query or exec channel, so each gets its own affected-rows count.
2. Do not include BEGIN or COMMIT in statements; the service controls the transaction.
3. Do not mix DDL and DML in one batch; run ALTER TABLE and the like as separate calls. MySQL DDL commits implicitly, so a mixed batch cannot roll back as a whole.
4. For SQL Server, writes with `OUTPUT` and `EXEC` stored procedures are handled as queries with affected rows 0; writes starting with `WITH` are also handled as queries with 0; `GO` is a client-side batch separator the protocol layer does not know, use semicolons for multiple statements.
5. SQLite uses the absolute file path as host, database name should be main, and port, username, password and driver class stay empty; connections enable WAL and busy waiting automatically with a pool cap of 4; writes with `RETURNING` return row data.

### 5.2 MongoDB

1. Each command goes to RunCommand verbatim and the server decides validity; the command name must be the first field of the document.
2. Commands run in order with no transaction; already-run commands are not rolled back on failure.
3. Replies are flattened by top-level fields into one row; nested documents and arrays print as JSON text; `rowCount` counts reply documents, not documents inside a cursor.
4. `find` returns only `cursor.firstBatch`; fetch more with `getMore` and the cursor id.
5. Partial failures of write commands sit in the reply's `writeErrors` while the top level still says `ok: 1`; check that field.
6. Commands are parsed as extended JSON into ordered documents; the key order of `sort` and `$sort` is the sort priority; `$oid`, `$date`, `$numberLong`, `$numberInt` and `$numberDouble` are supported.
7. A 24-char hex string on `_id` is treated as an ObjectId; to match it as a string write `{"_id": {"$eq": "value"}}`.
8. Commands that must run on the admin database, such as `renameCollection`, need a datasource with databaseName admin registered under another alias.
9. Username and password may both be empty; once a password is set, a username is required.

### 5.3 Redis

1. One command per line, space-separated arguments, quoted when they contain spaces, lines starting with # are comments; commands run in order with no transaction.
2. Database name is the logical DB index 0 to 65535; multiple indexes can be configured.
3. Reply shapes: a scalar prints as one row and one `result` column, arrays print one element per row, maps print as JSON text, non-UTF-8 binary prints as Base64.
4. `affectedRows` is the number of successfully executed commands.
5. Commands that would monopolize a connection or must stay on one connection are rejected: `SUBSCRIBE`, `PSUBSCRIBE`, `MONITOR`, `SYNC`, `PSYNC` take over the connection; `MULTI`, `EXEC`, `DISCARD`, `WATCH`, `UNWATCH` require the same connection, use a Lua script for transactions.
6. `SELECT` changes the logical DB of pooled connections, switch databases by editing the datasource instead; `HELLO` switches the protocol version; `CLIENT REPLY`, `CLIENT KILL`, `CLIENT PAUSE`, `CLIENT TRACKING` drop replies or affect other clients. All of these are blocked, while read-only diagnostics like `CLIENT LIST` and `CLIENT INFO` keep working.
7. Blocking commands are bounded by `readtimeout`, 30000 milliseconds by default.
8. Password-only without username fits standalone Redis; fill both when ACL users are needed.

## 6. Security

1. An empty `auth_token` means no check and loopback-only use; any service leaving localhost must set a token.
2. The admin page is always on with a single basic-auth pair and no permission system; the account and password are mandatory and startup fails when either is missing, so there is no unauthenticated path. Basic auth travels over plain HTTP; wrap it in HTTPS or an SSH tunnel across machines.
3. Passwords are plaintext in `file` mode, watch file permissions; choose `mysql` mode for encrypted storage.
4. `log.sql` writes statements into logs; set it to false for sensitive environments.
5. The proxy does no business-level filtering and only blocks protocol-level commands that would break the connection; what can be executed is decided by the database account, so configure read-only or restricted accounts per datasource.

## 7. Known limitations

1. The cumulative row cap defaults to 1000, beyond which rows are truncated and `truncated` is set; truncation only limits the response while the database still runs the full query, so add LIMIT or TOP yourself on large tables.
2. DDL with block bodies gets split wrongly on semicolons: SQL Server stored procedures and `BEGIN ... END`, MySQL procedures defined with `DELIMITER`. Create such objects in another client.
3. Extreme custom types of KingbaseES and PostgreSQL may degrade to text output.
4. Binary prints as Base64; bytes that are valid UTF-8 display as text; trim large columns in the query.
5. SQLite is a single-writer model; concurrent writes wait on each other.
6. SQL Server named instances should fill the listening port directly; SQL Browser discovery is not supported.
7. MongoDB and Redis see chapter 5.
8. The admin page only maintains datasources; Excel import and export are not included.

## 8. Development

### 8.1 Directory layout

```text
cmd/database-mcp-go           entry point, wiring only
internal/config               config loading and env var injection
internal/cryptox              AES-256-GCM encryption for instance passwords
internal/model                config, results and the unified response body
internal/driver               driver contract, registry, value normalization
internal/driver/mysql         MySQL protocol family, covering MySQL and MariaDB, TiDB, OceanBase
internal/driver/kingbase      KingbaseES
internal/driver/postgresql    PostgreSQL
internal/driver/sqlserver     SQL Server
internal/driver/sqlite        SQLite
internal/driver/mongodb       MongoDB
internal/driver/redis         Redis
internal/driver/sqlsplit      statement splitting for MySQL, PostgreSQL, T-SQL and SQLite dialects
internal/pool                 lazy connection pools keyed by alias and database
internal/repository           datasource storage with file and mysql implementations
internal/service              orchestration: execution, CRUD, config cache invalidation and re-read
internal/tool                 MCP tool definitions
internal/transport            HTTP routing, MCP endpoint, auth middleware
internal/admin                admin page and admin APIs; static holds the Vue and Element Plus build artifacts
internal/logx                 logging and rolling cleanup
```

### 8.2 Local debugging

With `mcp.stateless: true` tools can be called directly:

```bash
curl -s -X POST http://127.0.0.1:47319/mcp \
  -H 'Authorization: Bearer change-me' \
  -H 'Content-Type: application/json' \
  -H 'Accept: application/json, text/event-stream' \
  -d '{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"get-datasources","arguments":{}}}'
```

The response is SSE; take the JSON line after `data:`. In session mode initialize first and carry `Mcp-Session-Id`.

### 8.3 Static checks

```bash
go vet ./...
```

### 8.4 Third-party dependencies

1. Official MCP Go SDK: `github.com/modelcontextprotocol/go-sdk`.
2. Database drivers: `go-sql-driver/mysql`, `jackc/pgx/v5`, `microsoft/go-mssqldb`, `modernc.org/sqlite`, `go.mongodb.org/mongo-driver/v2`, `github.com/redis/go-redis/v9`, all pure Go.
3. Admin page assets are the official Vue 3 and Element Plus release packages, MIT licensed, shipped with the binary.
