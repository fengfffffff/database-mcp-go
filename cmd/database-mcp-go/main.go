// database-mcp-go 服务入口:装配配置、驱动、业务与传输层
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/fengfffffff/database-mcp-go/internal/config"
	"github.com/fengfffffff/database-mcp-go/internal/driver"
	_ "github.com/fengfffffff/database-mcp-go/internal/driver/kingbase"
	_ "github.com/fengfffffff/database-mcp-go/internal/driver/mongodb"
	_ "github.com/fengfffffff/database-mcp-go/internal/driver/mysql"
	_ "github.com/fengfffffff/database-mcp-go/internal/driver/postgresql"
	_ "github.com/fengfffffff/database-mcp-go/internal/driver/redis"
	_ "github.com/fengfffffff/database-mcp-go/internal/driver/sqlite"
	_ "github.com/fengfffffff/database-mcp-go/internal/driver/sqlserver"
	"github.com/fengfffffff/database-mcp-go/internal/logx"
	"github.com/fengfffffff/database-mcp-go/internal/pool"
	"github.com/fengfffffff/database-mcp-go/internal/repository"
	"github.com/fengfffffff/database-mcp-go/internal/service"
	"github.com/fengfffffff/database-mcp-go/internal/tool"
	"github.com/fengfffffff/database-mcp-go/internal/transport"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// version 构建版本,可用 -ldflags 覆盖
var version = "dev"

func main() {

	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "启动失败: %v\n", err)
		os.Exit(1)
	}
}

func run() error {

	configPath := flag.String("config", "config.yaml", "配置文件路径")
	showVersion := flag.Bool("version", false, "输出版本号")
	flag.Parse()

	if *showVersion {
		fmt.Println("database-mcp-go " + version)
		return nil
	}

	cfg, err := config.Load(*configPath)
	if err != nil {
		return err
	}
	logger, stopLog, err := logx.New(cfg.Log.Dir, cfg.Log.Level, cfg.Log.RetainDays, cfg.Log.Console)
	if err != nil {
		return err
	}
	defer stopLog()

	logger.Info("database-mcp-go 启动中", "version", version, "port", cfg.Server.Port,
		"source", cfg.Source.Type, "drivers", driver.Default().Supported())
	if cfg.Admin.Enabled && cfg.Admin.Username == "" {
		logger.Warn("管理页已启用但未配置账号, 当前无认证, 仅限本机与隧道访问", "path", "/admin")
	}

	// 数据源存储
	source, err := newSource(cfg, logger)
	if err != nil {
		return err
	}
	defer source.Close()

	pools := pool.NewManager(driver.Default(), logger)
	defer pools.Close()

	proxy := service.NewProxy(source, pools, logger, cfg.Database, cfg.Log.SQL)
	loadCtx, loadCancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer loadCancel()
	if err := proxy.Load(loadCtx); err != nil {
		return fmt.Errorf("加载数据源配置失败: %w", err)
	}

	// MCP 服务与工具
	server := mcp.NewServer(&mcp.Implementation{Name: "database-mcp-go", Version: version}, nil)
	tool.Register(server, proxy)

	handler := transport.NewHandler(cfg, server, proxy, logger)
	httpServer := &http.Server{
		Addr:              fmt.Sprintf(":%d", cfg.Server.Port),
		Handler:           handler,
		ReadHeaderTimeout: 10 * time.Second,
	}

	errCh := make(chan error, 1)
	go func() {
		logger.Info("HTTP 服务已启动", "addr", httpServer.Addr, "mcp_endpoint", cfg.MCP.Endpoint)
		if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)

	select {
	case err := <-errCh:
		return err
	case sig := <-stop:
		logger.Info("收到退出信号, 开始关停", "signal", sig.String())
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		logger.Error("HTTP 服务关停异常", "err", err)
	}
	logger.Info("database-mcp-go 已退出")
	return nil
}

// newSource 按配置构造数据源存储
func newSource(cfg *config.Config, logger *slog.Logger) (repository.Source, error) {

	switch cfg.Source.Type {
	case "file":
		return repository.NewFileSource(cfg.Source.File.Path)
	case "mysql":
		return repository.NewMySQLSource(cfg.Source.MySQL.DSN, cfg.Source.MySQL.AESKey, cfg.Source.MySQL.Table, logger)
	default:
		return nil, fmt.Errorf("不支持的 source.type: %s", cfg.Source.Type)
	}
}
