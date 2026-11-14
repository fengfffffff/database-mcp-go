// 日志:按天切分文件、超过保留天数自动删除并清理因此变空的子目录,同时可选输出到控制台
package logx

import (
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

const (
	filePrefix = "database-mcp-go-"
	fileSuffix = ".log"
)

// New 创建日志器,返回的 stop 函数用于停止清理协程
func New(dir, level string, retainDays int, console bool) (*slog.Logger, func(), error) {

	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, nil, fmt.Errorf("创建日志目录失败: %w", err)
	}
	w := &dailyWriter{dir: dir}
	if err := w.rotate(time.Now()); err != nil {
		return nil, nil, err
	}

	var out io.Writer = w
	if console {
		out = io.MultiWriter(w, os.Stdout)
	}
	handler := slog.NewTextHandler(out, &slog.HandlerOptions{Level: parseLevel(level)})
	logger := slog.New(handler)

	stop := make(chan struct{})
	go func() {
		ticker := time.NewTicker(time.Hour)
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				return
			case <-ticker.C:
				// 切分交给 Write, 它本身按日期判断, 这里只做超期清理, 避免并发切换文件
				if err := cleanup(dir, retainDays); err != nil {
					logger.Error("日志清理失败", "err", err)
				}
			}
		}
	}()

	once := sync.Once{}
	return logger, func() {
		once.Do(func() {
			close(stop)
			_ = w.Close()
		})
	}, nil
}

func parseLevel(level string) slog.Level {

	switch strings.ToLower(level) {
	case "debug":
		return slog.LevelDebug
	case "warn":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

// dailyWriter 按日期切换文件
type dailyWriter struct {
	mu   sync.Mutex
	dir  string
	day  string
	file *os.File
}

func (w *dailyWriter) Write(p []byte) (int, error) {

	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.rotate(time.Now()); err != nil {
		return 0, err
	}
	return w.file.Write(p)
}

// Close 关闭当前文件
func (w *dailyWriter) Close() error {

	w.mu.Lock()
	defer w.mu.Unlock()
	if w.file == nil {
		return nil
	}
	err := w.file.Close()
	w.file = nil
	return err
}

// rotate 日期变化时切换到新文件,调用方需自行持有锁或保证串行
func (w *dailyWriter) rotate(now time.Time) error {

	day := now.Format("20060102")
	if w.file != nil && w.day == day {
		return nil
	}
	if w.file != nil {
		_ = w.file.Close()
		w.file = nil
	}
	path := filepath.Join(w.dir, filePrefix+day+fileSuffix)
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return fmt.Errorf("打开日志文件失败: %w", err)
	}
	w.file = f
	w.day = day
	return nil
}

// cleanup 删除超过保留天数的日志文件,并清理因此产生的空目录
func cleanup(dir string, retainDays int) error {

	if retainDays <= 0 {
		return nil
	}
	cutoff := time.Now().AddDate(0, 0, -retainDays)
	removedDirs := make(map[string]struct{})

	err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			// 单个路径不可访问不影响其余文件清理
			return nil
		}
		if info.IsDir() {
			return nil
		}
		name := info.Name()
		if !strings.HasPrefix(name, filePrefix) || !strings.HasSuffix(name, fileSuffix) {
			return nil
		}
		// 按文件名中的日期判断,比修改时间更贴合按天切分的语义
		dayPart := strings.TrimSuffix(strings.TrimPrefix(name, filePrefix), fileSuffix)
		day, perr := time.ParseInLocation("20060102", dayPart, time.Local)
		if perr != nil {
			return nil
		}
		if day.Before(cutoff) {
			if rmErr := os.Remove(path); rmErr == nil {
				removedDirs[filepath.Dir(path)] = struct{}{}
			}
		}
		return nil
	})
	if err != nil {
		return err
	}

	// 由深到浅清理空目录,但不删除配置的日志根目录
	dirs := make([]string, 0, len(removedDirs))
	for d := range removedDirs {
		dirs = append(dirs, d)
	}
	sort.Sort(sort.Reverse(sort.StringSlice(dirs)))
	rootAbs, _ := filepath.Abs(dir)
	for _, d := range dirs {
		abs, _ := filepath.Abs(d)
		if abs == rootAbs {
			continue
		}
		if entries, rerr := os.ReadDir(d); rerr == nil && len(entries) == 0 {
			_ = os.Remove(d)
		}
	}
	return nil
}
