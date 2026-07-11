// Package logger provides a small structured logger built on the standard
// library log package. It supports leveled logging (DEBUG/INFO/WARN/ERROR)
// and optional daily-rotated file output.
package logger

import (
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// Level is the logging severity.
type Level int

const (
	LevelDebug Level = iota
	LevelInfo
	LevelWarn
	LevelError
)

var levelNames = map[Level]string{
	LevelDebug: "DEBUG",
	LevelInfo:  "INFO",
	LevelWarn:  "WARN",
	LevelError: "ERROR",
}

// ParseLevel parses a level string. Unknown values default to INFO.
func ParseLevel(s string) Level {
	switch s {
	case "DEBUG", "debug":
		return LevelDebug
	case "WARN", "warn", "WARNING", "warning":
		return LevelWarn
	case "ERROR", "error":
		return LevelError
	default:
		return LevelInfo
	}
}

type Logger struct {
	mu        sync.Mutex
	level     Level
	stdLogger *log.Logger
	file      *os.File
	maxFiles  int
	fileDir   string
	fileDay   string
	fileLevel Level
}

var defaultLogger = New()

// New creates a new Logger writing to stderr.
func New() *Logger {
	return &Logger{
		level:     LevelInfo,
		stdLogger: log.New(os.Stderr, "", log.LstdFlags|log.Lmsgprefix),
		fileLevel: LevelInfo,
	}
}

// Default returns the process-global logger.
func Default() *Logger { return defaultLogger }

// Setup configures the global logger level and optional file logging.
func Setup(level string, fileLogging bool, fileDir string, maxFiles int) {
	defaultLogger.mu.Lock()
	defer defaultLogger.mu.Unlock()
	defaultLogger.level = ParseLevel(level)
	defaultLogger.fileDir = fileDir
	defaultLogger.maxFiles = maxFiles
	if defaultLogger.maxFiles <= 0 {
		defaultLogger.maxFiles = 7
	}
	if fileLogging && fileDir != "" {
		if err := os.MkdirAll(fileDir, 0o755); err == nil {
			defaultLogger.openFileLocked()
		}
	}
}

// Reload re-applies level / file settings without re-creating the global logger.
func Reload(level string, fileLevel string, maxFiles int) {
	defaultLogger.mu.Lock()
	defer defaultLogger.mu.Unlock()
	defaultLogger.level = ParseLevel(level)
	if fileLevel != "" {
		defaultLogger.fileLevel = ParseLevel(fileLevel)
	}
	if maxFiles > 0 {
		defaultLogger.maxFiles = maxFiles
	}
}

func (l *Logger) openFileLocked() {
	if l.fileDir == "" {
		return
	}
	day := time.Now().Format("2006-01-02")
	if l.file != nil && l.fileDay == day {
		return
	}
	if l.file != nil {
		l.file.Close()
		l.file = nil
	}
	path := filepath.Join(l.fileDir, "grok2api."+day+".log")
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return
	}
	l.file = f
	l.fileDay = day
	l.rotateLocked()
}

func (l *Logger) rotateLocked() {
	if l.fileDir == "" || l.maxFiles <= 0 {
		return
	}
	entries, err := os.ReadDir(l.fileDir)
	if err != nil {
		return
	}
	type fi struct {
		name string
		mt   time.Time
	}
	var files []fi
	for _, e := range entries {
		n := e.Name()
		if !matchLogName(n) {
			continue
		}
		info, err := e.Info()
		if err != nil {
			continue
		}
		files = append(files, fi{n, info.ModTime()})
	}
	for len(files) > l.maxFiles {
		var oldest fi
		var oldestIdx = -1
		for i, f := range files {
			if oldestIdx == -1 || f.mt.Before(oldest.mt) {
				oldest = f
				oldestIdx = i
			}
		}
		if oldestIdx < 0 {
			break
		}
		_ = os.Remove(filepath.Join(l.fileDir, oldest.name))
		files = append(files[:oldestIdx], files[oldestIdx+1:]...)
	}
}

func matchLogName(name string) bool {
	if len(name) < 9 || name[:9] != "grok2api." || len(name) < 14 {
		return false
	}
	if name[len(name)-4:] != ".log" {
		return false
	}
	return true
}

func (l *Logger) logf(lvl Level, format string, args ...any) {
	if lvl < l.level {
		return
	}
	msg := fmt.Sprintf(format, args...)
	line := fmt.Sprintf("%s [%s] %s", time.Now().Format("2006-01-02 15:04:05.000"), levelNames[lvl], msg)
	l.mu.Lock()
	l.stdLogger.Println(line)
	if l.file != nil && lvl >= l.fileLevel {
		l.openFileLocked()
		if l.file != nil {
			_, _ = io.WriteString(l.file, line+"\n")
		}
	}
	l.mu.Unlock()
}

func (l *Logger) Debugf(format string, args ...any) { l.logf(LevelDebug, format, args...) }
func (l *Logger) Infof(format string, args ...any)  { l.logf(LevelInfo, format, args...) }
func (l *Logger) Warnf(format string, args ...any)  { l.logf(LevelWarn, format, args...) }
func (l *Logger) Errorf(format string, args ...any) { l.logf(LevelError, format, args...) }

func (l *Logger) Debug(args ...any) { l.logf(LevelDebug, "%s", fmt.Sprint(args...)) }
func (l *Logger) Info(args ...any)  { l.logf(LevelInfo, "%s", fmt.Sprint(args...)) }
func (l *Logger) Warn(args ...any)  { l.logf(LevelWarn, "%s", fmt.Sprint(args...)) }
func (l *Logger) Error(args ...any)  { l.logf(LevelError, "%s", fmt.Sprint(args...)) }

// Package-level convenience functions using the default logger.
func Debugf(format string, args ...any) { defaultLogger.Debugf(format, args...) }
func Infof(format string, args ...any)  { defaultLogger.Infof(format, args...) }
func Warnf(format string, args ...any)  { defaultLogger.Warnf(format, args...) }
func Errorf(format string, args ...any) { defaultLogger.Errorf(format, args...) }
