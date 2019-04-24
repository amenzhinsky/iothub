package common

import (
	"fmt"
	"log"
	"os"
	"strings"
)

// Logger is common logging interface.
type Logger interface {
	Errorf(format string, v ...interface{})
	Warnf(format string, v ...interface{})
	Infof(format string, v ...interface{})
	Debugf(format string, v ...interface{})
}

// make sure that LevelLogger implements Logger interface.
var _ Logger = (*LevelLogger)(nil)

// NewLoggerFromEnv returns a LevelLogger with the name prefix and
// severity based on the named environment variable or it
// falls back to LevelWarn if it's missing.
//
// It uses the standard log.Print function for output
// so it can be controlled via the exposed configuration methods.
func NewLoggerFromEnv(name, key string) *LevelLogger {
	lvl := LevelWarn
	switch strings.ToLower(os.Getenv(key)) {
	case "e", "err", "error":
		lvl = LevelWarn
	case "w", "warn", "warning":
		lvl = LevelWarn
	case "i", "info":
		lvl = LevelInfo
	case "d", "debug":
		lvl = LevelDebug
	}
	return NewLogger(name, lvl, log.Print)
}

// LogLevel is logging severity.
type LogLevel uint8

const (
	LevelError LogLevel = iota
	LevelWarn
	LevelInfo
	LevelDebug
)

// String returns log level string representation.
func (lvl LogLevel) String() string {
	switch lvl {
	case LevelError:
		return "ERROR"
	case LevelWarn:
		return "WARN"
	case LevelInfo:
		return "INFO"
	case LevelDebug:
		return "DEBUG"
	default:
		return ""
	}
}

// PrintFunc is used for writing logs that works as fmt.Print.
type PrintFunc func(v ...interface{})

// NewLogger creates a new leveled logger instance with the given parameters.
func NewLogger(name string, lvl LogLevel, print PrintFunc) *LevelLogger {
	return &LevelLogger{name: name, lvl: lvl, print: print}
}

// LevelLogger is a logger that supports log levels.
type LevelLogger struct {
	name  string
	lvl   LogLevel
	print PrintFunc
}

func (l *LevelLogger) Errorf(format string, v ...interface{}) {
	l.logf(LevelError, format, v...)
}

func (l *LevelLogger) Infof(format string, v ...interface{}) {
	l.logf(LevelInfo, format, v...)
}

func (l *LevelLogger) Warnf(format string, v ...interface{}) {
	l.logf(LevelWarn, format, v...)
}

func (l *LevelLogger) Debugf(format string, v ...interface{}) {
	l.logf(LevelDebug, format, v...)
}

func (l *LevelLogger) logf(lvl LogLevel, format string, v ...interface{}) {
	if l.print != nil && lvl <= l.lvl {
		l.print(l.name, ": ", lvl.String(), " ", fmt.Sprintf(format, v...))
	}
}
