package common

import (
	"log"
	"os"
)

// Logger is common logging interface.
type Logger interface {
	Errorf(format string, v ...interface{})
	Warnf(format string, v ...interface{})
	Infof(format string, v ...interface{})
	Debugf(format string, v ...interface{})
}

// NewLogWrapper creates a wrapper of the std logger that implements the Logger interface.
func NewLogWrapper(debug bool) Logger {
	return &logWrapper{log.New(os.Stderr, "", 0), debug}
}

type logWrapper struct {
	*log.Logger
	debug bool
}

func (w *logWrapper) Errorf(format string, v ...interface{}) {
	w.Printf("E "+format, v...)
}

func (w *logWrapper) Warnf(format string, v ...interface{}) {
	w.Printf("W "+format, v...)
}

func (w *logWrapper) Infof(format string, v ...interface{}) {
	w.Printf("I "+format, v...)
}

func (w *logWrapper) Debugf(format string, v ...interface{}) {
	if w.debug {
		w.Printf("D "+format, v...)
	}
}
