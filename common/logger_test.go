package common

import (
	"os"
	"testing"
)

func TestNewEnvLogger(t *testing.T) {
	const envName = "__test_iotlog_env_logger"

	if err := os.Setenv(envName, "debug"); err != nil {
		t.Fatal(err)
	}
	l := NewLoggerFromEnv("test", envName)
	if l.lvl != LevelDebug {
		t.Errorf("logger level = %d, want %d", l.lvl, LevelDebug)
	}

	l.Errorf("error")
	l.Warnf("warn")
	l.Infof("info")
	l.Debugf("debug")
}
