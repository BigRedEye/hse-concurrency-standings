package logging

import (
	"fmt"
	"path"
	"runtime"
	"time"

	log "github.com/sirupsen/logrus"
)

func InitLogging(level string) error {
	logLevel := log.InfoLevel
	if level != "" {
		var err error
		logLevel, err = log.ParseLevel(level)
		if err != nil {
			return fmt.Errorf("failed to parse log level: %w", err)
		}
	}

	callerPrettyfier := func(f *runtime.Frame) (string, string) {
		filename := path.Base(f.File)
		return "", fmt.Sprintf("%s:%d", filename, f.Line)
	}

	log.SetFormatter(&log.TextFormatter{
		FullTimestamp:    true,
		CallerPrettyfier: callerPrettyfier,
		TimestampFormat:  time.RFC3339,
	})
	log.SetReportCaller(true)
	log.SetLevel(logLevel)
	return nil
}
