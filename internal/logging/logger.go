package logging

import (
	"io"
	"os"
	"path/filepath"

	"github.com/rs/zerolog"
	"github.com/srkyaganti/pm-crypto-insurance-bot/internal/config"
)

func New(cfg *config.LoggingConfig) (*zerolog.Logger, *RotatingWriter, error) {
	dir := filepath.Dir(cfg.FilePath)
	if dir != "" {
		if err := os.MkdirAll(dir, 0755); err != nil {
			return nil, nil, err
		}
	}

	var writers []io.Writer

	consoleWriter := zerolog.ConsoleWriter{
		Out:        os.Stdout,
		TimeFormat: "2006-01-02T15:04:05.000Z07:00",
		NoColor:    false,
	}
	writers = append(writers, consoleWriter)

	var rw *RotatingWriter
	if cfg.FilePath != "" {
		rw = NewRotatingWriter(dir, S3Config{
			Enabled: cfg.S3Enabled,
			Bucket:  cfg.S3Bucket,
			Region:  cfg.S3Region,
			Prefix:  cfg.S3Prefix,
		})
		if err := rw.Initialize(); err != nil {
			return nil, nil, err
		}
		writers = append(writers, rw)
	}

	multiWriter := io.MultiWriter(writers...)

	level, err := parseLevel(cfg.Level)
	if err != nil {
		level = zerolog.InfoLevel
	}

	logger := zerolog.New(multiWriter).
		With().
		Timestamp().
		Logger().
		Level(level)

	return &logger, rw, nil
}

func parseLevel(level string) (zerolog.Level, error) {
	switch level {
	case "debug":
		return zerolog.DebugLevel, nil
	case "info":
		return zerolog.InfoLevel, nil
	case "warn":
		return zerolog.WarnLevel, nil
	case "error":
		return zerolog.ErrorLevel, nil
	default:
		return zerolog.InfoLevel, nil
	}
}

func Component(logger *zerolog.Logger, component string) *zerolog.Logger {
	l := logger.With().Str("component", component).Logger()
	return &l
}
