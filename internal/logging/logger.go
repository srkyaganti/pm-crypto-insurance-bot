package logging

import (
	"io"
	"os"
	"path/filepath"

	"github.com/rs/zerolog"
	"github.com/srkyaganti/pm-crypto-insurance-bot/internal/config"
)

func New(cfg *config.LoggingConfig) (*zerolog.Logger, error) {
	if err := ensureLogDir(cfg.FilePath); err != nil {
		return nil, err
	}

	var writers []io.Writer

	consoleWriter := zerolog.ConsoleWriter{
		Out:        os.Stdout,
		TimeFormat: "2006-01-02T15:04:05.000Z07:00",
		NoColor:    false,
	}
	writers = append(writers, consoleWriter)

	if cfg.FilePath != "" {
		file, err := os.OpenFile(cfg.FilePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
		if err != nil {
			return nil, err
		}
		writers = append(writers, file)
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

	return &logger, nil
}

func ensureLogDir(filePath string) error {
	if filePath == "" {
		return nil
	}
	dir := filepath.Dir(filePath)
	return os.MkdirAll(dir, 0755)
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
