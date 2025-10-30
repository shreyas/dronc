package logger

import (
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var Log *zap.SugaredLogger

// Initialize sets up the global logger
func Initialize() error {
	config := zap.NewProductionConfig()
	config.EncoderConfig.TimeKey = "timestamp"
	config.EncoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder

	logger, err := config.Build()
	if err != nil {
		return err
	}

	Log = logger.Sugar()
	return nil
}

// Sync flushes any buffered log entries
func Sync() error {
	if Log != nil {
		return Log.Sync()
	}
	return nil
}

// Info logs an info message with structured fields
func Info(msg string, keysAndValues ...interface{}) {
	Log.Infow(msg, keysAndValues...)
}

// Debug logs a debug message with structured fields
func Debug(msg string, keysAndValues ...interface{}) {
	Log.Debugw(msg, keysAndValues...)
}

// Warn logs a warning message with structured fields
func Warn(msg string, keysAndValues ...interface{}) {
	Log.Warnw(msg, keysAndValues...)
}

// Error logs an error message with structured fields
func Error(msg string, keysAndValues ...interface{}) {
	Log.Errorw(msg, keysAndValues...)
}

// Fatal logs a fatal message with structured fields and exits
func Fatal(msg string, keysAndValues ...interface{}) {
	Log.Fatalw(msg, keysAndValues...)
}
