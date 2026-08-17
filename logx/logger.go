// Package logx defines a small logging interface used across agcron so that
// callers can plug in their own logger (zap, slog, logrus, ...) instead of the
// standard library logger. A default implementation is provided via Default.
package logx

import (
	"log"
	"os"
)

// Logger is the minimal logging surface agcron needs. Implementations must be
// safe for concurrent use from multiple goroutines.
type Logger interface {
	// Debugf logs a formatted message at debug level.
	Debugf(format string, args ...any)
	// Infof logs a formatted message at info level.
	Infof(format string, args ...any)
	// Warnf logs a formatted message at warning level.
	Warnf(format string, args ...any)
	// Errorf logs a formatted message at error level.
	Errorf(format string, args ...any)
}

// stdLogger is the default Logger backed by the standard library "log".
// It prefixes each line with the level so output stays readable.
type stdLogger struct {
	l *log.Logger
}

// Default returns a Logger that writes to stderr using the standard library
// logger, tagging each line with its level.
func Default() Logger {
	return &stdLogger{l: log.New(os.Stderr, "", log.LstdFlags)}
}

func (s *stdLogger) Debugf(format string, args ...any) {
	s.l.Printf("[DEBUG] "+format, args...)
}

func (s *stdLogger) Infof(format string, args ...any) {
	s.l.Printf("[INFO] "+format, args...)
}

func (s *stdLogger) Warnf(format string, args ...any) {
	s.l.Printf("[WARN] "+format, args...)
}

func (s *stdLogger) Errorf(format string, args ...any) {
	s.l.Printf("[ERROR] "+format, args...)
}

// Noop returns a Logger that discards everything. Useful for tests or silent runs.
func Noop() Logger {
	return noopLogger{}
}

type noopLogger struct{}

func (noopLogger) Debugf(format string, args ...any) {}
func (noopLogger) Infof(format string, args ...any)  {}
func (noopLogger) Warnf(format string, args ...any)  {}
func (noopLogger) Errorf(format string, args ...any) {}

// withLogger returns lg when non-nil, otherwise the Default logger. It is a
// convenience so constructors can accept a nullable Logger and always end up
// with a usable one.
func With(lg Logger) Logger {
	if lg == nil {
		return Default()
	}
	return lg
}
