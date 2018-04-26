package account

// Logger is the Go kit's interface which is mainly used here to debug account services.
// During development excessive logging helps to debug, although errors should be
// either returned or logged. To adhere to this rule, there is an option to use debug releases
// https://dave.cheney.net/2014/09/28/using-build-to-switch-between-debug-and-release.
//
// Log creates a log event from keyvals, a variadic sequence of alternating keys and values.
// Implementations must be safe for concurrent use by multiple goroutines.
// In particular, any implementation of Logger that appends to keyvals or
// modifies or retains any of its elements must make a copy first.
//
// There is "Standard logger interface" discussion https://github.com/go-commons/commons/issues/1
// where the consensus is to emit events instead of logging in the library.
// There is no design doc yet https://github.com/go-commons/event/issues/1 at the time of writing.
// Anyhow I needed to debug account services during development and decided to use Logger interface
// for structured logs from https://github.com/go-kit/kit project.
type Logger interface {
	Log(keyvals ...interface{}) error
}

// LoggerFunc is an adapter to allow use of ordinary functions as Loggers. If
// f is a function with the appropriate signature, LoggerFunc(f) is a Logger
// object that calls f.
type LoggerFunc func(...interface{}) error

// Log implements Logger by calling f(keyvals...).
func (f LoggerFunc) Log(keyvals ...interface{}) error {
	return f(keyvals...)
}

// NoopLogger provides a logger that discards logs.
type NoopLogger struct{}

// Log implements no-op Logger.
func (l *NoopLogger) Log(_ ...interface{}) error {
	return nil
}
