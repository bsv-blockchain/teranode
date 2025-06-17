package daemon

import (
	"context"

	"github.com/bitcoin-sv/teranode/ulogger"
)

// Option is a functional option type for configuring the Daemon.
type Option func(*Daemon)

// WithLoggerFactory provides a custom logger factory for the Daemon and its services.
func WithLoggerFactory(factory func(serviceName string) ulogger.Logger) Option {
	return func(d *Daemon) {
		d.loggerFactory = factory
	}
}

// WithContext allows setting a custom context for the Daemon.
func WithContext(ctx context.Context) Option {
	return func(d *Daemon) {
		d.Ctx = ctx
	}
}
