package util

import (
	"runtime"

	"github.com/bsv-blockchain/teranode/ulogger"
	"golang.org/x/sync/errgroup"
)

// SafeSetLimit sets the active-goroutine limit on an errgroup.Group, guarding
// against an invalid limit.
//
// errgroup.SetLimit(0) creates a zero-capacity semaphore, which leaves the
// group unable to ever start a goroutine — every subsequent Go call blocks
// forever. A zero (or negative) limit reaching this helper is therefore almost
// always a configuration mistake (e.g. a *Concurrency setting left at 0). These
// call sites all want a bounded-but-positive number of workers, so rather than
// panic or deadlock, SafeSetLimit falls back to a safe default of
// runtime.NumCPU() whenever the requested limit is less than 1.
//
// The fallback is logged at WARN so the underlying misconfiguration is never
// silent — the system stays up, but the bad setting is still visible.
//
// Parameters:
//   - logger: used to warn when the limit is clamped to the default
//   - g: The errgroup.Group to set the limit on
//   - limit: The maximum number of goroutines active at once; values < 1 fall
//     back to runtime.NumCPU().
func SafeSetLimit(logger ulogger.Logger, g *errgroup.Group, limit int) {
	if limit < 1 {
		def := runtime.NumCPU()
		logger.Warnf("SafeSetLimit: limit %d < 1, clamping to runtime.NumCPU()=%d", limit, def)

		limit = def
	}

	g.SetLimit(limit)
}
