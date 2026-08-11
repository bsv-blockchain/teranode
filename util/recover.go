package util

import (
	"runtime/debug"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/ulogger"
)

// RecoverToError converts a panic in the calling goroutine into an error stored
// in *retErr, for use as the first line of a fan-out goroutine.
//
// errgroup deliberately does not propagate panics from its children
// (golang.org/x/sync/errgroup), and echo's middleware.Recover only wraps the
// request goroutine, so a bare g.Go on a request path takes the whole process
// down with it. Goroutines that outlive their request — the ones streaming into
// an io.Pipe whose reader is handed back to the caller — cannot be reached by
// middleware.Recover even in principle.
//
// The panic value goes to the log; the returned error names only the fan-out
// site, so no panic detail reaches a client.
//
//	g.Go(func() (err error) {
//		defer util.RecoverToError(logger, &err, nil, "getTxs batch at %d", offset)()
//		...
//	})
//
// onPanic, when non-nil, runs after *retErr is set and receives that same error.
// Use it for the cleanup a panic would otherwise skip — failing an io.Pipe the
// consumer is blocked on, or aborting a half-written blob. It does not run on a
// normal return, so a goroutine's own error paths keep their existing handling.
//
// args are format parameters for format and nothing else. A trailing error is
// dropped rather than passed on: errors.New would extract it as the wrapped cause
// and render it into the message, folding internal detail into the client-facing
// error this helper exists to keep clean.
//
// The panic site is recoverable from the logged stack only. The error's own
// location metadata (file/line/function) points at this helper, because that is
// where it is constructed — not at the goroutine that panicked, and not at the
// panicking frame.
//
// Parameters:
//   - logger: receives the fan-out site label, the panic value and the stack
//   - retErr: named return of the fan-out goroutine, set only on panic
//   - onPanic: optional panic-only cleanup, receives the error stored in retErr
//   - format, args: identifies the fan-out site in both the log and the error
func RecoverToError(logger ulogger.Logger, retErr *error, onPanic func(err error), format string, args ...any) func() {
	return func() {
		r := recover()
		if r == nil {
			return
		}

		if n := len(args); n > 0 {
			if _, isErr := args[n-1].(error); isErr {
				args = args[:n-1]
			}
		}

		// Built on a fresh slice: appending into args would write the panic value
		// past the length a caller handed in, scribbling on an array it still owns.
		logArgs := append(append(make([]any, 0, len(args)+2), args...), r, debug.Stack())
		logger.Errorf("recovered panic in "+format+": %v\n%s", logArgs...)

		err := errors.NewProcessingError("internal error in "+format, args...)
		*retErr = err

		if onPanic != nil {
			onPanic(err)
		}
	}
}
