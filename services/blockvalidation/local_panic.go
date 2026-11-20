package blockvalidation

import (
	"runtime/debug"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/ulogger"
)

// localPanicked marks an error that stands for a panic in this node (BV-STARTUP-007). Two places
// make one. runOncePerBlock gives it to the waiters of a validation that panicked on another
// goroutine, and recoverChildPanic gives it to the parent of an errgroup goroutine that panicked
// (issue 4993). In both cases the caller has no panic of its own, so it
// cannot tell from its own state that the failure is a bug in this node. Without the marker,
// catchup scores the error against the peer that served the block.
//
// It is a Go type and not an error code or an errors-package sentinel. teranode's errors.Is
// matches on the code, so a sentinel made with NewProcessingError matches each ERR_PROCESSING
// error. NewProcessingError turns a trailing cause that is not an *Error into a plain message, so
// the marker goes into the chain with SetWrappedErr. isLocalPanic finds it by type with
// errors.As, which does no text matching. Over gRPC the marker is lost, and the error is a plain
// ERR_PROCESSING error, as before.
type localPanicked struct{}

func (*localPanicked) Error() string {
	return "a goroutine in this node panicked"
}

// newConcurrentValidationPanicError returns the ERR_PROCESSING error that runOncePerBlock stores
// for the waiters when validate panics. Its text has no word that IsNetworkError,
// IsMaliciousResponseError or IsContextError matches.
func newConcurrentValidationPanicError(blockHash *chainhash.Hash) error {
	err := errors.NewProcessingError("[ValidateBlock][%s] panic during a concurrent validation of this block, see the PANIC log line", blockHash.String())
	err.SetWrappedErr(&localPanicked{})

	return err
}

// newChildPanicError returns the ERR_PROCESSING error that recoverChildPanic hands to the parent
// of a panicked errgroup goroutine. The text is fixed: no panic value and no call-site detail,
// so no word that IsNetworkError, IsMaliciousResponseError or IsContextError matches can get in.
// The PANIC log line carries the site, the panic value and the stack.
func newChildPanicError() error {
	err := errors.NewProcessingError("[blockvalidation] panic in a goroutine of this node, see the PANIC log line")
	err.SetWrappedErr(&localPanicked{})

	return err
}

// isLocalPanic reports whether err stands for a panic in this node: a waiter's error from
// runOncePerBlock, or a recovered errgroup goroutine. Such an error is a local fault, never the
// peer's.
func isLocalPanic(err error) bool {
	if err == nil {
		return false
	}

	var marker *localPanicked

	return errors.As(err, &marker)
}

// recoverChildPanic recovers a panic in an errgroup goroutine (issue 4993). errgroup does not
// recover its children's panics, and a recover in the parent goroutine cannot see them, so a
// panic in a bare g.Go ends the process. Defer it as the first statement of the goroutine:
//
//	g.Go(func() (err error) {
//		defer recoverChildPanic(u.logger, recoveredPanicPathCatchupFetch, &err, nil, "blockWorker %d", workerID)
//		...
//	})
//
// It must be the deferred function itself, not called from one, because recover only works when
// the deferred function calls it directly.
//
// On a panic it counts the panic under path, logs the site, the panic value and the stack on one
// line, and sets *retErr to the error from newChildPanicError. That error carries the
// localPanicked marker, so the scorers read it as a bug in this node and charge no peer. A nil
// retErr leaves the goroutine's return value alone. Use that where the group must not see an
// error, and pass the error on with onPanic instead.
//
// onPanic, when not nil, runs after *retErr is set and receives the same error. Use it where the
// parent reads a result the goroutine fills, so a panicked goroutine leaves a fail-safe value
// rather than a zero one.
//
// format and args name the site in the log line only. They never reach the error text.
func recoverChildPanic(logger ulogger.Logger, path string, retErr *error, onPanic func(err error), format string, args ...any) {
	r := recover()
	if r == nil {
		return
	}

	countRecoveredPanic(path)

	if logger != nil {
		// A fresh slice: appending to args could write past the length the caller passed.
		logArgs := append(append(make([]any, 0, len(args)+2), args...), r, debug.Stack())
		logger.Errorf("[blockvalidation] PANIC in goroutine "+format+": %v stack=%q", logArgs...)
	}

	err := newChildPanicError()

	if retErr != nil {
		*retErr = err
	}

	if onPanic != nil {
		onPanic(err)
	}
}
