package util

import (
	"fmt"
	"runtime/debug"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/ulogger"
)

// SignalBatchPanic is the panic safety net for go-batcher dispatch functions.
//
// go-batcher recovers panics raised inside the batch fn (see dispatchAndRecord
// in go-batcher/v2: it wraps b.fn(batch) in a deferred recover). Without our own
// guard, a panic part-way through a dispatch fn leaves the not-yet-completed
// items un-signalled: the worker survives, but the submitter goroutines waiting
// on the shared completion.Group park for as long as their wait allows. That is
// the mechanism behind the production goroutine leak in the utxo store.
//
// Install it as the FIRST statement of each dispatch fn:
//
//	defer func() {
//	    util.SignalBatchPanic(recover(), batch, "sendGetBatch", s.logger, func(it *batchGetItem, err error) {
//	        it.complete(err)
//	    })
//	}()
//
// signal MUST be non-blocking and idempotent. Production passes it.complete,
// which is CAS-guarded, so signalling an item an earlier stage already completed
// is a safe no-op (no double-signal, no block).
//
// Returns true if a panic was actually handled (recovered != nil), so a caller
// with its own metrics can bump them on the true branch.
//
// Both the recovered value and the stack are rendered with %q: debug.Stack() is
// multi-line by construction, and a panic value can be multi-line too, so
// escaping both is what keeps the record on a single line.
func SignalBatchPanic[T any](recovered any, batch []T, fnName string, logger ulogger.Logger, signal func(item T, err error)) bool {
	if recovered == nil {
		return false
	}

	logger.Errorf("[%s] recovered panic, failing %d batch item(s): recovered=%q stack=%q",
		fnName, len(batch), fmt.Sprintf("%v", recovered), debug.Stack())

	err := errors.NewProcessingError("panic in %s: %v", fnName, recovered)
	for _, item := range batch {
		signal(item, err)
	}

	return true
}
