package aerospike

import (
	"runtime/debug"
	"time"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util"
)

// signalBatchPanic is the panic safety net for the batcher dispatch functions
// (sendGetBatch, sendStoreBatch, sendSpendBatchLua, sendOutpointBatch,
// sendIncrementBatch, sendSetDAHBatch, setLockedBatch).
//
// go-batcher recovers panics raised inside the batch fn (see
// dispatchAndRecord in go-batcher/v2: it wraps b.fn(batch) in a deferred
// recover). Without our own guard, a panic part-way through a dispatch fn
// leaves every not-yet-signalled per-item completion channel orphaned: the
// worker survives, but the submitter goroutines waiting on those channels park
// forever (the contexts threaded down from legacy sync / validation have no
// deadline). That is the mechanism behind the production goroutine leak
// (thousands parked in (*Store).get's select).
//
// Install it as the FIRST statement of each dispatch fn:
//
//	defer func() {
//	    signalBatchPanic(recover(), batch, "sendGetBatch", s.logger, func(it *batchGetItem, err error) {
//	        util.SafeSend(it.done, batchGetItemData{Err: err}, batchSignalTimeout)
//	    })
//	}()
//
// signal MUST be non-blocking / panic-safe (wrap util.SafeSend) so that a
// submitter that already left (e.g. timed out) cannot block the worker, and a
// double-send on a buffered-1 channel cannot deadlock it. Returns true if a
// panic was actually handled (recovered != nil).
func signalBatchPanic[T any](recovered any, batch []T, fnName string, logger ulogger.Logger, signal func(item T, err error)) bool {
	if recovered == nil {
		return false
	}

	if prometheusUtxoMapErrors != nil {
		prometheusUtxoMapErrors.WithLabelValues("Batch", "PanicRecovered").Inc()
	}

	logger.Errorf("[%s] recovered panic, failing %d batch item(s): %v\n%s", fnName, len(batch), recovered, debug.Stack())

	err := errors.NewProcessingError("panic in %s: %v", fnName, recovered)
	for _, item := range batch {
		signal(item, err)
	}

	return true
}

// trySignal delivers v on a completion channel without blocking and without
// re-delivering when a result is already queued. It is the correct primitive for
// the buffered-1 completion channels used across the batchers (get/outpoint/
// spend/increment/setDAH/locked): a still-waiting submitter receives the value
// immediately, an already-signalled channel (buffer full) is left untouched, and
// a departed submitter cannot wedge the worker. Do NOT use it for unbuffered
// channels — there it would race the receiver and silently drop the signal.
func trySignal[T any](ch chan T, v T) {
	select {
	case ch <- v:
	default:
	}
}

// batchSignalTimeout bounds a single non-blocking completion send. It exists
// only so an unbuffered completion channel whose submitter has already departed
// cannot wedge the worker. Buffered-1 channels (the common case) never reach the
// timeout. Kept small because it is paid per item only on the rare error/panic
// fan-out paths.
const batchSignalTimeout = 5 * time.Second

// batcherWaitTimeout bounds how long a submitter waits for a batcher to deliver
// a result before giving up with a ServiceUnavailable error. This is the
// keystone guarantee against permanent leaks: even if a dispatch fn never
// signals (panic, missed code path) or stays wedged inside a stuck v8 batch op,
// the caller goroutine is released after this bound instead of parking for the
// life of the process.
//
// It is derived from the batch policy's own TotalTimeout (the maximum a healthy
// batch can legitimately take, retries included) plus grace, so it never fires
// during normal slow operation — only on a genuine wedge. Falls back to a sane
// default when the policy carries no total timeout.
func batcherWaitTimeout(tSettings *settings.Settings) time.Duration {
	d := util.GetAerospikeBatchPolicy(tSettings).TotalTimeout
	if d <= 0 {
		d = 2 * time.Minute
	}

	return d + 30*time.Second
}
