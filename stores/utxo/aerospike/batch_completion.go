package aerospike

import (
	"context"
	"time"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util"
)

// signalBatchPanic wraps util.SignalBatchPanic, adding the store's own
// PanicRecovered counter, which is package-private here. See
// util.SignalBatchPanic for the mechanism and the signal-fn contract.
func signalBatchPanic[T any](recovered any, batch []T, fnName string, logger ulogger.Logger, signal func(item T, err error)) bool {
	handled := util.SignalBatchPanic(recovered, batch, fnName, logger, signal)

	if handled && prometheusUtxoMapErrors != nil {
		prometheusUtxoMapErrors.WithLabelValues("Batch", "PanicRecovered").Inc()
	}

	return handled
}

// isContextWaitErr reports whether a completion.Group.Wait error came from the context arm
// rather than the timer arm. Classification is by the error Wait RETURNED; the helper
// deliberately takes no context, so it cannot re-derive the verdict from a context that may
// have been cancelled after Wait returned. See the policy note in Spend/IncrementSpentRecords.
func isContextWaitErr(waitErr error) bool {
	return errors.Is(waitErr, context.Canceled) || errors.Is(waitErr, context.DeadlineExceeded)
}

// batcherWaitTimeout bounds how long a submitter waits for a batcher to deliver
// a result before giving up with a ServiceUnavailable error. This is the
// keystone guarantee against permanent leaks: even if a dispatch fn never
// signals (panic, missed code path) or stays wedged inside a stuck v8 batch op,
// the caller goroutine is released after this bound instead of parking for the
// life of the process.
//
// It must outlast the longest a batch can *legitimately* take, or it fires
// during normal slow operation and aborts work the lower layers would still
// have completed (the legacy-sync stall this guard once caused). Two layers
// contribute to that legitimate time:
//
//   - the batch policy TotalTimeout — the ceiling on a single BatchOperate, and
//   - the uaerospike overload-retry wrapper, which runs an initial BatchOperate
//     and *then* starts an OverloadRetryMaxElapsed budget clock
//     (retryBatchOnOverload sets its deadline after the first attempt, see
//     util/uaerospike/overload_retry.go), re-issuing the still-overloaded
//     records until that budget is spent.
//
// So the legitimate submit-to-completion wall time is roughly
// initial-attempt (≤ TotalTimeout) + overload budget. The guard sums both —
// rather than taking the larger — because the budget clock starts only after
// the initial attempt, so the two windows are sequential, not overlapping.
// Summing also keeps the coupling from silently going stale: lowering a
// context's TotalTimeout can no longer drop the guard below the overload
// budget.
//
// Best-effort, not a strict bound: the initial connection-permit wait
// (~TotalTimeout/10, acquirePermit) and the untimed permit re-acquisition
// between retries (reacquirePermit) still fall outside the sum; the +30s grace
// only partially absorbs them. Falls back to a sane default when the policy
// carries no total timeout; adds nothing when the overload retry layer is
// disabled (OverloadRetryMaxElapsed <= 0), preserving the prior behaviour.
func batcherWaitTimeout(tSettings *settings.Settings) time.Duration {
	return batcherWaitFor(
		util.GetAerospikeBatchPolicy(tSettings).TotalTimeout,
		tSettings.Aerospike.OverloadRetryMaxElapsed,
	)
}

// batcherWaitFor is the pure leak-guard formula extracted from batcherWaitTimeout
// so the coverage invariant (guard always outlasts the overload budget) can be
// unit-tested without a live Aerospike client populating the batch policy. See
// batcherWaitTimeout for the rationale behind summing the two windows.
func batcherWaitFor(totalTimeout, overloadBudget time.Duration) time.Duration {
	d := totalTimeout
	if d <= 0 {
		d = 2 * time.Minute
	}

	if overloadBudget > 0 {
		d += overloadBudget
	}

	return d + 30*time.Second
}
