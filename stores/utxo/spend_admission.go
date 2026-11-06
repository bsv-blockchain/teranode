package utxo

import (
	"context"
	"sync/atomic"

	"github.com/bsv-blockchain/teranode/errors"
)

const (
	spendQueued uint32 = iota
	spendStarted
	spendAbandoned
)

// SpendAdmission arbitrates dispatch against caller abandonment. Its zero value
// is queued. It must not be copied after use. Starting wins responsibility for
// an in-flight operation; abandonment prevents an operation that is still queued
// from starting. It does not undo a backend write that has already started.
type SpendAdmission struct {
	state atomic.Uint32
}

// Start reserves this item for a backend part, unless its caller has stopped
// waiting. The deadline check also handles a caller whose timeout handler has
// not been scheduled yet. A context ended with the cause context.DeadlineExceeded
// counts as expired, so a wait timer that cancels a context reads as a timeout. A nil context is allowed for internal synchronous use.
func (a *SpendAdmission) Start(ctx context.Context) error {
	if ctx != nil && ctx.Err() != nil {
		a.Abandon()
		if errors.Is(context.Cause(ctx), context.DeadlineExceeded) {
			return errors.NewServiceUnavailableError("spend expired before backend dispatch", ctx.Err())
		}
		return errors.NewContextCanceledError("spend canceled before backend dispatch", ctx.Err())
	}
	if !a.state.CompareAndSwap(spendQueued, spendStarted) {
		return errors.NewServiceUnavailableError("spend is no longer queued for backend dispatch")
	}
	return nil
}

// Abandon prevents future dispatch if Start has not already won the handoff.
// It reports whether abandonment won. The caller must abandon queued inputs
// before beginning rollback.
func (a *SpendAdmission) Abandon() bool {
	return a.state.CompareAndSwap(spendQueued, spendAbandoned)
}

// Started reports whether Start won the handoff for this item. Only then does
// the dispatching goroutine own the item's per-request state; an abandoned item
// belongs to whichever party won its Abandon.
func (a *SpendAdmission) Started() bool {
	return a.state.Load() == spendStarted
}

// SpendOrphan arbitrates who rolls back a spend whose caller stopped waiting
// for it after the backend had started it. The caller marks the item orphaned
// and the batcher publishes the item's result; each side then asks Claim, and
// exactly one of them wins the right to roll the spend back, whichever order
// the two happen in. The side that publishes must store its published flag
// before it calls Claim, and the caller must call Mark before it reads that
// flag, so at least one side sees the other. Callers mark an item only when the
// batch rollback is not suppressed. Its zero value is not orphaned.
type SpendOrphan struct {
	orphaned atomic.Bool
	claimed  atomic.Bool
}

// Mark records that the caller no longer waits for the item.
func (o *SpendOrphan) Mark() {
	o.orphaned.Store(true)
}

// Claim reports whether the caller of Claim must roll the spend back. It is
// true at most once, and only after Mark.
func (o *SpendOrphan) Claim() bool {
	return o.orphaned.Load() && o.claimed.CompareAndSwap(false, true)
}
