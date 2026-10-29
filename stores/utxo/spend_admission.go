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
// not been scheduled yet. A nil context is allowed for internal synchronous use.
func (a *SpendAdmission) Start(ctx context.Context) error {
	if ctx != nil && ctx.Err() != nil {
		a.Abandon()
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
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
