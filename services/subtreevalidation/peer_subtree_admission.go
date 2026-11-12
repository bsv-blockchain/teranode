package subtreevalidation

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/bsv-blockchain/teranode/errors"
)

// admissionGate bounds how many peer subtrees are being validated at once
// (subtreevalidation_peerSubtreeConcurrency), so a burst of peer subtrees
// after a block cannot each allocate its working set (about 1 GiB for 1M
// transactions) at the same time. It is created on first use so
// struct-literal Servers (tests) get it too.
type admissionGate struct {
	once    sync.Once
	slots   chan struct{}
	waiting atomic.Int32 // callers blocked waiting for a slot
}

// acquire waits for a slot and returns the function that frees it. A limit of
// zero or less disables the gate; the limit is read on the first call only.
func (g *admissionGate) acquire(ctx context.Context, limit int) (func(), error) {
	g.once.Do(func() {
		InitPrometheusMetrics() // the gate reports its waiters

		if limit > 0 {
			g.slots = make(chan struct{}, limit)
		}
	})

	if g.slots == nil {
		return func() {}, nil
	}

	release := func() { <-g.slots }

	select {
	case g.slots <- struct{}{}:
		return release, nil
	default:
	}

	g.waiting.Add(1)
	prometheusPeerSubtreeAdmissionWaiting.Inc()

	defer func() {
		g.waiting.Add(-1)
		prometheusPeerSubtreeAdmissionWaiting.Dec()
	}()

	select {
	case g.slots <- struct{}{}:
		return release, nil
	case <-ctx.Done():
		return nil, errors.NewContextCanceledError("[subtreesHandler] cancelled while waiting for peer subtree admission", ctx.Err())
	}
}
