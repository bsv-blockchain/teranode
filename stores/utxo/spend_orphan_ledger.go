package utxo

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
)

// OrphanLedger lists the spends of a spending transaction whose caller gave up
// while the backend had started them, until each one is resolved. A resolved
// orphan either failed or had its spend rolled back.
//
// Storage keeps no attempt identity: two attempts at one transaction store
// identical spending data. An orphan rollback that runs after a retry spent the
// same inputs would therefore clear the retry's spend. A new Spend of the
// transaction waits in the ledger until the earlier attempts are resolved, so
// the rollback never overlaps a retry in the same process. It does not order
// attempts made through other processes.
type OrphanLedger struct {
	pending atomic.Int64
	mu      sync.Mutex
	byTx    map[chainhash.Hash]*orphanGroup
}

type orphanGroup struct {
	count int
	done  chan struct{}
}

// Begin records an unresolved orphan of spender.
func (l *OrphanLedger) Begin(spender *chainhash.Hash) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.byTx == nil {
		l.byTx = make(map[chainhash.Hash]*orphanGroup)
	}

	group := l.byTx[*spender]
	if group == nil {
		group = &orphanGroup{done: make(chan struct{})}
		l.byTx[*spender] = group
	}

	group.count++

	l.pending.Add(1)
}

// End resolves one orphan of spender recorded by Begin.
func (l *OrphanLedger) End(spender *chainhash.Hash) {
	l.mu.Lock()
	defer l.mu.Unlock()

	group := l.byTx[*spender]
	if group == nil {
		return
	}

	group.count--

	l.pending.Add(-1)

	if group.count == 0 {
		delete(l.byTx, *spender)
		close(group.done)
	}
}

// Wait blocks until spender has no unresolved orphan, or until ctx ends. It
// returns the error of ctx in that case. Without any orphan it takes no lock.
func (l *OrphanLedger) Wait(ctx context.Context, spender *chainhash.Hash) error {
	for l.pending.Load() > 0 {
		l.mu.Lock()
		group := l.byTx[*spender]
		l.mu.Unlock()

		if group == nil {
			return nil
		}

		select {
		case <-group.done:
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	return nil
}

// AwaitSpend holds a Spend of spender until its earlier attempts have no
// unresolved orphan, for at most timeout. It returns a context error when ctx
// ends, and a service unavailable error when the wait budget ends first.
func (l *OrphanLedger) AwaitSpend(ctx context.Context, spender *chainhash.Hash, timeout time.Duration) error {
	waitCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	if err := l.Wait(waitCtx, spender); err != nil {
		if ctx.Err() != nil {
			return errors.NewContextCanceledError("[SPEND][%s] context canceled while an earlier attempt's late spend is rolled back", spender)
		}

		return errors.NewServiceUnavailableError("[SPEND][%s] an earlier attempt's late spend was not resolved after %s", spender, timeout)
	}

	return nil
}
