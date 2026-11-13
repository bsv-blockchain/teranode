package utxo

import (
	"context"
	"sync"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
)

const orphanLedgerShards = 64

// OrphanLedger orders the attempts at one spending transaction inside a
// process. An attempt holds its transaction from the start of Spend until it
// returns and every late spend it left behind is resolved. A late spend is one
// the backend had started when its caller gave up.
//
// Storage keeps no attempt identity: two attempts at one transaction store
// identical spending data. A rollback of a late spend that runs beside another
// attempt would clear that attempt's spend. A new Spend of the transaction
// therefore waits until the earlier attempt and its late spends are resolved.
// The ledger does not order attempts made through other processes.
type OrphanLedger struct {
	shards [orphanLedgerShards]orphanShard
}

type orphanShard struct {
	mu   sync.Mutex
	byTx map[chainhash.Hash]*orphanGroup
}

type orphanGroup struct {
	count int
	done  chan struct{}
}

func (l *OrphanLedger) shard(spender *chainhash.Hash) *orphanShard {
	return &l.shards[int(spender[0])%orphanLedgerShards]
}

// Begin adds one hold on spender. The caller must already own a hold on it, so
// the transaction stays held without a gap.
func (l *OrphanLedger) Begin(spender *chainhash.Hash) {
	s := l.shard(spender)

	s.mu.Lock()
	defer s.mu.Unlock()

	if group := s.byTx[*spender]; group != nil {
		group.count++

		return
	}

	s.add(spender)
}

// End releases one hold on spender.
func (l *OrphanLedger) End(spender *chainhash.Hash) {
	s := l.shard(spender)

	s.mu.Lock()
	defer s.mu.Unlock()

	group := s.byTx[*spender]
	if group == nil {
		return
	}

	group.count--

	if group.count == 0 {
		delete(s.byTx, *spender)
		close(group.done)
	}
}

func (s *orphanShard) add(spender *chainhash.Hash) {
	if s.byTx == nil {
		s.byTx = make(map[chainhash.Hash]*orphanGroup)
	}

	s.byTx[*spender] = &orphanGroup{count: 1, done: make(chan struct{})}
}

// Acquire takes the first hold on spender once no other attempt holds it. It
// returns the error of ctx when ctx ends first. The caller releases the hold
// with End.
func (l *OrphanLedger) Acquire(ctx context.Context, spender *chainhash.Hash) error {
	s := l.shard(spender)

	for {
		s.mu.Lock()

		group := s.byTx[*spender]
		if group == nil {
			s.add(spender)
			s.mu.Unlock()

			return nil
		}

		done := group.done

		s.mu.Unlock()

		select {
		case <-done:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// AwaitSpend takes the hold on spender for a Spend, waiting for earlier
// attempts for at most timeout. It returns a context error when ctx ends, and
// a service unavailable error when the wait budget ends first. The caller
// releases the hold with End.
func (l *OrphanLedger) AwaitSpend(ctx context.Context, spender *chainhash.Hash, timeout time.Duration) error {
	waitCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	if err := l.Acquire(waitCtx, spender); err != nil {
		if ctx.Err() != nil {
			return errors.NewContextCanceledError("[SPEND][%s] context canceled while an earlier attempt is unresolved", spender)
		}

		return errors.NewServiceUnavailableError("[SPEND][%s] an earlier attempt was not resolved after %s", spender, timeout)
	}

	return nil
}
