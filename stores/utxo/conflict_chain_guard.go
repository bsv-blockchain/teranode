package utxo

import (
	"context"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// CurrentChainGuard protects transactions confirmed in any current-chain block.
// The triggering WAL block alone cannot establish whether undo is still valid.
// BlockAssembly serializes chain movement; this is not a distributed writer lock.
type CurrentChainGuard struct {
	check   func(context.Context, []uint32) (bool, error)
	blockID func(context.Context, chainhash.Hash) (uint32, error)
}

func NewCurrentChainGuard(check func(context.Context, []uint32) (bool, error)) CurrentChainGuard {
	return CurrentChainGuard{check: check}
}

// WithBlockID lets the guard resolve the numeric ID of a block hash. Forward
// recovery needs it: the winners of a forward operation are mined in the block
// being added, which is already on the current chain, so that one block must not
// make them look confirmed elsewhere.
func (g CurrentChainGuard) WithBlockID(resolve func(context.Context, chainhash.Hash) (uint32, error)) CurrentChainGuard {
	g.blockID = resolve
	return g
}

func (g CurrentChainGuard) protect(s Store) Store {
	if g.check == nil {
		return s
	}
	return &chainProtectedConflictStore{Store: s, guard: g}
}

func (g CurrentChainGuard) checkTransactions(ctx context.Context, s Store, hashes []chainhash.Hash) error {
	if g.check == nil {
		return nil
	}
	return (&chainProtectedConflictStore{Store: s, guard: g}).checkUnconfirmed(ctx, hashes)
}

// protectForward wraps s for the rollback of a forward operation. The winners may carry the
// ID of ownBlock without being refused. They are still refused when they carry the ID of any
// other current-chain block. It returns nil when the guard has no check.
func (g CurrentChainGuard) protectForward(ctx context.Context, s Store, winners []chainhash.Hash, ownBlock chainhash.Hash) *chainProtectedConflictStore {
	if g.check == nil {
		return nil
	}
	p := &chainProtectedConflictStore{Store: s, guard: g}
	if len(winners) > 0 && g.blockID != nil {
		// An unknown block, or a failed lookup, grants no exemption. The guard then stays strict, which
		// can only refuse more, never overwrite a confirmed spend.
		if id, err := g.blockID(ctx, ownBlock); err == nil {
			p.ownBlockID = id
			p.winners = make(map[chainhash.Hash]struct{}, len(winners))

			for _, h := range winners {
				p.winners[h] = struct{}{}
			}
		}
	}

	return p
}

type chainProtectedConflictStore struct {
	Store
	guard      CurrentChainGuard
	winners    map[chainhash.Hash]struct{}
	ownBlockID uint32
}

func (s *chainProtectedConflictStore) checkUnconfirmed(ctx context.Context, hashes []chainhash.Hash) error {
	unresolved := make([]*UnresolvedMetaData, len(hashes))
	for i, h := range hashes {
		unresolved[i] = &UnresolvedMetaData{Hash: h, Idx: i}
	}
	if len(unresolved) == 0 {
		return nil
	}
	if err := s.Store.BatchDecorate(ctx, unresolved, fields.BlockIDs); err != nil {
		return err
	}
	ids := make(map[uint32]struct{})
	for _, item := range unresolved {
		if item.Err != nil {
			return errors.NewProcessingError("cannot authorize conflict undo for %s", item.Hash.String(), item.Err)
		}
		if item.Data == nil {
			return errors.NewProcessingError("cannot authorize conflict undo: transaction %s is missing", item.Hash.String())
		}
		_, isWinner := s.winners[item.Hash]
		for _, id := range item.Data.BlockIDs {
			if isWinner && id == s.ownBlockID {
				continue
			}
			ids[id] = struct{}{}
		}
	}
	if len(ids) == 0 {
		return nil
	}
	blockIDs := make([]uint32, 0, len(ids))
	for id := range ids {
		blockIDs = append(blockIDs, id)
	}
	confirmed, err := s.guard.check(ctx, blockIDs)
	if err != nil {
		return errors.NewProcessingError("cannot authorize conflict undo: current-chain lookup failed", err)
	}
	if confirmed {
		return errors.NewProcessingError("refusing conflict undo: transaction is confirmed on the current chain")
	}
	return nil
}

func (s *chainProtectedConflictStore) SetConflicting(ctx context.Context, hashes []chainhash.Hash, value bool) ([]*Spend, []chainhash.Hash, error) {
	if value {
		if err := s.checkUnconfirmed(ctx, hashes); err != nil {
			return nil, nil, err
		}
	}
	return s.Store.SetConflicting(ctx, hashes, value)
}

func (s *chainProtectedConflictStore) Unspend(ctx context.Context, spends []*Spend, locked ...bool) error {
	var owners hashSet
	for _, sp := range spends {
		if sp != nil && sp.SpendingData != nil && sp.SpendingData.TxID != nil {
			owners.add(*sp.SpendingData.TxID)
		}
	}
	if err := s.checkUnconfirmed(ctx, owners.order); err != nil {
		return err
	}
	return s.Store.Unspend(ctx, spends, locked...)
}

var prometheusConflictWALCompletionFailures = promauto.NewCounter(prometheus.CounterOpts{
	Namespace: "teranode", Subsystem: "utxo", Name: "conflict_wal_completion_failures_total",
	Help: "Conflict WAL completion writes that failed; the operation remains pending",
})

// CompleteConflictIntent counts and returns cleanup failures. Callers must log
// or propagate the error; successful state writes do not imply WAL deletion.
func CompleteConflictIntent(ctx context.Context, s Store, id chainhash.Hash) error {
	if err := s.CompleteConflictIntent(ctx, id); err != nil {
		prometheusConflictWALCompletionFailures.Inc()
		return errors.NewStorageError("failed to complete conflict WAL intent %s", id.String(), err)
	}
	return nil
}
