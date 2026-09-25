package utxo

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

// ghostChainFixture is P -> C -> D -> E plus a sibling S that also spends P.
// C is the transaction the store rejects as a pruned replay.
type ghostChainFixture struct {
	parent, child, grandchild, greatGrandchild, sibling *bt.Tx
}

func newGhostChainFixture(t *testing.T) ghostChainFixture {
	t.Helper()

	parent := bt.NewTx()
	require.NoError(t, parent.From("1111111111111111111111111111111111111111111111111111111111111111", 0, "51", 30000))
	require.NoError(t, parent.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 4000))
	require.NoError(t, parent.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 4000))

	spendOf := func(prev *bt.Tx, vout uint32) *bt.Tx {
		tx := bt.NewTx()
		require.NoError(t, tx.From(prev.TxID(), vout, prev.Outputs[vout].LockingScript.String(), prev.Outputs[vout].Satoshis))
		require.NoError(t, tx.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", prev.Outputs[vout].Satoshis-500))

		return tx
	}

	child := spendOf(parent, 0)
	grandchild := spendOf(child, 0)
	greatGrandchild := spendOf(grandchild, 0)
	sibling := spendOf(parent, 1)

	return ghostChainFixture{parent: parent, child: child, grandchild: grandchild, greatGrandchild: greatGrandchild, sibling: sibling}
}

func hashesOf(txs ...*bt.Tx) []*chainhash.Hash {
	out := make([]*chainhash.Hash, 0, len(txs))
	for _, tx := range txs {
		out = append(out, tx.TxIDChainHash())
	}

	return out
}

func createdSet(txs ...*bt.Tx) func(*chainhash.Hash) bool {
	set := make(map[chainhash.Hash]struct{}, len(txs))
	for _, tx := range txs {
		set[*tx.TxIDChainHash()] = struct{}{}
	}

	return func(h *chainhash.Hash) bool {
		_, ok := set[*h]

		return ok
	}
}

func TestPrunedReplayGhosts(t *testing.T) {
	f := newGhostChainFixture(t)
	block := []*bt.Tx{f.child, f.grandchild, f.greatGrandchild, f.sibling}
	rejected := hashesOf(f.child)

	t.Run("nothing rejected means nothing to remove", func(t *testing.T) {
		require.Nil(t, PrunedReplayGhosts(block, nil, createdSet(block...)))
	})

	t.Run("a rejected transaction this attempt did not create is left alone", func(t *testing.T) {
		// A marker hit alone does not prove the record is a replay's leftover:
		// the pruner marks a parent before it deletes the child and holds the
		// child back when a sibling parent's marker fails, so a live mined child
		// can sit behind a marker. Deleting it removed a live transaction. The
		// leftover of an earlier attempt is still caught, through createdHere
		// (LeftoversAmong: locked and carrying this block's id).
		require.Empty(t, PrunedReplayGhosts(block, rejected, createdSet()))
		require.Empty(t, PrunedReplayGhosts(block, rejected, createdSet(f.grandchild, f.greatGrandchild)),
			"nor is anything that spends it, since it is not a ghost")
	})

	t.Run("a rejected transaction this attempt created is a ghost", func(t *testing.T) {
		ghosts := PrunedReplayGhosts(block, rejected, createdSet(f.child))
		require.ElementsMatch(t, hashesOf(f.child), hashesOf(ghosts...))
	})

	t.Run("no createdHere answer removes nothing", func(t *testing.T) {
		require.Nil(t, PrunedReplayGhosts(block, rejected, nil))
	})

	t.Run("dependents this attempt created are ghosts, transitively", func(t *testing.T) {
		ghosts := PrunedReplayGhosts(block, rejected, createdSet(block...))
		require.ElementsMatch(t, hashesOf(f.child, f.grandchild, f.greatGrandchild), hashesOf(ghosts...))
		require.Equal(t, f.child.TxIDChainHash(), ghosts[0].TxIDChainHash(), "the rejected transaction comes first")
	})

	t.Run("a dependent the store already held is left alone, and so is its subtree", func(t *testing.T) {
		// D pre-existed: a pruned parent says nothing about whether a still
		// unpruned child is legitimate. E was created here but spends D, not a
		// ghost, so it is not one either.
		ghosts := PrunedReplayGhosts(block, rejected, createdSet(f.child, f.greatGrandchild, f.sibling))
		require.ElementsMatch(t, hashesOf(f.child), hashesOf(ghosts...))
	})

	t.Run("an unrelated sibling is never a ghost", func(t *testing.T) {
		ghosts := PrunedReplayGhosts(block, rejected, createdSet(block...))
		require.NotContains(t, hashesOf(ghosts...), f.sibling.TxIDChainHash())
	})

	t.Run("order of transactions does not matter", func(t *testing.T) {
		reversed := []*bt.Tx{f.sibling, f.greatGrandchild, f.grandchild, f.child}
		ghosts := PrunedReplayGhosts(reversed, rejected, createdSet(reversed...))
		require.ElementsMatch(t, hashesOf(f.child, f.grandchild, f.greatGrandchild), hashesOf(ghosts...))
	})

	t.Run("duplicate rejections collapse", func(t *testing.T) {
		ghosts := PrunedReplayGhosts(block, hashesOf(f.child, f.child), createdSet(f.child))
		require.Len(t, ghosts, 1)
	})
}

// TestRollbackSet pins which of a failed spend's successful inputs are reversed.
func TestRollbackSet(t *testing.T) {
	fresh := []*Spend{{Vout: 1}}
	idem := []*Spend{{Vout: 2}}

	require.Equal(t, append(append([]*Spend{}, fresh...), idem...), RollbackSet(append([]*Spend{}, fresh...), idem, false),
		"an idempotent match is reversed when no input can be a marker hit: it may be an orphan of an earlier failed attempt")
	require.Equal(t, fresh, RollbackSet(fresh, idem, true),
		"an idempotent match is held back when an input may be a marker hit: it is then the confirmed spend")
	require.Equal(t, idem, RollbackSet(nil, idem, false),
		"a call whose only successful inputs were idempotent matches is still healed")
	require.Empty(t, RollbackSet(nil, idem, true))
}

// TestReplayRejectionsFirst: errors.JoinCapped keeps only the first few links,
// so the links that identify a pruned replay must lead.
func TestReplayRejectionsFirst(t *testing.T) {
	spent := errors.NewStorageError("device overload")
	missing := errors.NewTxNotFoundError("parent missing")
	pruned := errors.NewUtxoSpendingTxPrunedError("pruned")

	errs := make([]error, 0, 23)
	for i := 0; i < 20; i++ {
		errs = append(errs, spent)
	}

	errs = append(errs, missing, nil, pruned)

	ordered := ReplayRejectionsFirst(errs)
	require.Len(t, ordered, 22, "nil entries are dropped, nothing else is")
	require.ErrorIs(t, ordered[0], errors.ErrUtxoSpendingTxPruned)
	require.ErrorIs(t, ordered[1], errors.ErrTxNotFound)
	require.ErrorIs(t, errors.JoinCapped(10, ordered...), errors.ErrUtxoSpendingTxPruned,
		"the marker rejection survives the cap on a wide transaction")
	require.NotErrorIs(t, errors.JoinCapped(10, errs...), errors.ErrUtxoSpendingTxPruned,
		"control: in input order the cap drops it")
}

// flakyDeleteStore fails DeleteComplete a fixed number of times before
// succeeding, and records every Unspend and decorate it is asked for. Only
// those three methods are ever called on it.
type flakyDeleteStore struct {
	Store
	failuresLeft atomic.Int32
	calls        atomic.Int32
	mu           sync.Mutex
	deleted      []chainhash.Hash
	unspent      []*Spend
	decorated    []chainhash.Hash
}

func (s *flakyDeleteStore) DeleteComplete(_ context.Context, hash *chainhash.Hash) error {
	s.calls.Add(1)

	if s.failuresLeft.Add(-1) >= 0 {
		return errors.NewStorageError("transient")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	s.deleted = append(s.deleted, *hash)

	return nil
}

func (s *flakyDeleteStore) Unspend(_ context.Context, spends []*Spend, _ ...bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.unspent = append(s.unspent, spends...)

	return nil
}

func (s *flakyDeleteStore) PreviousOutputsDecorate(_ context.Context, tx *bt.Tx) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.decorated = append(s.decorated, *tx.TxIDChainHash())

	for _, input := range tx.Inputs {
		if input.PreviousTxScript == nil {
			input.PreviousTxScript = bscript.NewFromBytes([]byte{0x51})
			input.PreviousTxSatoshis = 1
		}
	}

	return nil
}

func TestDeleteCreatedRetriesATransientFailure(t *testing.T) {
	f := newGhostChainFixture(t)
	ctx := context.Background()

	t.Run("a failure inside the budget is retried to success", func(t *testing.T) {
		store := &flakyDeleteStore{}
		store.failuresLeft.Store(deleteCreatedAttempts - 1)

		require.NoError(t, DeleteCreated(ctx, ulogger.TestLogger{}, store, []*bt.Tx{f.child}, 1))
		require.Equal(t, int32(deleteCreatedAttempts), store.calls.Load())
		require.Equal(t, []chainhash.Hash{*f.child.TxIDChainHash()}, store.deleted, "end state: the record is gone")
	})

	t.Run("a failure past the budget is reported", func(t *testing.T) {
		store := &flakyDeleteStore{}
		store.failuresLeft.Store(deleteCreatedAttempts)

		err := DeleteCreated(ctx, ulogger.TestLogger{}, store, []*bt.Tx{f.child}, 1)
		require.Error(t, err)
		require.Equal(t, int32(deleteCreatedAttempts), store.calls.Load())
		require.Empty(t, store.deleted, "end state: the record is still there, and the caller was told")
	})

	t.Run("nothing to delete is a no-op", func(t *testing.T) {
		store := &flakyDeleteStore{}
		require.NoError(t, DeleteCreated(ctx, ulogger.TestLogger{}, store, nil, 1))
		require.Zero(t, store.calls.Load())
	})
}

// decorateStore answers BatchDecorate from a fixed table. Only that method is
// ever called on it.
type decorateStore struct {
	Store
	data map[chainhash.Hash]*meta.Data
	errs map[chainhash.Hash]error
}

func (s *decorateStore) BatchDecorate(_ context.Context, items []*UnresolvedMetaData, _ ...fields.FieldName) error {
	for _, item := range items {
		if err, ok := s.errs[item.Hash]; ok {
			item.Err = err

			continue
		}

		item.Data = s.data[item.Hash]
	}

	return nil
}

// TestLeftoversAmong pins what counts as this block's own unfinished write.
func TestLeftoversAmong(t *testing.T) {
	ctx := context.Background()
	const thisBlock = uint32(1400)

	ours := chainhash.HashH([]byte("ours"))
	sibling := chainhash.HashH([]byte("sibling-block"))
	committed := chainhash.HashH([]byte("committed"))
	unreadable := chainhash.HashH([]byte("unreadable"))

	store := &decorateStore{
		data: map[chainhash.Hash]*meta.Data{
			ours:      {Locked: true, BlockIDs: []uint32{thisBlock}},
			sibling:   {Locked: true, BlockIDs: []uint32{1399}},
			committed: {Locked: false, BlockIDs: []uint32{thisBlock}},
		},
		errs: map[chainhash.Hash]error{unreadable: errors.NewStorageError("device overload")},
	}

	t.Run("locked and carrying this block's id is a leftover; locked by another block is not", func(t *testing.T) {
		// A concurrently validating block that shares the transaction wrote it
		// locked with ITS id. Filing that as this block's own write kept this
		// block's id off it and made it deletable as this block's ghost.
		leftovers, err := LeftoversAmong(ctx, store, []*chainhash.Hash{&ours, &sibling, &committed}, thisBlock)
		require.NoError(t, err)
		require.Equal(t, map[chainhash.Hash]struct{}{ours: {}}, leftovers)
	})

	t.Run("a record the store could not read fails the call", func(t *testing.T) {
		// BatchDecorate reports a per-record failure in the item and returns nil.
		// Reading that as "not locked" filed an unknown record as pre-existing and
		// switched the already-blessed fallback back on for it.
		_, err := LeftoversAmong(ctx, store, []*chainhash.Hash{&ours, &unreadable}, thisBlock)
		require.Error(t, err)
		require.ErrorIs(t, err, errors.ErrStorageError)
	})

	t.Run("a record the store returned no data for fails the call", func(t *testing.T) {
		missing := chainhash.HashH([]byte("missing"))
		_, err := LeftoversAmong(ctx, store, []*chainhash.Hash{&missing}, thisBlock)
		require.Error(t, err)
	})

	t.Run("a record that is gone is not a leftover and does not fail the call", func(t *testing.T) {
		// The pruner can delete a record between its create answering
		// ErrTxExists and this read. Gone is an answer, not an unreadable record.
		gone := chainhash.HashH([]byte("gone"))
		goneStore := &decorateStore{
			data: store.data,
			errs: map[chainhash.Hash]error{gone: errors.NewTxNotFoundError("%v not found", gone)},
		}

		leftovers, err := LeftoversAmong(ctx, goneStore, []*chainhash.Hash{&ours, &gone}, thisBlock)
		require.NoError(t, err)
		require.Equal(t, map[chainhash.Hash]struct{}{ours: {}}, leftovers)
	})

	t.Run("the coinbase placeholder is skipped", func(t *testing.T) {
		// The Aerospike store answers the placeholder with neither data nor an
		// error, which the no-data check would otherwise read as a failure.
		placeholder := subtree.CoinbasePlaceholderHashValue
		leftovers, err := LeftoversAmong(ctx, store, []*chainhash.Hash{&ours, &placeholder}, thisBlock)
		require.NoError(t, err)
		require.Equal(t, map[chainhash.Hash]struct{}{ours: {}}, leftovers)
	})
}

// TestDeleteCreatedNeverTouchesSpends pins the design: the compensation deletes
// records and issues no Unspend and no decorate, because a ghost's spends of
// surviving outputs are the historical ones and clearing them would hand a
// confirmed output to a new spender.
func TestDeleteCreatedNeverTouchesSpends(t *testing.T) {
	f := newGhostChainFixture(t)
	ctx := context.Background()

	descendant := bt.NewTx()
	require.NoError(t, descendant.From(f.child.TxID(), 0, f.child.Outputs[0].LockingScript.String(), f.child.Outputs[0].Satoshis))
	require.NoError(t, descendant.From(f.parent.TxID(), 1, f.parent.Outputs[1].LockingScript.String(), f.parent.Outputs[1].Satoshis))
	require.NoError(t, descendant.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 7000))

	store := &flakyDeleteStore{}
	store.failuresLeft.Store(0)

	require.NoError(t, DeleteCreated(ctx, ulogger.TestLogger{}, store, []*bt.Tx{f.child, descendant}, 2))
	require.Empty(t, store.unspent, "no spend may be reversed")
	require.Empty(t, store.decorated)
	require.ElementsMatch(t, []chainhash.Hash{*f.child.TxIDChainHash(), *descendant.TxIDChainHash()}, store.deleted)
}

// TestRollbackSetLeavesCallerSliceUntouched pins that RollbackSet never writes
// into the caller's backing array. With spare capacity, append(written, ...)
// would put the idempotent matches into memory the caller still owns.
func TestRollbackSetLeavesCallerSliceUntouched(t *testing.T) {
	fresh := &Spend{Vout: 1}
	idem := &Spend{Vout: 2}

	backing := make([]*Spend, 1, 4)
	backing[0] = fresh

	rollback := RollbackSet(backing, []*Spend{idem}, false)

	require.Equal(t, []*Spend{fresh, idem}, rollback)
	require.Nil(t, backing[:2][1], "the caller's spare capacity must not receive the idempotent match")
}
