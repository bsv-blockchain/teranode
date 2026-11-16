package pebble

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/stretchr/testify/require"
)

// TestSpendAbsorbsBlessedNotFound pins the "parent pruned, spending tx already
// blessed" fallback both mature stores implement. After a parent is DAH-pruned,
// re-validating an already-blessed child is routine — block validation and
// catchup both do it. Returning TX_NOT_FOUND there wedges the node: the tx
// never validates, its subtree never validates, the block never finalises.
func TestSpendAbsorbsBlessedNotFound(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)

	parent := newExtendedTx(t, 2, 95_000)
	mustCreate(t, store, parent, 100)

	child := newSpendingTx(t, parent, 0)

	// Bless the child: spend the parent output and create the child record.
	_, _, err := store.SpendAndCreate(ctx, child, 101)
	require.NoError(t, err)

	// The parent is pruned away.
	require.NoError(t, store.Delete(ctx, parent.TxIDChainHash()))

	// Re-validating the same child must succeed, not fail TX_NOT_FOUND.
	_, err = store.Spend(ctx, child, 102)
	require.NoError(t, err, "re-validating an already-blessed child must be absorbed")

	spends, err := utxo.GetSpends(child)
	require.NoError(t, err)

	_, err = store.Spend(ctx, child, 102)
	require.NoError(t, err)

	for _, sp := range spends {
		require.NoError(t, sp.Err)
	}

	// A genuinely unknown child, however, must still fail.
	orphan := newSpendingTx(t, parent, 1)

	_, err = store.Spend(ctx, orphan, 102)
	require.Error(t, err, "a spend whose own tx is absent must still fail")
	require.True(t, errors.Is(err, errors.ErrTxNotFound), "got %v", err)
}

// TestUnspendClearsLockWhenAsked pins the tri-state flagAsLocked contract.
// ProcessConflicting locks affected parents with true and
// ReverseProcessConflicting unlocks them with false; collapsing that to "set or
// do nothing" left a reorg's demoted parents locked forever, and every later
// spend of them failed TX_LOCKED.
func TestUnspendClearsLockWhenAsked(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)

	parent := newExtendedTx(t, 2, 96_000)
	mustCreate(t, store, parent, 100)

	spendTx := newSpendingTx(t, parent, 0)

	spends, err := store.Spend(ctx, spendTx, 101)
	require.NoError(t, err)

	// Lock on the way in, as ProcessConflicting does.
	require.NoError(t, store.Unspend(ctx, spends, true))

	m, err := store.getMaster(parent.TxIDChainHash())
	require.NoError(t, err)
	require.NotZero(t, m.flags&flagLocked, "precondition: Unspend(_, true) locks the parent")

	// Spending a locked parent is refused.
	_, err = store.Spend(ctx, newSpendingTx(t, parent, 0), 102)
	require.Error(t, err)

	// Unlock on the way back out, as ReverseProcessConflicting does.
	spends2, err := store.Spend(ctx, spendTx, 102, utxo.IgnoreFlags{IgnoreLocked: true})
	require.NoError(t, err)

	require.NoError(t, store.Unspend(ctx, spends2, false))

	m, err = store.getMaster(parent.TxIDChainHash())
	require.NoError(t, err)
	require.Zero(t, m.flags&flagLocked, "Unspend(_, false) must clear the locked flag")

	// And the parent is spendable again.
	_, err = store.Spend(ctx, newSpendingTx(t, parent, 0), 103)
	require.NoError(t, err, "an unlocked parent must be spendable")

	// Omitting the argument leaves the flag alone.
	require.NoError(t, store.SetLocked(ctx, []chainhash.Hash{*parent.TxIDChainHash()}, true))

	spends3, err := store.Spend(ctx, newSpendingTx(t, parent, 1), 104, utxo.IgnoreFlags{IgnoreLocked: true})
	require.NoError(t, err)
	require.NoError(t, store.Unspend(ctx, spends3))

	m, err = store.getMaster(parent.TxIDChainHash())
	require.NoError(t, err)
	require.NotZero(t, m.flags&flagLocked, "Unspend with no flag argument must not change the lock")
}

// TestPruneReclaimsStaleIndexEntries pins the zombie-DAH finding. stageDelete
// removes the index key at the master's current deleteAtHeight, so a DAH that
// moved after the scan orphaned the scanned key — which was then rescanned,
// re-locked and re-counted as a deletion on every later block, inflating the
// counts OnPruneComplete feeds to the pruning metrics and backpressure.
func TestPruneReclaimsStaleIndexEntries(t *testing.T) {
	ctx := context.Background()

	ResetPrunerServiceForTests()
	t.Cleanup(ResetPrunerServiceForTests)

	store := newTestStore(t)

	tx := newExtendedTx(t, 1, 97_000)
	mustCreate(t, store, tx, 100)

	hash := tx.TxIDChainHash()

	// Two index entries for one transaction: the shape a DAH move leaves.
	require.NoError(t, store.setDirect(heightIndexKey(prefixDAHIdx, 140, hash[:]), nil))
	require.NoError(t, store.setDirect(heightIndexKey(prefixDAHIdx, 150, hash[:]), nil))

	// Point the master at just one of them.
	m, err := store.getMaster(hash)
	require.NoError(t, err)

	old := *m
	m.deleteAtHeight = 150

	batch := store.db.NewBatch()
	require.NoError(t, store.stageMaster(batch, hash[:], &old, m))
	require.NoError(t, store.commit(batch))
	_ = batch.Close()

	svc, err := store.GetPrunerService()
	require.NoError(t, err)

	deleted, err := svc.Prune(ctx, 200, "<review>")
	require.NoError(t, err)
	require.Equal(t, int64(1), deleted, "one transaction was removed, so the count must be 1, not 2")

	_, err = store.Get(ctx, hash)
	require.True(t, errors.Is(err, errors.ErrTxNotFound))

	// No index entry may survive, or every later block re-processes it.
	require.Zero(t, countPrefix(t, store, []byte{prefixDAHIdx}),
		"the pruner must leave no stale delete-at-height entries behind")

	// A second pass must find nothing and report nothing.
	deleted, err = svc.Prune(ctx, 200, "<review>")
	require.NoError(t, err)
	require.Zero(t, deleted, "a second pass must not report phantom deletions")
}
