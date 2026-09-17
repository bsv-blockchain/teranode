package utxoset

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/stretchr/testify/require"
)

// TestUnminedSetIsEmptyNotUnimplemented is what lets the node boot.
//
// Block assembly calls both of these during startup, before a single block is
// processed, and treats an error from either as fatal (BlockAssembler.go:936-942 and
// :2629-2634). Answering "not implemented" therefore crash-loops the daemon and the node
// never syncs at all, which is what a deploy of this store did.
//
// Empty is not a shortcut here, it is the true answer. Both the unmined set and the
// conflict-intent log are transaction-level state that lives in tx_meta, and this store
// has no tx_meta, so it cannot be holding an unmined transaction or a pending intent.
// Reporting "none" describes the store exactly. When tx_meta lands these must start
// returning real data, and the conformance suite is what will catch it if they do not.
func TestUnminedSetIsEmptyNotUnimplemented(t *testing.T) {
	s, ctx := newTestStore(t)

	it, err := s.GetUnminedTxIterator()
	require.NoError(t, err, "block assembly treats an error here as fatal at startup")
	require.NotNil(t, it, "a nil iterator panics the caller, which only checks err")

	defer func() { require.NoError(t, it.Close()) }()

	batch, err := it.Next(ctx)
	require.NoError(t, err)
	require.Empty(t, batch, "a store with no tx_meta cannot hold an unmined transaction")
	require.NoError(t, it.Err())

	intents, err := s.PendingConflictIntents(ctx)
	require.NoError(t, err, "block assembly replays these at startup")
	require.Empty(t, intents)
}

// TestUnminedFamilyIsEmptyNotUnimplemented covers the rest of the unmined-tracking
// family, which the node hits on background timers rather than at startup.
//
// GetPrunableUnminedTxIterator and ProcessExpiredPreservations both fired repeatedly
// against a live mainnet sync, logging errors on every cycle. Non-fatal, but a store that
// errors on a timer teaches everyone to ignore its errors.
//
// The preservation pair does real work now (see TestPreservedParentOutlivesItsMembershipWindow),
// so what this pins is the empty case the pruner hits on every cycle of a healthy node: no old
// unmined transactions, no parents named, nothing preserved and nothing to expire. Each of
// those has to be a quiet success rather than an error on a timer.
func TestUnminedFamilyIsEmptyNotUnimplemented(t *testing.T) {
	s, ctx := newTestStore(t)

	it, err := s.GetPrunableUnminedTxIterator(1_000)
	require.NoError(t, err)
	require.NotNil(t, it)

	defer func() { require.NoError(t, it.Close()) }()

	batch, err := it.Next(ctx)
	require.NoError(t, err)
	require.Empty(t, batch)

	require.NoError(t, s.ProcessExpiredPreservations(ctx, 1_000),
		"an empty preservation table expires nothing and says so quietly")
	require.NoError(t, s.PreserveTransactions(ctx, nil, 1_000),
		"the pruner names no parents when no transaction has been waiting too long")

	old, err := s.QueryOldUnminedTransactions(ctx, 1_000)
	require.NoError(t, err)
	require.Empty(t, old)
}

// preservedRows counts the preservation side table, which is what tells a test whether
// PreserveTransactions found a membership row to copy or correctly found nothing.
func preservedRows(t *testing.T, s *Store, ctx context.Context) int {
	t.Helper()

	var n int
	require.NoError(t, s.pool.QueryRow(ctx, `SELECT count(*) FROM preserved_parent`).Scan(&n))

	return n
}

// TestPreservedParentOutlivesItsMembershipWindow: the pruner's parent-preservation phase names
// the parents of old unmined transactions; a preserved parent still answers a lookup after
// its membership window has been dropped and its coins are gone.
func TestPreservedParentOutlivesItsMembershipWindow(t *testing.T) {
	s, ctx := newTestStore(t)

	parent := mkTx(t, 1, 5_000)
	_, err := s.Create(ctx, parent, 100, utxo.WithMinedBlockInfo(
		utxo.MinedBlockInfo{BlockID: 7, BlockHeight: 100, OnLongestChain: true}))
	require.NoError(t, err)

	spendOneOutput(t, s, ctx, parent, 0, 150) // the child stays unmined

	require.NoError(t, s.PreserveTransactions(ctx, []chainhash.Hash{*parent.TxIDChainHash()}, 5_000))

	_, err = s.dropTxMinedWindowsBelow(ctx, 2_000)
	require.NoError(t, err)

	got, err := s.Get(ctx, parent.TxIDChainHash(), fields.BlockIDs)
	require.NoError(t, err)
	require.Equal(t, []uint32{7}, got.BlockIDs)

	require.NoError(t, s.ProcessExpiredPreservations(ctx, 6_000))

	// The spend at 150 left a journal row carrying the parent's block facts, and the read
	// order's last step answers from it, so "gone like any other" now means past the journal's
	// retention as well as past preservation. That is the point of the journal step: a
	// fully-spent parent stays answerable for a window AFTER the spend, which is what both the
	// base branch and aerospike do.
	got, err = s.Get(ctx, parent.TxIDChainHash(), fields.BlockIDs)
	require.NoError(t, err)
	require.Equal(t, []uint32{7}, got.BlockIDs)

	_, err = s.dropSpendJournalPartitionsBelow(ctx, 2_000)
	require.NoError(t, err)

	_, err = s.Get(ctx, parent.TxIDChainHash(), fields.BlockIDs)
	require.True(t, errors.Is(err, errors.ErrTxNotFound), "past both retentions the parent is gone like any other")
}

// TestPreserveTransactionsCopiesOnlyWhatMembershipHolds: the pruner names a parent by hash and
// knows nothing about where it lives, so both of the hashes that have no membership row have
// to be no-ops rather than errors.
//
// A mempool parent needs nothing preserved: its identity row is what keeps it, and that row
// stays for as long as the transaction is unmined. A parent already gone cannot be recovered
// from a table that only ever copies a live membership row.
func TestPreserveTransactionsCopiesOnlyWhatMembershipHolds(t *testing.T) {
	s, ctx := newTestStore(t)

	mempool := mkTx(t, 1, 5_000)
	_, err := s.Create(ctx, mempool, 100)
	require.NoError(t, err)
	require.True(t, identExists(t, s, ctx, mempool), "a mempool arrival is held by its identity row")

	var unknown chainhash.Hash
	unknown[0] = 0xab

	require.NoError(t, s.PreserveTransactions(ctx,
		[]chainhash.Hash{*mempool.TxIDChainHash(), unknown, unknown}, 5_000))

	require.Equal(t, 0, preservedRows(t, s, ctx),
		"neither a mempool-only hash nor an unknown one has a membership row to copy")
}

// TestPreservedParentStillAnswersItsContest: the contest is attached for every transaction ANY
// read step answered, and the preservation step is one of them.
//
// This is the case that matters most, because the parent whose child is still unmined is
// exactly the parent whose child may be a double spend.
func TestPreservedParentStillAnswersItsContest(t *testing.T) {
	s, ctx := newTestStore(t)

	parent := mkTx(t, 1, 5_000)
	_, err := s.Create(ctx, parent, 100, utxo.WithMinedBlockInfo(
		utxo.MinedBlockInfo{BlockID: 7, BlockHeight: 100, OnLongestChain: true}))
	require.NoError(t, err)

	child := spendOneOutput(t, s, ctx, parent, 0, 150)
	plantConflictNote(t, s, ctx, 150, hashBytes(parent), hashBytes(child))

	require.NoError(t, s.PreserveTransactions(ctx, []chainhash.Hash{*parent.TxIDChainHash()}, 5_000))

	_, err = s.dropTxMinedWindowsBelow(ctx, 2_000)
	require.NoError(t, err)

	got, err := s.Get(ctx, parent.TxIDChainHash(), fields.ConflictingChildren)
	require.NoError(t, err)
	require.Equal(t, []chainhash.Hash{*child.TxIDChainHash()}, got.ConflictingChildren,
		"a preserved parent is contested exactly as a mined one is")
}

// TestPreservingASecondTimeKeepsTheLongerPromise: the pruner names a parent again on every
// cycle its child is still waiting, and two children of one parent do not age together. A
// second, nearer expiry must not shorten a promise already made to the older child.
//
// It also pins the other end of the row's life: Delete removes the preservation copy too, so
// the offline rewind tool leaves nothing behind that would keep answering for a transaction
// the operator has just erased.
func TestPreservingASecondTimeKeepsTheLongerPromise(t *testing.T) {
	s, ctx := newTestStore(t)

	parent := mkTx(t, 1, 5_000)
	_, err := s.Create(ctx, parent, 100, utxo.WithMinedBlockInfo(
		utxo.MinedBlockInfo{BlockID: 7, BlockHeight: 100, OnLongestChain: true}))
	require.NoError(t, err)

	preserved := []chainhash.Hash{*parent.TxIDChainHash()}

	require.NoError(t, s.PreserveTransactions(ctx, preserved, 5_000))
	require.NoError(t, s.PreserveTransactions(ctx, preserved, 3_000))

	var until int32
	require.NoError(t, s.pool.QueryRow(ctx,
		`SELECT preserve_until FROM preserved_parent WHERE txid = $1`, hashBytes(parent)).Scan(&until))
	require.Equal(t, int32(5_000), until, "a nearer expiry must not shorten a promise already made")

	require.NoError(t, s.ProcessExpiredPreservations(ctx, 4_000),
		"the row has not reached its expiry yet")
	require.Equal(t, 1, preservedRows(t, s, ctx))

	require.NoError(t, s.Delete(ctx, parent.TxIDChainHash()))
	require.Equal(t, 0, preservedRows(t, s, ctx),
		"Delete promises to remove every trace, and a preservation copy is a trace")
}
