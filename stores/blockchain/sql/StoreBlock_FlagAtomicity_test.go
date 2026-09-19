package sql

import (
	"context"
	"net/url"
	"testing"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/stores/blockchain/options"
	"github.com/bsv-blockchain/teranode/test/utils/postgres"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// blockRowExists reports whether the blocks table holds a row for this hash.
func blockRowExists(t *testing.T, s *SQL, hashBytes []byte) bool {
	t.Helper()

	var n int

	err := s.db.QueryRow(`SELECT count(*) FROM blocks WHERE hash = $1`, hashBytes).Scan(&n)
	require.NoError(t, err)

	return n > 0
}

// TestStoreBlockRollsBackWhenTheFlagReconciliationFails is the reason the fork path
// runs its INSERT and its on_main_chain reconciliation in one transaction.
//
// Before that change the INSERT auto-committed and the reconciliation ran afterwards
// as its own statement, whose failure was logged while StoreBlock returned success. A
// block could therefore be committed carrying a flag that nothing would revisit: the
// reconciliation only runs again on another fork, and its recursive walk is bounded to
// the recent lineage, so once the block sinks past that window no path repairs it.
//
// With the two in one transaction, a failed reconciliation takes the block row with it
// and the caller is told, so the block is retried rather than silently wrong.
func TestStoreBlockRollsBackWhenTheFlagReconciliationFails(t *testing.T) {
	s := newOnMainChainTestStore(t)

	storeBlocks(t, s, block1, block2, block3)

	calls := 0
	s.reconcileHook = func() error {
		calls++
		return errors.NewStorageError("injected reconciliation failure")
	}

	// blockAlternative2 forks from block1, so it takes the fork path.
	_, _, err := s.StoreBlock(context.Background(), blockAlternative2, "peer")
	require.Error(t, err, "a failed reconciliation must fail the store, not be logged and swallowed")
	require.Equal(t, 1, calls, "a non-retriable failure is returned at once, not retried")

	require.False(t, blockRowExists(t, s, blockAlternative2.Hash().CloneBytes()),
		"the rolled-back block must leave no row behind")

	require.True(t, getOnMainChain(t, s, block1.Hash().CloneBytes()), "block1 still on the main chain")
	require.True(t, getOnMainChain(t, s, block2.Hash().CloneBytes()), "block2 untouched")
	require.True(t, getOnMainChain(t, s, block3.Hash().CloneBytes()), "block3 untouched")
}

// TestStoreBlockRetryAfterAFailedReconciliationSucceeds: the rollback leaves the store
// in a state a retry can complete, which is what makes failing the call the right
// response rather than a wedge.
func TestStoreBlockRetryAfterAFailedReconciliationSucceeds(t *testing.T) {
	s := newOnMainChainTestStore(t)

	storeBlocks(t, s, block1, block2, block3)

	// The hook lives on this store, not in a package global, so a failure below
	// cannot leak it into other tests.
	s.reconcileHook = func() error { return errors.NewStorageError("injected reconciliation failure") }

	_, _, err := s.StoreBlock(context.Background(), blockAlternative2, "peer")
	require.Error(t, err)

	s.reconcileHook = nil

	_, _, err = s.StoreBlock(context.Background(), blockAlternative2, "peer")
	require.NoError(t, err, "the retry must succeed once the reconciliation works")

	require.True(t, blockRowExists(t, s, blockAlternative2.Hash().CloneBytes()))
	require.False(t, getOnMainChain(t, s, blockAlternative2.Hash().CloneBytes()),
		"a fork that is not the best chain is flagged off-chain")
	require.True(t, getOnMainChain(t, s, block3.Hash().CloneBytes()), "the longer chain keeps the flag")
}

// TestStoreBlockRetriesTheWholeTransactionOnATransientFailure: the fork path runs on
// a transaction, which the pool's per-statement retry cannot reach. A transient
// failure must roll the attempt back and re-run BEGIN, INSERT and the reconciliation,
// so the caller sees one clean success and exactly one block row with the right flag.
//
// The injected error is a lock timeout as it reaches RetryTx in production: already
// wrapped in a teranode error, which keeps the driver's message but not its type, so
// classification rests on the message. The sqlitememory store has retry enabled by
// default; PostgreSQL has it off unless postgres_retryEnabled is set.
func TestStoreBlockRetriesTheWholeTransactionOnATransientFailure(t *testing.T) {
	s := newOnMainChainTestStore(t)

	storeBlocks(t, s, block1, block2, block3)

	calls := 0
	s.reconcileHook = func() error {
		calls++
		if calls == 1 {
			return errors.NewStorageError("reconcileOnMainChain: failed to apply diff: database is locked")
		}

		return nil
	}

	_, _, err := s.StoreBlock(context.Background(), blockAlternative2, "peer")
	require.NoError(t, err, "a transient failure is retried inside StoreBlock")
	require.Equal(t, 2, calls, "the whole transaction ran twice")

	var rows int
	require.NoError(t, s.db.QueryRow(`SELECT count(*) FROM blocks WHERE hash = $1`,
		blockAlternative2.Hash().CloneBytes()).Scan(&rows))
	require.Equal(t, 1, rows, "the rolled-back attempt left nothing behind")

	require.False(t, getOnMainChain(t, s, blockAlternative2.Hash().CloneBytes()), "the fork is off-chain")
	require.True(t, getOnMainChain(t, s, block3.Hash().CloneBytes()), "the longer chain keeps the flag")
}

// TestReorgFlagsAreCorrectWithTheReconciliationInTheTransaction repeats the existing
// reorg expectations against the transactional path, so moving the reconciliation
// inside the transaction cannot quietly change which blocks end up flagged.
func TestReorgFlagsAreCorrectWithTheReconciliationInTheTransaction(t *testing.T) {
	s := newOnMainChainTestStore(t)

	storeBlocks(t, s, block1, block2, block3)

	forkBlock3 := createBlock3OnFork(blockAlternative2)
	forkBlock4 := createBlock3OnFork(forkBlock3)
	storeBlocks(t, s, blockAlternative2, forkBlock3, forkBlock4)

	require.True(t, getOnMainChain(t, s, block1.Hash().CloneBytes()), "common ancestor stays on chain")
	require.False(t, getOnMainChain(t, s, block2.Hash().CloneBytes()), "old chain is cleared")
	require.False(t, getOnMainChain(t, s, block3.Hash().CloneBytes()), "old chain is cleared")
	require.True(t, getOnMainChain(t, s, blockAlternative2.Hash().CloneBytes()), "new chain is flagged")
	require.True(t, getOnMainChain(t, s, forkBlock3.Hash().CloneBytes()), "new chain is flagged")
	require.True(t, getOnMainChain(t, s, forkBlock4.Hash().CloneBytes()), "new tip is flagged")

	var flagged int
	require.NoError(t, s.db.QueryRow(`SELECT count(*) FROM blocks WHERE on_main_chain = true`).Scan(&flagged))
	require.Equal(t, 5, flagged, "genesis plus the four blocks of the winning chain, and nothing else")
}

// BenchmarkStoreBlockForkPath measures what the fork path costs now that it carries a
// transaction and the reconciliation. That is the number the design rests on: the common
// extend keeps its single auto-committed INSERT and is unaffected.
func BenchmarkStoreBlockForkPath(b *testing.B) {
	t := &testing.T{}
	s := newOnMainChainTestStore(t)
	storeBlocks(t, s, block1, block2, block3)

	tip := block3
	forks := make([]*model.Block, 0, b.N)

	for i := 0; i < b.N; i++ {
		blk := createBlock3OnFork(tip)
		forks = append(forks, blk)
		tip = blk
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if _, _, err := s.StoreBlock(context.Background(), forks[i], "peer"); err != nil {
			b.Fatalf("store fork block %d: %v", i, err)
		}
	}
}

// TestLegacySequenceLeavesACommittedBlockBehind documents what the old ordering did,
// by running it directly: insert on the pool, which auto-commits, then fail the
// reconciliation. The row survives with whatever flag the insert guessed, and no caller
// is told. This is the behaviour the transaction removes, and it is kept as a test so
// the difference is visible rather than asserted in a comment.
func TestLegacySequenceLeavesACommittedBlockBehind(t *testing.T) {
	s := newOnMainChainTestStore(t)

	storeBlocks(t, s, block1, block2, block3)

	_, _, _, _, err := s.storeBlock(context.Background(), s.db, blockAlternative2, "peer",
		options.StoreBlockOptions{}, false)
	require.NoError(t, err)

	// The reconciliation that would have followed fails here.
	require.True(t, blockRowExists(t, s, blockAlternative2.Hash().CloneBytes()),
		"the old ordering commits the block before the flag is reconciled")
}

// TestStoreBlockFlagAtomicity_PostgreSQL runs the rollback and reorg expectations
// against PostgreSQL, the engine production uses, because transaction and
// recursive-CTE semantics there are what the fix actually depends on.
func TestStoreBlockFlagAtomicity_PostgreSQL(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping PostgreSQL tests in short mode")
	}

	newPostgresStore := func(t *testing.T) *SQL {
		t.Helper()

		connStr, teardown, err := postgres.SetupTestPostgresContainer()
		if err != nil {
			t.Skipf("PostgreSQL container not available: %v", err)
		}

		t.Cleanup(func() { _ = teardown() })

		storeURL, err := url.Parse(connStr)
		require.NoError(t, err)

		s, err := New(ulogger.TestLogger{}, storeURL, test.CreateBaseTestSettings(t))
		require.NoError(t, err)

		t.Cleanup(func() { _ = s.Close(context.Background()) })
		waitForStartupRebuild(t, s)

		return s
	}

	t.Run("failed reconciliation leaves no block row", func(t *testing.T) {
		s := newPostgresStore(t)
		storeBlocks(t, s, block1, block2, block3)

		s.reconcileHook = func() error { return errors.NewStorageError("injected reconciliation failure") }

		_, _, err := s.StoreBlock(context.Background(), blockAlternative2, "peer")
		require.Error(t, err)
		require.False(t, blockRowExists(t, s, blockAlternative2.Hash().CloneBytes()))
		require.True(t, getOnMainChain(t, s, block3.Hash().CloneBytes()))

		s.reconcileHook = nil

		_, _, err = s.StoreBlock(context.Background(), blockAlternative2, "peer")
		require.NoError(t, err)
		require.True(t, blockRowExists(t, s, blockAlternative2.Hash().CloneBytes()))
		require.False(t, getOnMainChain(t, s, blockAlternative2.Hash().CloneBytes()))
	})

	t.Run("reorg flags are reconciled inside the transaction", func(t *testing.T) {
		s := newPostgresStore(t)
		storeBlocks(t, s, block1, block2, block3)

		forkBlock3 := createBlock3OnFork(blockAlternative2)
		forkBlock4 := createBlock3OnFork(forkBlock3)
		storeBlocks(t, s, blockAlternative2, forkBlock3, forkBlock4)

		require.True(t, getOnMainChain(t, s, block1.Hash().CloneBytes()))
		require.False(t, getOnMainChain(t, s, block2.Hash().CloneBytes()))
		require.False(t, getOnMainChain(t, s, block3.Hash().CloneBytes()))
		require.True(t, getOnMainChain(t, s, forkBlock4.Hash().CloneBytes()))

		var flagged int
		require.NoError(t, s.db.QueryRow(`SELECT count(*) FROM blocks WHERE on_main_chain = true`).Scan(&flagged))
		require.Equal(t, 5, flagged)
	})
}
