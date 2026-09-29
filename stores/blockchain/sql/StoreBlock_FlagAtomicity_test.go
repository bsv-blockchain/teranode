package sql

import (
	"context"
	"fmt"
	"net/url"
	"testing"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/blockchain/options"
	"github.com/bsv-blockchain/teranode/test/utils/postgres"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/bsv-blockchain/teranode/util/usql"
	"github.com/jackc/pgx/v5/pgconn"
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
// The injected error is a teranode error carrying only a message, so classification
// rests on the message pattern. TestStoreBlockRetriesARawDriverErrorByItsType covers a
// driver error classified by its type. The sqlitememory store has retry enabled by
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

// TestStoreBlockRetriesARawDriverErrorByItsType: RetryTx classifies a driver error by
// its concrete type, so the fork path must hand it the error before any teranode
// wrapping, which keeps the message and drops the type. A serialization failure
// (SQLSTATE 40001) is the case that shows it: its message matches none of the
// retriable patterns, so only the type check can retry it.
func TestStoreBlockRetriesARawDriverErrorByItsType(t *testing.T) {
	s := newOnMainChainTestStore(t)

	storeBlocks(t, s, block1, block2, block3)

	calls := 0
	s.reconcileHook = func() error {
		calls++
		if calls == 1 {
			return &pgconn.PgError{Code: usql.PgErrSerializationFail, Message: "could not serialize access"}
		}

		return nil
	}

	_, _, err := s.StoreBlock(context.Background(), blockAlternative2, "peer")
	require.NoError(t, err, "a serialization failure is retried by its SQLSTATE")
	require.Equal(t, 2, calls, "the whole transaction ran twice")

	require.True(t, blockRowExists(t, s, blockAlternative2.Hash().CloneBytes()))
	require.False(t, getOnMainChain(t, s, blockAlternative2.Hash().CloneBytes()), "the fork is off-chain")
	require.True(t, getOnMainChain(t, s, block3.Hash().CloneBytes()), "the longer chain keeps the flag")
}

// TestStoreBlockRolledBackForkLeavesNoTimestampInTheCache: the median-time-past cache
// must only describe committed rows. A fork attempt that rolls back must leave the
// cached timestamps of the main chain as they were, or the next block's MTP is
// computed from a block that is not in the database.
func TestStoreBlockRolledBackForkLeavesNoTimestampInTheCache(t *testing.T) {
	s := newOnMainChainTestStore(t)

	storeBlocks(t, s, block1, block2, block3)

	want := []uint32{block1.Header.Timestamp, block2.Header.Timestamp, block3.Header.Timestamp}
	require.Equal(t, want, s.blockTimestampCache.GetRange(1, 3), "precondition: the main chain is cached")
	require.NotEqual(t, block2.Header.Timestamp, blockAlternative2.Header.Timestamp,
		"precondition: the fork's timestamp is distinguishable")

	s.reconcileHook = func() error { return errors.NewStorageError("injected reconciliation failure") }

	_, _, err := s.StoreBlock(context.Background(), blockAlternative2, "peer")
	require.Error(t, err)
	require.False(t, blockRowExists(t, s, blockAlternative2.Hash().CloneBytes()))

	require.Equal(t, want, s.blockTimestampCache.GetRange(1, 3),
		"the rolled-back fork must not replace or evict cached main-chain timestamps")
}

// TestStoreBlockInvalidWriteDoesNotDependOnTheReconciliation: an invalid row is written
// with on_main_chain=false and is never in best_block's lineage, so the reconciliation
// has nothing to do for it. It must not run, or a failure there would roll back the
// record that the block is invalid, and the block would be re-validated on every
// re-announcement. Covers both an explicit WithInvalid and invalidity inherited from
// an invalid parent.
func TestStoreBlockInvalidWriteDoesNotDependOnTheReconciliation(t *testing.T) {
	s := newOnMainChainTestStore(t)

	storeBlocks(t, s, block1, block2, block3)

	calls := 0
	s.reconcileHook = func() error {
		calls++
		return errors.NewStorageError("injected reconciliation failure")
	}

	_, _, err := s.StoreBlock(context.Background(), blockAlternative2, "peer", options.WithInvalid(true))
	require.NoError(t, err, "an explicitly invalid block is recorded despite a failing reconciliation")
	require.True(t, blockRowExists(t, s, blockAlternative2.Hash().CloneBytes()))

	child := createBlock3OnFork(blockAlternative2)

	_, _, err = s.StoreBlock(context.Background(), child, "peer")
	require.NoError(t, err, "a child inheriting invalidity is recorded despite a failing reconciliation")
	require.True(t, blockRowExists(t, s, child.Hash().CloneBytes()))

	require.Equal(t, 0, calls, "the reconciliation does not run for an invalid row")

	var invalid bool
	require.NoError(t, s.db.QueryRow(`SELECT invalid FROM blocks WHERE hash = $1`, child.Hash().CloneBytes()).Scan(&invalid))
	require.True(t, invalid, "the child is stored invalid because its parent is")

	require.False(t, getOnMainChain(t, s, blockAlternative2.Hash().CloneBytes()))
	require.False(t, getOnMainChain(t, s, child.Hash().CloneBytes()))
	require.True(t, getOnMainChain(t, s, block3.Hash().CloneBytes()), "the valid chain keeps the flag")
}

// competingChildOfBlock1 returns a sibling of block2 that is distinct for each i, so a
// run of them are all forks while block3 stays the best block.
func competingChildOfBlock1(i uint32) *model.Block {
	return &model.Block{
		Header: &model.BlockHeader{
			Version:        blockAlternative2.Header.Version,
			Timestamp:      blockAlternative2.Header.Timestamp,
			Nonce:          blockAlternative2.Header.Nonce + i + 1,
			HashPrevBlock:  blockAlternative2.Header.HashPrevBlock,
			HashMerkleRoot: blockAlternative2.Header.HashMerkleRoot,
			Bits:           blockAlternative2.Header.Bits,
		},
		CoinbaseTx:       blockAlternative2.CoinbaseTx,
		TransactionCount: blockAlternative2.TransactionCount,
		Subtrees:         blockAlternative2.Subtrees,
	}
}

// BenchmarkStoreBlockForkPath measures what the fork path costs now that it carries a
// transaction and the reconciliation. Every block is a child of block1 while block3 is
// the best, so each one is a fork and takes the transactional branch; the extend path
// is not measured here.
//
// UseInMemoryChainCheck is pinned in both directions rather than read from local
// settings, because it changes what a fork store does: with it on, every fork also
// rebuilds the forked set after the write. Leaving it to the environment made the
// numbers depend on whichever settings_local.conf the run happened to see.
func BenchmarkStoreBlockForkPath(b *testing.B) {
	for _, inMemory := range []bool{false, true} {
		b.Run(fmt.Sprintf("inMemoryChainCheck=%v", inMemory), func(b *testing.B) {
			benchmarkStoreBlockForkPath(b, inMemory)
		})
	}
}

func benchmarkStoreBlockForkPath(b *testing.B, inMemoryChainCheck bool) {
	s := newOnMainChainTestStoreWith(b, func(tSettings *settings.Settings) {
		tSettings.BlockChain.UseInMemoryChainCheck = inMemoryChainCheck
	})
	require.Equal(b, inMemoryChainCheck, s.useInMemoryChainCheck, "the pinned setting must reach the store")
	storeBlocks(b, s, block1, block2, block3)

	forks := make([]*model.Block, 0, b.N)
	for i := uint32(0); i < uint32(b.N); i++ { //nolint:gosec // b.N is a small benchmark count
		forks = append(forks, competingChildOfBlock1(i))
	}

	b.ResetTimer()

	for i := range forks {
		if _, _, err := s.StoreBlock(context.Background(), forks[i], "peer"); err != nil {
			b.Fatalf("store fork block %d: %v", i, err)
		}
	}

	b.StopTimer()

	// Only the fork path writes these rows off-chain, so this proves the branch ran.
	for i, blk := range forks {
		if getOnMainChain(b, s, blk.Hash().CloneBytes()) {
			b.Fatalf("fork block %d is flagged on the main chain; the benchmark did not take the fork path", i)
		}
	}

	if !getOnMainChain(b, s, block3.Hash().CloneBytes()) {
		b.Fatal("block3 must stay the best block")
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

// TestStoreBlockLabelsAFailedReconciliation: an error the reconciliation itself returned
// is reported as the reconciliation's, so whoever chases it starts at the right query.
// The injected error is a raw, non-retriable driver error, so it reaches StoreBlock on
// the first attempt with no teranode type of its own to pass through.
func TestStoreBlockLabelsAFailedReconciliation(t *testing.T) {
	s := newOnMainChainTestStore(t)

	storeBlocks(t, s, block1, block2, block3)

	s.reconcileHook = func() error { return &pgconn.PgError{Code: "23505", Message: "injected"} }

	_, _, err := s.StoreBlock(context.Background(), blockAlternative2, "peer")
	require.Error(t, err)
	require.Contains(t, err.Error(), "StoreBlock: reconcileOnMainChain")
}

// TestStoreBlockDoesNotBlameTheReconciliationForAFailureAfterIt is ChiR8 on PR 1763. A
// retriable reconciliation failure sends RetryTx into its backoff, and a context
// cancelled there ends the call before the reconciliation runs again. The error that
// comes back is the context's, not the reconciliation's, so it must not carry the
// reconciliation's label. A flag set before the reconciliation and reset only at the
// start of the next attempt was still set here, and labelled it anyway.
func TestStoreBlockDoesNotBlameTheReconciliationForAFailureAfterIt(t *testing.T) {
	s := newOnMainChainTestStore(t)

	storeBlocks(t, s, block1, block2, block3)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	calls := 0
	s.reconcileHook = func() error {
		calls++
		cancel()

		// SQLSTATE 40001 is retriable, so RetryTx waits out a backoff before the next
		// attempt, and the cancelled context ends that wait.
		return &pgconn.PgError{Code: "40001", Message: "injected serialization failure"}
	}

	_, _, err := s.StoreBlock(ctx, blockAlternative2, "peer")
	require.Error(t, err)
	require.Equal(t, 1, calls, "the cancelled backoff must stop the retry")
	require.Contains(t, err.Error(), context.Canceled.Error(), "the context's error is what is returned")
	require.NotContains(t, err.Error(), "StoreBlock: reconcileOnMainChain",
		"a failure after the reconciliation attempt must not be labelled as the reconciliation's")

	require.False(t, blockRowExists(t, s, blockAlternative2.Hash().CloneBytes()),
		"the rolled-back attempt must leave no row behind")
}

// TestSameErrorIsIdentityAndNeverPanics: sameError must not match two distinct errors
// that share a teranode code, which errors.Is would, and must not panic on a pair of
// uncomparable values of one type, which == would.
func TestSameErrorIsIdentityAndNeverPanics(t *testing.T) {
	a := errors.NewStorageError("a")
	b := errors.NewStorageError("b")

	require.True(t, sameError(a, a))
	require.False(t, sameError(a, b), "same code, different value")
	require.False(t, sameError(a, context.Canceled), "different types")

	require.NotPanics(t, func() {
		require.False(t, sameError(uncomparableErr{"x"}, uncomparableErr{"x"}))
	})
}

type uncomparableErr []string

func (e uncomparableErr) Error() string { return "uncomparable" }
