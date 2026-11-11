package utxoset

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/stretchr/testify/require"
)

// forceChunkOfOne puts every transaction of a list in its own chunk for the test.
func forceChunkOfOne(t *testing.T) {
	t.Helper()

	old := multiCreateChunkTxs
	multiCreateChunkTxs = 1

	t.Cleanup(func() { multiCreateChunkTxs = old })
}

// TestSpendAndCreateMultiAStoppedWriteLeavesEveryCommittedChunkWhole is the review's request in
// the family-chunk form: a write that fails, or is cancelled, partway through a list leaves each
// chunk either committed whole or not there at all. A transaction is stored if and only if its
// coins are spent, so a different transaction can spend the coin of any transaction that was not
// stored, with no give-back to run.
func TestSpendAndCreateMultiAStoppedWriteLeavesEveryCommittedChunkWhole(t *testing.T) {
	failure := errors.NewProcessingError("injected failure")

	t.Run("a chunk fails with its spend made; an earlier chunk stays committed", func(t *testing.T) {
		s, ctx := newUncheckpointedStore(t)
		forceChunkOfOne(t)

		root := storedParent(t, s, ctx)
		parent := spendOutput(t, root, 0, 2)
		child := spendOutput(t, parent, 0, 1)

		// Chunk 0 holds parent, chunk 1 holds child, which waits for it.
		multiChunkFault = func(chunk int, stage string) error {
			if chunk == 1 && stage == "spent" {
				return failure
			}

			return nil
		}

		t.Cleanup(func() { multiChunkFault = nil })

		_, err := s.SpendAndCreateMulti(ctx, []*bt.Tx{parent, child}, 1_100, utxo.WithIgnoreLocked(true))
		multiChunkFault = nil
		require.ErrorIs(t, err, failure)

		require.Equal(t, 1, identityCount(t, s, ctx, parent), "parent's chunk committed, so parent is stored")
		require.Equal(t, 1, utxoCount(t, s, ctx, root), "root's output 0 stays spent by parent, which is stored")

		// The cut edge wrote parent's output 0 as a real coin, and child's chunk rolled back.
		requireCoinStillSpendable(t, s, ctx, parent, child)
	})

	t.Run("the caller cancels between chunks", func(t *testing.T) {
		s, ctx := newUncheckpointedStore(t)
		forceChunkOfOne(t)

		root := storedParent(t, s, ctx)
		parent := spendOutput(t, root, 0, 2)
		child := spendOutput(t, parent, 0, 1)

		callCtx, cancel := context.WithCancel(ctx)
		defer cancel()

		multiChunkFault = func(chunk int, stage string) error {
			if chunk == 0 && stage == "committed" {
				cancel()
			}

			return nil
		}

		t.Cleanup(func() { multiChunkFault = nil })

		_, err := s.SpendAndCreateMulti(callCtx, []*bt.Tx{parent, child}, 1_100, utxo.WithIgnoreLocked(true))
		multiChunkFault = nil
		require.ErrorIs(t, err, context.Canceled)

		require.Equal(t, 1, identityCount(t, s, ctx, parent), "parent's chunk committed before the cancel")
		requireCoinStillSpendable(t, s, ctx, parent, child)
	})

	t.Run("an independent chunk fails; every transaction is whole or absent", func(t *testing.T) {
		s, ctx := newUncheckpointedStore(t)
		forceChunkOfOne(t)

		a := storedParent(t, s, ctx)

		// Two unrelated transactions on two parents, each in its own chunk, run in parallel.
		pa := spendOutput(t, a, 0, 1)
		other := mkTx(t, 2, 6_000)
		_, err := s.Create(ctx, other, 1_000, utxo.WithMinedBlockInfo(utxo.MinedBlockInfo{BlockID: 1, BlockHeight: 1_000, OnLongestChain: true}))
		require.NoError(t, err)

		po := spendOutput(t, other, 0, 1)

		multiChunkFault = func(chunk int, stage string) error {
			if chunk == 1 && stage == "spent" {
				return failure
			}

			return nil
		}

		t.Cleanup(func() { multiChunkFault = nil })

		_, err = s.SpendAndCreateMulti(ctx, []*bt.Tx{pa, po}, 1_100, utxo.WithIgnoreLocked(true))
		multiChunkFault = nil
		require.ErrorIs(t, err, failure)

		// Chunk 1 is absent. Chunk 0 is either whole or, if it was never started, absent.
		requireNoHalfWrite(t, s, ctx, a, pa)
		require.Equal(t, 0, identityCount(t, s, ctx, po))
		requireCoinStillSpendable(t, s, ctx, other, po)
	})
}

// TestSpendAndCreateMultiACrashRollsBackTheOpenChunk is the crash form. A crash is the process
// gone: whatever the chunk had done in its open database transaction is rolled back by the
// server, and whatever earlier chunks committed stays. Nothing runs to clean up, and nothing
// needs to, so a retry of the list writes records identical to a clean run.
func TestSpendAndCreateMultiACrashRollsBackTheOpenChunk(t *testing.T) {
	for _, stage := range []string{"locked", "spent", "created"} {
		t.Run("crash at "+stage, func(t *testing.T) {
			s, ctx := newUncheckpointedStore(t)
			parent := storedParent(t, s, ctx)
			child := spendOutput(t, parent, 0, 1)

			multiChunkFault = func(_ int, at string) error {
				if at == stage {
					return errMultiCrash
				}

				return nil
			}

			t.Cleanup(func() { multiChunkFault = nil })

			_, err := s.SpendAndCreateMulti(ctx, []*bt.Tx{child}, 1_100, utxo.WithIgnoreLocked(true))
			multiChunkFault = nil
			require.ErrorIs(t, err, errMultiCrash)

			requireCoinStillSpendable(t, s, ctx, parent, child)
		})
	}
}

// storedParent stores a mined transaction with two outputs.
func storedParent(t *testing.T, s *Store, ctx context.Context) *bt.Tx {
	t.Helper()

	parent := mkTx(t, 2, 5_000)
	_, err := s.Create(ctx, parent, 1_000, utxo.WithMinedBlockInfo(
		utxo.MinedBlockInfo{BlockID: 1, BlockHeight: 1_000, OnLongestChain: true}))
	require.NoError(t, err)

	return parent
}

// requireCoinStillSpendable asserts that child, which the stopped write did not write, holds no spend
// of parent's output 0, and that a different transaction can spend it.
func requireCoinStillSpendable(t *testing.T, s *Store, ctx context.Context, parent, child *bt.Tx) {
	t.Helper()

	require.Equal(t, 0, identityCount(t, s, ctx, child), "child was not written")
	require.Equal(t, 2, utxoCount(t, s, ctx, parent), "parent's output 0 stayed spent by a transaction that does not exist")
	require.Equal(t, 0, journalRowsFor(t, s, ctx, parent, 0))

	rival := spendOutput(t, parent, 0, 2)
	_, _, err := s.SpendAndCreate(ctx, rival, 1_101, utxo.WithIgnoreLocked(true))
	require.NoError(t, err, "a valid spend of the coin was refused")
}
