package utxoset

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/stretchr/testify/require"
)

// TestSpendAndCreateMultiGivesBackTheSpendsOfAWriteThatStopped is the review's request: a write
// that stops between its two steps, with no retry of the block after it, must not keep the
// parent's coins spent by a transaction that was never written. Each subtest stops the write
// once and then spends the same coin with a different transaction, which must succeed.
func TestSpendAndCreateMultiGivesBackTheSpendsOfAWriteThatStopped(t *testing.T) {
	failure := errors.NewProcessingError("injected failure")

	t.Run("a spend of an output outside the list, step 2 fails", func(t *testing.T) {
		s, ctx := newUncheckpointedStore(t)
		parent := storedParent(t, s, ctx)
		child := spendOutput(t, parent, 0, 1)

		multiFault = func(at string) error {
			if at == "spent" {
				return failure
			}

			return nil
		}

		_, err := s.SpendAndCreateMulti(ctx, []*bt.Tx{child}, 1_100, utxo.WithIgnoreLocked(true))
		multiFault = nil
		require.ErrorIs(t, err, failure)

		requireCoinGivenBack(t, s, ctx, parent, child)
	})

	t.Run("a spend of an output outside the list, the caller cancels between the steps", func(t *testing.T) {
		s, ctx := newUncheckpointedStore(t)
		parent := storedParent(t, s, ctx)
		child := spendOutput(t, parent, 0, 1)

		callCtx, cancel := context.WithCancel(ctx)
		defer cancel()

		multiFault = func(at string) error {
			if at == "spent" {
				cancel()
			}

			return nil
		}

		_, err := s.SpendAndCreateMulti(callCtx, []*bt.Tx{child}, 1_100, utxo.WithIgnoreLocked(true))
		multiFault = nil
		require.ErrorIs(t, err, context.Canceled)

		requireCoinGivenBack(t, s, ctx, parent, child)
	})

	t.Run("a netted spend of a parent in the list, a later group fails", func(t *testing.T) {
		s, ctx := newUncheckpointedStore(t)

		old := multiCreateChunkTxs
		multiCreateChunkTxs = 1

		t.Cleanup(func() { multiCreateChunkTxs = old })

		root := storedParent(t, s, ctx)
		parent := spendOutput(t, root, 0, 2)
		child := spendOutput(t, parent, 0, 1)

		// Group 0 writes parent, with the journal row of child's netted spend of its output 0.
		// The failure comes before group 1 writes child.
		multiFault = func(at string) error {
			if at == "created group 0" {
				return failure
			}

			return nil
		}

		_, err := s.SpendAndCreateMulti(ctx, []*bt.Tx{parent, child}, 1_100, utxo.WithIgnoreLocked(true))
		multiFault = nil
		require.ErrorIs(t, err, failure)

		require.Equal(t, 1, identityCount(t, s, ctx, parent), "parent was written, so it keeps its spend of root")
		require.Equal(t, 1, utxoCount(t, s, ctx, root), "root's output 0 stays spent by parent")

		requireCoinGivenBack(t, s, ctx, parent, child)
	})
}

// TestSpendAndCreateMultiKeepsTheSpendsOfACrash pins the other side: a crash runs no cleanup, so
// the spends stay for a retry of the block to complete. The give-back must not hide that from the
// crash tests, which depend on it.
func TestSpendAndCreateMultiKeepsTheSpendsOfACrash(t *testing.T) {
	s, ctx := newUncheckpointedStore(t)
	parent := storedParent(t, s, ctx)
	child := spendOutput(t, parent, 0, 1)

	multiFault = func(at string) error {
		if at == "spent" {
			return errMultiCrash
		}

		return nil
	}

	_, err := s.SpendAndCreateMulti(ctx, []*bt.Tx{child}, 1_100, utxo.WithIgnoreLocked(true))
	multiFault = nil
	require.ErrorIs(t, err, errMultiCrash)

	require.Equal(t, 1, utxoCount(t, s, ctx, parent))
	require.Equal(t, 1, journalRowsFor(t, s, ctx, parent, 0))
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

// requireCoinGivenBack asserts that child, which the stopped write did not write, holds no spend
// of parent's output 0, and that a different transaction can spend it.
func requireCoinGivenBack(t *testing.T, s *Store, ctx context.Context, parent, child *bt.Tx) {
	t.Helper()

	require.Equal(t, 0, identityCount(t, s, ctx, child), "child was not written")
	require.Equal(t, 2, utxoCount(t, s, ctx, parent), "parent's output 0 stayed spent by a transaction that does not exist")
	require.Equal(t, 0, journalRowsFor(t, s, ctx, parent, 0))

	rival := spendOutput(t, parent, 0, 2)
	_, _, err := s.SpendAndCreate(ctx, rival, 1_101, utxo.WithIgnoreLocked(true))
	require.NoError(t, err, "a valid spend of the coin was refused")
}
