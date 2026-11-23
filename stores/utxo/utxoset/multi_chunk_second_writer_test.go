package utxoset

import (
	"context"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/stretchr/testify/require"
)

// These tests pin one rule of the family-chunk write against a second writer of the same
// transaction T, which is the validator or another block that holds T: T is stored if and only
// if its parent coin is spent by T, and a rival spend of that coin is refused. Each test ends by
// asserting the store's state, not which function ran. No writer here knows whether the other
// exists or is alive, because every commit leaves a true state.

// requireCoinStaysSpentByChild asserts the safe end state when child is stored: its spend of
// parent's output 0 stands, and a rival spend of the coin is refused.
func requireCoinStaysSpentByChild(t *testing.T, s *Store, ctx context.Context, parent, child *bt.Tx) {
	t.Helper()

	require.Equal(t, 1, identityCount(t, s, ctx, child), "child is stored")
	require.Equal(t, 1, utxoCount(t, s, ctx, parent), "parent's output 0 came back live while child, which spends it, is stored")
	require.Equal(t, 1, journalRowsFor(t, s, ctx, parent, 0), "child's journal row for the coin is gone")

	rival := spendOutput(t, parent, 0, 2)
	_, _, err := s.SpendAndCreate(ctx, rival, 1_101, utxo.WithIgnoreLocked(true))
	require.Error(t, err, "a second spend of a coin a stored transaction holds was accepted")
}

// requireNoHalfWrite asserts that child is not stored with its parent coin live, and not spent
// without being stored: either it is stored and holds the coin, or it is not stored and the coin
// is live and unclaimed.
func requireNoHalfWrite(t *testing.T, s *Store, ctx context.Context, parent, child *bt.Tx) {
	t.Helper()

	if identityCount(t, s, ctx, child) == 1 {
		requireCoinStaysSpentByChild(t, s, ctx, parent, child)
		return
	}

	require.Equal(t, 2, utxoCount(t, s, ctx, parent), "child is not stored, so parent's output 0 must be live")
	require.Equal(t, 0, journalRowsFor(t, s, ctx, parent, 0))
}

// TestSpendAndCreateMultiKeepsTheSpendsOfAStoredTransaction is the review's two orders of a
// second writer of T, and the commit the client did not hear about.
func TestSpendAndCreateMultiKeepsTheSpendsOfAStoredTransaction(t *testing.T) {
	failure := errors.NewProcessingError("injected failure")

	t.Run("T stored before the multi-write, which finds it stored", func(t *testing.T) {
		s, ctx := newUncheckpointedStore(t)
		parent := storedParent(t, s, ctx)
		child := spendOutput(t, parent, 0, 1)

		_, _, err := s.SpendAndCreate(ctx, child, 1_100, utxo.WithIgnoreLocked(true))
		require.NoError(t, err)

		results, err := s.SpendAndCreateMulti(ctx, []*bt.Tx{child}, 1_100, utxo.WithIgnoreLocked(true))
		require.NoError(t, err)
		require.Equal(t, utxo.MultiTxExisted, results[0].Status)

		requireCoinStaysSpentByChild(t, s, ctx, parent, child)
	})

	t.Run("the multi-write stops with its spend made, then the other writer stores T", func(t *testing.T) {
		s, ctx := newUncheckpointedStore(t)
		parent := storedParent(t, s, ctx)
		child := spendOutput(t, parent, 0, 1)

		multiChunkFault = func(_ int, stage string) error {
			if stage == "spent" {
				return failure
			}

			return nil
		}

		t.Cleanup(func() { multiChunkFault = nil })

		_, err := s.SpendAndCreateMulti(ctx, []*bt.Tx{child}, 1_100, utxo.WithIgnoreLocked(true))
		multiChunkFault = nil
		require.ErrorIs(t, err, failure)

		// The chunk rolled back whole: T is not stored and its coin is live.
		requireNoHalfWrite(t, s, ctx, parent, child)
		require.Equal(t, 0, identityCount(t, s, ctx, child))

		// The other writer now stores T, and the coin is spent by it.
		_, _, err = s.SpendAndCreate(ctx, spendOutput(t, parent, 0, 1), 1_100, utxo.WithIgnoreLocked(true))
		require.NoError(t, err)

		requireCoinStaysSpentByChild(t, s, ctx, parent, child)
	})

	t.Run("the commit lands on the server and the client sees an error", func(t *testing.T) {
		s, ctx := newUncheckpointedStore(t)
		parent := storedParent(t, s, ctx)
		child := spendOutput(t, parent, 0, 1)

		multiCreateAfterCommit = func() error { return failure }

		t.Cleanup(func() { multiCreateAfterCommit = nil })

		_, err := s.SpendAndCreateMulti(ctx, []*bt.Tx{child}, 1_100, utxo.WithIgnoreLocked(true))
		multiCreateAfterCommit = nil
		require.ErrorIs(t, err, failure)

		requireCoinStaysSpentByChild(t, s, ctx, parent, child)

		// The caller retries the list: T is stored, so it is Existed, and nothing changes.
		results, err := s.SpendAndCreateMulti(ctx, []*bt.Tx{child}, 1_100, utxo.WithIgnoreLocked(true))
		require.NoError(t, err)
		require.Equal(t, utxo.MultiTxExisted, results[0].Status)

		requireCoinStaysSpentByChild(t, s, ctx, parent, child)
	})
}

// TestSpendAndCreateMultiAgainstASecondMultiWrite covers two multi-writes of the same T that
// overlap in time. The first holds T's lock from before its spend to its commit, so the second
// waits, and then finds whatever the first left: T stored, or nothing.
func TestSpendAndCreateMultiAgainstASecondMultiWrite(t *testing.T) {
	failure := errors.NewProcessingError("injected failure")

	// overlap runs the first write with its chunk paused after the coin is deleted, starts the
	// second write there, and waits long enough for a second write that did not wait on the lock
	// to have finished. Then it lets the first write finish with firstErr.
	overlap := func(t *testing.T, s *Store, ctx context.Context, parent, child *bt.Tx, firstErr error) (first, second []utxo.SpendAndCreateMultiResult, errFirst, errSecond error) {
		t.Helper()

		secondDone := make(chan struct{})

		var (
			secondResults []utxo.SpendAndCreateMultiResult
			secondErr     error
			fired         bool
		)

		multiChunkFault = func(_ int, stage string) error {
			if stage != "spent" || fired {
				return nil
			}

			fired = true

			go func() {
				secondResults, secondErr = s.SpendAndCreateMulti(context.Background(), []*bt.Tx{spendOutput(t, parent, 0, 1)}, 1_100, utxo.WithIgnoreLocked(true))
				close(secondDone)
			}()

			select {
			case <-secondDone:
				t.Error("the second write finished while the first held T's lock with its coin deleted")
			case <-time.After(400 * time.Millisecond):
			}

			return firstErr
		}

		t.Cleanup(func() { multiChunkFault = nil })

		first, errFirst = s.SpendAndCreateMulti(ctx, []*bt.Tx{child}, 1_100, utxo.WithIgnoreLocked(true))

		// The second write reads the hook until it ends, so the hook stays until then.
		<-secondDone

		multiChunkFault = nil

		return first, secondResults, errFirst, secondErr
	}

	t.Run("the first commits; the second finds T stored and spends nothing", func(t *testing.T) {
		s, ctx := newUncheckpointedStore(t)
		parent := storedParent(t, s, ctx)
		child := spendOutput(t, parent, 0, 1)

		first, second, errFirst, errSecond := overlap(t, s, ctx, parent, child, nil)
		require.NoError(t, errFirst)
		require.NoError(t, errSecond)
		require.Equal(t, utxo.MultiTxCreated, first[0].Status)
		require.Equal(t, utxo.MultiTxExisted, second[0].Status)

		requireCoinStaysSpentByChild(t, s, ctx, parent, child)
	})

	t.Run("the first stops; the second then writes T whole", func(t *testing.T) {
		s, ctx := newUncheckpointedStore(t)
		parent := storedParent(t, s, ctx)
		child := spendOutput(t, parent, 0, 1)

		_, second, errFirst, errSecond := overlap(t, s, ctx, parent, child, failure)
		require.ErrorIs(t, errFirst, failure)
		require.NoError(t, errSecond)
		require.Equal(t, utxo.MultiTxCreated, second[0].Status, "the first write rolled back, so the coin was free")

		requireCoinStaysSpentByChild(t, s, ctx, parent, child)
	})
}

// TestSpendAndCreateWaitsOnAMultiWriteOfTheSameTransaction is the same overlap with the second
// writer on the single and the batched SpendAndCreate path: it takes T's lock before its spends,
// so it waits for the chunk and then finds T stored.
func TestSpendAndCreateWaitsOnAMultiWriteOfTheSameTransaction(t *testing.T) {
	for _, tc := range []struct {
		name    string
		batcher int
	}{
		{name: "single path", batcher: 1},
		{name: "batched path", batcher: 4},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, ctx := newTestStoreWith(t, func(ts *settings.Settings) {
				withCheckpoints(ts, nil)
				ts.UtxoStore.StoreBatcherSize = tc.batcher
			})
			require.Equal(t, tc.batcher > 1, s.spendAndCreateBatcher != nil)

			parent := storedParent(t, s, ctx)
			child := spendOutput(t, parent, 0, 1)

			done := make(chan error, 1)
			finished := make(chan struct{})
			fired := false

			multiChunkFault = func(_ int, stage string) error {
				if stage != "spent" || fired {
					return nil
				}

				fired = true

				go func() {
					_, _, err := s.SpendAndCreate(context.Background(), spendOutput(t, parent, 0, 1), 1_100, utxo.WithIgnoreLocked(true))
					done <- err

					close(finished)
				}()

				select {
				case <-finished:
					t.Error("SpendAndCreate finished while the chunk held T's lock with its coin deleted")
				case <-time.After(400 * time.Millisecond):
				}

				return nil
			}

			t.Cleanup(func() { multiChunkFault = nil })

			results, err := s.SpendAndCreateMulti(ctx, []*bt.Tx{child}, 1_100, utxo.WithIgnoreLocked(true))
			multiChunkFault = nil
			require.NoError(t, err)
			require.Equal(t, utxo.MultiTxCreated, results[0].Status)

			// The single write finds T stored and reports it, with the spend the chunk made
			// left in place.
			err = <-done
			if err != nil {
				require.ErrorIs(t, err, errors.ErrTxExists)
			}

			requireCoinStaysSpentByChild(t, s, ctx, parent, child)
		})
	}
}

// A chunk holds the advisory lock of every transaction it writes from before its spends to its
// commit. A rival that only creates the transaction, with no spend whose row lock would make it
// wait anyway, must wait for the chunk, and then find the transaction stored.
func TestSpendAndCreateMultiHoldsTheLockOfEachTransactionItWrites(t *testing.T) {
	s, ctx := newUncheckpointedStore(t)

	parent := storedParent(t, s, ctx)
	child := spendOutput(t, parent, 0, 1)

	done := make(chan error, 1)
	finished := make(chan struct{})
	fired := false

	multiChunkFault = func(_ int, stage string) error {
		if stage != "spent" || fired {
			return nil
		}

		fired = true

		go func() {
			_, _, err := s.SpendAndCreate(context.Background(), spendOutput(t, parent, 0, 1), 1_100, utxo.WithCreateOnly())
			done <- err

			close(finished)
		}()

		select {
		case <-finished:
			t.Error("a create of T finished while the chunk held T's lock")
		case <-time.After(400 * time.Millisecond):
		}

		return nil
	}

	t.Cleanup(func() { multiChunkFault = nil })

	results, err := s.SpendAndCreateMulti(ctx, []*bt.Tx{child}, 1_100, utxo.WithIgnoreLocked(true))
	require.NoError(t, err)
	require.Equal(t, utxo.MultiTxCreated, results[0].Status)

	require.ErrorIs(t, <-done, errors.ErrTxExists, "the rival found T stored")
	multiChunkFault = nil

	requireCoinStaysSpentByChild(t, s, ctx, parent, child)
}
