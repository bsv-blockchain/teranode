package utxoset

import (
	"context"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	spendpkg "github.com/bsv-blockchain/teranode/stores/utxo/spend"
	"github.com/stretchr/testify/require"
)

// These tests pin the give-back of a stopped SpendAndCreateMulti against a second writer of the
// same transaction T: the validator, or another block that holds T. A write that stops must never
// leave T stored with its parent coin live, because a second transaction could then spend that
// coin. Each test ends by asserting the store's state, not which function ran.

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

// requireNoHalfWrite asserts that child is not stored with its parent coin live: either it is
// stored and holds the coin, or it is not stored and the coin is live and unclaimed.
func requireNoHalfWrite(t *testing.T, s *Store, ctx context.Context, parent, child *bt.Tx) {
	t.Helper()

	if identityCount(t, s, ctx, child) == 1 {
		requireCoinStaysSpentByChild(t, s, ctx, parent, child)
		return
	}

	require.Equal(t, 2, utxoCount(t, s, ctx, parent), "child is not stored, so parent's output 0 must be live")
	require.Equal(t, 0, journalRowsFor(t, s, ctx, parent, 0))
}

// TestSpendAndCreateMultiGiveBackKeepsTheSpendsOfAStoredTransaction is the review's two orders:
// the other writer stores T before the multi-write's step 1, or between its steps. Then a
// stopped write that the caller believed did not commit.
func TestSpendAndCreateMultiGiveBackKeepsTheSpendsOfAStoredTransaction(t *testing.T) {
	failure := errors.NewProcessingError("injected failure")

	t.Run("T stored before step 1, which repeats its spend; step 2 stops", func(t *testing.T) {
		s, ctx := newUncheckpointedStore(t)
		parent := storedParent(t, s, ctx)
		child := spendOutput(t, parent, 0, 1)

		_, _, err := s.SpendAndCreate(ctx, child, 1_100, utxo.WithIgnoreLocked(true))
		require.NoError(t, err)

		multiFault = func(at string) error {
			if at == "spent" {
				return failure
			}

			return nil
		}

		_, err = s.SpendAndCreateMulti(ctx, []*bt.Tx{child}, 1_100, utxo.WithIgnoreLocked(true))
		multiFault = nil
		require.ErrorIs(t, err, failure)

		requireCoinStaysSpentByChild(t, s, ctx, parent, child)
	})

	t.Run("T stored between step 1, which took the coin, and step 2, which stops", func(t *testing.T) {
		s, ctx := newUncheckpointedStore(t)
		parent := storedParent(t, s, ctx)
		child := spendOutput(t, parent, 0, 1)

		multiFault = func(at string) error {
			if at != "spent" {
				return nil
			}

			// The other writer finds the coin spent by child, settles that as a repeat, and
			// stores child.
			_, _, err := s.SpendAndCreate(ctx, spendOutput(t, parent, 0, 1), 1_100, utxo.WithIgnoreLocked(true))
			require.NoError(t, err)

			return failure
		}

		_, err := s.SpendAndCreateMulti(ctx, []*bt.Tx{child}, 1_100, utxo.WithIgnoreLocked(true))
		multiFault = nil
		require.ErrorIs(t, err, failure)

		requireCoinStaysSpentByChild(t, s, ctx, parent, child)
	})

	t.Run("step 2 commits on the server and the client sees an error", func(t *testing.T) {
		s, ctx := newUncheckpointedStore(t)
		parent := storedParent(t, s, ctx)
		child := spendOutput(t, parent, 0, 1)

		multiCreateAfterCommit = func() error { return failure }

		_, err := s.SpendAndCreateMulti(ctx, []*bt.Tx{child}, 1_100, utxo.WithIgnoreLocked(true))
		multiCreateAfterCommit = nil
		require.ErrorIs(t, err, failure)

		requireCoinStaysSpentByChild(t, s, ctx, parent, child)
	})
}

// TestSpendAndCreateMultiAgainstASecondMultiWrite covers two multi-writes of the same T. Step 1
// and step 2 are separate database transactions, so a lock held in one does not carry to the
// other, and the second write's step 1 repeats the first's spend without taking anything.
func TestSpendAndCreateMultiAgainstASecondMultiWrite(t *testing.T) {
	failure := errors.NewProcessingError("injected failure")

	t.Run("the write that repeated the spend stops; it must not give back the other's spend", func(t *testing.T) {
		s, ctx := newUncheckpointedStore(t)
		parent := storedParent(t, s, ctx)
		child := spendOutput(t, parent, 0, 1)

		calls := 0
		multiFault = func(at string) error {
			if at != "spent" {
				return nil
			}

			calls++

			if calls == 1 {
				// A has taken the coin. B repeats the spend and stops.
				_, err := s.SpendAndCreateMulti(ctx, []*bt.Tx{spendOutput(t, parent, 0, 1)}, 1_100, utxo.WithIgnoreLocked(true))
				require.ErrorIs(t, err, failure)

				return nil
			}

			return failure
		}

		results, err := s.SpendAndCreateMulti(ctx, []*bt.Tx{child}, 1_100, utxo.WithIgnoreLocked(true))
		multiFault = nil
		require.NoError(t, err)
		require.Equal(t, utxo.MultiTxCreated, results[0].Status)

		requireCoinStaysSpentByChild(t, s, ctx, parent, child)
	})

	t.Run("the write that took the coin gives it back; the one that repeated must not create T", func(t *testing.T) {
		s, ctx := newUncheckpointedStore(t)
		parent := storedParent(t, s, ctx)
		child := spendOutput(t, parent, 0, 1)
		childID := child.TxIDChainHash()

		calls := 0
		multiFault = func(at string) error {
			if at != "spent" {
				return nil
			}

			calls++

			switch calls {
			case 1:
				// A has taken the coin. B repeats the spend, and before its step 2 A stops
				// and gives the coin back, which it may: child is not stored yet.
				_, err := s.SpendAndCreateMulti(ctx, []*bt.Tx{spendOutput(t, parent, 0, 1)}, 1_100, utxo.WithIgnoreLocked(true))
				require.Error(t, err, "B created child although the spend it relied on was given back")

				return failure
			case 2:
				require.NoError(t, s.giveBackUnlessWritten(ctx, []*utxo.Spend{{
					TxID: parent.TxIDChainHash(), Vout: 0, SpendingData: spendpkg.NewSpendingData(childID, 0),
				}}))
			}

			return nil
		}

		_, err := s.SpendAndCreateMulti(ctx, []*bt.Tx{child}, 1_100, utxo.WithIgnoreLocked(true))
		multiFault = nil
		require.ErrorIs(t, err, failure)

		requireNoHalfWrite(t, s, ctx, parent, child)
	})
}

// TestSpendAndCreateHoldsTheLockAcrossARepeatedSpend is the review's second part: SpendAndCreate
// takes the transaction's lock before its spends. A SpendAndCreate whose spend repeats a stopped
// multi-write's must not have that spend given back between its spend and its create. The
// SpendAndCreate pauses there; the give-back must wait for it.
func TestSpendAndCreateHoldsTheLockAcrossARepeatedSpend(t *testing.T) {
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
			failure := errors.NewProcessingError("injected failure")

			repeated := make(chan struct{})
			done := make(chan error, 1)

			spendAndCreateBeforeCreate = func() {
				close(repeated)
				// Long enough for a give-back that does not wait to finish well inside it.
				time.Sleep(500 * time.Millisecond)
			}

			t.Cleanup(func() { spendAndCreateBeforeCreate = nil })

			multiFault = func(at string) error {
				if at != "spent" {
					return nil
				}

				go func() {
					_, _, err := s.SpendAndCreate(context.Background(), spendOutput(t, parent, 0, 1), 1_100, utxo.WithIgnoreLocked(true))
					done <- err
				}()

				<-repeated

				return failure
			}

			_, err := s.SpendAndCreateMulti(ctx, []*bt.Tx{child}, 1_100, utxo.WithIgnoreLocked(true))
			multiFault = nil
			require.ErrorIs(t, err, failure)
			require.NoError(t, <-done)

			spendAndCreateBeforeCreate = nil

			requireCoinStaysSpentByChild(t, s, ctx, parent, child)
		})
	}
}
