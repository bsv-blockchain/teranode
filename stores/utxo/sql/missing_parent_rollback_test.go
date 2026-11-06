package sql

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	spendpkg "github.com/bsv-blockchain/teranode/stores/utxo/spend"
	"github.com/stretchr/testify/require"
)

// TestMissingParentRollsBackFreshSiblingSpend: a ghost C spends S:0, whose
// record survives, and G:0, whose record the pruner removed. The spend answers
// G with ErrTxNotFound, which the block paths read as a pruned replay and
// compensate by deleting C's record. DeleteCreated never reverses a ghost's
// spends, so the store must roll back the fresh spend of S:0 in the same call.
// Before the missing-parent answer was a rollback error, S:0 was left spent by
// a transaction the store no longer held.
//
// The other half: when S:0 already recorded C's spend, that is the confirmed,
// historical one, and the rollback must leave it alone.
func TestMissingParentRollsBackFreshSiblingSpend(t *testing.T) {
	forEachBackend(t, func(t *testing.T, ctx context.Context, store *Store) {
		for i, perRow := range []bool{false, true} {
			store.settings.UtxoStore.BatchSQLOperations = !perRow

			newParent := func(seed string) *bt.Tx {
				parent := bt.NewTx()
				require.NoError(t, parent.From("11111111111111111111111111111111111111111111111111111111111111"+seed, 0, "51", 30000))
				parent.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{0x51})
				require.NoError(t, parent.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 4000))

				return parent
			}

			suffix := string(rune('0' + i))
			survivor, gone := newParent("a"+suffix), newParent("b"+suffix)
			_, err := store.Create(ctx, survivor, 1000)
			require.NoError(t, err)

			ghost := bt.NewTx()
			for _, parent := range []*bt.Tx{survivor, gone} {
				require.NoError(t, ghost.From(parent.TxID(), 0, parent.Outputs[0].LockingScript.String(), parent.Outputs[0].Satoshis))
			}

			for j := range ghost.Inputs {
				ghost.Inputs[j].UnlockingScript = bscript.NewFromBytes([]byte{0x51})
			}

			require.NoError(t, ghost.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 3000))

			_, err = store.Spend(ctx, ghost, 1200, utxo.IgnoreFlags{SpenderCreatedByCaller: true})
			require.ErrorIs(t, err, errors.ErrTxNotFound, "control: the pruned parent answers not found")
			require.Nil(t, outputSpendingData(t, ctx, store, survivor, 0),
				"the fresh spend of the surviving parent must be rolled back (perRow=%v)", perRow)

			// Historical: the surviving output already records the ghost's spend.
			historical := spendpkg.NewSpendingData(ghost.TxIDChainHash(), 0).Bytes()
			_, err = store.db.ExecContext(ctx,
				"UPDATE outputs SET spending_data = $2 WHERE idx = 0 AND transaction_id IN (SELECT id FROM transactions WHERE hash = $1)",
				survivor.TxIDChainHash()[:], historical)
			require.NoError(t, err)

			_, err = store.Spend(ctx, ghost, 1200, utxo.IgnoreFlags{SpenderCreatedByCaller: true})
			require.ErrorIs(t, err, errors.ErrTxNotFound)
			require.Equal(t, historical, outputSpendingData(t, ctx, store, survivor, 0),
				"an idempotent match next to a missing parent is the historical spend and must survive (perRow=%v)", perRow)
		}
	})
}

// TestBlessedInputWithSpenderRecordKeepsTheSpends: T spends S:0 (present), G:0
// (whose record is gone) and X:0 (which another transaction already holds). T's
// own record exists, so the store blesses the missing parent and counts G:0 as
// succeeded, and X:0 answers ErrSpent, so the call fails.
//
// For an ordinary caller the rollback is gated on the spender having no record
// (#1214), and T has one, so S:0 stays spent by T: clearing it would clear a
// slot a live transaction owns. The bless needs that same record, so on this
// path a blessed input never meets a rollback that fires. Spend still keeps a
// blessed input out of the rollback set as a backstop: it wrote nothing and has
// no row, and Unspend answering NotFound for it used to discard the whole
// rollback's database transaction.
func TestBlessedInputWithSpenderRecordKeepsTheSpends(t *testing.T) {
	forEachBackend(t, func(t *testing.T, ctx context.Context, store *Store) {
		for i, perRow := range []bool{false, true} {
			store.settings.UtxoStore.BatchSQLOperations = !perRow

			newParent := func(seed string) *bt.Tx {
				parent := bt.NewTx()
				require.NoError(t, parent.From("22222222222222222222222222222222222222222222222222222222222222"+seed, 0, "51", 30000))
				parent.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{0x51})
				require.NoError(t, parent.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 4000))

				return parent
			}

			spending := func(parents ...*bt.Tx) *bt.Tx {
				tx := bt.NewTx()
				for _, parent := range parents {
					require.NoError(t, tx.From(parent.TxID(), 0, parent.Outputs[0].LockingScript.String(), parent.Outputs[0].Satoshis))
				}

				for j := range tx.Inputs {
					tx.Inputs[j].UnlockingScript = bscript.NewFromBytes([]byte{0x51})
				}

				require.NoError(t, tx.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 3000))

				return tx
			}

			suffix := string(rune('0' + i))
			survivor, gone, taken := newParent("a"+suffix), newParent("b"+suffix), newParent("c"+suffix)

			for _, parent := range []*bt.Tx{survivor, taken} {
				_, err := store.Create(ctx, parent, 1000)
				require.NoError(t, err)
			}

			other := spending(taken)
			_, err := store.Spend(ctx, other, 1100)
			require.NoError(t, err, "fixture: another transaction holds X:0")

			spender := spending(survivor, gone, taken)
			_, err = store.Create(ctx, spender, 1100, utxo.WithSkipExtendedInputs(true))
			require.NoError(t, err, "fixture: T's own record exists, so the missing parent is blessed")

			_, err = store.Spend(ctx, spender, 1200)
			require.ErrorIs(t, err, errors.ErrSpent, "X:0 is held by another transaction")
			require.NotErrorIs(t, err, errors.ErrTxNotFound, "control: the missing parent was blessed")
			require.Equal(t, spendpkg.NewSpendingData(spender.TxIDChainHash(), 0).Bytes(), outputSpendingData(t, ctx, store, survivor, 0),
				"T has a record, so an ordinary caller's spend of S:0 stays (perRow=%v)", perRow)
		}
	})
}

// TestRequireParentOutputsRollsBackLikeSpenderCreatedByCaller: the second wave
// of a WithBelowCheckpoint list spends with RequireParentOutputs, after its first
// wave wrote the spender's record. C spends S:0, whose record survives, and
// G:0, whose record the pruner removed, and C's own record already exists. The
// store must reverse the fresh spend of S:0 as it does for
// SpenderCreatedByCaller: the block path then deletes C's record, and a spend
// left in place would name a transaction the store no longer holds. The
// ordinary rule would probe for C's record, find the one the first wave wrote,
// and keep the spend.
func TestRequireParentOutputsRollsBackLikeSpenderCreatedByCaller(t *testing.T) {
	forEachBackend(t, func(t *testing.T, ctx context.Context, store *Store) {
		for i, perRow := range []bool{false, true} {
			store.settings.UtxoStore.BatchSQLOperations = !perRow

			newParent := func(seed string) *bt.Tx {
				parent := bt.NewTx()
				require.NoError(t, parent.From("33333333333333333333333333333333333333333333333333333333333333"+seed, 0, "51", 30000))
				parent.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{0x51})
				require.NoError(t, parent.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 4000))

				return parent
			}

			suffix := string(rune('0' + i))
			survivor, gone := newParent("a"+suffix), newParent("b"+suffix)
			_, err := store.Create(ctx, survivor, 1000)
			require.NoError(t, err)

			ghost := bt.NewTx()
			for _, parent := range []*bt.Tx{survivor, gone} {
				require.NoError(t, ghost.From(parent.TxID(), 0, parent.Outputs[0].LockingScript.String(), parent.Outputs[0].Satoshis))
			}

			for j := range ghost.Inputs {
				ghost.Inputs[j].UnlockingScript = bscript.NewFromBytes([]byte{0x51})
			}

			require.NoError(t, ghost.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 3000))

			_, err = store.Create(ctx, ghost, 1100, utxo.WithSkipExtendedInputs(true))
			require.NoError(t, err, "fixture: the first wave wrote C's record")

			_, err = store.Spend(ctx, ghost, 1200, utxo.IgnoreFlags{RequireParentOutputs: true})
			require.ErrorIs(t, err, errors.ErrTxNotFound, "control: the pruned parent answers not found")
			require.Nil(t, outputSpendingData(t, ctx, store, survivor, 0),
				"the fresh spend of the surviving parent must be rolled back (perRow=%v)", perRow)
		}
	})
}

// TestSecondWaveFailureKeepsALiveSpendersSpend: T spends S:0 and B:0 and was
// fully applied by an ordinary SpendAndCreate before this list ran, so its
// record exists and both outputs record T as spender. B is then flagged
// conflicting. A WithBelowCheckpoint list [S, T] answers T as existing in the
// first wave and re-spends it in the second with RequireParentOutputs: S:0 is
// an idempotent match and B:0 fails. The list did not write T's record, so the
// rollback must leave S:0 alone: a live transaction owns it.
func TestSecondWaveFailureKeepsALiveSpendersSpend(t *testing.T) {
	forEachBackend(t, func(t *testing.T, ctx context.Context, store *Store) {
		for i, perRow := range []bool{false, true} {
			store.settings.UtxoStore.BatchSQLOperations = !perRow

			newParent := func(seed string) *bt.Tx {
				parent := bt.NewTx()
				require.NoError(t, parent.From("44444444444444444444444444444444444444444444444444444444444444"+seed, 0, "51", 30000))
				parent.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{0x51})
				require.NoError(t, parent.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 4000))

				return parent
			}

			suffix := string(rune('0' + i))
			survivor, contested := newParent("a"+suffix), newParent("b"+suffix)

			for _, parent := range []*bt.Tx{survivor, contested} {
				_, err := store.Create(ctx, parent, 1000)
				require.NoError(t, err)
			}

			spender := bt.NewTx()
			for _, parent := range []*bt.Tx{survivor, contested} {
				require.NoError(t, spender.From(parent.TxID(), 0, parent.Outputs[0].LockingScript.String(), parent.Outputs[0].Satoshis))
			}

			for j := range spender.Inputs {
				spender.Inputs[j].UnlockingScript = bscript.NewFromBytes([]byte{0x51})
			}

			require.NoError(t, spender.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 3000))

			_, _, err := store.SpendAndCreate(ctx, spender, 1100)
			require.NoError(t, err, "fixture: T fully applied by an ordinary writer")

			spentByT := spendpkg.NewSpendingData(spender.TxIDChainHash(), 0).Bytes()
			require.Equal(t, spentByT, outputSpendingData(t, ctx, store, survivor, 0))

			_, err = store.db.ExecContext(ctx, "UPDATE transactions SET conflicting = true WHERE hash = $1", contested.TxIDChainHash()[:])
			require.NoError(t, err)

			results, err := store.SpendAndCreateMulti(ctx, []*bt.Tx{survivor, spender}, 1200,
				utxo.WithBelowCheckpoint(), utxo.WithIgnoreLocked(true))
			require.NoError(t, err)
			require.Len(t, results, 2)
			require.False(t, results[1].Created, "control: the list did not write T's record")
			require.ErrorIs(t, results[1].Err, errors.ErrTxConflicting, "control: B:0 fails the second-wave spend")

			require.Equal(t, spentByT, outputSpendingData(t, ctx, store, survivor, 0),
				"S:0 belongs to live T, whose record the list did not write (perRow=%v)", perRow)
		}
	})
}
