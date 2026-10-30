package aerospike_test

import (
	"testing"

	"github.com/bsv-blockchain/aerospike-client-go/v8"
	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// TestMissingParentRollsBackFreshSiblingSpend: a ghost C spends S:0, whose
// record survives, and G:0, whose record is gone. The spend answers G with
// ErrTxNotFound, which the block paths read as a pruned replay and compensate
// by deleting C's record. DeleteCreated never reverses a ghost's spends, so the
// store must roll back its fresh spend of S:0 in the same call; before the
// missing-parent answer was a rollback error, S:0 was left spent by a
// transaction the store no longer held.
//
// The other half: when S:0 already recorded C's spend (C spent both, then G was
// pruned), that is the confirmed, historical spend and the rollback must leave
// it in place.
func TestMissingParentRollsBackFreshSiblingSpend(t *testing.T) {
	for _, expressions := range []bool{false, true} {
		name := "udf"
		if expressions {
			name = "expressions"
		}

		t.Run(name, func(t *testing.T) {
			s := test.CreateBaseTestSettings(t)
			s.Aerospike.EnableSpendFilterExpressions = expressions

			if expressions {
				// useExpressionSpend requires one utxo per record.
				s.UtxoStore.UtxoBatchSize = 1
			}

			_, store, ctx, cleanup := initAerospike(t, s, ulogger.New("missing-parent-rollback"))
			t.Cleanup(cleanup)
			require.NoError(t, store.SetBlockHeight(1000))

			newParent := func(seed string) *bt.Tx {
				parent := bt.NewTx()
				require.NoError(t, parent.From(seed, 0, "51", 30000))
				require.NoError(t, parent.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 4000))

				return parent
			}

			spendOf := func(parents ...*bt.Tx) *bt.Tx {
				tx := bt.NewTx()
				for _, parent := range parents {
					require.NoError(t, tx.From(parent.TxID(), 0, parent.Outputs[0].LockingScript.String(), parent.Outputs[0].Satoshis))
				}

				require.NoError(t, tx.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 3000))

				return tx
			}

			spendOfS0 := func(parent *bt.Tx) *utxo.Spend {
				hash, err := util.UTXOHashFromOutput(parent.TxIDChainHash(), parent.Outputs[0], 0)
				require.NoError(t, err)

				return &utxo.Spend{TxID: parent.TxIDChainHash(), Vout: 0, UTXOHash: hash}
			}

			// Fresh: the survivor's output is unspent before the replay.
			survivor := newParent("1111111111111111111111111111111111111111111111111111111111111111")
			gone := newParent("2222222222222222222222222222222222222222222222222222222222222222")
			_, err := store.Create(ctx, survivor, 1000)
			require.NoError(t, err)

			ghost := spendOf(survivor, gone)
			_, err = store.Spend(ctx, ghost, 1200, utxo.IgnoreFlags{SpenderCreatedByCaller: true})
			require.ErrorIs(t, err, errors.ErrTxNotFound, "control: the missing parent answers not found")

			after, err := store.GetSpend(ctx, spendOfS0(survivor))
			require.NoError(t, err)
			require.Nil(t, after.SpendingData, "the fresh spend of the surviving parent must be rolled back")

			// Historical: C spent both outputs, then G's record went.
			survivor2 := newParent("3333333333333333333333333333333333333333333333333333333333333333")
			gone2 := newParent("4444444444444444444444444444444444444444444444444444444444444444")

			for _, parent := range []*bt.Tx{survivor2, gone2} {
				_, err = store.Create(ctx, parent, 1000)
				require.NoError(t, err)
			}

			ghost2 := spendOf(survivor2, gone2)
			_, err = store.Spend(ctx, ghost2, 1000)
			require.NoError(t, err)
			require.NoError(t, store.DeleteComplete(ctx, gone2.TxIDChainHash()))

			_, err = store.Spend(ctx, ghost2, 1200, utxo.IgnoreFlags{SpenderCreatedByCaller: true})
			require.ErrorIs(t, err, errors.ErrTxNotFound)

			kept, err := store.GetSpend(ctx, spendOfS0(survivor2))
			require.NoError(t, err)
			require.NotNil(t, kept.SpendingData, "an idempotent match next to a missing parent is the historical spend and must survive")
			require.Equal(t, *ghost2.TxIDChainHash(), *kept.SpendingData.TxID)
		})
	}
}

// TestBlessedInputWithSpenderRecordKeepsTheSpends: T spends G:0 (whose record
// is gone), S:0 (present) and X:0 (which another transaction already holds).
// T's own record exists, so the store blesses the missing parent and counts G:0
// as succeeded. X:0 answers ErrSpent, so the call fails.
//
// For an ordinary caller the rollback is gated on the spender having no record
// (#1214), and T has one, so S:0 stays spent by T: clearing it would clear a
// slot a live transaction owns. The bless needs that same record, so on this
// path a blessed input never meets a rollback that fires. Spend still keeps a
// blessed input out of the rollback set as a backstop: it wrote nothing and has
// no record, and Unspend failing on it first used to stop the rollback before
// S:0.
func TestBlessedInputWithSpenderRecordKeepsTheSpends(t *testing.T) {
	s := test.CreateBaseTestSettings(t)

	_, store, ctx, cleanup := initAerospike(t, s, ulogger.New("blessed-input-rollback"))
	t.Cleanup(cleanup)
	require.NoError(t, store.SetBlockHeight(1000))

	newParent := func(seed string) *bt.Tx {
		parent := bt.NewTx()
		require.NoError(t, parent.From(seed, 0, "51", 30000))
		require.NoError(t, parent.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 4000))

		return parent
	}

	spendOf := func(parents ...*bt.Tx) *bt.Tx {
		tx := bt.NewTx()
		for _, parent := range parents {
			require.NoError(t, tx.From(parent.TxID(), 0, parent.Outputs[0].LockingScript.String(), parent.Outputs[0].Satoshis))
		}

		require.NoError(t, tx.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 3000))

		return tx
	}

	gone := newParent("5555555555555555555555555555555555555555555555555555555555555555")
	survivor := newParent("6666666666666666666666666666666666666666666666666666666666666666")
	taken := newParent("7777777777777777777777777777777777777777777777777777777777777777")

	for _, parent := range []*bt.Tx{survivor, taken} {
		_, err := store.Create(ctx, parent, 1000)
		require.NoError(t, err)
	}

	_, err := store.Spend(ctx, spendOf(taken), 1100)
	require.NoError(t, err, "fixture: another transaction holds X:0")

	// G first, so a rollback that tried to reverse it stopped before S.
	spender := spendOf(gone, survivor, taken)
	_, err = store.Create(ctx, spender, 1100)
	require.NoError(t, err, "fixture: T's own record exists, so the missing parent is blessed")

	_, err = store.Spend(ctx, spender, 1200)
	require.ErrorIs(t, err, errors.ErrSpent, "X:0 is held by another transaction")
	require.NotErrorIs(t, err, errors.ErrTxNotFound, "control: the missing parent was blessed")

	hash, err := util.UTXOHashFromOutput(survivor.TxIDChainHash(), survivor.Outputs[0], 0)
	require.NoError(t, err)

	after, err := store.GetSpend(ctx, &utxo.Spend{TxID: survivor.TxIDChainHash(), Vout: 0, UTXOHash: hash})
	require.NoError(t, err)
	require.NotNil(t, after.SpendingData, "T has a record, so an ordinary caller's spend of S:0 stays")
	require.Equal(t, *spender.TxIDChainHash(), *after.SpendingData.TxID)
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
	s := test.CreateBaseTestSettings(t)

	_, store, ctx, cleanup := initAerospike(t, s, ulogger.New("require-parent-outputs-rollback"))
	t.Cleanup(cleanup)
	require.NoError(t, store.SetBlockHeight(1000))

	newParent := func(seed string) *bt.Tx {
		parent := bt.NewTx()
		require.NoError(t, parent.From(seed, 0, "51", 30000))
		require.NoError(t, parent.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 4000))

		return parent
	}

	gone := newParent("8888888888888888888888888888888888888888888888888888888888888888")
	survivor := newParent("9999999999999999999999999999999999999999999999999999999999999999")

	_, err := store.Create(ctx, survivor, 1000)
	require.NoError(t, err)

	ghost := bt.NewTx()
	for _, parent := range []*bt.Tx{gone, survivor} {
		require.NoError(t, ghost.From(parent.TxID(), 0, parent.Outputs[0].LockingScript.String(), parent.Outputs[0].Satoshis))
	}

	require.NoError(t, ghost.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 3000))

	_, err = store.Create(ctx, ghost, 1100)
	require.NoError(t, err, "fixture: the first wave wrote C's record")

	_, err = store.Spend(ctx, ghost, 1200, utxo.IgnoreFlags{RequireParentOutputs: true})
	require.ErrorIs(t, err, errors.ErrTxNotFound, "control: the pruned parent answers not found")

	hash, err := util.UTXOHashFromOutput(survivor.TxIDChainHash(), survivor.Outputs[0], 0)
	require.NoError(t, err)

	after, err := store.GetSpend(ctx, &utxo.Spend{TxID: survivor.TxIDChainHash(), Vout: 0, UTXOHash: hash})
	require.NoError(t, err)
	require.Nil(t, after.SpendingData, "the fresh spend of the surviving parent must be rolled back")
}

// TestSecondWaveFailureKeepsALiveSpendersSpend: T spends S:0 and B:0 and was
// fully applied by an ordinary SpendAndCreate before this list ran, so its
// record exists and both outputs record T as spender. B is then flagged
// conflicting. A WithBelowCheckpoint list [S, T] answers T as existing in the
// first wave and re-spends it in the second with RequireParentOutputs: S:0 is
// an idempotent match and B:0 fails. The list did not write T's record, so the
// rollback must leave S:0 alone: a live transaction owns it.
func TestSecondWaveFailureKeepsALiveSpendersSpend(t *testing.T) {
	s := test.CreateBaseTestSettings(t)

	client, store, ctx, cleanup := initAerospike(t, s, ulogger.New("second-wave-live-spender"))
	t.Cleanup(cleanup)
	require.NoError(t, store.SetBlockHeight(1000))

	newParent := func(seed string) *bt.Tx {
		parent := bt.NewTx()
		require.NoError(t, parent.From(seed, 0, "51", 30000))
		require.NoError(t, parent.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 4000))

		return parent
	}

	survivor := newParent("aaaa555555555555555555555555555555555555555555555555555555555555")
	contested := newParent("bbbb555555555555555555555555555555555555555555555555555555555555")

	for _, parent := range []*bt.Tx{survivor, contested} {
		_, err := store.Create(ctx, parent, 1000)
		require.NoError(t, err)
	}

	spender := bt.NewTx()
	for _, parent := range []*bt.Tx{survivor, contested} {
		require.NoError(t, spender.From(parent.TxID(), 0, parent.Outputs[0].LockingScript.String(), parent.Outputs[0].Satoshis))
	}

	require.NoError(t, spender.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 3000))

	_, _, err := store.SpendAndCreate(ctx, spender, 1100)
	require.NoError(t, err, "fixture: T fully applied by an ordinary writer")

	contestedKey, err := aerospike.NewKey(store.GetNamespace(), store.GetName(), contested.TxIDChainHash().CloneBytes())
	require.NoError(t, err)
	require.NoError(t, client.Put(nil, contestedKey, aerospike.BinMap{fields.Conflicting.String(): true}))

	results, err := store.SpendAndCreateMulti(ctx, []*bt.Tx{survivor, spender}, 1200,
		utxo.WithBelowCheckpoint(), utxo.WithIgnoreLocked(true))
	require.NoError(t, err)
	require.Len(t, results, 2)
	require.False(t, results[1].Created, "control: the list did not write T's record")
	require.ErrorIs(t, results[1].Err, errors.ErrTxConflicting, "control: B:0 fails the second-wave spend")

	hash, err := util.UTXOHashFromOutput(survivor.TxIDChainHash(), survivor.Outputs[0], 0)
	require.NoError(t, err)

	after, err := store.GetSpend(ctx, &utxo.Spend{TxID: survivor.TxIDChainHash(), Vout: 0, UTXOHash: hash})
	require.NoError(t, err)
	require.NotNil(t, after.SpendingData, "S:0 belongs to live T, whose record the list did not write")
	require.Equal(t, *spender.TxIDChainHash(), *after.SpendingData.TxID)
}
