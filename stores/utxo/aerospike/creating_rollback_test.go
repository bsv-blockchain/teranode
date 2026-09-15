package aerospike_test

import (
	"testing"

	aerospike "github.com/bsv-blockchain/aerospike-client-go/v8"
	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/tests"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// TestPartialSpendRollbackSuppressedByCreatingParent covers the store's SECOND
// two-phase-commit window, which the shared suite structurally cannot reach.
//
// A spend is rejected while the PARENT's record carries creating=true
// (teranode.lua), surfacing as ErrTxCreating. That flag is set in create phase 1
// and cleared in phase 2, so with utxostore_utxoBatchSize defaulting to 128 any
// parent with more than 128 outputs passes through the window on every create —
// routine for batch payouts and exchange transactions.
//
// It is transient for exactly the reason ErrTxLocked is, so it must suppress the
// partial-spend rollback for the same reason: a concurrent attempt at this same
// txid can be past the window and winning right now, and spendingData is
// {TxID, Vin} with no attempt identity, so a rollback cannot tell that winner's
// slots from its own. Rolling back would clear a slot a live — possibly mined —
// transaction owns, which is the inverse of #1214 and worse.
//
// This test is Aerospike-only by necessity: the SQL store has no creating state,
// which is why all six shared-suite rollback subtests pass while this window is
// open. Once create-first (#1355) is enabled a parent's 2PC window becomes a
// creating window rather than a locked one, so this becomes the common case.
func TestPartialSpendRollbackSuppressedByCreatingParent(t *testing.T) {
	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)

	client, store, ctx, deferFn := initAerospike(t, tSettings, logger)
	t.Cleanup(deferFn)

	dummy := bscript.NewFromBytes([]byte{0x00, 0x48, 0x30, 0x45})

	// parent of the two parents
	_, _, err := store.SpendAndCreate(ctx, tests.Tx, 1000, utxo.WithCreateOnly())
	require.NoError(t, err)

	// Pay out slightly less than the input so the store's fee check passes; the
	// per-parent delta also keeps the two parents' txids distinct.
	newParent := func(vout uint32, feeDelta uint64) *bt.Tx {
		in := tests.Tx.Outputs[vout]

		tx := bt.NewTx()
		require.NoError(t, tx.From(tests.Tx.TxIDChainHash().String(), vout,
			in.LockingScript.String(), in.Satoshis))
		tx.Inputs[0].UnlockingScript = dummy
		require.NoError(t, tx.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", in.Satoshis-feeDelta))

		_, _, createErr := store.SpendAndCreate(ctx, tx, 1000, utxo.WithCreateOnly())
		require.NoError(t, createErr)

		return tx
	}

	good := newParent(0, 1_000)
	creating := newParent(1, 2_000)

	// Put the second parent into the create-phase-1 state the Lua spend rejects.
	key, err := aerospike.NewKey(store.GetNamespace(), store.GetName(), creating.TxIDChainHash()[:])
	require.NoError(t, err)

	_, err = client.Operate(nil, key, aerospike.PutOp(aerospike.NewBin("creating", true)))
	require.NoError(t, err)

	child := bt.NewTx()
	for _, p := range []*bt.Tx{good, creating} {
		require.NoError(t, child.From(p.TxIDChainHash().String(), 0,
			p.Outputs[0].LockingScript.String(), p.Outputs[0].Satoshis))
	}

	for _, in := range child.Inputs {
		in.UnlockingScript = dummy
	}

	childIn := good.Outputs[0].Satoshis + creating.Outputs[0].Satoshis
	require.NoError(t, child.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", childIn-1_000))

	_, spends, err := store.SpendAndCreate(ctx, child, store.GetBlockHeight()+1, utxo.WithSpendOnly())
	require.Error(t, err, "spend must fail while one parent is mid-create")
	require.Len(t, spends, 2)
	require.NoError(t, spends[0].Err, "the settled parent's spend is expected to succeed")
	require.ErrorIs(t, spends[1].Err, errors.ErrTxCreating)

	// The rollback must NOT have fired: a concurrent attempt at this same txid may
	// be past the creating window and legitimately own this slot.
	utxoHash, err := util.UTXOHashFromOutput(good.TxIDChainHash(), good.Outputs[0], 0)
	require.NoError(t, err)

	resp, err := store.GetSpend(ctx, &utxo.Spend{TxID: good.TxIDChainHash(), Vout: 0, UTXOHash: utxoHash})
	require.NoError(t, err)
	require.NotNil(t, resp.SpendingData,
		"ErrTxCreating is a transient 2PC window like ErrTxLocked: rolling back can clear a slot a concurrent winner owns")
	require.Equal(t, child.TxIDChainHash().String(), resp.SpendingData.TxID.String())
}
