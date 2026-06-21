package teraslab_test

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// txWithDataOutput returns a copy of newTestTx with an extra OP_RETURN (data)
// output appended. Create stores a zero UTXO hash for data outputs and never
// computes a real one, so the store holds a real slot with an all-zero hash for
// the OP_RETURN output.
func txWithDataOutput(t *testing.T) (*bt.Tx, uint32) {
	t.Helper()

	tx := newTestTx(t)

	dataScript := &bscript.Script{}
	require.NoError(t, dataScript.AppendOpcodes(bscript.OpFALSE, bscript.OpRETURN))
	require.NoError(t, dataScript.AppendPushData([]byte("teraslab-data-output")))

	tx.Outputs = append(tx.Outputs, &bt.Output{
		Satoshis:      0,
		LockingScript: dataScript,
	})

	dataVout := uint32(len(tx.Outputs) - 1) //nolint:gosec
	require.True(t, tx.Outputs[dataVout].LockingScript.IsData(), "appended output must be a data output")

	return tx, dataVout
}

// TestSetConflictingWithDataOutput verifies that SetConflicting succeeds for a
// transaction that mixes a normal P2PKH output with an OP_RETURN data output.
// Before the fix, the spender-discovery loop recomputed a UTXO hash for the data
// output, which mismatched the stored zero hash and made GetSpend return
// ErrTxNotFound, aborting SetConflicting with TX_NOT_FOUND.
func TestSetConflictingWithDataOutput(t *testing.T) {
	store, _, deferFn := initTeraSlabWithDefaults(t)
	defer deferFn()
	ctx := context.Background()

	tx, dataVout := txWithDataOutput(t)
	require.Greater(t, dataVout, uint32(0), "tx must also have at least one spendable output")

	_, err := store.Create(ctx, tx, 0)
	require.NoError(t, err)

	_, _, err = store.SetConflicting(ctx, []chainhash.Hash{*tx.TxIDChainHash()}, true)
	require.NoError(t, err)

	resp, err := store.Get(ctx, tx.TxIDChainHash(), fields.Conflicting)
	require.NoError(t, err)
	assert.True(t, resp.Conflicting)
}

// TestGetSpendDataOutputNotFound verifies that GetSpend on a data (OP_RETURN)
// output returns Status_NOT_FOUND with a nil error, matching Aerospike's
// treatment of an absent UTXO. The stored slot has an all-zero hash, which
// uniquely identifies a non-spendable data slot.
func TestGetSpendDataOutputNotFound(t *testing.T) {
	store, _, deferFn := initTeraSlabWithDefaults(t)
	defer deferFn()
	ctx := context.Background()

	tx, dataVout := txWithDataOutput(t)

	_, err := store.Create(ctx, tx, 0)
	require.NoError(t, err)

	// No UTXOHash supplied: the zero-hash check must fire before any hash
	// comparison and report the data slot as absent.
	resp, err := store.GetSpend(ctx, &utxo.Spend{
		TxID: tx.TxIDChainHash(),
		Vout: dataVout,
	})
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.Equal(t, int(utxo.Status_NOT_FOUND), resp.Status)

	// A normal output of the same tx still reports OK, confirming only the
	// zero-hash data slot is treated as absent.
	utxoHash0, err := util.UTXOHashFromOutput(tx.TxIDChainHash(), tx.Outputs[0], 0)
	require.NoError(t, err)

	okResp, err := store.GetSpend(ctx, &utxo.Spend{
		TxID:     tx.TxIDChainHash(),
		Vout:     0,
		UTXOHash: utxoHash0,
	})
	require.NoError(t, err)
	assert.Equal(t, int(utxo.Status_OK), okResp.Status)
}

// TestGetSpendImmatureAfterReassign verifies GetSpend's IMMATURE detection
// compares the stored spendable-at height against the current block height
// rather than merely checking it is non-zero. ReAssignUTXO sets a reassignment
// cooldown (block_height + ReAssignedUtxoSpendableAfterBlocks) in the unspent
// slot, so the reassigned UTXO is IMMATURE until the chain reaches that height
// and OK afterwards — matching Aerospike's maturity semantics.
func TestGetSpendImmatureAfterReassign(t *testing.T) {
	store, _, deferFn := initTeraSlabWithDefaults(t)
	defer deferFn()
	ctx := context.Background()
	tSettings := test.CreateBaseTestSettings(t)

	const createHeight = 100
	require.NoError(t, store.SetBlockHeight(createHeight))

	tx := newTestTx(t)
	_, err := store.Create(ctx, tx, createHeight)
	require.NoError(t, err)

	oldHash, err := util.UTXOHashFromOutput(tx.TxIDChainHash(), tx.Outputs[0], 0)
	require.NoError(t, err)

	// A UTXO can only be reassigned after it has been frozen (reassignment is an
	// alert-system operation on frozen funds; the server rejects reassigning an
	// unfrozen slot).
	require.NoError(t, store.FreezeUTXOs(ctx, []*utxo.Spend{
		{TxID: tx.TxIDChainHash(), Vout: 0, UTXOHash: oldHash},
	}, tSettings))

	// Reassign output 0 to a fresh hash; the new slot carries a cooldown of
	// createHeight + ReAssignedUtxoSpendableAfterBlocks.
	newHash, err := util.UTXOHashFromOutput(tx.TxIDChainHash(), tx.Outputs[1], 1)
	require.NoError(t, err)

	err = store.ReAssignUTXO(ctx,
		&utxo.Spend{TxID: tx.TxIDChainHash(), Vout: 0, UTXOHash: oldHash},
		&utxo.Spend{TxID: tx.TxIDChainHash(), Vout: 0, UTXOHash: newHash},
		tSettings)
	require.NoError(t, err)

	spendableAt := uint32(createHeight + utxo.ReAssignedUtxoSpendableAfterBlocks)

	reassigned := &utxo.Spend{TxID: tx.TxIDChainHash(), Vout: 0, UTXOHash: newHash}

	t.Run("immature before cooldown height", func(t *testing.T) {
		require.NoError(t, store.SetBlockHeight(spendableAt-1))

		resp, err := store.GetSpend(ctx, reassigned)
		require.NoError(t, err)
		assert.Equal(t, int(utxo.Status_IMMATURE), resp.Status)
	})

	t.Run("mature at cooldown height", func(t *testing.T) {
		require.NoError(t, store.SetBlockHeight(spendableAt))

		resp, err := store.GetSpend(ctx, reassigned)
		require.NoError(t, err)
		assert.Equal(t, int(utxo.Status_OK), resp.Status)
	})

	// Reassigning a slot that was never frozen is rejected server-side, so the
	// failed item is surfaced as a PartialError and must be mapped through
	// partialErrorToError (the A6 fix) rather than returned raw. A non-Docker
	// unit test cannot exercise this because Store holds a concrete client; the
	// mapping itself is covered by TestPartialErrorToError.
	t.Run("partial failure is surfaced as a mapped error", func(t *testing.T) {
		// Output 1 of tx was never frozen (only output 0 was). Reassigning an
		// unfrozen slot is rejected per-item by the server, so the failure comes
		// back as a PartialError and must be mapped through partialErrorToError
		// (the A6 fix) — i.e. the surfaced error names the "ReAssignUTXO" op.
		unfrozenOld, err := util.UTXOHashFromOutput(tx.TxIDChainHash(), tx.Outputs[1], 1)
		require.NoError(t, err)

		err = store.ReAssignUTXO(ctx,
			&utxo.Spend{TxID: tx.TxIDChainHash(), Vout: 1, UTXOHash: unfrozenOld},
			&utxo.Spend{TxID: tx.TxIDChainHash(), Vout: 1, UTXOHash: oldHash},
			tSettings)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "ReAssignUTXO")
	})
}
