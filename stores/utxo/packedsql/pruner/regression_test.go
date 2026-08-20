package pruner_test

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	packedpruner "github.com/bsv-blockchain/teranode/stores/utxo/packedsql/pruner"
	"github.com/bsv-blockchain/teranode/stores/utxo/tests"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

// newShiftedParentTx builds a parent whose vout 0 is provably unspendable (OP_FALSE
// OP_RETURN) and whose vout 1 is a real UTXO. page0_count is therefore 1 while the spend
// slot that matters lives at index 1 — a defensive scan driven by page0_count would only
// look at slot 0 and never see the spender.
func newShiftedParentTx(t *testing.T) *bt.Tx {
	t.Helper()

	tx := bt.NewTx()
	require.NoError(t, tx.FromUTXOs(&bt.UTXO{
		TxIDHash:      tests.Tx.TxIDChainHash(),
		Vout:          0,
		LockingScript: tests.Tx.Outputs[0].LockingScript,
		Satoshis:      tests.Tx.Outputs[0].Satoshis,
	}))

	tx.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{0x00, 0x48, 0x30, 0x45})

	tx.AddOutput(&bt.Output{
		Satoshis:      0,
		LockingScript: bscript.NewFromBytes([]byte{bscript.OpFALSE, bscript.OpRETURN, 0x01, 0x02}),
	})

	require.NoError(t, tx.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 830_000))

	return tx
}

func newChildOfVout(t *testing.T, parent *bt.Tx, vout uint32) *bt.Tx {
	t.Helper()

	tx := bt.NewTx()
	require.NoError(t, tx.FromUTXOs(&bt.UTXO{
		TxIDHash:      parent.TxIDChainHash(),
		Vout:          vout,
		LockingScript: parent.Outputs[vout].LockingScript,
		Satoshis:      parent.Outputs[vout].Satoshis,
	}))

	tx.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{0x51})
	require.NoError(t, tx.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 1000))

	return tx
}

// TestPruneDefensiveScansAllSlots pins that defensive pruning inspects every spend slot,
// not just the first page0_count of them. With an unspendable output at vout 0 the spender
// recorded at slot 1 must still hold the row back until the child is stably mined.
func TestPruneDefensiveScansAllSlots(t *testing.T) {
	store, pool, tSettings := newStoreAndPool(t, true)
	ctx := context.Background()

	parent := newShiftedParentTx(t)

	_, err := store.Create(ctx, parent, 100)
	require.NoError(t, err)

	var page0Count int

	require.NoError(t, pool.QueryRow(ctx,
		`SELECT page0_count FROM packed_txs WHERE hash = $1`, parent.TxIDChainHash()[:]).Scan(&page0Count))
	require.Equal(t, 1, page0Count, "only vout 1 is a UTXO, so the spender slot sits past page0_count")

	child := newChildOfVout(t, parent, 1)

	_, _, err = store.SpendAndCreate(ctx, child, 101)
	require.NoError(t, err)

	// Tombstone the parent directly: the point under test is the defensive scan, not the
	// path that sets delete_at_height.
	_, err = pool.Exec(ctx,
		`UPDATE packed_txs SET delete_at_height = 150 WHERE hash = $1`, parent.TxIDChainHash()[:])
	require.NoError(t, err)

	svc, err := packedpruner.NewService(tSettings, packedpruner.Options{Logger: ulogger.TestLogger{}, Pool: pool})
	require.NoError(t, err)

	deleted, err := svc.Prune(ctx, 200, "testblock")
	require.NoError(t, err)
	require.Zero(t, deleted, "parent spent by an unmined child must not be deleted")

	var n int

	require.NoError(t, pool.QueryRow(ctx,
		`SELECT count(*) FROM packed_txs WHERE hash = $1`, parent.TxIDChainHash()[:]).Scan(&n))
	require.Equal(t, 1, n)
}
