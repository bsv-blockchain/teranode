package postgres

import (
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/stretchr/testify/require"
)

// makeZeroSpendableTx returns a transaction whose only output is an OP_FALSE
// OP_RETURN data carrier — so out_count = 1 but spendable_count = 0. lockTime
// varies the txid so several distinct rows can coexist in one store. It reuses
// testExtendedTx's input so the tx is otherwise well-formed.
func makeZeroSpendableTx(t *testing.T, lockTime uint32) *bt.Tx {
	t.Helper()

	base := testExtendedTx(t)
	opReturnScript := bscript.NewFromBytes([]byte{0x00, 0x6a, 0x04, 0xba, 0xdc, 0x0f, 0xfe})

	tx := bt.NewTx()
	tx.Version = base.Version
	tx.LockTime = lockTime
	tx.Inputs = base.Inputs
	tx.Outputs = []*bt.Output{
		{Satoshis: 0, LockingScript: opReturnScript},
	}
	return tx
}

// TestZeroSpendableMinedCreate_BulkPath asserts a zero-spendable tx created
// ALREADY mined via the batched (bulk UNNEST) Create path is stamped
// delete_at_height = mined + 1 + retention and fed into pending_deletes — the
// fold/reconciler both skip spendable_count = 0 and this path bypasses
// SetMinedMulti, so without the inline stamp the row would leak forever.
func TestZeroSpendableMinedCreate_BulkPath(t *testing.T) {
	store, ctx := setupTestStore(t)
	minedHeight := uint32(100)
	require.NoError(t, store.SetBlockHeight(minedHeight))
	retention := int32(store.settings.GetUtxoStoreBlockHeightRetention())

	// Mined-at-create → must be stamped.
	minedTx := makeZeroSpendableTx(t, 1)
	minedHash := minedTx.TxIDChainHash()
	_, err := store.Create(ctx, minedTx, minedHeight, utxo.WithMinedBlockInfo(utxo.MinedBlockInfo{
		BlockID: 1, BlockHeight: minedHeight, SubtreeIdx: 0,
	}))
	require.NoError(t, err)

	var outCount, spendableCount int32
	var dah *int32
	require.NoError(t, store.pool.QueryRow(ctx,
		`SELECT out_count, spendable_count, delete_at_height FROM txs WHERE hash = $1`, minedHash[:],
	).Scan(&outCount, &spendableCount, &dah))
	require.Equal(t, int32(1), outCount, "one OP_RETURN output")
	require.Equal(t, int32(0), spendableCount, "OP_RETURN output is not spendable")
	require.NotNil(t, dah, "zero-spendable mined-at-create tx must be stamped for deletion")
	require.Equal(t, int32(minedHeight)+1+retention, *dah, "delete_at_height = mined + 1 + retention")

	var pdHeight int32
	require.NoError(t, store.pool.QueryRow(ctx,
		`SELECT delete_at_height FROM pending_deletes WHERE hash = $1`, minedHash[:]).Scan(&pdHeight),
		"stamped row must be in pending_deletes (the only pruner path)")
	require.Equal(t, *dah, pdHeight, "pending_deletes height must match txs.delete_at_height")

	// Unmined create of the same shape → must NOT be stamped (SetMinedMulti will
	// stamp it later when it is actually mined).
	unminedTx := makeZeroSpendableTx(t, 2)
	unminedHash := unminedTx.TxIDChainHash()
	_, err = store.Create(ctx, unminedTx, minedHeight)
	require.NoError(t, err)

	var unminedDAH *int32
	require.NoError(t, store.pool.QueryRow(ctx,
		`SELECT delete_at_height FROM txs WHERE hash = $1`, unminedHash[:]).Scan(&unminedDAH))
	require.Nil(t, unminedDAH, "unmined zero-spendable tx must not be stamped at create")

	var n int
	require.NoError(t, store.pool.QueryRow(ctx,
		`SELECT count(*) FROM pending_deletes WHERE hash = $1`, unminedHash[:]).Scan(&n))
	require.Zero(t, n, "unmined zero-spendable tx must not be in pending_deletes")
}

// TestZeroSpendableMinedCreate_DirectPath asserts the same contract on the
// single-item createDirect path (batcher bypassed), so both INSERT code paths
// are covered.
func TestZeroSpendableMinedCreate_DirectPath(t *testing.T) {
	store, ctx := setupTestStore(t)
	minedHeight := uint32(100)
	require.NoError(t, store.SetBlockHeight(minedHeight))
	retention := int32(store.settings.GetUtxoStoreBlockHeightRetention())

	minedTx := makeZeroSpendableTx(t, 3)
	minedHash := minedTx.TxIDChainHash()
	options := &utxo.CreateOptions{}
	utxo.WithMinedBlockInfo(utxo.MinedBlockInfo{BlockID: 1, BlockHeight: minedHeight, SubtreeIdx: 0})(options)
	_, err := store.createDirect(ctx, minedTx, minedHeight, options)
	require.NoError(t, err)

	var spendableCount int32
	var dah *int32
	require.NoError(t, store.pool.QueryRow(ctx,
		`SELECT spendable_count, delete_at_height FROM txs WHERE hash = $1`, minedHash[:],
	).Scan(&spendableCount, &dah))
	require.Equal(t, int32(0), spendableCount)
	require.NotNil(t, dah, "createDirect must stamp a zero-spendable mined-at-create tx")
	require.Equal(t, int32(minedHeight)+1+retention, *dah)

	var pdHeight int32
	require.NoError(t, store.pool.QueryRow(ctx,
		`SELECT delete_at_height FROM pending_deletes WHERE hash = $1`, minedHash[:]).Scan(&pdHeight))
	require.Equal(t, *dah, pdHeight)

	// Unmined via createDirect → not stamped.
	unminedTx := makeZeroSpendableTx(t, 4)
	unminedHash := unminedTx.TxIDChainHash()
	_, err = store.createDirect(ctx, unminedTx, minedHeight, &utxo.CreateOptions{})
	require.NoError(t, err)

	var unminedDAH *int32
	require.NoError(t, store.pool.QueryRow(ctx,
		`SELECT delete_at_height FROM txs WHERE hash = $1`, unminedHash[:]).Scan(&unminedDAH))
	require.Nil(t, unminedDAH, "unmined zero-spendable createDirect tx must not be stamped")
}
