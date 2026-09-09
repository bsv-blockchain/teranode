package postgres

import (
	"testing"

	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/stores/utxo/tests"
	"github.com/stretchr/testify/require"
)

// TestGetOutOfBoundsSpendIndexNoPanic verifies that a spends row whose
// prev_output_idx exceeds the transaction's output count is handled gracefully
// by Get (returning a processing error) instead of triggering an
// index-out-of-range panic when subscripting data.SpendingDatas.
//
// prev_output_idx is an INT with no CHECK constraint, so a corrupt,
// truncated, or orphaned row can carry an out-of-range index. The guard added in
// getInternal (get.go) turns that into an error rather than a process-killing
// panic reachable from any caller-supplied tx hash.
func TestGetOutOfBoundsSpendIndexNoPanic(t *testing.T) {
	store, ctx := setupTestStore(t)

	_, err := store.Create(ctx, tests.Tx, 0)
	require.NoError(t, err)

	txHash := tests.Tx.TxIDChainHash()

	// Inject a malformed spends row: an output index far beyond the tx's outputs.
	badIdx := int64(len(tests.Tx.Outputs) + 50)
	spendingData := make([]byte, 36) // 32-byte hash + 4-byte vout; content irrelevant — the bounds check fires first.
	_, err = store.pool.Exec(ctx,
		`INSERT INTO spends (prev_tx_hash, prev_output_idx, spending_data, spent_at_height) VALUES ($1, $2, $3, $4)`,
		txHash[:], badIdx, spendingData, int64(1))
	require.NoError(t, err)

	// Reading the UTXOs must not panic; it must surface a graceful error.
	require.NotPanics(t, func() {
		_, err = store.Get(ctx, txHash, fields.Tx, fields.Utxos)
	})
	require.Error(t, err, "Get must return an error for an out-of-bounds spend index, not panic")
}
