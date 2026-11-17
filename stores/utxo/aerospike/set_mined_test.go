package aerospike

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/stretchr/testify/require"
)

// txWithOutputs builds a transaction with numOutputs outputs that the store
// accepts; its only input spends output 0 of the zero hash.
func txWithOutputs(t *testing.T, numOutputs int) *bt.Tx {
	t.Helper()

	tx := bt.NewTx()
	require.NoError(t, tx.From("0000000000000000000000000000000000000000000000000000000000000000", 0,
		"76a914000000000000000000000000000000000000000088ac", 1000))

	for range numOutputs {
		require.NoError(t, tx.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 1000))
	}

	return tx
}

// TestReadExtraRecordCounts checks that only a transaction with extra records
// comes back, with its count, and that a missing master is skipped quietly.
func TestReadExtraRecordCounts(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping Aerospike integration test in short mode")
	}

	ctx := context.Background()
	store, _ := setupStoreForLockTest(t)
	store.utxoBatchSize = 4

	single := txWithOutputs(t, 3)
	paginated := txWithOutputs(t, 10)

	_, err := store.Create(ctx, single, 100)
	require.NoError(t, err)

	_, err = store.Create(ctx, paginated, 100)
	require.NoError(t, err)

	missing := chainhash.Hash{0xde, 0xad}

	items, err := store.readExtraRecordCounts([]chainhash.Hash{missing, *single.TxIDChainHash(), *paginated.TxIDChainHash()})
	require.NoError(t, err)
	require.Len(t, items, 1)
	require.Equal(t, *paginated.TxIDChainHash(), *items[0].txID)
	require.Equal(t, 2, items[0].childCount, "10 outputs at batch size 4 must give two extra records")
}

// TestApplyLockClearWorkReportsEveryFailure checks that a failed read of the
// extra record count, a failed clear and a failed unlock all reach the caller.
func TestApplyLockClearWorkReportsEveryFailure(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping Aerospike integration test in short mode")
	}

	store, _ := setupStoreForLockTest(t)

	txID := chainhash.Hash{0x01}
	other := chainhash.Hash{0x02}

	// Every Aerospike call fails once the client is closed.
	store.client.Close()

	t.Run("clear fails alone", func(t *testing.T) {
		err := store.applyLockClearWork(context.Background(), lockClearWork{
			items: []lockClearItem{{txID: &txID, childCount: 1}},
		})
		require.ErrorContains(t, err, "clearLockedOnRecordsMulti")
	})

	t.Run("read, clear and unlock all fail", func(t *testing.T) {
		err := store.applyLockClearWork(context.Background(), lockClearWork{
			items:      []lockClearItem{{txID: &txID, childCount: 1}},
			fullUnlock: []chainhash.Hash{other},
		})
		require.ErrorContains(t, err, "readExtraRecordCounts")
		require.ErrorContains(t, err, "clearLockedOnRecordsMulti")
	})
}
