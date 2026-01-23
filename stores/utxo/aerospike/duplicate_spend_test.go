package aerospike_test

import (
	"testing"

	"github.com/aerospike/aerospike-client-go/v8"
	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestDuplicateSpendLargeTx tests that duplicate spend attempts on a transaction
// with more than utxoBatchSize outputs don't cause spentExtraRecs to exceed totalExtraRecs
func TestDuplicateSpendLargeTx(t *testing.T) {
	batchSize := 2
	numOutputs := 10

	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)
	// Set batch size to 128 as in production
	tSettings.UtxoStore.UtxoBatchSize = batchSize

	client, store, ctx, deferFn := initAerospike(t, tSettings, logger)
	t.Cleanup(func() {
		deferFn()
	})

	// Build a transaction with many outputs
	largeTx := bt.NewTx()

	// Add a dummy input - use the From method which is the public API
	// Use a non-zero hash to avoid being treated as coinbase
	err := largeTx.From(
		"1111111111111111111111111111111111111111111111111111111111111111",
		0,
		"76a914000000000000000000000000000000000000000088ac", // dummy script
		uint64(numOutputs*1000+1000),                         // enough satoshis for all outputs plus fee
	)
	require.NoError(t, err)

	// Add many outputs using PayToAddress
	for i := 0; i < numOutputs; i++ {
		err = largeTx.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 1000)
		require.NoError(t, err)
	}

	txID := largeTx.TxIDChainHash()

	// Store the transaction with UTXOs
	_, err = store.Create(ctx, largeTx, 1)
	require.NoError(t, err)

	// Verify the transaction was stored correctly
	txKey, err := aerospike.NewKey(store.GetNamespace(), store.GetName(), txID.CloneBytes())
	require.NoError(t, err)

	rec, err := client.Get(util.GetAerospikeReadPolicy(tSettings), txKey, "totalExtraRecs", "spentExtraRecs")
	require.NoError(t, err)
	require.NotNil(t, rec)

	totalExtraRecs, ok := rec.Bins["totalExtraRecs"].(int)
	require.True(t, ok)
	// With 1001 outputs and batch size 128, we should have 7 extra records
	expectedExtraRecs := (numOutputs / batchSize) - 1
	assert.Equal(t, expectedExtraRecs, totalExtraRecs)

	// spentExtraRecs should not exist yet
	spentExtraRecs, ok := rec.Bins["spentExtraRecs"]
	assert.False(t, ok)
	assert.Nil(t, spentExtraRecs)

	// Create a spending transaction that spends all outputs
	spendingTx := bt.NewTx()

	// Add all outputs as inputs to the spending transaction
	for i := 0; i < numOutputs; i++ {
		err = spendingTx.From(
			txID.String(),
			uint32(i),
			largeTx.Outputs[i].LockingScript.String(),
			largeTx.Outputs[i].Satoshis,
		)
		require.NoError(t, err)
	}

	// Add a single output to the spending transaction
	err = spendingTx.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", uint64(numOutputs*1000-500))
	require.NoError(t, err)

	// First spend attempt - should succeed
	spends, err := store.Spend(ctx, spendingTx, store.GetBlockHeight()+1)
	require.NoError(t, err, "Failed on first spend attempt")
	require.NotNil(t, spends)
	require.Len(t, spends, numOutputs)

	// Check spentExtraRecs after first spend
	rec, err = client.Get(util.GetAerospikeReadPolicy(tSettings), txKey, "totalExtraRecs", "spentExtraRecs")
	require.NoError(t, err)
	require.NotNil(t, rec)

	spentExtraRecsAfterFirst, ok := rec.Bins["spentExtraRecs"].(int)
	require.True(t, ok)
	// All extra records should be marked as spent
	assert.Equal(t, totalExtraRecs, spentExtraRecsAfterFirst)

	// CRITICAL TEST: Attempt to spend the same outputs again with the same spending transaction
	// This should NOT increment spentExtraRecs beyond totalExtraRecs
	spends2, err := store.Spend(ctx, spendingTx, store.GetBlockHeight()+1)
	// Should succeed (idempotent behavior - already spent with same data is OK)
	require.NoError(t, err, "Failed on duplicate spend attempt")
	require.NotNil(t, spends2)
	require.Len(t, spends2, numOutputs)

	// Check spentExtraRecs after duplicate spend attempt
	rec, err = client.Get(util.GetAerospikeReadPolicy(tSettings), txKey, "totalExtraRecs", "spentExtraRecs")
	require.NoError(t, err)
	require.NotNil(t, rec)

	spentExtraRecsAfterDuplicate, ok := rec.Bins["spentExtraRecs"].(int)
	require.True(t, ok)

	// VERIFY THE FIX: spentExtraRecs should NOT exceed totalExtraRecs
	assert.LessOrEqual(t, spentExtraRecsAfterDuplicate, totalExtraRecs,
		"spentExtraRecs (%d) exceeded totalExtraRecs (%d) after duplicate spend attempt",
		spentExtraRecsAfterDuplicate, totalExtraRecs)

	// Should remain the same as after first spend
	assert.Equal(t, spentExtraRecsAfterFirst, spentExtraRecsAfterDuplicate,
		"spentExtraRecs changed from %d to %d after duplicate spend attempt",
		spentExtraRecsAfterFirst, spentExtraRecsAfterDuplicate)

	// // Try a third time to be really sure
	spends3, err := store.Spend(ctx, spendingTx, store.GetBlockHeight()+1)
	require.NoError(t, err, "Failed on third spend attempt")
	require.NotNil(t, spends3)
	require.Len(t, spends3, numOutputs)

	rec, err = client.Get(util.GetAerospikeReadPolicy(tSettings), txKey, "totalExtraRecs", "spentExtraRecs")
	require.NoError(t, err)
	require.NotNil(t, rec)

	spentExtraRecsAfterThird, ok := rec.Bins["spentExtraRecs"].(int)
	require.True(t, ok)

	// Still should not exceed
	assert.LessOrEqual(t, spentExtraRecsAfterThird, totalExtraRecs,
		"spentExtraRecs (%d) exceeded totalExtraRecs (%d) after third spend attempt",
		spentExtraRecsAfterThird, totalExtraRecs)

	assert.Equal(t, spentExtraRecsAfterFirst, spentExtraRecsAfterThird,
		"spentExtraRecs changed from %d to %d after third spend attempt",
		spentExtraRecsAfterFirst, spentExtraRecsAfterThird)
}
