package large_tx_reorg

import (
	"testing"
	"time"

	"github.com/aerospike/aerospike-client-go/v8"
	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/teranode/daemon"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockassembly/blockassembly_api"
	"github.com/bsv-blockchain/teranode/settings"
	aerospikestore "github.com/bsv-blockchain/teranode/stores/utxo/aerospike"
	"github.com/bsv-blockchain/teranode/test"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/stretchr/testify/require"
)

const (
	// lowUtxoBatchSize triggers multi-record storage with fewer outputs
	// With batchSize=2, a transaction with 10 outputs will span 5 records:
	// - 1 main record (outputs 0-1)
	// - 4 extra records (outputs 2-3, 4-5, 6-7, 8-9)
	// This means totalExtraRecs=4
	lowUtxoBatchSize = 2

	// largeOutputCount ensures we span multiple Aerospike records
	// With batchSize=2, 10 outputs = 5 records (1 main + 4 extra)
	largeOutputCount = 10

	// blockWait is the timeout for waiting for block processing
	blockWait = 15 * time.Second
)

// setupLargeTransactionTest creates a test environment configured for testing
// large transactions that span multiple Aerospike records.
// Returns a TestDaemon and block3 for building forks.
func setupLargeTransactionTest(t *testing.T, utxoStoreType string) (*daemon.TestDaemon, *model.Block) {
	t.Helper()
	// Default to aerospike if not specified
	if utxoStoreType == "" {
		utxoStoreType = "aerospike"
	}

	td := daemon.NewTestDaemon(t, daemon.TestOptions{
		UTXOStoreType: utxoStoreType,
		SettingsOverrideFunc: test.ComposeSettings(
			test.SystemTestSettings(),
			func(tSettings *settings.Settings) {
				tSettings.ChainCfgParams.CoinbaseMaturity = 2
				// KEY: Set low batch size to force multi-record storage
				// This is the critical setting that makes large transactions
				// span multiple Aerospike records and triggers the
				// spentExtraRecs/totalExtraRecs counter logic
				tSettings.UtxoStore.UtxoBatchSize = lowUtxoBatchSize
			},
		),
	})

	// Set the FSM state to RUNNING
	err := td.BlockchainClient.Run(td.Ctx, "test")
	require.NoError(t, err)

	// Generate initial blocks (0, 1, 2, 3)
	err = td.BlockAssemblyClient.GenerateBlocks(td.Ctx, &blockassembly_api.GenerateBlocksRequest{Count: 3})
	require.NoError(t, err)

	block3, err := td.BlockchainClient.GetBlockByHeight(td.Ctx, 3)
	require.NoError(t, err)

	// Wait for block3 to be processed by blockchain service (not block assembly)
	// Block assembly wait can timeout in some test scenarios
	td.WaitForBlockHeight(t, block3, blockWait, false)

	return td, block3
}

// verifySpentExtraRecs directly queries Aerospike to verify the spentExtraRecs
// and totalExtraRecs counters for a transaction.
// This is critical for detecting the bug where spentExtraRecs > totalExtraRecs.
//
// For Aerospike stores:
// - Queries the main transaction record
// - Verifies totalExtraRecs matches expected
// - Verifies spentExtraRecs matches expected
// - CRITICAL: Verifies spentExtraRecs <= totalExtraRecs (the bug condition)
//
// For non-Aerospike stores (postgres):
// - Skips verification (counters don't apply)
func verifySpentExtraRecs(t *testing.T, td *daemon.TestDaemon, tx *bt.Tx, expectedSpentRecs int, expectedTotalRecs int) {
	t.Helper()
	// Only Aerospike uses the spentExtraRecs/totalExtraRecs counters
	// Check if UTXO store URL scheme is aerospike
	if td.Settings.UtxoStore.UtxoStore == nil || td.Settings.UtxoStore.UtxoStore.Scheme != "aerospike" {
		storeType := "unknown"
		if td.Settings.UtxoStore.UtxoStore != nil {
			storeType = td.Settings.UtxoStore.UtxoStore.Scheme
		}
		t.Logf("Skipping spentExtraRecs verification for non-Aerospike store: %s", storeType)
		return
	}

	txID := tx.TxIDChainHash()

	// Cast UTXO store to Aerospike store to access client and settings
	aeroStore, ok := td.UtxoStore.(*aerospikestore.Store)
	require.True(t, ok, "UTXO store must be Aerospike store")

	// Get the underlying Aerospike client
	client := aeroStore.GetClient()
	require.NotNil(t, client, "Aerospike client should be available")

	// Create key for the main transaction record
	key, err := aerospike.NewKey(
		aeroStore.GetNamespace(),
		aeroStore.GetSet(),
		txID.CloneBytes(),
	)
	require.NoError(t, err, "Failed to create Aerospike key for tx %s", txID)

	// Fetch only the counter bins we care about
	policy := util.GetAerospikeReadPolicy(td.Settings)
	rec, err := client.Get(policy, key, "totalExtraRecs", "spentExtraRecs")
	require.NoError(t, err, "Failed to read Aerospike record for tx %s", txID)
	require.NotNil(t, rec, "Aerospike record should exist for tx %s", txID)

	// Extract counter values
	totalRecs := 0
	spentRecs := 0

	if val, ok := rec.Bins["totalExtraRecs"].(int); ok {
		totalRecs = val
	}
	if val, ok := rec.Bins["spentExtraRecs"].(int); ok {
		spentRecs = val
	}

	// Verify expected values
	require.Equal(t, expectedTotalRecs, totalRecs,
		"totalExtraRecs mismatch for tx %s", txID)
	require.Equal(t, expectedSpentRecs, spentRecs,
		"spentExtraRecs mismatch for tx %s", txID)

	// CRITICAL BUG CHECK: spentExtraRecs must never exceed totalExtraRecs
	// This is the exact condition that caused the production panic
	require.LessOrEqual(t, spentRecs, totalRecs,
		"PRODUCTION BUG DETECTED: spentExtraRecs (%d) > totalExtraRecs (%d) for tx %s",
		spentRecs, totalRecs, txID)

	t.Logf("✓ Counter verification passed for tx %s: spentExtraRecs=%d, totalExtraRecs=%d",
		txID, spentRecs, totalRecs)
}

// calculateExpectedTotalExtraRecs calculates the expected totalExtraRecs value
// for a transaction with the given number of outputs and batch size.
//
// Formula: totalExtraRecs = ceil(numOutputs / batchSize) - 1
// The "-1" is because the main record counts as the first batch.
//
// Example with batchSize=2:
// - 2 outputs  = 1 record  (main) = 0 extra records
// - 4 outputs  = 2 records (main + 1 extra) = 1 extra record
// - 10 outputs = 5 records (main + 4 extra) = 4 extra records
func calculateExpectedTotalExtraRecs(numOutputs int, batchSize int) int {
	if numOutputs <= batchSize {
		return 0
	}
	// ceil(numOutputs / batchSize) - 1
	totalRecords := (numOutputs + batchSize - 1) / batchSize
	return totalRecords - 1
}

// calculateExpectedSpentExtraRecs calculates the expected spentExtraRecs value
// when a specific number of outputs are spent from a transaction.
//
// Logic:
// - Count how many complete extra records (beyond the main record) are fully spent
// - A record is "fully spent" when all its outputs are spent
//
// Example with batchSize=2, 10 outputs (5 records total):
// - Records: [0-1], [2-3], [4-5], [6-7], [8-9]
// - Main record: [0-1]
// - Extra records: [2-3], [4-5], [6-7], [8-9] (4 extra)
//
// If outputs 0-5 are spent:
// - Main [0-1]: fully spent (but doesn't count toward spentExtraRecs)
// - Extra [2-3]: fully spent (+1)
// - Extra [4-5]: fully spent (+1)
// - Extra [6-7]: not spent
// - Extra [8-9]: not spent
// Result: spentExtraRecs = 2
func calculateExpectedSpentExtraRecs(numOutputsSpent int, batchSize int) int {
	if numOutputsSpent <= batchSize {
		// Only main record affected, no extra records fully spent
		return 0
	}
	// Outputs beyond the main record
	outputsBeyondMain := numOutputsSpent - batchSize
	// Number of complete extra records fully spent
	return outputsBeyondMain / batchSize
}
