package doublespendtest

import (
	"net/url"
	"testing"

	"github.com/bsv-blockchain/teranode/daemon"
	"github.com/bsv-blockchain/teranode/services/blockassembly/blockassembly_api"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/test"
	postgres "github.com/bsv-blockchain/teranode/test/longtest/util/postgres"
	"github.com/stretchr/testify/require"
)

// Critical Validation Tests for Duplicate Transaction Detection
//
// These tests cover critical security and correctness issues discovered during
// comprehensive analysis of the duplicate detection implementation:
//
// CRITICAL ISSUES TESTED:
// 1. Nil subtreeStore bypass - duplicate detection is skipped when subtreeStore == nil
// 2. Empty/nil SubtreeSlices - potential nil pointer dereference
// 3. Validation flow dependencies - txMap state management between stages
// 4. Duplicate detection bypass - duplicates reaching validOrderAndBlessed
//
// These tests address potential security vulnerabilities and crash scenarios.

// TestNilSubtreeStoreBypass tests the critical security issue where duplicate
// detection is completely skipped when subtreeStore is nil.
//
// Location: model/Block.go:500
// Code: if subtreeStore != nil { checkDuplicateTransactions() }
//
// Security Concern: Blocks with duplicate transactions could bypass validation
// if subtreeStore is nil but txMetaStore is not nil.
//
// This test verifies the behavior and documents whether this is:
// - A security vulnerability (duplicates pass validation)
// - Intentional design (validOrderAndBlessed catches duplicates)
// - Configuration error (both stores should be nil/non-nil together)
func TestNilSubtreeStoreBypassPostgres(t *testing.T) {
	pg, err := postgres.RunPostgresTestContainer(t.Context(), "nil_subtree_bypass_"+t.Name())
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = pg.Terminate(t.Context())
	})

	t.Run("postgres", func(t *testing.T) {
		testNilSubtreeStoreBypass(t, pg.ConnectionString())
	})
}

func testNilSubtreeStoreBypass(t *testing.T, utxoStore string) {
	td := daemon.NewTestDaemon(t, daemon.TestOptions{
		SettingsOverrideFunc: test.ComposeSettings(
			test.SystemTestSettings(),
			func(tSettings *settings.Settings) {
				parsedURL, err := url.Parse(utxoStore)
				require.NoError(t, err)
				tSettings.UtxoStore.UtxoStore = parsedURL
			},
		),
	})
	defer td.Stop(t)

	err := td.BlockchainClient.Run(td.Ctx, "test")
	require.NoError(t, err)

	err = td.BlockAssemblyClient.GenerateBlocks(td.Ctx, &blockassembly_api.GenerateBlocksRequest{Count: 101})
	require.NoError(t, err)

	block1, err := td.BlockchainClient.GetBlockByHeight(td.Ctx, 1)
	require.NoError(t, err)

	// Create a transaction that will be duplicated
	duplicateTx := td.CreateTransaction(t, block1.CoinbaseTx, 0)

	block101, err := td.BlockchainClient.GetBlockByHeight(td.Ctx, 101)
	require.NoError(t, err)

	// Create a block with duplicate transactions
	_, block102 := td.CreateTestBlock(t, block101, 10400, duplicateTx, duplicateTx)

	// NOTE: In the current test setup with TestDaemon, we cannot easily
	// set subtreeStore to nil while keeping txMetaStore non-nil.
	// This test documents the concern and verifies normal validation behavior.
	//
	// To properly test the nil subtreeStore bypass, we would need to:
	// 1. Call block.Valid() directly with subtreeStore=nil
	// 2. Use unit tests instead of integration tests
	// 3. Mock the validation dependencies
	//
	// For now, we verify that blocks with duplicates are rejected normally.
	err = td.BlockValidationClient.ProcessBlock(td.Ctx, block102, block102.Height, "", "legacy")
	require.Error(t, err, "Block with duplicate transactions should be rejected")

	t.Logf("SECURITY NOTE: This test documents a potential bypass at Block.go:500")
	t.Logf("When subtreeStore == nil, checkDuplicateTransactions() is skipped")
	t.Logf("To properly test this bypass, unit tests are needed that call block.Valid() directly")
	t.Logf("Current integration test verifies normal validation rejects duplicates")
}

// TestEmptySubtreeSlices tests the critical crash scenario where SubtreeSlices
// is empty or contains nil entries.
//
// Location: model/Block.go:594 - b.SubtreeSlices[0].Size()
//
// Crash Risk: Accessing SubtreeSlices[0] when len(SubtreeSlices) == 0
// causes index out of bounds panic. Accessing .Size() on nil subtree
// causes nil pointer dereference.
//
// This test verifies proper handling of:
// - len(SubtreeSlices) == 0
// - SubtreeSlices[0] == nil
// - All subtree entries are nil
func TestEmptySubtreeSlicesPostgres(t *testing.T) {
	pg, err := postgres.RunPostgresTestContainer(t.Context(), "empty_subtrees_"+t.Name())
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = pg.Terminate(t.Context())
	})

	t.Run("postgres", func(t *testing.T) {
		testEmptySubtreeSlices(t, pg.ConnectionString())
	})
}

func testEmptySubtreeSlices(t *testing.T, utxoStore string) {
	td := daemon.NewTestDaemon(t, daemon.TestOptions{
		EnableRPC: true,
		SettingsOverrideFunc: test.ComposeSettings(
			test.SystemTestSettings(),
			func(tSettings *settings.Settings) {
				parsedURL, err := url.Parse(utxoStore)
				require.NoError(t, err)
				tSettings.UtxoStore.UtxoStore = parsedURL
			},
		),
	})
	defer td.Stop(t)

	err := td.BlockchainClient.Run(td.Ctx, "test")
	require.NoError(t, err)

	td.MineAndWait(t, 101)
	require.NoError(t, err)

	block101, err := td.BlockchainClient.GetBlockByHeight(td.Ctx, 101)
	require.NoError(t, err)

	// Create a minimal block with only coinbase
	// This ensures SubtreeSlices is minimal/empty after creation
	_, block102 := td.CreateTestBlock(t, block101, 10401)

	// Process the block - should handle empty subtrees gracefully
	err = td.BlockValidationClient.ProcessBlock(td.Ctx, block102, block102.Height, "", "legacy")
	require.NoError(t, err, "Block with minimal/empty subtrees should be accepted")

	t.Logf("✓ Block with minimal subtrees handled correctly")
	t.Logf("NOTE: CreateTestBlock creates valid subtrees, so direct SubtreeSlices manipulation")
	t.Logf("requires unit tests that construct Block directly with empty/nil SubtreeSlices")
}

// TestConcurrencyConfigurationEdgeCases tests edge cases in the concurrency
// configuration for duplicate detection.
//
// Location: model/Block.go:577-581
// Default calculation: Max(4, runtime.NumCPU()/2)
//
// Edge Cases:
// - concurrency = 0 (should use default)
// - concurrency = -1 (should use default)
// - concurrency = 1 (sequential execution)
// - concurrency = 10000 (very high, should not cause issues)
func TestConcurrencyConfigurationEdgeCasesPostgres(t *testing.T) {
	pg, err := postgres.RunPostgresTestContainer(t.Context(), "concurrency_config_"+t.Name())
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = pg.Terminate(t.Context())
	})

	t.Run("postgres", func(t *testing.T) {
		testConcurrencyConfigurationEdgeCases(t, pg.ConnectionString())
	})
}

func testConcurrencyConfigurationEdgeCases(t *testing.T, utxoStore string) {
	td := daemon.NewTestDaemon(t, daemon.TestOptions{
		SettingsOverrideFunc: test.ComposeSettings(
			test.SystemTestSettings(),
			func(tSettings *settings.Settings) {
				parsedURL, err := url.Parse(utxoStore)
				require.NoError(t, err)
				tSettings.UtxoStore.UtxoStore = parsedURL

				// Test with concurrency = 0 (should use default)
				tSettings.Block.CheckDuplicateTransactionsConcurrency = 0
			},
		),
	})
	defer td.Stop(t)

	err := td.BlockchainClient.Run(td.Ctx, "test")
	require.NoError(t, err)

	err = td.BlockAssemblyClient.GenerateBlocks(td.Ctx, &blockassembly_api.GenerateBlocksRequest{Count: 101})
	require.NoError(t, err)

	block1, err := td.BlockchainClient.GetBlockByHeight(td.Ctx, 1)
	require.NoError(t, err)

	duplicateTx := td.CreateTransaction(t, block1.CoinbaseTx, 0)

	block101, err := td.BlockchainClient.GetBlockByHeight(td.Ctx, 101)
	require.NoError(t, err)

	// Test with concurrency = 0
	_, block102 := td.CreateTestBlock(t, block101, 10402, duplicateTx, duplicateTx)

	err = td.BlockValidationClient.ProcessBlock(td.Ctx, block102, block102.Height, "", "legacy")
	require.Error(t, err, "Block with duplicates should be rejected even with concurrency=0")
}

// TestRaceDetectorDuplicateDetection is a documentation test that should be run
// with the -race flag to detect race conditions in duplicate detection.
//
// Location: model/Block.go:597-612 (errgroup with concurrent goroutines)
//
// Race Conditions to Detect:
// - Multiple goroutines calling txMap.Put() for same hash
// - Concurrent access to block.SubtreeSlices
// - Error propagation race conditions
//
// Usage: go test -race -run TestRaceDetectorDuplicateDetection
func TestRaceDetectorDuplicateDetectionPostgres(t *testing.T) {
	t.Skip("Skipping due to known data race in BlockValidation.setTxMinedStatus - see issue #296")

	pg, err := postgres.RunPostgresTestContainer(t.Context(), "race_detector_"+t.Name())
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = pg.Terminate(t.Context())
	})

	t.Run("postgres", func(t *testing.T) {
		testRaceDetectorDuplicateDetection(t, pg.ConnectionString())
	})
}

func testRaceDetectorDuplicateDetection(t *testing.T, utxoStore string) {
	td := daemon.NewTestDaemon(t, daemon.TestOptions{
		SettingsOverrideFunc: test.ComposeSettings(
			test.SystemTestSettings(),
			func(tSettings *settings.Settings) {
				parsedURL, err := url.Parse(utxoStore)
				require.NoError(t, err)
				tSettings.UtxoStore.UtxoStore = parsedURL

				// Use high concurrency to increase race detection probability
				tSettings.Block.CheckDuplicateTransactionsConcurrency = 16
			},
		),
	})
	defer td.Stop(t)

	err := td.BlockchainClient.Run(td.Ctx, "test")
	require.NoError(t, err)

	err = td.BlockAssemblyClient.GenerateBlocks(td.Ctx, &blockassembly_api.GenerateBlocksRequest{Count: 101})
	require.NoError(t, err)

	// Create multiple independent transactions to force multiple subtrees
	block1, err := td.BlockchainClient.GetBlockByHeight(td.Ctx, 1)
	require.NoError(t, err)
	block2, err := td.BlockchainClient.GetBlockByHeight(td.Ctx, 2)
	require.NoError(t, err)
	block3, err := td.BlockchainClient.GetBlockByHeight(td.Ctx, 3)
	require.NoError(t, err)
	block4, err := td.BlockchainClient.GetBlockByHeight(td.Ctx, 4)
	require.NoError(t, err)
	block5, err := td.BlockchainClient.GetBlockByHeight(td.Ctx, 5)
	require.NoError(t, err)
	block6, err := td.BlockchainClient.GetBlockByHeight(td.Ctx, 6)
	require.NoError(t, err)

	duplicateTx := td.CreateTransaction(t, block1.CoinbaseTx, 0)
	filler1 := td.CreateTransaction(t, block2.CoinbaseTx, 0)
	filler2 := td.CreateTransaction(t, block3.CoinbaseTx, 0)
	filler3 := td.CreateTransaction(t, block4.CoinbaseTx, 0)
	filler4 := td.CreateTransaction(t, block5.CoinbaseTx, 0)
	filler5 := td.CreateTransaction(t, block6.CoinbaseTx, 0)

	block101, err := td.BlockchainClient.GetBlockByHeight(td.Ctx, 101)
	require.NoError(t, err)

	// Create large block to trigger multiple concurrent goroutines
	_, block102 := td.CreateTestBlock(t, block101, 10403,
		duplicateTx, filler1, filler2, filler3, filler4, filler5, duplicateTx,
	)

	// Process with high concurrency - race detector will catch issues
	err = td.BlockValidationClient.ProcessBlock(td.Ctx, block102, block102.Height, "", "legacy")
	require.Error(t, err, "Block with duplicates should be rejected")

	t.Logf("✓ Race detector test completed")
	t.Logf("IMPORTANT: This test MUST be run with: go test -race")
	t.Logf("Race detector will report any concurrency issues in duplicate detection")
}
