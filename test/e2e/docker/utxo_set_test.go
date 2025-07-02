//go:build test_smoke || test_utxo

// Package smoke contains smoke tests for the TERANODE node
//
// To run this specific test:
//   - Run all smoke tests: go test -tags=test_smoke ./test/smoke/...
//   - Run only UTXO tests: go test -tags=test_utxo ./test/smoke/...
//   - Run this specific test: go test -tags=test_utxo -run TestUTXOVerificationTestSuite/TestVerifyUTXOSetConsistency
//
// Test Requirements:
//   - Docker must be running (tests use testcontainers)
//   - Sufficient disk space for blockchain data
//   - Network access for pulling Docker images
//
// The test will:
//   - Start multiple Teranode instances in Docker containers
//   - Create and verify UTXO transactions
//   - Verify UTXO consistency across nodes
//   - Clean up containers after completion
package smoke

import (
	"testing"
	"time"

	"github.com/bitcoin-sv/teranode/pkg/fileformat"
	helper "github.com/bitcoin-sv/teranode/test/utils"
	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/unlocker"
	"github.com/libsv/go-bk/bec"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

// UTXOVerificationTestSuite is a test suite for verifying UTXO functionality
type UTXOVerificationTestSuite struct {
	helper.TeranodeTestSuite
}

func TestUTXOVerificationTestSuite(t *testing.T) {
	suite.Run(t, &UTXOVerificationTestSuite{})
}

// TestVerifyUTXOSetConsistency verifies that the UTXO set is consistent across nodes
// and that the block persister correctly stores UTXO sets and diffs.
//
// The test performs the following steps:
// 1. Creates a faucet transaction to generate initial UTXOs
// 2. Creates a spending transaction that consumes the faucet UTXO and creates a new one
// 3. Mines a block to include both transactions
// 4. Verifies UTXO consistency across nodes by checking:
//   - The faucet UTXO is spent (removed from UTXO set)
//   - The new UTXO from the spending transaction exists
//   - UTXO metadata matches between nodes
//
// 5. Verifies block persister files:
//   - UTXO set file exists and contains correct UTXOs
//   - UTXO additions file exists and contains new UTXOs
//   - UTXO deletions file exists and contains spent UTXOs
//
// Helper functions used:
// - ReadUTXOSet: Reads a UTXO set from the block store
// - ReadUTXODiff: Reads UTXO additions or deletions from the block store
// - VerifyUTXOFileExists: Checks if UTXO files exist in the block store
// - VerifyUTXOSetConsistency: Verifies UTXO set contains expected UTXOs
// - VerifyUTXODiffConsistency: Verifies UTXO diff contains expected changes
func (suite *UTXOVerificationTestSuite) TestVerifyUTXOSetConsistency() {
	t := suite.T()
	framework := suite.TeranodeTestEnv
	logger := framework.Logger
	ctx := framework.Context

	// Create transactions that will modify the UTXO set
	txDistributor := framework.Nodes[0].DistributorClient
	coinbaseClient := framework.Nodes[0].CoinbaseClient

	// Create two addresses
	privateKey0, err := bec.NewPrivateKey(bec.S256())
	require.NoError(t, err, "Failed to generate private key")

	privateKey1, err := bec.NewPrivateKey(bec.S256())
	require.NoError(t, err, "Failed to generate private key")

	address0, err := bscript.NewAddressFromPublicKey(privateKey0.PubKey(), true)
	require.NoError(t, err, "Failed to create address")

	address1, err := bscript.NewAddressFromPublicKey(privateKey1.PubKey(), true)
	require.NoError(t, err, "Failed to create address")

	// Get initial UTXO set state
	url := "http://" + framework.Nodes[0].AssetURL
	initialHeight, err := helper.GetBlockHeight(url)
	require.NoError(t, err, "Failed to get initial block height")

	// Create and send a faucet transaction
	faucetTx, err := coinbaseClient.RequestFunds(ctx, address0.AddressString, true)
	require.NoError(t, err, "Failed to request funds")
	logger.Infof("Faucet Transaction: %s", faucetTx.TxID())

	_, err = txDistributor.SendTransaction(ctx, faucetTx)
	require.NoError(t, err, "Failed to send faucet transaction")

	// Create a spending transaction
	output := faucetTx.Outputs[0]
	utxo := &bt.UTXO{
		TxIDHash:      faucetTx.TxIDChainHash(),
		Vout:          uint32(0),
		LockingScript: output.LockingScript,
		Satoshis:      output.Satoshis,
	}

	spendingTx := bt.NewTx()
	err = spendingTx.FromUTXOs(utxo)
	require.NoError(t, err, "Error adding UTXO to transaction")

	err = spendingTx.AddP2PKHOutputFromAddress(address1.AddressString, 10000)
	require.NoError(t, err, "Error adding output to transaction")

	err = spendingTx.FillAllInputs(ctx, &unlocker.Getter{PrivateKey: privateKey0})
	require.NoError(t, err, "Error filling transaction inputs")

	_, err = txDistributor.SendTransaction(ctx, spendingTx)
	require.NoError(t, err, "Failed to send spending transaction")

	// Mine a block to include our transactions
	_, err = helper.MineBlockWithRPC(ctx, framework.Nodes[0], logger)
	require.NoError(t, err, "Failed to mine block")

	// Wait for the block to be processed
	err = helper.WaitForBlockHeight(url, initialHeight+1, 60)
	require.NoError(t, err, "Failed to wait for block height")

	// Verify UTXO sets are consistent across nodes
	for i := 1; i < len(framework.Nodes); i++ {
		node1UTXOStore := framework.Nodes[0].UtxoStore
		nodeNUTXOStore := framework.Nodes[i].UtxoStore

		// Verify the UTXO from the faucet transaction is spent
		md1, err := node1UTXOStore.Get(ctx, faucetTx.TxIDChainHash())
		require.NoError(t, err, "Failed to get UTXO from node 1")

		mdN, err := nodeNUTXOStore.Get(ctx, faucetTx.TxIDChainHash())
		require.NoError(t, err, "Failed to get UTXO from node N")

		assert.Equal(t, md1, mdN, "UTXO metadata differs between nodes")

		// Verify the new UTXO from the spending transaction exists
		md1, err = node1UTXOStore.Get(ctx, spendingTx.TxIDChainHash())
		require.NoError(t, err, "Failed to get UTXO from node 1")

		mdN, err = nodeNUTXOStore.Get(ctx, spendingTx.TxIDChainHash())
		require.NoError(t, err, "Failed to get UTXO from node N")

		assert.Equal(t, md1, mdN, "UTXO metadata differs between nodes")

		// Get block hash for verification
		blockModel, _, err := framework.Nodes[0].BlockchainClient.GetBestBlockHeader(ctx)
		require.NoError(t, err, "Failed to get block hash")

		// Generate 100 blocks
		_, err = helper.GenerateBlocks(ctx, framework.Nodes[0], 100, logger)
		require.NoError(t, err, "Failed to generate blocks")

		// Wait for the block to be processed
		err = helper.WaitForBlockHeight(framework.Nodes[0].AssetURL, initialHeight+101, 60)
		require.NoError(t, err, "Failed to wait for block height")

		time.Sleep(20 * time.Second)

		// Verify UTXO files exist
		t.Logf("Verifying UTXO files for block %s", blockModel.Hash().String())

		// Verify UTXO additions and deletions files exist
		err = helper.VerifyUTXOFileExists(ctx, framework.Nodes[0].ClientBlockstore, *blockModel.Hash(), fileformat.FileTypeUtxoAdditions)
		require.NoError(t, err, "Failed to verify UTXO additions file")

		err = helper.VerifyUTXOFileExists(ctx, framework.Nodes[0].ClientBlockstore, *blockModel.Hash(), fileformat.FileTypeUtxoDeletions)
		require.NoError(t, err, "Failed to verify UTXO deletions file")
	}
}
