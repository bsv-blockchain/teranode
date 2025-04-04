//go:build test_tna || debug

// Package tna implements acceptance tests for Teranode's transaction and block handling.
//
// TNA-2 Test Suite
// This test suite verifies that Teranode correctly collects new transactions into a block.
// It tests:
// 1. Single transaction propagation across all nodes
// 2. Multiple transaction propagation in sequence
// 3. Concurrent transaction propagation under load
//
// How to run this test manually:
// $ go test -v -run "^TestTNA2TestSuite$/TestSingleTransactionPropagation$" -tags test_tna ./test/tna/tna2_test.go
// $ go test -v -run "^TestTNA2TestSuite$/TestMultipleTransactionsPropagation$" -tags test_tna ./test/tna/tna2_test.go
// $ go test -v -run "^TestTNA2TestSuite$/TestConcurrentTransactionsPropagation$" -tags test_tna ./test/tna/tna2_test.go
//
// To run all TNA tests:
// $ go test -v -tags test_tna ./...

package tna

import (
	"testing"
	"time"

	helper "github.com/bitcoin-sv/teranode/test/utils"
	"github.com/bitcoin-sv/teranode/test/utils/tconfig"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type TNA2TestSuite struct {
	helper.TeranodeTestSuite
}

func TestTNA2TestSuite(t *testing.T) {
	suite.Run(t, &TNA2TestSuite{
		TeranodeTestSuite: helper.TeranodeTestSuite{
			TConfig: tconfig.LoadTConfig(
				map[string]any{
					tconfig.KeyTeranodeContexts: []string{
						"docker.teranode1.test.tna1Test",
						"docker.teranode2.test.tna1Test",
						"docker.teranode3.test.tna1Test",
					},
				},
			),
		},
	},
	)
}

func (suite *TNA2TestSuite) TestSingleTransactionPropagation() {
	testEnv := suite.TeranodeTestEnv
	ctx := testEnv.Context
	t := suite.T()

	node1 := testEnv.Nodes[0]
	block1, err := node1.BlockchainClient.GetBlockByHeight(ctx, 1)
	require.NoError(t, err)
	parenTx := block1.CoinbaseTx
	sentTx, err := node1.CreateAndSendTx(t, ctx, parenTx)

	// Check if the tx is into the UTXOStore
	txRes, errTxRes := node1.UtxoStore.Get(ctx, sentTx.TxIDChainHash())
	if txRes == nil {
		t.Fatalf("Tx not found: %v", txRes)
	}
	if errTxRes != nil {
		t.Fatalf("Failed to create and send transaction: %v", errTxRes)
	}

	timeout := time.After(10 * time.Second)
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for _, node := range testEnv.Nodes {
		success := false
		for !success {
			select {
			case <-timeout:
				t.Fatalf("Timeout waiting for transaction to appear in block assembly")
			case <-ticker.C:
				// If we can remove the tx, it means it's in block assembly
				err = node.BlockassemblyClient.RemoveTx(ctx, sentTx.TxIDChainHash())
				if err == nil {
					success = true
				}
			}
		}
	}
}

func (suite *TNA2TestSuite) TestMultipleTransactionsPropagation() {
	testEnv := suite.TeranodeTestEnv
	ctx := testEnv.Context
	t := suite.T()

	// Send multiple transactions
	node1 := testEnv.Nodes[0]
	numTxs := 5
	block1, err := node1.BlockchainClient.GetBlockByHeight(ctx, 1)
	require.NoError(t, err)
	parenTx := block1.CoinbaseTx
	_, sentTxHashes, err := node1.CreateAndSendTxs(t, ctx, parenTx, numTxs)
	if err != nil {
		t.Fatalf("Failed to create and send multiple transactions: %v", err)
	}

	// Check if 1 tx is into the UTXOStore
	txRes, errTxRes := node1.UtxoStore.Get(ctx, sentTxHashes[0])
	if txRes == nil {
		t.Fatalf("Tx not found: %v", txRes)
	}
	if errTxRes != nil {
		t.Fatalf("Failed to create and send transaction: %v", errTxRes)
	}

	timeout := time.After(10 * time.Second)
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	// Verify all transactions exist in block assembly on all nodes
	for _, txHash := range sentTxHashes {
		for _, node := range testEnv.Nodes {
			success := false
			for !success {
				select {
				case <-timeout:
					t.Fatalf("Timeout waiting for transaction to appear in block assembly")
				case <-ticker.C:
					err := node.BlockassemblyClient.RemoveTx(ctx, txHash)
					if err == nil {
						success = true
					}
				}
			}
		}
	}
}

func (suite *TNA2TestSuite) TestConcurrentTransactionsPropagation() {
	testEnv := suite.TeranodeTestEnv
	ctx := testEnv.Context
	t := suite.T()

	// Send transactions concurrently
	node1 := testEnv.Nodes[0]
	numTxs := 10
	block1, err := node1.BlockchainClient.GetBlockByHeight(ctx, 1)
	require.NoError(t, err)
	parenTx := block1.CoinbaseTx
	_, sentTxHashes, err := node1.CreateAndSendTxsConcurrently(t, ctx, parenTx, numTxs)
	if err != nil {
		t.Fatalf("Failed to create and send concurrent transactions: %v", err)
	}

	// Check if 1 tx is into the UTXOStore
	_, errTxRes := testEnv.Nodes[0].UtxoStore.Get(ctx, sentTxHashes[0])
	require.NoError(t, errTxRes)

	timeout := time.After(10 * time.Second)
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for _, txHash := range sentTxHashes {
		for _, node := range testEnv.Nodes {
			success := false
			for !success {
				select {
				case <-timeout:
					t.Fatalf("Timeout waiting for transaction to appear in block assembly")
				case <-ticker.C:
					err := node.BlockassemblyClient.RemoveTx(ctx, txHash)
					if err == nil {
						success = true
					}
				}
			}
		}
	}
}

// TODO: We did not check in this test, if all 10 txs in a block when it is mined
// OR
// TODO: We did not check in this test, if all 10 txs in a mining candidate
