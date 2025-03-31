//go:build test_all || test_tna

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
// $ go test -v -run "^TestTNA2TestSuite$/TestTxsReceivedAllNodes$" -tags test_tna ./test/tna/tna2_test.go

package tna

import (
	"testing"

	helper "github.com/bitcoin-sv/teranode/test/utils"
	"github.com/bitcoin-sv/teranode/test/utils/tconfig"
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

func (suite *TNA2TestSuite) TestTxsReceivedAllNodes() {
	testEnv := suite.TeranodeTestEnv
	ctx := testEnv.Context
	t := suite.T()

	t.Run("Single transaction propagation", func(t *testing.T) {
		// Create and send a single transaction
		txHash, err := helper.CreateAndSendTxToSliceOfNodes(ctx, testEnv.Nodes)
		if err != nil {
			t.Fatalf("Failed to create and send transaction: %v", err)
		}

		// Check if the tx is into the UTXOStore
		txRes, errTxRes := testEnv.Nodes[0].UtxoStore.Get(ctx, &txHash)

		if txRes == nil {
			t.Fatalf("Tx not found: %v", txRes)
		}

		if errTxRes != nil {
			t.Fatalf("Failed to create and send transaction: %v", errTxRes)
		}

		// Verify transaction exists in block assembly on all nodes
		for i, node := range testEnv.Nodes {
			err := node.BlockassemblyClient.RemoveTx(ctx, &txHash)
			if err != nil {
				t.Errorf("Transaction not found in block assembly on node %d: %v", i, err)
			}
		}
	})

	t.Run("Multiple transactions propagation", func(t *testing.T) {
		// Send multiple transactions
		numTxs := 5
		txHashes, err := helper.CreateAndSendTxsToASliceOfNodes(ctx, testEnv.Nodes, numTxs)

		if err != nil {
			t.Fatalf("Failed to create and send multiple transactions: %v", err)
		}

		// Check if 1 tx is into the UTXOStore
		txRes, errTxRes := testEnv.Nodes[0].UtxoStore.Get(ctx, &txHashes[0])

		if txRes == nil {
			t.Fatalf("Tx not found: %v", txRes)
		}

		if errTxRes != nil {
			t.Fatalf("Failed to create and send transaction: %v", errTxRes)
		}

		// Verify all transactions exist in block assembly on all nodes
		for _, txHash := range txHashes {
			for i, node := range testEnv.Nodes {
				err := node.BlockassemblyClient.RemoveTx(ctx, &txHash)
				if err != nil {
					t.Errorf("Transaction %s not found in block assembly on node %d: %v", txHash, i, err)
				}
			}
		}
	})

	t.Run("Concurrent transactions propagation", func(t *testing.T) {
		// Send transactions concurrently
		numTxs := 10
		txHashes, err := helper.CreateAndSendTxsConcurrently(ctx, testEnv.Nodes[0], numTxs)
		if err != nil {
			t.Fatalf("Failed to create and send concurrent transactions: %v", err)
		}

		// Check if 1 tx is into the UTXOStore
		txRes, errTxRes := testEnv.Nodes[0].UtxoStore.Get(ctx, &txHashes[0])

		if txRes == nil {
			t.Fatalf("Tx not found: %v", txRes)
		}

		if errTxRes != nil {
			t.Fatalf("Failed to create and send transaction: %v", errTxRes)
		}

		// Verify all transactions exist in block assembly on all nodes
		for _, txHash := range txHashes {
			for i, node := range testEnv.Nodes {
				err := node.BlockassemblyClient.RemoveTx(ctx, &txHash)
				if err != nil {
					t.Errorf("Transaction %s not found in block assembly on node %d: %v", txHash, i, err)
				}
			}
		}
	})
}

// TODO: We did not check in this test, if all 10 txs in a block when it is mined
// OR
// TODO: We did not check in this test, if all 10 txs in a mining candidate

