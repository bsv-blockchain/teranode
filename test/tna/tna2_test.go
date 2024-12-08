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
// $ cd test/tna
// $ go test -v -run "^TestTNA2TestSuite$/TestTxsReceivedAllNodes$" -tags test_tna

package tna

import (
	"testing"
	"time"

	helper "github.com/bitcoin-sv/ubsv/test/utils"
	"github.com/stretchr/testify/suite"
)

type TNA2TestSuite struct {
	helper.TeranodeTestSuite
}

func (suite *TNA2TestSuite) InitSuite() {
	suite.SettingsMap = map[string]string{
		"SETTINGS_CONTEXT_1": "docker.ubsv1.test.tna1Test",
		"SETTINGS_CONTEXT_2": "docker.ubsv2.test.tna1Test",
		"SETTINGS_CONTEXT_3": "docker.ubsv3.test.tna1Test",
	}
}

func (suite *TNA2TestSuite) SetupTest() {
	suite.InitSuite()
	suite.SetupTestEnv(suite.SettingsMap, suite.DefaultComposeFiles(), false)
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

		// Wait for transaction propagation
		time.Sleep(2 * time.Second)

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

		// Wait for transaction propagation
		time.Sleep(2 * time.Second)

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

		// Wait for transaction propagation
		time.Sleep(3 * time.Second)

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

func TestTNA2TestSuite(t *testing.T) {
	suite.Run(t, new(TNA2TestSuite))
}
