package smoke

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/daemon"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/test/utils/transactions"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestInvalidSubtree_BanScoreConfiguration tests that ban score configuration works
// Note: Creating actual invalid subtree scenarios in e2e tests is complex because
// invalid subtrees are detected at the HTTP protocol level during subtree fetching,
// not at the block structure level. This test verifies the configuration and
// basic ban score accumulation using invalid blocks as a proxy.
func TestInvalidSubtree_BanScoreConfiguration(t *testing.T) {
	// Create node1 with custom ban settings
	node1 := daemon.NewTestDaemon(t, daemon.TestOptions{
		EnableRPC: true,
		EnableP2P: true,
		SettingsOverrideFunc: func(s *settings.Settings) {
			s.P2P.BanThreshold = 30 // Lower threshold for testing
			s.P2P.BanDuration = 60 * time.Second
			s.ChainCfgParams.CoinbaseMaturity = 2
		},
	})
	defer node1.Stop(t)

	node2 := daemon.NewTestDaemon(t, daemon.TestOptions{
		EnableRPC:         true,
		EnableP2P:         true,
		SkipRemoveDataDir: true,
		SettingsOverrideFunc: func(s *settings.Settings) {
			s.P2P.BanThreshold = 30
			s.P2P.BanDuration = 60 * time.Second
			s.ChainCfgParams.CoinbaseMaturity = 2
		},
	})
	defer node2.Stop(t)

	// connect node2 to node1 via p2p
	node2.ConnectToPeer(t, node1)
	node1.ConnectToPeer(t, node2)

	// Mine blocks on node1
	coinbaseTx := node1.MineToMaturityAndGetSpendableCoinbaseTx(t, node1.Ctx)

	// Create a valid transaction
	tx := node1.CreateTransactionWithOptions(t,
		transactions.WithInput(coinbaseTx, 0),
		transactions.WithP2PKHOutputs(1, 10000),
	)

	// Get block2 and wait for node2 to sync
	block2, err := node1.BlockchainClient.GetBlockByHeight(node1.Ctx, 2)
	require.NoError(t, err)
	node2.WaitForBlockHeight(t, block2, 10*time.Second)

	// Process transaction on both nodes
	err = node1.PropagationClient.ProcessTransaction(node1.Ctx, tx)
	require.NoError(t, err)
	err = node2.PropagationClient.ProcessTransaction(node2.Ctx, tx)
	require.NoError(t, err)

	// Create an invalid block with duplicate transaction to trigger ban score
	// This simulates misbehavior that would increase ban score
	_, invalidBlock := node2.CreateTestBlock(t, block2, 1, tx, tx) // duplicate tx

	err = node2.BlockchainClient.AddBlock(node2.Ctx, invalidBlock, "")
	require.NoError(t, err)

	time.Sleep(1 * time.Second)

	// Node1 should reject the invalid block
	err = node1.BlockValidationClient.ValidateBlock(node1.Ctx, invalidBlock, nil)
	require.Error(t, err, "Block with duplicate tx should be rejected")

	// Wait for ban score to be updated
	time.Sleep(2 * time.Second)

	// Check peer info to verify ban score was increased
	resp, err := node1.CallRPC(node1.Ctx, "getpeerinfo", nil)
	require.NoError(t, err)

	var peerInfoResp GetPeerInfoResponse
	err = json.Unmarshal([]byte(resp), &peerInfoResp)
	require.NoError(t, err)
	require.Nil(t, peerInfoResp.Error)

	// Verify that a peer has an increased ban score
	found := false
	for _, peer := range peerInfoResp.Result {
		if peer.BanScore > 0 {
			found = true
			t.Logf("Peer %s has BanScore %d after invalid block", peer.Addr, peer.BanScore)
			// Invalid block gives 10 points (same as invalid subtree)
			assert.GreaterOrEqual(t, peer.BanScore, int32(10), "Ban score should be at least 10")
		}
	}
	require.True(t, found, "At least one peer should have increased ban score after invalid block")

	// Small delay to allow background operations to complete before cleanup
	time.Sleep(500 * time.Millisecond)
}
