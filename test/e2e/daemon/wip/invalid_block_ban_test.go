package smoke

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/daemon"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/settings"
	helper "github.com/bsv-blockchain/teranode/test/utils"
	"github.com/bsv-blockchain/teranode/test/utils/transactions"
	"github.com/stretchr/testify/require"
)

// TestInvalidBlockKafkaP2P_E2E spins up two daemons with P2P and Kafka enabled, generates a block with a duplicate transaction, submits it, and checks that the invalid block is detected and reported.
func TestInvalidBlockBanScore(t *testing.T) {
	SharedTestLock.Lock()
	defer SharedTestLock.Unlock()

	node1 := daemon.NewTestDaemon(t, daemon.TestOptions{
		EnableRPC: true,
		EnableP2P: true,
		FSMState:  blockchain.FSMStateRUNNING,
		SettingsOverrideFunc: func(s *settings.Settings) {
			s.ChainCfgParams.CoinbaseMaturity = 1
		},
	})
	defer node1.Stop(t)

	node2 := daemon.NewTestDaemon(t, daemon.TestOptions{
		EnableRPC: true,
		EnableP2P: true,
		FSMState:  blockchain.FSMStateRUNNING,
		SettingsOverrideFunc: func(s *settings.Settings) {
			s.ChainCfgParams.CoinbaseMaturity = 1
		},
	})
	defer node2.Stop(t)

	// node2.ConnectToPeer(t, node1)
	node1.ConnectToPeer(t, node2)

	coinbaseTx := node1.MineToMaturityAndGetSpendableCoinbaseTx(t, node1.Ctx)

	block2, err := node1.BlockchainClient.GetBlockByHeight(node1.Ctx, 2)
	require.NoError(t, err)

	blockWaitTime := 30 * time.Second
	err = helper.WaitForNodeBlockHeight(t.Context(), node2.BlockchainClient, block2.Height, blockWaitTime)
	require.NoError(t, err)

	tx := node1.CreateTransactionWithOptions(t,
		transactions.WithInput(coinbaseTx, 0),
		transactions.WithP2PKHOutputs(1, 10000),
	)

	err = node1.PropagationClient.ProcessTransaction(node1.Ctx, tx)
	require.NoError(t, err)
	err = node2.PropagationClient.ProcessTransaction(node2.Ctx, tx)
	require.NoError(t, err)

	// Create a block with a duplicate transaction using CreateTestBlock
	_, invalidBlock := node2.CreateTestBlock(t, block2, 1, tx, tx) // duplicate tx

	err = node2.BlockchainClient.AddBlock(node2.Ctx, invalidBlock, "")
	require.NoError(t, err)

	time.Sleep(1 * time.Second)

	// Submit the invalid block to node1
	err = node1.BlockValidationClient.ValidateBlock(node1.Ctx, invalidBlock, nil)
	require.Error(t, err, "Block with duplicate tx should be rejected")

	bestHeight, _, err := node1.BlockchainClient.GetBestHeightAndTime(node1.Ctx)
	require.NoError(t, err)
	require.Equal(t, uint32(2), bestHeight, "Invalid block should not advance chain height")

	// After checking chain height, wait for ban to be processed
	time.Sleep(1 * time.Second)

	resp, err := node1.CallRPC(node1.Ctx, "getpeerinfo", nil)
	require.NoError(t, err)

	var peerInfoResp GetPeerInfoResponse
	err = json.Unmarshal([]byte(resp), &peerInfoResp)
	require.NoError(t, err)
	require.Nil(t, peerInfoResp.Error)

	found := false

	for _, peer := range peerInfoResp.Result {
		if peer.BanScore == 10 {
			found = true

			t.Logf("Peer %s, peerId %s has BanScore %d", peer.Addr, peer.PeerID, peer.BanScore)
		}
	}

	require.True(t, found, "No peer had a nonzero BanScore after invalid block")
}
