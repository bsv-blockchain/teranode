package smoke

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/bitcoin-sv/teranode/daemon"
	"github.com/bitcoin-sv/teranode/services/rpc/bsvjson"
	utils "github.com/bitcoin-sv/teranode/test/utils"
	"github.com/bitcoin-sv/teranode/test/utils/transactions"
	"github.com/stretchr/testify/require"
)

type GetPeerInfoResponse struct {
	Result []bsvjson.GetPeerInfoResult `json:"result"`
	Error  *utils.JSONError            `json:"error"`
	ID     int                         `json:"id"`
}

// TestInvalidBlockKafkaP2P_E2E spins up two daemons with P2P and Kafka enabled, generates a block with a duplicate transaction, submits it, and checks that the invalid block is detected and reported.
func TestInvalidBlockKafkaP2P_E2E(t *testing.T) {
	node1 := daemon.NewTestDaemon(t, daemon.TestOptions{
		EnableRPC:         true,
		EnableP2P:         true,
		SettingsContext:   "docker.host.teranode1.daemon",
		EnableFullLogging: false,
	})
	defer node1.Stop(t)

	node2 := daemon.NewTestDaemon(t, daemon.TestOptions{
		EnableRPC:         true,
		EnableP2P:         true,
		SettingsContext:   "docker.host.teranode2.daemon",
		SkipRemoveDataDir: true,
		EnableFullLogging: false,
	})
	defer node2.Stop(t)

	time.Sleep(10 * time.Second)

	coinbaseTx := node1.MineToMaturityAndGetSpendableCoinbaseTx(t, node1.Ctx)

	tx := node1.CreateTransactionWithOptions(t,
		transactions.WithInput(coinbaseTx, 0),
		transactions.WithP2PKHOutputs(1, 10000),
	)

	block2, err := node1.BlockchainClient.GetBlockByHeight(node1.Ctx, 2)
	require.NoError(t, err)
	node2.WaitForBlockHeight(t, block2, 10*time.Second)

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
	err = node1.BlockValidationClient.ValidateBlock(node1.Ctx, invalidBlock)
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
