//go:build test_all || test_smoke || test_functional || debug

// How to run each test:
// Clean up docker containers before running the test manually
// $ cd test/smoke/
// $ go test -v -run "^TestSanityTestSuite$/TestShouldAllowFairTx$" -tags test_functional

package smoke

import (
	"testing"

	helper "github.com/bitcoin-sv/teranode/test/utils"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type SanityTestSuite struct {
	helper.TeranodeTestSuite
}

func TestSanityTestSuite(t *testing.T) {
	suite.Run(t, &SanityTestSuite{})
}

func (suite *SanityTestSuite) TestShouldAllowFairTx() {
	t := suite.T()
	testEnv := suite.TeranodeTestEnv
	ctx := testEnv.Context
	logger := testEnv.Logger
	url := "http://" + testEnv.Nodes[0].AssetURL

	txDistributor := testEnv.Nodes[0].DistributorClient
	block1, err := testEnv.Nodes[0].BlockchainClient.GetBlockByHeight(ctx, 1)
	require.NoError(t, err)

	tx, err := testEnv.Nodes[0].CreateAndSendTx(t, ctx, block1.CoinbaseTx)
	require.NoError(t, err)
	_, err = txDistributor.SendTransaction(ctx, tx)
	require.NoError(t, err)

	teranode1RPCEndpoint := testEnv.Nodes[0].RPCURL
	teranode1RPCEndpoint = "http://" + teranode1RPCEndpoint
	// Generate blocks
	_, err = helper.CallRPC(teranode1RPCEndpoint, "generate", []interface{}{110})
	require.NoError(t, err, "Failed to generate blocks: %v", err)

	blockStore := testEnv.Nodes[0].ClientBlockstore
	// subtreeStore := testEnv.Nodes[0].ClientSubtreestore
	blockchainClient := testEnv.Nodes[0].BlockchainClient
	bl := false
	targetHeight := uint32(102)

	// for i := 0; i < 5; i++ {
	// 	err := helper.WaitForBlockHeight(url, targetHeight, 60)
	// 	if err != nil {
	// 		t.Errorf("Failed to wait for block height: %v", err)
	// 	}

	// 	header, meta, err := blockchainClient.GetBlockHeadersFromHeight(ctx, targetHeight, 1)
	// 	if err != nil {
	// 		t.Errorf("Failed to get block headers: %v", err)
	// 	}

	// 	t.Logf("Testing on Best block header: %v", header[0].Hash())

	// 	t.Logf("Testing on Best block meta: %v", meta[0].Height)

	// 	bl, err = helper.TestTxInBlock(ctx, logger, blockStore, subtreeStore, header[0].Hash()[:], *tx.TxIDChainHash())
	// 	if err != nil {
	// 		t.Errorf("error checking if tx exists in block: %v, error %v", meta[0].Height, err)
	// 	}

	// 	if bl {
	// 		break
	// 	}

	// 	targetHeight++
	// }

	// require.True(t, bl, "Test Tx not found in block")

	header, _, err := blockchainClient.GetBlockHeadersFromHeight(ctx, targetHeight, 1)
	require.NoError(t, err, "Failed to get block headers: %v", err)

	block202Subtrees, err := helper.GetBlockSubtreeHashes(ctx, testEnv.Logger, header[0].Hash()[:], blockStore)
	require.NoError(t, err, "Failed to get block 202: %v", err)

	for _, subtree := range block202Subtrees {
		t.Logf("Subtree: %v", subtree)
	}
	// assert.Equal(t, true, bl, "Test Tx not found in block")

	txHashes, err := helper.GetSubtreeTxHashes(ctx, logger, block202Subtrees[0], url, testEnv.Nodes[0].Settings)
	require.NoError(t, err, "Failed to get subtree tx hashes: %v", err)

	t.Logf("Subtree tx hashes: %v", txHashes)

	// verify if the tx is in the txHashes
	for _, txHash := range txHashes {
		if txHash == *tx.TxIDChainHash() {
			t.Logf("Test Tx found in subtree")
			bl = true
			break
		}
	}
	require.True(t, bl, "Test Tx not found in subtree")
}
