//go:build test_all || test_tnc || debug

// How to run this test manually:
// $ go test -v -run "^TestTNC1TestSuite$/TestCandidateContainsAllTxs$" -tags test_tnc ./test/tnc/tnc1_3_test.go
// $ go test -v -run "^TestTNC1TestSuite$/TestCheckHashPrevBlockCandidate$" -tags test_tnc ./test/tnc/tnc1_3_test.go
// $ go test -v -run "^TestTNC1TestSuite$/TestCoinbaseTXAmount$" -tags test_tnc ./test/tnc/tnc1_3_test.go
// $ go test -v -run "^TestTNC1TestSuite$/TestCoinbaseTXAmount2$" -tags test_tnc ./test/tnc/tnc1_3_test.go

package tnc

import (
	"testing"
	"time"

	"github.com/bitcoin-sv/teranode/model"
	helper "github.com/bitcoin-sv/teranode/test/utils"
	"github.com/bitcoin-sv/teranode/test/utils/tconfig"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/ordishs/go-utils"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type TNC1TestSuite struct {
	helper.TeranodeTestSuite
}

func TestTNC1TestSuite(t *testing.T) {
	suite.Run(t, &TNC1TestSuite{
		TeranodeTestSuite: helper.TeranodeTestSuite{
			TConfig: tconfig.LoadTConfig(
				map[string]any{
					tconfig.KeyTeranodeContexts: []string{
						"docker.teranode1.test.tnc1Test",
						"docker.teranode2.test.tnc1Test",
						"docker.teranode3.test.tnc1Test",
					},
				},
			),
		},
	},
	)
}

// TNC-1.3
func (suite *TNC1TestSuite) TestCandidateContainsAllTxs() {
	testEnv := suite.TeranodeTestEnv
	ctx := testEnv.Context
	t := suite.T()
	node1 := testEnv.Nodes[0]
	blockchainClientNode1 := node1.BlockchainClient

	var receivedSubtreeHashes1 []*chainhash.Hash
	var receivedSubtreeHashes2 []*chainhash.Hash

	logger := testEnv.Logger

	blockchainSubscription1, err := blockchainClientNode1.Subscribe(ctx, "test-tnc1")
	if err != nil {
		t.Errorf("error subscribing to blockchain service: %v", err)
		return
	}

	blockchainSubscription2, err := blockchainClientNode1.Subscribe(ctx, "test-tnc1")
	if err != nil {
		t.Errorf("error subscribing to blockchain service: %v", err)
		return
	}

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case notification := <-blockchainSubscription1:
				if notification.Type == model.NotificationType_Subtree {
					hash, err := chainhash.NewHash(notification.Hash)
					require.NoError(t, err)

					receivedSubtreeHashes1 = append(receivedSubtreeHashes1, hash)

					t.Logf("Length of hashes 1: %d", len(receivedSubtreeHashes1))
				} else {
					t.Logf("other notifications than subtrees")
					t.Logf("notification type: %v", notification.Type)
				}
			case notification := <-blockchainSubscription2:
				if notification.Type == model.NotificationType_Subtree {
					hash, err := chainhash.NewHash(notification.Hash)
					require.NoError(t, err)

					receivedSubtreeHashes2 = append(receivedSubtreeHashes2, hash)

					t.Logf("Length of hashes 2: %d", len(receivedSubtreeHashes2))
				} else {
					t.Logf("other notifications than subtrees")
					t.Logf("notification type: %v", notification.Type)
				}
			}
		}
	}()

	//Create tx
	block1, err := node1.BlockchainClient.GetBlockByHeight(ctx, 1)
	require.NoError(t, err)
	t.Logf(("Block 1: %v"), block1.Header.Hash().String())

	coinbaseTx := block1.CoinbaseTx

	

	node1.CreateAndSendTxs(t, ctx, coinbaseTx, 100)

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	timeout := time.After(10 * time.Second)
	
	success := false
	for !success {
		select {
		case <-timeout:
			t.Fatalf("Timeout waiting for transaction to appear in block assembly")
		case <-ticker.C:
			if len(receivedSubtreeHashes1) > 0 && len(receivedSubtreeHashes2) > 0 {
				t.Logf("First element of hashes: %v", receivedSubtreeHashes1[0])
				success = true
			} else {
				t.Log("hashes is empty!")
			}
		}
	}

	mc0, err0 := helper.GetMiningCandidate(ctx, testEnv.Nodes[0].BlockassemblyClient, logger)
	mc1, err1 := helper.GetMiningCandidate(ctx, testEnv.Nodes[1].BlockassemblyClient, logger)
	mc2, err2 := helper.GetMiningCandidate(ctx, testEnv.Nodes[2].BlockassemblyClient, logger)
	mp0 := utils.ReverseAndHexEncodeSlice(mc0.GetMerkleProof()[0])
	mp1 := utils.ReverseAndHexEncodeSlice(mc1.GetMerkleProof()[0])
	mp2 := utils.ReverseAndHexEncodeSlice(mc2.GetMerkleProof()[0])

	t.Log("Merkleproof 0:", mp0)
	t.Log("Merkleproof 1:", mp1)
	t.Log("Merkleproof 2:", mp2)

	if len(receivedSubtreeHashes1) > 0 && len(receivedSubtreeHashes2) > 0 {
		t.Log("First element of hashes 1:", receivedSubtreeHashes1[0])
		t.Log("First element of hashes 2:", receivedSubtreeHashes2[0])
	} else {
		t.Errorf("No subtrees detected: cannot calculate Merkleproofs")
	}
	t.Log("num of subtrees 1:", len(receivedSubtreeHashes1))
	t.Log("num of subtrees 2:", len(receivedSubtreeHashes2))

	if mp0 != mp1 || mp1 != mp2 {
		t.Errorf("Merkle proofs are different")
	}

	// Calculate MerkleProof for other TXs sent
	if err0 != nil {
		t.Errorf("Failed to get mining candidate 0: %v", err0)
	}

	if err1 != nil {
		t.Errorf("Failed to get mining candidate 1: %v", err1)
	}

	if err2 != nil {
		t.Errorf("Failed to get mining candidate 2: %v", err2)
	}
}

// TNC-1.2
func (suite *TNC1TestSuite) TestCheckHashPrevBlockCandidate() {
	testEnv := suite.TeranodeTestEnv
	ctx := testEnv.Context
	t := suite.T()
	logger := testEnv.Logger
	ba := testEnv.Nodes[0].BlockassemblyClient
	bc := testEnv.Nodes[0].BlockchainClient

	block1, err := testEnv.Nodes[0].BlockchainClient.GetBlockByHeight(ctx, 1)
	require.NoError(t, err)
	coinbaseTx := block1.CoinbaseTx

	_, _, err = testEnv.Nodes[0].CreateAndSendTxs(t, ctx, coinbaseTx, 100)
	if err != nil {
		t.Errorf("Failed to create and send txs: %v", err)
	}

	_, errMine0 := helper.MineBlockWithRPC(ctx, testEnv.Nodes[0], logger)
	if errMine0 != nil {
		t.Errorf("Failed to mine block: %v", errMine0)
	}

	mc0, errmc0 := ba.GetMiningCandidate(ctx)
	if errmc0 != nil {
		t.Errorf("error getting mining candidate: %v", errmc0)
	}

	bestBlockheader, _, errBbh := bc.GetBestBlockHeader(ctx)
	if errBbh != nil {
		t.Errorf("error getting best block header: %v", errBbh)
	}

	prevHash, errHash := chainhash.NewHash(mc0.PreviousHash)

	if errHash != nil {
		t.Errorf("error getting previous hash: %v", errHash)
	}

	if bestBlockheader.String() != prevHash.String() {
		t.Errorf("Teranode working on incorrect prevHash")
	}
}

// TNC-1.3-TC-01
func (suite *TNC1TestSuite) TestCoinbaseTXAmount() {
	testEnv := suite.TeranodeTestEnv
	ctx := testEnv.Context
	t := suite.T()

	logger := testEnv.Logger

	ba := testEnv.Nodes[0].BlockassemblyClient
	bc := testEnv.Nodes[0].BlockchainClient

	mc0, errmc0 := ba.GetMiningCandidate(ctx)
	if errmc0 != nil {
		t.Errorf("Error getting mining candidate on node 0")
	}

	coinbaseValueBlock := mc0.CoinbaseValue
	logger.Infof("Coinbase value mining candidate 0: %d", coinbaseValueBlock)

	_, bbhmeta, errbb := bc.GetBestBlockHeader(ctx)
	if errbb != nil {
		t.Errorf("Error getting best block")
	}

	block, errblock := bc.GetBlockByHeight(ctx, bbhmeta.Height)

	if errblock != nil {
		t.Errorf("Error getting block by height")
	}

	coinbaseTX := block.CoinbaseTx
	amount := coinbaseTX.TotalOutputSatoshis()
	logger.Infof("Amount inside block coinbase tx: %d", amount)

	if amount != coinbaseValueBlock {
		t.Errorf("Error calculating Coinbase Tx amount")
	}
}

// TNC-1.3-TC-02
func (suite *TNC1TestSuite) TestCoinbaseTXAmount2() {
	testEnv := suite.TeranodeTestEnv
	ctx := testEnv.Context
	t := suite.T()

	ba := testEnv.Nodes[0].BlockassemblyClient

	logger := testEnv.Logger

	block1, err := testEnv.Nodes[0].BlockchainClient.GetBlockByHeight(ctx, 1)
	if err != nil {
		t.Errorf("Failed to get block by height: %v", err)
	}

	_, _, err = testEnv.Nodes[0].CreateAndSendTxs(t, ctx, block1.CoinbaseTx, 35)
	if err != nil {
		t.Errorf("Failed to create and send txs: %v", err)
	}

	mc0, errmc0 := ba.GetMiningCandidate(ctx)
	if errmc0 != nil {
		t.Errorf("Error getting mining candidate on node 0")
	}

	coinbaseValueBlock := mc0.CoinbaseValue
	logger.Infof("Coinbase value mining candidate 0: %d", coinbaseValueBlock)

	block, errblock := helper.GetBestBlockV2(ctx, testEnv.Nodes[0])
	if errblock != nil {
		t.Errorf("Error getting best block")
	}

	logger.Infof("Header of the best block %v", block.Header.String())
	logger.Infof("Length of subtree slices %d", len(block.SubtreeSlices))

	coinbaseTX := block.CoinbaseTx
	amount := coinbaseTX.TotalOutputSatoshis()
	logger.Infof("Amount inside block coinbase tx: %d\n", amount)
	logger.Infof("Fees: %d", coinbaseValueBlock-amount)

	if coinbaseValueBlock < amount {
		t.Errorf("Error calculating fees")
	}
}
