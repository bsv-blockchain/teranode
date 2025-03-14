//go:build test_all || test_tnc

// How to run this test manually:
// $ go test -v -run "^TestTNC1TestSuite$/TestCandidateContainsAllTxs$" -tags test_tnc ./test/tnc/tnc1_3_test.go
// $ go test -v -run "^TestTNC1TestSuite$/TestCheckHashPrevBlockCandidate$" -tags test_tnc ./test/tnc/tnc1_3_test.go
// $ go test -v -run "^TestTNC1TestSuite$/TestCoinbaseTXAmount$" -tags test_tnc ./test/tnc/tnc1_3_test.go
// $ go test -v -run "^TestTNC1TestSuite$/TestCoinbaseTXAmount2$" -tags test_tnc ./test/tnc/tnc1_3_test.go

package tnc

import (
	"encoding/hex"
	"fmt"
	"testing"

	"github.com/bitcoin-sv/teranode/model"
	"github.com/bitcoin-sv/teranode/stores/utxo"
	helper "github.com/bitcoin-sv/teranode/test/utils"
	"github.com/bitcoin-sv/teranode/test/utils/tconfig"
	"github.com/bitcoin-sv/teranode/util"
	"github.com/libsv/go-bk/bec"
	"github.com/libsv/go-bk/wif"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/bscript"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/libsv/go-bt/v2/unlocker"
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

// TNC-1.1
func (suite *TNC1TestSuite) TestCandidateContainsAllTxs() {
	testEnv := suite.TeranodeTestEnv
	ctx := testEnv.Context
	t := suite.T()
	node0 := testEnv.Nodes[0]
	blockchainClientNode0 := node0.BlockchainClient

	var hashes []*chainhash.Hash

	logger := testEnv.Logger

	blockchainSubscription, err := blockchainClientNode0.Subscribe(ctx, "test-tnc1")
	if err != nil {
		t.Errorf("error subscribing to blockchain service: %v", err)
		return
	}

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case notification := <-blockchainSubscription:
				if notification.Type == model.NotificationType_Subtree {
					hash, err := chainhash.NewHash(notification.Hash)
					require.NoError(t, err)

					hashes = append(hashes, hash)

					fmt.Println("Length of hashes:", len(hashes))
				} else {
					fmt.Println("other notifications than subtrees")
					fmt.Println(notification.Type)
				}
			}
		}
	}()

	//Create tx
	block1, err := node0.BlockchainClient.GetBlockByHeight(ctx, 1)
	require.NoError(t, err)
	t.Logf(("Block 1: %v"), block1.Header.Hash().String())

	coinbaseTx := block1.CoinbaseTx

	coinbasePrivKey1 := node0.Settings.BlockAssembly.MinerWalletPrivateKeys[0]
	coinbasePrivateKey1, err := wif.DecodeWIF(coinbasePrivKey1)
	require.NoError(t, err)
	address, err := bscript.NewAddressFromPublicKey(coinbasePrivateKey1.PrivKey.PubKey(), true)
	require.NoError(t, err)
	t.Log("Address 0:", address.AddressString)

	coinbasePrivKey2 := node0.Settings.BlockAssembly.MinerWalletPrivateKeys[1]
	coinbasePrivateKey2, err := wif.DecodeWIF(coinbasePrivKey2)
	require.NoError(t, err)
	address, err = bscript.NewAddressFromPublicKey(coinbasePrivateKey2.PrivKey.PubKey(), true)
	require.NoError(t, err)
	t.Log("Address 1:", address.AddressString)

	coinbasePrivKey3 := node0.Settings.BlockAssembly.MinerWalletPrivateKeys[2]
	coinbasePrivateKey3, err := wif.DecodeWIF(coinbasePrivKey3)
	require.NoError(t, err)
	address, err = bscript.NewAddressFromPublicKey(coinbasePrivateKey3.PrivKey.PubKey(), true)
	require.NoError(t, err)
	t.Log("Address 2:", address.AddressString)

	privateKey, err := bec.NewPrivateKey(bec.S256())
	require.NoError(t, err)

	address, err = bscript.NewAddressFromPublicKey(privateKey.PubKey(), true)
	require.NoError(t, err)

	output := coinbaseTx.Outputs[0]

	utxoHash, _ := util.UTXOHashFromOutput(coinbaseTx.TxIDChainHash(), output, uint32(0))
	//check the tx is in the utxostore
	testSpend0 := &utxo.Spend{
		TxID:     coinbaseTx.TxIDChainHash(),
		Vout:     uint32(0),
		UTXOHash: utxoHash,
	}
	resp, err := node0.UtxoStore.GetSpend(ctx, testSpend0)
	require.NoError(t, err)
	t.Logf("UTXO: %v", resp)

	addrs, err := output.LockingScript.Addresses()
	require.NoError(t, err)
	t.Logf("Output script: %v", addrs)
	t.Logf(("Output Satoshis: %d"), output.Satoshis)

	utxo := &bt.UTXO{
		TxIDHash:      coinbaseTx.TxIDChainHash(),
		Vout:          uint32(0),
		LockingScript: output.LockingScript,
		Satoshis:      output.Satoshis,
	}

	splits := uint64(100)

	// Split the utxo into 100 outputs satoshis
	sats := utxo.Satoshis / splits
	remainder := utxo.Satoshis % splits

	newTx := bt.NewTx()
	err = newTx.FromUTXOs(utxo)
	require.NoError(t, err)

	// err = newTx.AddP2PKHOutputFromAddress("1Jp7AZdMQ3hyfMfk3kJe31TDj8oppZLYdK", coinbaseTx.TotalInputSatoshis())
	// require.NoError(t, err)

	err = newTx.PayToAddress(address.AddressString, sats+remainder)
	require.NoError(t, err)

	err = newTx.FillAllInputs(ctx, &unlocker.Getter{PrivateKey: coinbasePrivateKey1.PrivKey})
	require.NoError(t, err)

	t.Logf("Sending New Transaction with RPC: %s\n", newTx.TxIDChainHash())
	txBytes := hex.EncodeToString(newTx.ExtendedBytes())

	_, err = helper.CallRPC("http://"+node0.RPCURL, "sendrawtransaction", []interface{}{txBytes})
	require.NoError(t, err)

	mc0, err0 := helper.GetMiningCandidate(ctx, testEnv.Nodes[0].BlockassemblyClient, logger)
	mc1, err1 := helper.GetMiningCandidate(ctx, testEnv.Nodes[1].BlockassemblyClient, logger)
	mc2, err2 := helper.GetMiningCandidate(ctx, testEnv.Nodes[2].BlockassemblyClient, logger)
	mp0 := utils.ReverseAndHexEncodeSlice(mc0.GetMerkleProof()[0])
	mp1 := utils.ReverseAndHexEncodeSlice(mc1.GetMerkleProof()[0])
	mp2 := utils.ReverseAndHexEncodeSlice(mc2.GetMerkleProof()[0])

	t.Log("Merkleproof 0:", mp0)
	t.Log("Merkleproof 1:", mp1)
	t.Log("Merkleproof 2:", mp2)

	if len(hashes) > 0 {
		fmt.Println("First element of hashes:", hashes[0])
	} else {
		t.Errorf("No subtrees detected: cannot calculate Merkleproofs")
	}

	t.Log("num of subtrees:", len(hashes))

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

	_, errTXs := helper.SendTXsWithDistributorV2(ctx, testEnv.Nodes[0], logger, testEnv.Nodes[0].Settings, 10000)
	if errTXs != nil {
		t.Errorf("Failed to send txs with distributor: %v", errTXs)
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

	_, errTXs := helper.SendTXsWithDistributorV2(ctx, testEnv.Nodes[0], logger, testEnv.Nodes[0].Settings, 9000)
	if errTXs != nil {
		t.Errorf("Failed to send txs with distributor: %v", errTXs)
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
