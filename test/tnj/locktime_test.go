//go:build test_all || test_tnj

// How to run this test manually:
//
// $ cd test/tnj
// $ go test -v -run "^$TNJLockTimeTestSuite/TestLocktimeScenarios$" --tags test_tnj
package tnj

import (
	"testing"
	"time"

	helper "github.com/bitcoin-sv/teranode/test/utils"
	"github.com/libsv/go-bk/bec"
	"github.com/libsv/go-bk/wif"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/bscript"
	"github.com/libsv/go-bt/v2/unlocker"
	"github.com/ordishs/gocore"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type TNJLockTimeTestSuite struct {
	helper.TeranodeTestSuite
}

func TestTNJLockTimeTestSuite(t *testing.T) {
	suite.Run(t, &TNJLockTimeTestSuite{})
}

type LockTimeScenario struct {
	name           string
	lockTime       uint32
	sequenceNumber uint32
	expectedFinal  bool
	expectInBlock  bool
}

func getFutureLockTime(daysAhead int) uint32 {
	now := time.Now()
	futureTime := now.Add(time.Duration(daysAhead*24) * time.Hour)

	return uint32(futureTime.Unix()) //nolint:gosec
}

func (suite *TNJLockTimeTestSuite) TestLocktimeScenarios() {
	// Define the scenarios
	scenarios := []LockTimeScenario{
		{
			name:           "Future Height Non-Final",
			lockTime:       350,
			sequenceNumber: 0x1,
			expectedFinal:  false,
			expectInBlock:  false,
		},
		{
			name:           "Future Height Final",
			lockTime:       350,
			sequenceNumber: 0xFFFFFFFF,
			expectedFinal:  true,
			expectInBlock:  true,
		},
		{
			name:           "Future Timestamp Non-Final",
			lockTime:       getFutureLockTime(10),
			sequenceNumber: 0x1,
			expectedFinal:  false,
			expectInBlock:  false,
		},
		{
			name:           "Future Timestamp Final",
			lockTime:       getFutureLockTime(10),
			sequenceNumber: 0xFFFFFFFF,
			expectedFinal:  true,
			expectInBlock:  true,
		},
	}

	// Iterate over each scenario and run the common test logic with specific parameters
	for _, scenario := range scenarios {
		suite.Run(scenario.name, func() {
			suite.runLocktimeScenario(scenario)
		})
	}
}

// Common logic function to handle each scenario
func (suite *TNJLockTimeTestSuite) runLocktimeScenario(scenario LockTimeScenario) {
	t := suite.T()
	testEnv := suite.TeranodeTestEnv
	ctx := testEnv.Context
	url := "http://" + testEnv.Nodes[0].AssetURL

	logger := testEnv.Logger

	txDistributor := testEnv.Nodes[0].DistributorClient

	coinbaseClient := testEnv.Nodes[0].CoinbaseClient
	utxoBalanceBefore, _, _ := coinbaseClient.GetBalance(ctx)

	// Generate private keys and addresses
	coinbasePrivKey, _ := gocore.Config().Get("coinbase_wallet_private_key")
	coinbasePrivateKey, err := wif.DecodeWIF(coinbasePrivKey)
	require.NoError(t, err)

	coinbaseAddr, _ := bscript.NewAddressFromPublicKey(coinbasePrivateKey.PrivKey.PubKey(), true)

	privateKey, err := bec.NewPrivateKey(bec.S256())
	require.NoError(t, err)

	address, err := bscript.NewAddressFromPublicKey(privateKey.PubKey(), true)
	require.NoError(t, err)

	// Request funds and send initial transaction
	tx, err := coinbaseClient.RequestFunds(ctx, address.AddressString, true)
	require.NoError(t, err)

	_, err = txDistributor.SendTransaction(ctx, tx)
	require.NoError(t, err)

	output := tx.Outputs[0]
	utxo := &bt.UTXO{
		TxIDHash:      tx.TxIDChainHash(),
		Vout:          uint32(0),
		LockingScript: output.LockingScript,
		Satoshis:      output.Satoshis,
	}

	newTx := bt.NewTx()
	err = newTx.FromUTXOs(utxo)
	require.NoError(t, err)

	err = newTx.AddP2PKHOutputFromAddress(coinbaseAddr.AddressString, 10000)
	require.NoError(t, err)

	err = newTx.FillAllInputs(ctx, &unlocker.Getter{PrivateKey: privateKey})
	require.NoError(t, err)

	newTx.Inputs[0].SequenceNumber = scenario.sequenceNumber
	newTx.LockTime = scenario.lockTime

	_, err = txDistributor.SendTransaction(ctx, newTx)
	require.NoError(t, err)

	t.Logf("Transaction sent: %s %s\n", newTx.TxIDChainHash(), newTx.TxID())
	time.Sleep(10 * time.Second)

	height, _ := helper.GetBlockHeight(url)
	logger.Infof("Block height before mining: %d\n", height)

	utxoBalanceAfter, _, _ := coinbaseClient.GetBalance(ctx)
	logger.Infof("utxoBalanceBefore: %d, utxoBalanceAfter: %d\n", utxoBalanceBefore, utxoBalanceAfter)

	_, err = helper.MineBlockWithRPC(ctx, testEnv.Nodes[0], logger)
	require.NoError(t, err)

	blockStore := testEnv.Nodes[0].ClientBlockstore
	subtreeStore := testEnv.Nodes[0].ClientSubtreestore
	blockchainClient := testEnv.Nodes[0].BlockchainClient
	bl := false
	targetHeight := height + 1

	_, err = helper.GenerateBlocks(ctx, testEnv.Nodes[0], 100, testEnv.Logger)
	require.NoError(t, err)

	for i := 0; i < 30; i++ {
		err := helper.WaitForBlockHeight(url, targetHeight, 60)
		require.NoError(t, err)

		header, _, _ := blockchainClient.GetBlockHeadersFromHeight(ctx, targetHeight, 1)
		logger.Infof("Testing on Best block header: %v", header[0].Hash())

		bl, err = helper.TestTxInBlock(ctx, logger, blockStore, subtreeStore, header[0].Hash()[:], *newTx.TxIDChainHash())
		require.NoError(t, err)

		if bl {
			break
		}

		targetHeight++
		_, err = helper.GenerateBlocks(ctx, testEnv.Nodes[0], 100, testEnv.Logger)
		require.NoError(t, err)
	}

	assert.Equal(t, scenario.expectInBlock, bl, "Test Tx not found in block")
}
