//go:build test_all || test_smoke || test_rpc

// How to execute single tests: go test -v -run "^TestRPCTestSuite$/TestRPCReconsiderBlock$" -tags rpc
// Change TestRPCInvalidateBlock with the name of the test to execute

package smoke

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"math"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/model"
	ba "github.com/bitcoin-sv/teranode/services/blockassembly"
	"github.com/bitcoin-sv/teranode/services/blockchain"
	"github.com/bitcoin-sv/teranode/services/coinbase"
	"github.com/bitcoin-sv/teranode/settings"
	"github.com/bitcoin-sv/teranode/stores/blob"
	"github.com/bitcoin-sv/teranode/stores/blob/options"
	helper "github.com/bitcoin-sv/teranode/test/utils"
	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/bitcoin-sv/teranode/util"
	"github.com/bitcoin-sv/teranode/util/distributor"
	"github.com/bitcoin-sv/teranode/util/test"
	"github.com/libsv/go-bk/bec"
	"github.com/libsv/go-bk/wif"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/bscript"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/libsv/go-bt/v2/unlocker"
	"github.com/ordishs/gocore"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

var kafkaCmd *exec.Cmd
var appCmd *exec.Cmd
var appPID int

type RPCTestSuite struct {
	helper.TeranodeTestSuite
}

// waitForProcessToStop waits for a process matching the given pattern to stop
func waitForProcessToStop(pattern string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		cmd := exec.Command("pgrep", "-f", pattern)
		if err := cmd.Run(); err != nil {
			// Process not found, which means it has stopped
			return nil
		}

		time.Sleep(100 * time.Millisecond)
	}

	return errors.NewProcessingError("process did not stop within timeout")
}

func startKafka(logFile string, _ ulogger.Logger) error {
	kafkaCmd = exec.Command("../../deploy/dev/kafka.sh")
	kafkaLog, err := os.Create(logFile)

	if err != nil {
		return err
	}

	defer kafkaLog.Close()

	kafkaCmd.Stdout = kafkaLog
	kafkaCmd.Stderr = kafkaLog

	return kafkaCmd.Start()
}

func startApp(logFile string, logger ulogger.Logger) error {
	isGitHubActions := os.Getenv("GITHUB_ACTIONS") == stringTrue
	err := helper.RemoveDataDirectory("../../data", isGitHubActions)

	if err != nil {
		return err
	}

	appCmd := exec.Command("go", "run", "../../.")

	appCmd.Env = append(os.Environ(), "SETTINGS_CONTEXT=dev.system.test.rpc")

	appLog, err := os.Create(logFile)
	if err != nil {
		return err
	}
	defer appLog.Close()

	appCmd.Stdout = appLog
	appCmd.Stderr = appLog

	logger.Infof("Starting app in the background...")

	if err := appCmd.Start(); err != nil {
		return err
	}

	appPID = appCmd.Process.Pid

	logger.Infof("Waiting for app to be ready...")

	for {
		_, err = util.DoHTTPRequest(context.Background(), "http://localhost:8000/health/liveness")
		if err == nil {
			break
		}

		time.Sleep(100 * time.Millisecond)
	}

	logger.Infof("App ready")

	return nil
}

func stopKafka(logger ulogger.Logger) {
	logger.Infof("Stopping Kafka...")

	// First stop the container
	stopCmd := exec.Command("docker", "stop", "kafka-server")
	if err := stopCmd.Run(); err != nil {
		logger.Infof("Failed to stop Kafka container: %v\n", err)
	}

	// Wait for container to fully stop
	time.Sleep(2 * time.Second)

	// Remove the container
	rmCmd := exec.Command("docker", "rm", "-f", "kafka-server")
	if err := rmCmd.Run(); err != nil {
		logger.Infof("Failed to remove Kafka container: %v\n", err)
	} else {
		logger.Infof("Kafka container removed successfully")
	}

	// Remove any leftover Kafka data
	if err := os.RemoveAll("./kafka-data"); err != nil {
		logger.Infof("Failed to remove Kafka data directory: %v\n", err)
	}

	logger.Infof("Kafka cleanup completed")
}

func stopTeranode(logger ulogger.Logger) {
	isGitHubActions := os.Getenv("GITHUB_ACTIONS") == stringTrue

	logger.Infof("Stopping TERANODE...")

	// First try graceful shutdown using SIGTERM
	termCmd := exec.Command("pkill", "-TERM", "-f", "teranode")
	if err := termCmd.Run(); err != nil {
		logger.Infof("Failed to stop TERANODE gracefully: %v\n", err)

		// If graceful shutdown fails, try SIGINT
		intCmd := exec.Command("pkill", "-INT", "-f", "teranode")
		if err := intCmd.Run(); err != nil {
			logger.Infof("Failed to stop TERANODE with SIGINT: %v\n", err)

			// As a last resort, force kill
			killCmd := exec.Command("pkill", "-9", "-f", "teranode")
			if err := killCmd.Run(); err != nil {
				logger.Infof("Failed to force stop TERANODE: %v\n", err)
			}
		}
	}

	// Wait up to 5 seconds for the process to fully stop
	if err := waitForProcessToStop("teranode", 5*time.Second); err != nil {
		logger.Infof("Warning: TERANODE process may not have fully stopped: %v\n", err)
	}

	// Remove data directory with proper error handling
	if err := helper.RemoveDataDirectory("./data", isGitHubActions); err != nil {
		logger.Infof("Failed to remove data directory: %v\n", err)
	} else {
		logger.Infof("Data directory removed successfully")
	}

	logger.Infof("TERANODE cleanup completed")
}

func (suite *RPCTestSuite) SetupTest() {
	var logLevelStr, _ = gocore.Config().Get("logLevel", "ERROR")
	log := ulogger.New("e2eTestRun", ulogger.WithLevel(logLevelStr))

	stopTeranode(log)

	if err := startKafka("kafka.log", log); err != nil {
		log.Errorf("Failed to start Kafka: %v", err)
	}

	// Wait for Kafka to be fully ready
	time.Sleep(2 * time.Second)

	// Start the app
	if err := startApp("app.log", log); err != nil {
		log.Errorf("Failed to start app: %v", err)
		// If app fails to start, make sure to clean up Kafka
		stopKafka(log)
	}
}

func (suite *RPCTestSuite) TearDownTest() {
	var logLevelStr, _ = gocore.Config().Get("logLevel", "ERROR")
	log := ulogger.New("e2eTestRun", ulogger.WithLevel(logLevelStr))
	// First stop TERANODE
	// stopTeranode(log)

	// Wait for TERANODE to fully stop before stopping Kafka
	time.Sleep(2 * time.Second)

	// Then stop Kafka
	stopKafka(log)

}

const (
	teranode1RPCEndpoint string = "http://localhost:9292"
	nullStr              string = "null"
	stringTrue                  = "true"
)

func (suite *RPCTestSuite) TestRPCGetBlockchainInfo() {
	var blockchainInfo helper.BlockchainInfo

	t := suite.T()
	resp, err := helper.CallRPC(teranode1RPCEndpoint, "getblockchaininfo", []interface{}{})

	if err != nil {
		t.Errorf("Error CallRPC: %v", err)
	}

	errJSON := json.Unmarshal([]byte(resp), &blockchainInfo)

	if errJSON != nil {
		t.Errorf("JSON decoding error: %v", errJSON)
		return
	}

	t.Logf("%s", resp)

	if blockchainInfo.Result.BestBlockHash == "" {
		t.Errorf("Test failed: BestBlockHash is empty")
	} else {
		t.Logf("Test succeeded: BestBlockHash is not empty")
	}

	if len(blockchainInfo.Result.Chainwork) < 64 || blockchainInfo.Result.Chainwork == "" {
		t.Errorf("Test failed: Chainwork value not valid")
	} else {
		t.Logf("Test succeeded: Chainwork value is valid: %v", blockchainInfo.Result.Chainwork)
	}
}

func (suite *RPCTestSuite) TestRPCGetPeerInfo() {
	t := suite.T()

	var p2pResp helper.P2PRPCResponse

	resp, err := helper.CallRPC(teranode1RPCEndpoint, "getpeerinfo", []interface{}{})

	if err != nil {
		t.Errorf("Error CallRPC: %v", err)
	}

	errJSON := json.Unmarshal([]byte(resp), &p2pResp)
	if err != nil {
		t.Errorf("JSON decoding error: %v", errJSON)
		return
	}

	t.Logf("%s", resp)

	if len(p2pResp.Result) == 0 {
		t.Errorf("Test failed: peers list is empty")
	} else {
		t.Logf("Test succeeded, retrieved P2P peers informations")
	}

	if p2pResp.Result[0].ID == 0 {
		t.Errorf("Test failed: peers list is empty")
	} else {
		t.Logf("Test succeeded, retrieved P2PNode ID informations")
	}

	if p2pResp.Result[0].Addr != "" {
		t.Errorf("Successfull check: peer addr not empty")
	} else {
		t.Logf("Test failed, peer addr is empty")
	}

	if p2pResp.Result[0].AddrLocal != "" {
		t.Errorf("Successfull check: peer local addr not empty")
	} else {
		t.Logf("Test failed, peer local addr is empty")
	}

	now := time.Now().Unix()
	if p2pResp.Result[0].LastSend > now || p2pResp.Result[0].LastRecv > now || p2pResp.Result[0].ConnTime > now {
		t.Errorf("Timestamp is not valid: it can't be in the future")
	}
}

func (suite *RPCTestSuite) TestRPCGetInfo() {
	t := suite.T()

	var getInfo helper.GetInfo

	resp, err := helper.CallRPC(teranode1RPCEndpoint, "getinfo", []interface{}{})

	if err != nil {
		t.Errorf("Error CallRPC: %v", err)
	}

	errJSON := json.Unmarshal([]byte(resp), &getInfo)
	if err != nil {
		t.Errorf("JSON decoding error: %v", errJSON)
		return
	}

	t.Logf("%s", resp)

	if getInfo.Error != nil {
		if getInfo.Error.Message != "" {
			t.Errorf("Test failed: getinfo RPC call returned error: %v", getInfo.Error.Message)
		} else {
			t.Errorf("Test failed: getinfo RPC call returned an unexpected error type: %v", getInfo.Error)
		}
	} else {
		t.Logf("Test succeeded, retrieved information from getinfo RPC call")
	}
}

func (suite *RPCTestSuite) TestRPCGetDifficulty() {
	t := suite.T()

	var getDifficulty helper.GetDifficultyResponse

	resp, err := helper.CallRPC(teranode1RPCEndpoint, "getdifficulty", []interface{}{})

	if err != nil {
		t.Errorf("Error CallRPC: %v", err)
	}

	errJSON := json.Unmarshal([]byte(resp), &getDifficulty)
	if err != nil {
		t.Errorf("JSON decoding error: %v", errJSON)
		return
	}

	t.Logf("%s", resp)

	if getDifficulty.Error != nil {
		if getDifficulty.Error.Message != nullStr {
			t.Errorf("Test failed: getdifficulty RPC call returned error: %v", getDifficulty.Error.Message)
		} else {
			t.Errorf("Test failed: getdifficulty RPC call returned an unexpected error type: %v", getDifficulty.Error)
		}
	} else {
		t.Logf("Test succeeded, retrieved information from getdifficulty RPC call")
	}
}

func (suite *RPCTestSuite) TestRPCGetBlockHash() {
	t := suite.T()
	block := 2

	tSettings := test.CreateBaseTestSettings()

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	var getBlockHash helper.GetBlockHashResponse

	blockchainClient, err := blockchain.NewClient(ctx, ulogger.TestLogger{}, tSettings, "test")
	require.NoError(t, err)

	err = blockchainClient.Run(ctx, "test")
	require.NoError(t, err, "Blockchain client failed to start")

	// Generate blocks
	_, err = helper.CallRPC(teranode1RPCEndpoint, "generate", []interface{}{5})
	require.NoError(t, err, "Failed to generate blocks")

	resp, err := helper.CallRPC(teranode1RPCEndpoint, "getblockhash", []interface{}{block})
	require.NoError(t, err, "Failed to generate blocks")

	errJSON := json.Unmarshal([]byte(resp), &getBlockHash)
	if errJSON != nil {
		t.Errorf("JSON decoding error: %v", errJSON)
		return
	}

	t.Logf("%s", resp)

	if getBlockHash.Error != nil {
		if getBlockHash.Error.Message != nullStr {
			t.Errorf("Test failed: getBlockHash RPC call returned error: %v", getBlockHash.Error.Message)
		} else {
			t.Errorf("Test failed: getBlockHash RPC call returned an unexpected error type: %v", getBlockHash.Error)
		}
	} else {
		if getBlockHash.Result != "" {
			t.Logf("Test succeeded, retrieved information from getblockhash RPC call")
		} else {
			t.Errorf("Test failed: getBlockHash RPC call returned an empty block hash: %v", getBlockHash.Result)
		}
	}
}

func (suite *RPCTestSuite) TestRPCGetBlockByHeight() {
	t := suite.T()
	height := 2

	tSettings := test.CreateBaseTestSettings()

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)

	defer cancel()

	var getBlockByHeightResp helper.GetBlockByHeightResponse

	blockchainClient, err := blockchain.NewClient(ctx, ulogger.TestLogger{}, tSettings, "test")
	require.NoError(t, err)

	err = blockchainClient.Run(ctx, "test")
	require.NoError(t, err, "Blockchain client failed to start")

	// Generate blocks
	_, err = helper.CallRPC(teranode1RPCEndpoint, "generate", []interface{}{101})
	require.NoError(t, err, "Failed to generate blocks")

	resp, err := helper.CallRPC(teranode1RPCEndpoint, "getblockbyheight", []interface{}{height})
	require.NoError(t, err, "Failed to get block by height")

	errJSON := json.Unmarshal([]byte(resp), &getBlockByHeightResp)
	if errJSON != nil {
		t.Errorf("JSON decoding error: %v", errJSON)
		return
	}

	t.Logf("Resp: %s", resp)

	if getBlockByHeightResp.Result.Height != height {
		t.Errorf("Expected height %d, got %d", height, getBlockByHeightResp.Result.Height)
	}
}

func (suite *RPCTestSuite) TestRPCGetMiningInfo() {
	var miningInfoResp helper.GetMiningInfoResponse

	t := suite.T()
	resp, err := helper.CallRPC(teranode1RPCEndpoint, "getmininginfo", []interface{}{})

	if err != nil {
		t.Errorf("Error CallRPC: %v", err)
	}

	errJSON := json.Unmarshal([]byte(resp), &miningInfoResp)

	if errJSON != nil {
		t.Errorf("JSON decoding error: %v", errJSON)
		return
	}

	t.Logf("%s", resp)

	if miningInfoResp.Result.NetworkHashPs == 0 {
		t.Errorf("Test failed: getmininginfo not working")
	} else {
		t.Logf("getmininginfo test succeeded")
	}
}

func (suite *RPCTestSuite) TestShouldAllowFairTxUseRpc() {
	tSettings := settings.NewSettings("dev.system.test")
	// tSettings.ChainCfgParams.Name = "regtest"

	logger := ulogger.New("e2eTestRun", ulogger.WithLevel(tSettings.LogLevel))

	logger.Infof("tSettings: %+v", tSettings.ChainCfgParams.Name)
	logger.Infof("tSettings: %+v", tSettings.Coinbase.DistributorTimeout)
	// tSettings.Coinbase.DistributorTimeout = 30 * time.Second
	// logger.Infof("tSettings: %+v", tSettings.Coinbase.DistributorTimeout)
	ctx := context.Background()
	t := suite.T()
	url := "http://localhost:8090"

	blockchainClient, err := blockchain.NewClient(ctx, logger, tSettings, "test")
	require.NoError(t, err)

	err = blockchainClient.Run(ctx, "test")
	require.NoError(t, err, "Failed to create Blockchain client")

	// tSettings.Coinbase = settings.CoinbaseSettings{
	// 	DistributorTimeout: 10 * time.Second,
	// }

	txDistributor, err := distributor.NewDistributor(ctx, logger, tSettings,
		distributor.WithBackoffDuration(200*time.Millisecond),
		distributor.WithRetryAttempts(3),
		distributor.WithFailureTolerance(0),
	)
	require.NoError(t, err, "Failed to create distributor client")

	_, err = helper.CallRPC(teranode1RPCEndpoint, "generate", []interface{}{101})
	if err != nil {
		t.Errorf("Failed to generate initial blocks: %v", err)
	}

	coinbaseClient, _ := coinbase.NewClient(ctx, logger, tSettings)
	utxoBalanceBefore, _, _ := coinbaseClient.GetBalance(ctx)
	t.Logf("utxoBalanceBefore: %d\n", utxoBalanceBefore)

	coinbasePrivKey := tSettings.Coinbase.WalletPrivateKey
	coinbasePrivateKey, _ := wif.DecodeWIF(coinbasePrivKey)
	coinbaseAddr, _ := bscript.NewAddressFromPublicKey(coinbasePrivateKey.PrivKey.PubKey(), true)
	privateKey, _ := bec.NewPrivateKey(bec.S256())
	address, _ := bscript.NewAddressFromPublicKey(privateKey.PubKey(), true)

	var tx *bt.Tx

	for attempts := 0; attempts < 5; attempts++ {
		tx, err = coinbaseClient.RequestFunds(ctx, address.AddressString, true)
		if err == nil {
			break
		}

		t.Logf("Attempt %d: Failed to request funds: %v. Retrying in 1 second...", attempts+1, err)
		time.Sleep(time.Second)

		utxoBalanceBefore, _, _ := coinbaseClient.GetBalance(ctx)
		t.Logf("utxoBalanceBefore: %d\n", utxoBalanceBefore)
	}

	require.NoError(t, err, "Failed to request funds after 5 attempts")

	t.Logf("Sending Faucet Transaction: %s\n", tx.TxIDChainHash())
	_, err = txDistributor.SendTransaction(ctx, tx)
	require.NoError(t, err, "Failed to broadcast faucet tx")

	t.Logf("Faucet Transaction sent: %s\n", tx.TxIDChainHash())

	output := tx.Outputs[0]
	utxo := &bt.UTXO{
		TxIDHash:      tx.TxIDChainHash(),
		Vout:          uint32(0),
		LockingScript: output.LockingScript,
		Satoshis:      output.Satoshis,
	}

	newTx := bt.NewTx()
	_ = newTx.FromUTXOs(utxo)
	_ = newTx.AddP2PKHOutputFromAddress(coinbaseAddr.AddressString, 10000)
	_ = newTx.FillAllInputs(ctx, &unlocker.Getter{PrivateKey: privateKey})

	t.Logf("Sending New Transaction with RPC: %s\n", newTx.TxIDChainHash())
	txBytes := hex.EncodeToString(newTx.ExtendedBytes())

	resp, err := helper.CallRPC(teranode1RPCEndpoint, "sendrawtransaction", []interface{}{txBytes})
	require.NoError(t, err, "Failed to send new tx with rpc")
	t.Logf("Transaction sent with RPC: %s\n", resp)

	delay := tSettings.BlockAssembly.DoubleSpendWindow
	if delay != 0 {
		t.Logf("Waiting %dms [block assembly has delay processing txs to catch double spends]\n", delay)
		time.Sleep(time.Duration(delay) * time.Millisecond)
	}

	height, _ := helper.GetBlockHeight(url)

	utxoBalanceAfter, _, _ := coinbaseClient.GetBalance(ctx)
	logger.Infof("utxoBalanceBefore: %d, utxoBalanceAfter: %d\n", utxoBalanceBefore, utxoBalanceAfter)

	blockStoreURL := tSettings.Block.BlockStore
	if blockStoreURL == nil {
		t.Errorf("Blockstore URL not found")
	}

	t.Logf("blockStoreURL: %s", blockStoreURL.String())

	require.NoError(t, err, "Error creating blockstore")

	targetHeight := height + 1

	_, err = helper.CallRPC(teranode1RPCEndpoint, "generate", []interface{}{101})
	if err != nil {
		t.Errorf("Failed to generate blocks: %v", err)
	}

	err = helper.WaitForBlockHeight(url, 202, 60)
	require.NoError(t, err)

	t.Logf("Target height: %d", targetHeight)

	block, err := blockchainClient.GetBlockByHeight(ctx, targetHeight)
	require.NoError(t, err)

	subtreeStore, err := blob.NewStore(logger, tSettings.SubtreeValidation.SubtreeStore, options.WithHashPrefix(2))
	require.NoError(t, err)

	fallbackGetFunc := func(subtreeHash chainhash.Hash) error {
		return block.SubTreesFromBytes(subtreeHash[:])
	}

	time.Sleep(30 * time.Second)

	subtree, err := block.GetSubtrees(ctx, logger, subtreeStore, fallbackGetFunc)
	require.NoError(t, err)

	blFound := false

	for i := 0; i < len(subtree); i++ {
		st := subtree[i]
		for _, node := range st.Nodes {
			t.Logf("node.Hash: %s", node.Hash.String())
			t.Logf("tx.TxIDChainHash().String(): %s", tx.TxIDChainHash().String())

			if node.Hash.String() == tx.TxIDChainHash().String() {
				blFound = true
				break
			}
		}
	}

	assert.True(t, blFound, "TX not found in the blockstore")
}

func (suite *RPCTestSuite) TestRPCInvalidateBlock() {
	var bestBlockHash helper.BestBlockHashResp

	var respInvalidateBlock helper.InvalidBlockResp

	tSettings := test.CreateBaseTestSettings()

	t := suite.T()

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)

	defer cancel()

	blockchainClient, err := blockchain.NewClient(ctx, ulogger.TestLogger{}, tSettings, "test")
	require.NoError(t, err)

	err = blockchainClient.Run(ctx, "test")
	require.NoError(t, err, "Blockchain client failed to start")

	// Generate blocks
	_, err = helper.CallRPC(teranode1RPCEndpoint, "generate", []interface{}{101})
	require.NoError(t, err, "Failed to generate blocks")

	resp, err := helper.CallRPC(teranode1RPCEndpoint, "getbestblockhash", []interface{}{})
	require.NoError(t, err, "Failed to get block hash")

	errJSON := json.Unmarshal([]byte(resp), &bestBlockHash)
	if errJSON != nil {
		t.Errorf("JSON decoding error: %v", errJSON)
		return
	}

	t.Logf("Best block hash: %s", bestBlockHash.Result)

	respInv, errInv := helper.CallRPC(teranode1RPCEndpoint, "invalidateblock", []interface{}{bestBlockHash.Result})

	if errInv != nil {
		t.Errorf("Error CallRPC invalidateblock: %v", err)
	}

	errJSONInv := json.Unmarshal([]byte(respInv), &respInvalidateBlock)

	if errJSONInv != nil {
		t.Errorf("JSON decoding error: %v", errJSONInv)
		return
	}

	t.Logf("%s", respInv)

	if respInvalidateBlock.Result != nil {
		t.Error("Error invalidating block")
	} else {
		t.Logf("Block invalidated successfully")
	}
}

func (suite *RPCTestSuite) TestRPCReconsiderBlock() {
	var bestBlockHash helper.BestBlockHashResp

	var respReconsiderBlock helper.InvalidBlockResp

	t := suite.T()

	tSettings := test.CreateBaseTestSettings()

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)

	defer cancel()

	blockchainClient, err := blockchain.NewClient(ctx, ulogger.TestLogger{}, tSettings, "test")
	require.NoError(t, err)

	err = blockchainClient.Run(ctx, "test")
	require.NoError(t, err, "Blockchain client failed to start")

	// Generate blocks
	_, err = helper.CallRPC(teranode1RPCEndpoint, "generate", []interface{}{101})
	require.NoError(t, err, "Failed to generate blocks")

	resp, err := helper.CallRPC(teranode1RPCEndpoint, "getbestblockhash", []interface{}{})
	require.NoError(t, err, "Failed to get block hash")

	errJSON := json.Unmarshal([]byte(resp), &bestBlockHash)
	if errJSON != nil {
		t.Errorf("JSON decoding error: %v", errJSON)
		return
	}

	t.Logf("Best block hash: %s", bestBlockHash.Result)

	respInv, errInv := helper.CallRPC(teranode1RPCEndpoint, "reconsiderblock", []interface{}{bestBlockHash.Result})

	if errInv != nil {
		t.Errorf("Error CallRPC invalidateblock: %v", err)
	}

	errJSONInv := json.Unmarshal([]byte(respInv), &respReconsiderBlock)

	if errJSONInv != nil {
		t.Errorf("JSON decoding error: %v", errJSONInv)
		return
	}

	if respReconsiderBlock.Result != nil {
		t.Error("Error reconsidering block")
	} else {
		t.Logf("Block reconsidered successfully")
	}
}

func (suite *RPCTestSuite) TestShouldAllowFairTxUseRpcUseCreateRawTx() {
	tSettings := test.CreateBaseTestSettings()

	logger := ulogger.New("e2eTestRun", ulogger.WithLevel(tSettings.LogLevel))

	ctx := context.Background()
	t := suite.T()
	// url := "http://localhost:8090"

	blockchainClient, err := blockchain.NewClient(ctx, logger, tSettings, "test")
	require.NoError(t, err)

	err = blockchainClient.Run(ctx, "test")
	require.NoError(t, err, "Failed to create Blockchain client")
	time.Sleep(20 * time.Second)

	txDistributor, err := distributor.NewDistributor(ctx, logger, tSettings,
		distributor.WithBackoffDuration(200*time.Millisecond),
		distributor.WithRetryAttempts(3),
		distributor.WithFailureTolerance(0),
	)
	require.NoError(t, err, "Failed to create distributor client")

	_, err = helper.CallRPC(teranode1RPCEndpoint, "generate", []interface{}{101})
	if err != nil {
		t.Errorf("Failed to generate initial blocks: %v", err)
	}

	coinbaseClient, _ := coinbase.NewClient(ctx, logger, tSettings)
	utxoBalanceBefore, _, _ := coinbaseClient.GetBalance(ctx)
	t.Logf("utxoBalanceBefore: %d\n", utxoBalanceBefore)

	coinbasePrivKey := tSettings.Coinbase.WalletPrivateKey
	coinbasePrivateKey, _ := wif.DecodeWIF(coinbasePrivKey)
	coinbaseAddr, _ := bscript.NewAddressFromPublicKey(coinbasePrivateKey.PrivKey.PubKey(), true)
	privateKey, _ := bec.NewPrivateKey(bec.S256())
	address, _ := bscript.NewAddressFromPublicKey(privateKey.PubKey(), true)

	tx, err := coinbaseClient.RequestFunds(ctx, address.AddressString, true)
	require.NoError(t, err, "Failed to request funds")
	t.Logf("Sending Faucet Transaction: %s\n", tx.TxIDChainHash())

	_, err = txDistributor.SendTransaction(ctx, tx)
	require.NoError(t, err, "Failed to broadcast faucet tx")

	t.Logf("Faucet Transaction sent: %s\n", tx.TxIDChainHash())

	// Create raw transaction using RPC
	inputs := []map[string]interface{}{
		{
			"txid": tx.TxIDChainHash().String(),
			"vout": 0,
		},
	}

	outputs := map[string]float64{
		coinbaseAddr.AddressString: 0.0001, // 10000 satoshis
	}

	// Call createrawtransaction RPC
	resp, err := helper.CallRPC(teranode1RPCEndpoint, "createrawtransaction", []interface{}{inputs, outputs})
	require.NoError(t, err, "Failed to create raw transaction")

	var createRawTxResp struct {
		Result string      `json:"result"`
		Error  interface{} `json:"error"`
		ID     string      `json:"id"`
	}

	err = json.Unmarshal([]byte(resp), &createRawTxResp)
	logger.Infof("Create raw transaction response: %v", createRawTxResp)
	require.NoError(t, err, "Failed to unmarshal createrawtransaction response")
	require.Nil(t, createRawTxResp.Error, "Create raw transaction returned error")

	t.Logf("Create raw transaction response result: %v", createRawTxResp.Result)
	newTx, _ := bt.NewTxFromString(createRawTxResp.Result)

	// Send the signed transaction
	t.Logf("Sending signed transaction: %s", newTx.TxIDChainHash())
	txHex := hex.EncodeToString(newTx.ExtendedBytes())

	_, err = helper.CallRPC(teranode1RPCEndpoint, "sendrawtransaction", []interface{}{txHex})
	require.NoError(t, err, "Failed to send transaction")

	// Mine a block to include our transaction
	_, err = helper.CallRPC(teranode1RPCEndpoint, "generate", []interface{}{1})
	require.NoError(t, err, "Failed to generate block")
}

func (suite *RPCTestSuite) TestShouldAllowSubmitMiningSolutionUsingMiningCandidateFromBA() {
	tSettings := test.CreateBaseTestSettings()

	logger := ulogger.New("e2eTestRun", ulogger.WithLevel(tSettings.LogLevel))

	ctx := context.Background()
	t := suite.T()
	url := "http://localhost:8090"

	blockchainClient, err := blockchain.NewClient(ctx, logger, tSettings, "test")
	require.NoError(t, err)

	blockassemblyClient, err := ba.NewClient(ctx, logger, tSettings)
	require.NoError(t, err)

	err = blockchainClient.Run(ctx, "test")
	require.NoError(t, err, "Failed to create Blockchain client")

	txDistributor, err := distributor.NewDistributor(ctx, logger, tSettings,
		distributor.WithBackoffDuration(200*time.Millisecond),
		distributor.WithRetryAttempts(3),
		distributor.WithFailureTolerance(0),
	)
	require.NoError(t, err, "Failed to create distributor client")

	_, err = helper.CallRPC(teranode1RPCEndpoint, "generate", []interface{}{101})
	if err != nil {
		t.Errorf("Failed to generate initial blocks: %v", err)
	}

	coinbaseClient, _ := coinbase.NewClient(ctx, logger, tSettings)
	utxoBalanceBefore, _, _ := coinbaseClient.GetBalance(ctx)
	t.Logf("utxoBalanceBefore: %d\n", utxoBalanceBefore)

	coinbasePrivKey := tSettings.Coinbase.WalletPrivateKey
	coinbasePrivateKey, _ := wif.DecodeWIF(coinbasePrivKey)
	coinbaseAddr, _ := bscript.NewAddressFromPublicKey(coinbasePrivateKey.PrivKey.PubKey(), true)
	privateKey, _ := bec.NewPrivateKey(bec.S256())
	address, _ := bscript.NewAddressFromPublicKey(privateKey.PubKey(), true)

	var tx *bt.Tx

	for attempts := 0; attempts < 5; attempts++ {
		tx, err = coinbaseClient.RequestFunds(ctx, address.AddressString, true)
		if err == nil {
			break
		}

		t.Logf("Attempt %d: Failed to request funds: %v. Retrying in 1 second...", attempts+1, err)
		time.Sleep(time.Second)

		utxoBalanceBefore, _, _ := coinbaseClient.GetBalance(ctx)
		t.Logf("utxoBalanceBefore: %d\n", utxoBalanceBefore)
	}

	require.NoError(t, err, "Failed to request funds after 5 attempts")

	t.Logf("Sending Faucet Transaction: %s\n", tx.TxIDChainHash())
	_, err = txDistributor.SendTransaction(ctx, tx)
	require.NoError(t, err, "Failed to broadcast faucet tx")

	t.Logf("Faucet Transaction sent: %s\n", tx.TxIDChainHash())

	output := tx.Outputs[0]
	utxo := &bt.UTXO{
		TxIDHash:      tx.TxIDChainHash(),
		Vout:          uint32(0),
		LockingScript: output.LockingScript,
		Satoshis:      output.Satoshis,
	}

	newTx := bt.NewTx()
	_ = newTx.FromUTXOs(utxo)
	_ = newTx.AddP2PKHOutputFromAddress(coinbaseAddr.AddressString, 10000)
	_ = newTx.FillAllInputs(ctx, &unlocker.Getter{PrivateKey: privateKey})

	t.Logf("Sending New Transaction with RPC: %s\n", newTx.TxIDChainHash())
	txBytes := hex.EncodeToString(newTx.ExtendedBytes())

	height, _ := helper.GetBlockHeight(url)

	utxoBalanceAfter, _, _ := coinbaseClient.GetBalance(ctx)
	logger.Infof("utxoBalanceBefore: %d, utxoBalanceAfter: %d\n", utxoBalanceBefore, utxoBalanceAfter)

	resp, err := helper.CallRPC(teranode1RPCEndpoint, "sendrawtransaction", []interface{}{txBytes})
	require.NoError(t, err, "Failed to send new tx with rpc")
	t.Logf("Transaction sent with RPC: %s\n", resp)

	delay := tSettings.BlockAssembly.DoubleSpendWindow
	if delay != 0 {
		t.Logf("Waiting %dms [block assembly has delay processing txs to catch double spends]\n", delay)
		time.Sleep(time.Duration(delay) * time.Millisecond)
	}

	miningCandidateResp, err := helper.CallRPC(teranode1RPCEndpoint, "getminingcandidate", []interface{}{})
	t.Logf("Mining candidate response from rpc %v", miningCandidateResp)
	require.NoError(t, err, "Failed to get mining candidate")

	var miningCandidate helper.MiningCandidate

	require.NoError(t, err, "Failed to marshal mining candidate")

	err = json.Unmarshal([]byte(miningCandidateResp), &miningCandidate)
	require.NoError(t, err, "Failed to unmarshal mining candidate")

	result := miningCandidate.Result
	mjson, err := json.Marshal(result)
	t.Logf("Mining candidate json from RPC: %s", mjson)

	require.NoError(t, err, "Failed to create chainhash from string")

	miningCandidateFromBAClient, err := blockassemblyClient.GetMiningCandidate(ctx)
	require.NoError(t, err, "Failed to get mining candidate from block assembly client")

	mcba := miningCandidateFromBAClient.Stringify(true)
	t.Logf("Mining candidate from block assembly client: %s", mcba)

	coinbase, err := CreateCoinbaseTxCandidate(miningCandidate)
	require.NoError(t, err, "Failed to create coinbase tx")

	// create a block header from the mining candidate
	blockHeader, err := model.NewBlockHeaderFromJSON(string(mjson), coinbase)
	require.NoError(t, err, "Failed to create block header from json")

	// print the block header
	t.Logf("Block Header Merkle Root: %s", blockHeader.HashMerkleRoot.String())

	blockHeaderFromMC, err := model.NewBlockHeaderFromMiningCandidate(miningCandidateFromBAClient, coinbase)

	var nonce uint32

	for ; nonce < math.MaxUint32; nonce++ {
		blockHeaderFromMC.Nonce = nonce

		headerValid, hash, err := blockHeaderFromMC.HasMetTargetDifficulty()
		if err != nil && !strings.Contains(err.Error(), "block header does not meet target") {
			t.Error(err)
			t.FailNow()
		}

		if headerValid {
			t.Logf("Found valid nonce: %d, hash: %s", nonce, hash)
			break
		}
	}
	// solution = model.MiningSolution
	solution := map[string]interface{}{
		"id":       result.ID,
		"nonce":    nonce,
		"time":     blockHeaderFromMC.Timestamp,
		"version":  blockHeaderFromMC.Version,
		"coinbase": hex.EncodeToString(coinbase.Bytes()),
	}

	submitSolnResp, err := helper.CallRPC(teranode1RPCEndpoint, "submitminingsolution", []interface{}{solution})
	t.Logf("Submit solution response from rpc %v", submitSolnResp)
	require.NoError(t, err, "Failed to submit mining solution")

	var getBlockHash helper.GetBlockHashResponse

	resp, err = helper.CallRPC(teranode1RPCEndpoint, "getbestblockhash", []interface{}{})

	require.NoError(t, err, "Error getting best blockhash")

	errJSON := json.Unmarshal([]byte(resp), &getBlockHash)

	require.NoError(t, errJSON, "Error unmarshalling getblock response")

	blockStoreURL := tSettings.Block.BlockStore

	if blockStoreURL == nil {
		t.Errorf("Error finding blockstore")
	}

	t.Logf("blockStoreURL: %s", blockStoreURL.String())

	blockStore, err := blob.NewStore(logger, blockStoreURL)
	require.NoError(t, err, "Error creating blockstore")

	bl := false

	targetHeight := height + 1

	for i := 0; i < 2; i++ {
		err := helper.WaitForBlockHeight(url, targetHeight, 60)
		if err != nil {
			t.Errorf("Failed to wait for block height: %v", err)
		}

		header, meta, err := blockchainClient.GetBlockHeadersFromHeight(ctx, targetHeight, 1)
		if err != nil {
			t.Errorf("Failed to get block headers: %v", err)
		}

		t.Logf("Testing on Best block header at height: %v %d", header[0].Hash(), meta[0].Height)

		bl, err = helper.CheckIfTxExistsInBlock(ctx, blockStore, blockStoreURL, header[0].Hash()[:], meta[0].Height, *newTx.TxIDChainHash(), logger)
		if err != nil {
			t.Errorf("error checking if tx exists in block: %v", err)
		}

		if bl {
			break
		}

		targetHeight++

		_, err = helper.CallRPC(teranode1RPCEndpoint, "generate", []interface{}{1})
		require.NoError(t, err, "Failed to generate blocks")

		if err != nil {
			t.Errorf("Failed to mine block: %v", err)
		}
	}

	assert.Equal(t, true, bl, "Test Tx not found in block")
}

func (suite *RPCTestSuite) TestShouldAllowSubmitMiningSolutionUsingMiningCandidateFromRPC() {
	tSettings := test.CreateBaseTestSettings()

	logger := ulogger.New("e2eTestRun", ulogger.WithLevel(tSettings.LogLevel))

	ctx := context.Background()
	t := suite.T()
	url := "http://localhost:8090"

	blockchainClient, err := blockchain.NewClient(ctx, logger, tSettings, "test")
	require.NoError(t, err)

	blockassemblyClient, err := ba.NewClient(ctx, logger, tSettings)
	require.NoError(t, err)

	err = blockchainClient.Run(ctx, "test")
	require.NoError(t, err, "Failed to create Blockchain client")

	txDistributor, err := distributor.NewDistributor(ctx, logger, tSettings,
		distributor.WithBackoffDuration(200*time.Millisecond),
		distributor.WithRetryAttempts(3),
		distributor.WithFailureTolerance(0),
	)
	require.NoError(t, err, "Failed to create distributor client")

	_, err = helper.CallRPC(teranode1RPCEndpoint, "generate", []interface{}{101})
	if err != nil {
		t.Errorf("Failed to generate initial blocks: %v", err)
	}

	coinbaseClient, _ := coinbase.NewClient(ctx, logger, tSettings)
	utxoBalanceBefore, _, _ := coinbaseClient.GetBalance(ctx)
	t.Logf("utxoBalanceBefore: %d\n", utxoBalanceBefore)

	coinbasePrivKey := tSettings.Coinbase.WalletPrivateKey
	coinbasePrivateKey, _ := wif.DecodeWIF(coinbasePrivKey)
	coinbaseAddr, _ := bscript.NewAddressFromPublicKey(coinbasePrivateKey.PrivKey.PubKey(), true)
	privateKey, _ := bec.NewPrivateKey(bec.S256())
	address, _ := bscript.NewAddressFromPublicKey(privateKey.PubKey(), true)

	var tx *bt.Tx

	for attempts := 0; attempts < 5; attempts++ {
		tx, err = coinbaseClient.RequestFunds(ctx, address.AddressString, true)
		if err == nil {
			break
		}

		t.Logf("Attempt %d: Failed to request funds: %v. Retrying in 1 second...", attempts+1, err)
		time.Sleep(time.Second)

		utxoBalanceBefore, _, _ := coinbaseClient.GetBalance(ctx)
		t.Logf("utxoBalanceBefore: %d\n", utxoBalanceBefore)
	}

	require.NoError(t, err, "Failed to request funds after 5 attempts")

	t.Logf("Sending Faucet Transaction: %s\n", tx.TxIDChainHash())
	_, err = txDistributor.SendTransaction(ctx, tx)
	require.NoError(t, err, "Failed to broadcast faucet tx")

	t.Logf("Faucet Transaction sent: %s\n", tx.TxIDChainHash())

	output := tx.Outputs[0]
	utxo := &bt.UTXO{
		TxIDHash:      tx.TxIDChainHash(),
		Vout:          uint32(0),
		LockingScript: output.LockingScript,
		Satoshis:      output.Satoshis,
	}

	newTx := bt.NewTx()
	_ = newTx.FromUTXOs(utxo)
	_ = newTx.AddP2PKHOutputFromAddress(coinbaseAddr.AddressString, 10000)
	_ = newTx.FillAllInputs(ctx, &unlocker.Getter{PrivateKey: privateKey})

	t.Logf("Sending New Transaction with RPC: %s\n", newTx.TxIDChainHash())
	txBytes := hex.EncodeToString(newTx.ExtendedBytes())

	height, _ := helper.GetBlockHeight(url)

	utxoBalanceAfter, _, _ := coinbaseClient.GetBalance(ctx)
	logger.Infof("utxoBalanceBefore: %d, utxoBalanceAfter: %d\n", utxoBalanceBefore, utxoBalanceAfter)

	resp, err := helper.CallRPC(teranode1RPCEndpoint, "sendrawtransaction", []interface{}{txBytes})
	require.NoError(t, err, "Failed to send new tx with rpc")
	t.Logf("Transaction sent with RPC: %s\n", resp)

	delay := tSettings.BlockAssembly.DoubleSpendWindow
	if delay != 0 {
		t.Logf("Waiting %dms [block assembly has delay processing txs to catch double spends]\n", delay)
		time.Sleep(time.Duration(delay) * time.Millisecond)
	}

	miningCandidateResp, err := helper.CallRPC(teranode1RPCEndpoint, "getminingcandidate", []interface{}{})
	t.Logf("Mining candidate response from rpc %v", miningCandidateResp)
	require.NoError(t, err, "Failed to get mining candidate")

	var miningCandidate helper.MiningCandidate

	require.NoError(t, err, "Failed to marshal mining candidate")

	err = json.Unmarshal([]byte(miningCandidateResp), &miningCandidate)
	require.NoError(t, err, "Failed to unmarshal mining candidate")

	result := miningCandidate.Result
	mjson, err := json.Marshal(result)
	t.Logf("Mining candidate json from RPC: %s", mjson)

	require.NoError(t, err, "Failed to create chainhash from string")

	miningCandidateFromBAClient, err := blockassemblyClient.GetMiningCandidate(ctx)
	require.NoError(t, err, "Failed to get mining candidate from block assembly client")

	mcba := miningCandidateFromBAClient.Stringify(true)
	t.Logf("Mining candidate from block assembly client: %s", mcba)

	coinbase, err := CreateCoinbaseTxCandidate(miningCandidate)
	require.NoError(t, err, "Failed to create coinbase tx")

	// create a block header from the mining candidate
	blockHeader, err := model.NewBlockHeaderFromJSON(string(mjson), coinbase)
	require.NoError(t, err, "Failed to create block header from json")

	// print the block header
	t.Logf("Block Header Merkle Root: %s", blockHeader.HashMerkleRoot.String())

	_, err = model.NewBlockHeaderFromMiningCandidate(miningCandidateFromBAClient, coinbase)
	require.NoError(t, err, "Failed to create block header from mining candidate")

	var nonce uint32
	var validHash *chainhash.Hash

	for ; nonce < math.MaxUint32; nonce++ {
		blockHeader.Nonce = nonce

		headerValid, hash, err := blockHeader.HasMetTargetDifficulty()
		if err != nil && !strings.Contains(err.Error(), "block header does not meet target") {
			t.Error(err)
			t.FailNow()
		}

		if headerValid {
			t.Logf("Found valid nonce: %d, hash: %s", nonce, hash)
			validHash = hash
			break
		}
	}
	// solution = model.MiningSolution
	solution := map[string]interface{}{
		"id":       result.ID,
		"nonce":    nonce,
		"time":     blockHeader.Timestamp,
		"version":  blockHeader.Version,
		"coinbase": hex.EncodeToString(coinbase.Bytes()),
	}

	submitSolnResp, err := helper.CallRPC(teranode1RPCEndpoint, "submitminingsolution", []interface{}{solution})
	t.Logf("Submit solution response from rpc %v", submitSolnResp)
	require.NoError(t, err, "Failed to submit mining solution")

	var getBlockHash helper.GetBlockHashResponse

	resp, err = helper.CallRPC(teranode1RPCEndpoint, "getbestblockhash", []interface{}{})

	require.NoError(t, err, "Error getting best blockhash")

	errJSON := json.Unmarshal([]byte(resp), &getBlockHash)
	assert.Equal(t, getBlockHash.Result, validHash.String(), "Best block hash mismatch")

	require.NoError(t, errJSON, "Error unmarshalling getblock response")

	blockStoreURL := tSettings.Block.BlockStore

	if blockStoreURL == nil {
		t.Errorf("Error finding blockstore")
	}

	t.Logf("blockStoreURL: %s", blockStoreURL.String())

	blockStore, err := blob.NewStore(logger, blockStoreURL)
	require.NoError(t, err, "Error creating blockstore")

	bl := false

	targetHeight := height + 1

	for i := 0; i < 2; i++ {
		err := helper.WaitForBlockHeight(url, targetHeight, 60)
		if err != nil {
			t.Errorf("Failed to wait for block height: %v", err)
		}

		header, meta, err := blockchainClient.GetBlockHeadersFromHeight(ctx, targetHeight, 1)
		if err != nil {
			t.Errorf("Failed to get block headers: %v", err)
		}

		t.Logf("Testing on Best block header at height: %v %d", header[0].Hash(), meta[0].Height)

		bl, err = helper.CheckIfTxExistsInBlock(ctx, blockStore, blockStoreURL, header[0].Hash()[:], meta[0].Height, *newTx.TxIDChainHash(), logger)
		if err != nil {
			t.Errorf("error checking if tx exists in block: %v", err)
		}

		if bl {
			break
		}

		targetHeight++

		_, err = helper.CallRPC(teranode1RPCEndpoint, "generate", []interface{}{1})
		require.NoError(t, err, "Failed to generate blocks")

		if err != nil {
			t.Errorf("Failed to mine block: %v", err)
		}
	}

	assert.Equal(t, true, bl, "Test Tx not found in block")
}

func (suite *RPCTestSuite) TestShouldAllowSubmitMiningSolutionUsingMiningCandidateFromRPCWithP2PK() {
	tSettings := settings.NewSettings("dev.system.test")

	logger := ulogger.New("e2eTestRun", ulogger.WithLevel(tSettings.LogLevel))

	ctx := context.Background()
	t := suite.T()
	url := "http://localhost:8090"

	blockchainClient, err := blockchain.NewClient(ctx, logger, tSettings, "test")
	require.NoError(t, err)

	err = blockchainClient.Run(ctx, "test")
	require.NoError(t, err, "Failed to create Blockchain client")

	txDistributor, err := distributor.NewDistributor(ctx, logger, tSettings,
		distributor.WithBackoffDuration(200*time.Millisecond),
		distributor.WithRetryAttempts(3),
		distributor.WithFailureTolerance(0),
	)
	require.NoError(t, err, "Failed to create distributor client")

	_, err = helper.CallRPC(teranode1RPCEndpoint, "generate", []interface{}{101})
	if err != nil {
		t.Errorf("Failed to generate initial blocks: %v", err)
	}

	coinbaseClient, _ := coinbase.NewClient(ctx, logger, tSettings)
	utxoBalanceBefore, _, _ := coinbaseClient.GetBalance(ctx)
	t.Logf("utxoBalanceBefore: %d\n", utxoBalanceBefore)

	coinbasePrivKey := tSettings.Coinbase.WalletPrivateKey
	coinbasePrivateKey, _ := wif.DecodeWIF(coinbasePrivKey)
	coinbaseAddr, _ := bscript.NewAddressFromPublicKey(coinbasePrivateKey.PrivKey.PubKey(), true)
	privateKey, _ := bec.NewPrivateKey(bec.S256())
	address, _ := bscript.NewAddressFromPublicKey(privateKey.PubKey(), true)

	var tx *bt.Tx

	for attempts := 0; attempts < 5; attempts++ {
		tx, err = coinbaseClient.RequestFunds(ctx, address.AddressString, true)
		if err == nil {
			break
		}

		t.Logf("Attempt %d: Failed to request funds: %v. Retrying in 1 second...", attempts+1, err)
		time.Sleep(time.Second)

		utxoBalanceBefore, _, _ := coinbaseClient.GetBalance(ctx)
		t.Logf("utxoBalanceBefore: %d\n", utxoBalanceBefore)
	}

	require.NoError(t, err, "Failed to request funds after 5 attempts")

	t.Logf("Sending Faucet Transaction: %s\n", tx.TxIDChainHash())
	_, err = txDistributor.SendTransaction(ctx, tx)
	require.NoError(t, err, "Failed to broadcast faucet tx")

	t.Logf("Faucet Transaction sent: %s\n", tx.TxIDChainHash())

	output := tx.Outputs[0]
	utxo := &bt.UTXO{
		TxIDHash:      tx.TxIDChainHash(),
		Vout:          uint32(0),
		LockingScript: output.LockingScript,
		Satoshis:      output.Satoshis,
	}

	newTx := bt.NewTx()
	_ = newTx.FromUTXOs(utxo)
	_ = newTx.AddP2PKHOutputFromAddress(coinbaseAddr.AddressString, 10000)
	_ = newTx.FillAllInputs(ctx, &unlocker.Getter{PrivateKey: privateKey})

	t.Logf("Sending New Transaction with RPC: %s\n", newTx.TxIDChainHash())
	txBytes := hex.EncodeToString(newTx.ExtendedBytes())

	height, _ := helper.GetBlockHeight(url)

	utxoBalanceAfter, _, _ := coinbaseClient.GetBalance(ctx)
	logger.Infof("utxoBalanceBefore: %d, utxoBalanceAfter: %d\n", utxoBalanceBefore, utxoBalanceAfter)

	resp, err := helper.CallRPC(teranode1RPCEndpoint, "sendrawtransaction", []interface{}{txBytes})
	require.NoError(t, err, "Failed to send new tx with rpc")
	t.Logf("Transaction sent with RPC: %s\n", resp)

	delay := tSettings.BlockAssembly.DoubleSpendWindow
	if delay != 0 {
		t.Logf("Waiting %dms [block assembly has delay processing txs to catch double spends]\n", delay)
		time.Sleep(delay * time.Millisecond)
	}

	miningCandidateResp, err := helper.CallRPC(teranode1RPCEndpoint, "getminingcandidate", []interface{}{true})
	t.Logf("Mining candidate response from rpc %v", miningCandidateResp)
	require.NoError(t, err, "Failed to get mining candidate")

	var miningCandidate helper.MiningCandidate

	require.NoError(t, err, "Failed to marshal mining candidate")

	err = json.Unmarshal([]byte(miningCandidateResp), &miningCandidate)
	require.NoError(t, err, "Failed to unmarshal mining candidate")

	result := miningCandidate.Result
	mjson, err := json.Marshal(result)
	t.Logf("Mining candidate json from RPC: %s", mjson)

	require.NoError(t, err, "Failed to create chainhash from string")

	// create a block header from the mining candidate
	coinbase, err := bt.NewTxFromString(result.Coinbase)
	require.NoError(t, err, "Failed to create coinbase tx")
	blockHeader, err := model.NewBlockHeaderFromJSON(string(mjson), coinbase)
	require.NoError(t, err, "Failed to create block header from json")

	// print the block header
	t.Logf("Block Header Merkle Root: %s", blockHeader.HashMerkleRoot.String())

	var (
		nonce       uint32
		hash        *chainhash.Hash
		headerValid bool
		validHash   *chainhash.Hash
	)

	for ; nonce < math.MaxUint32; nonce++ {
		blockHeader.Nonce = nonce

		headerValid, hash, err = blockHeader.HasMetTargetDifficulty()
		if err != nil && !strings.Contains(err.Error(), "block header does not meet target") {
			t.Error(err)
			t.FailNow()
		}

		if headerValid {
			t.Logf("Found valid nonce: %d, hash: %s", nonce, hash)
			validHash = hash

			break
		}
	}

	solution := map[string]interface{}{
		"id":       result.ID,
		"nonce":    nonce,
		"time":     blockHeader.Timestamp,
		"version":  blockHeader.Version,
		"coinbase": hex.EncodeToString(coinbase.Bytes()),
	}

	submitSolnResp, err := helper.CallRPC(teranode1RPCEndpoint, "submitminingsolution", []interface{}{solution})
	t.Logf("Submit solution response from rpc %v", submitSolnResp)
	require.NoError(t, err, "Failed to submit mining solution")

	var getBlockHash helper.GetBlockHashResponse

	resp, err = helper.CallRPC(teranode1RPCEndpoint, "getbestblockhash", []interface{}{})

	require.NoError(t, err, "Error getting best blockhash")

	errJSON := json.Unmarshal([]byte(resp), &getBlockHash)
	assert.Equal(t, getBlockHash.Result, validHash.String(), "Best block hash mismatch")

	require.NoError(t, errJSON, "Error unmarshalling getblock response")

	blockStoreURL := tSettings.Block.BlockStore

	if blockStoreURL == nil {
		t.Errorf("Error finding blockstore")
	}

	t.Logf("blockStoreURL: %s", blockStoreURL.String())

	targetHeight := height + 1

	_, err = helper.CallRPC(teranode1RPCEndpoint, "generate", []interface{}{101})
	if err != nil {
		t.Errorf("Failed to generate blocks: %v", err)
	}

	err = helper.WaitForBlockHeight(url, 202, 60)
	require.NoError(t, err)

	t.Logf("Target height: %d", targetHeight)

	block, err := blockchainClient.GetBlockByHeight(ctx, targetHeight)
	require.NoError(t, err)

	subtreeStore, err := blob.NewStore(logger, tSettings.SubtreeValidation.SubtreeStore, options.WithHashPrefix(2))
	require.NoError(t, err)

	fallbackGetFunc := func(subtreeHash chainhash.Hash) error {
		return block.SubTreesFromBytes(subtreeHash[:])
	}

	time.Sleep(30 * time.Second)

	subtree, err := block.GetSubtrees(ctx, logger, subtreeStore, fallbackGetFunc)
	require.NoError(t, err)

	blFound := false

	for i := 0; i < len(subtree); i++ {
		st := subtree[i]
		for _, node := range st.Nodes {
			t.Logf("node.Hash: %s", node.Hash.String())
			t.Logf("tx.TxIDChainHash().String(): %s", tx.TxIDChainHash().String())

			if node.Hash.String() == tx.TxIDChainHash().String() {
				blFound = true
				break
			}
		}
	}

	assert.True(t, blFound, "TX not found in the blockstore")
}

func TestRPCTestSuite(t *testing.T) {
	suite.Run(t, new(RPCTestSuite))
}

func newHashFromStr(hexStr string) *chainhash.Hash {
	hash, err := chainhash.NewHashFromStr(hexStr)
	if err != nil {
		panic(err)
	}

	return hash
}

func CreateCoinbaseTxCandidate(m helper.MiningCandidate) (*bt.Tx, error) {
	tSettings := test.CreateBaseTestSettings()

	arbitraryText := tSettings.Coinbase.ArbitraryText

	coinbasePrivKeys := tSettings.BlockAssembly.MinerWalletPrivateKeys
	if len(coinbasePrivKeys) == 0 {
		return nil, errors.NewConfigurationError("miner_wallet_private_keys not found in config")
	}

	walletAddresses := make([]string, len(coinbasePrivKeys))

	for i, coinbasePrivKey := range coinbasePrivKeys {
		privateKey, err := wif.DecodeWIF(coinbasePrivKey)
		if err != nil {
			return nil, errors.NewProcessingError("can't decode coinbase priv key", err)
		}

		walletAddress, err := bscript.NewAddressFromPublicKey(privateKey.PrivKey.PubKey(), true)
		if err != nil {
			return nil, errors.NewProcessingError("can't create coinbase address", err)
		}

		walletAddresses[i] = walletAddress.AddressString
	}

	a, b, err := model.GetCoinbaseParts(m.Result.Height, m.Result.CoinbaseValue, arbitraryText, walletAddresses)
	if err != nil {
		return nil, errors.NewProcessingError("error creating coinbase transaction", err)
	}

	extranonce := make([]byte, 12)
	_, _ = rand.Read(extranonce)
	a = append(a, extranonce...)
	a = append(a, b...)

	coinbaseTx, err := bt.NewTxFromBytes(a)
	if err != nil {
		return nil, errors.NewProcessingError("error decoding coinbase transaction", err)
	}

	return coinbaseTx, nil
}
