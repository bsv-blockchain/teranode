//go:build rpc

// How to execute single tests: go test -v -run "^TestRPCTestSuite$/TestRPCReconsiderBlock$" -tags rpc
// Change TestRPCInvalidateBlock with the name of the test to execute

package test

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/bitcoin-sv/ubsv/services/blockchain"
	"github.com/bitcoin-sv/ubsv/services/coinbase"
	"github.com/bitcoin-sv/ubsv/stores/blob"
	arrange "github.com/bitcoin-sv/ubsv/test/fixtures"
	helper "github.com/bitcoin-sv/ubsv/test/utils"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/bitcoin-sv/ubsv/util/distributor"
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
	arrange.TeranodeTestSuite
}

func (suite *RPCTestSuite) SetupTest() {
	if err := startKafka("kafka.log"); err != nil {
		log.Fatalf("Failed to start Kafka: %v", err)
	}

	// Start the app
	if err := startApp("app.log"); err != nil {
		log.Fatalf("Failed to start app: %v", err)
	}
}

func (suite *RPCTestSuite) TearDownTest() {
	stopKafka()
	// stopUbsv()
}

const (
	ubsv1RPCEndpoint string = "http://localhost:9292"
	nullStr          string = "null"
	stringTrue              = "true"
)

func (suite *RPCTestSuite) TestRPCGetBlockchainInfo() {
	var blockchainInfo BlockchainInfo

	t := suite.T()
	resp, err := helper.CallRPC(ubsv1RPCEndpoint, "getblockchaininfo", []interface{}{})

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
}

func (suite *RPCTestSuite) TestRPCGetPeerInfo() {
	t := suite.T()

	var p2pResp P2PRPCResponse

	resp, err := helper.CallRPC(ubsv1RPCEndpoint, "getpeerinfo", []interface{}{})

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
}

func (suite *RPCTestSuite) TestRPCGetInfo() {
	t := suite.T()

	var getInfo GetInfo

	resp, err := helper.CallRPC(ubsv1RPCEndpoint, "getinfo", []interface{}{})

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
		if strErr, ok := getInfo.Error.(string); ok && strErr == nullStr {
			t.Errorf("Test failed: getinfo RPC call returned error: %v", strErr)
		} else {
			t.Errorf("Test failed: getinfo RPC call returned an unexpected error type: %v", getInfo.Error)
		}
	} else {
		t.Logf("Test succeeded, retrieved information from getinfo RPC call")
	}
}

func (suite *RPCTestSuite) TestRPCGetDifficulty() {
	t := suite.T()

	var getDifficulty GetDifficultyResponse

	resp, err := helper.CallRPC(ubsv1RPCEndpoint, "getdifficulty", []interface{}{})

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
		if strErr, ok := getDifficulty.Error.(string); ok && strErr == "null" {
			t.Errorf("Test failed: getdifficulty RPC call returned error: %v", strErr)
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

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	var getBlockHash GetBlockHashResponse

	blockchainClient, err := blockchain.NewClient(ctx, ulogger.TestLogger{}, "test")
	require.NoError(t, err)

	err = blockchainClient.Run(ctx, "test")
	require.NoError(t, err, "Blockchain client failed to start")

	// Generate blocks
	_, err = helper.CallRPC(ubsv1RPCEndpoint, "generate", []interface{}{5})
	require.NoError(t, err, "Failed to generate blocks")

	resp, err := helper.CallRPC(ubsv1RPCEndpoint, "getblockhash", []interface{}{block})
	require.NoError(t, err, "Failed to generate blocks")

	errJSON := json.Unmarshal([]byte(resp), &getBlockHash)
	if errJSON != nil {
		t.Errorf("JSON decoding error: %v", errJSON)
		return
	}

	t.Logf("%s", resp)

	if getBlockHash.Error != nil {
		if strErr, ok := getBlockHash.Error.(string); ok && strErr == "null" {
			t.Errorf("Test failed: getBlockHash RPC call returned error: %v", strErr)
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
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)

	defer cancel()

	var getBlockByHeightResp GetBlockByHeightResponse

	blockchainClient, err := blockchain.NewClient(ctx, ulogger.TestLogger{}, "test")
	require.NoError(t, err)

	err = blockchainClient.Run(ctx, "test")
	require.NoError(t, err, "Blockchain client failed to start")

	// Generate blocks
	_, err = helper.CallRPC(ubsv1RPCEndpoint, "generate", []interface{}{101})
	require.NoError(t, err, "Failed to generate blocks")

	resp, err := helper.CallRPC(ubsv1RPCEndpoint, "getblockbyheight", []interface{}{height})
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
	var miningInfoResp GetMiningInfoResponse

	t := suite.T()
	resp, err := helper.CallRPC(ubsv1RPCEndpoint, "getmininginfo", []interface{}{})

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

func startKafka(logFile string) error {
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

func startApp(logFile string) error {
	// os.Setenv("blockchain_store_cache_enabled", "false")
	appCmd := exec.Command("go", "run", "../../.")

	appCmd.Env = append(os.Environ(), "SETTINGS_CONTEXT=dev.system.test.rpc")

	appLog, err := os.Create(logFile)
	if err != nil {
		return err
	}
	defer appLog.Close()

	appCmd.Stdout = appLog
	appCmd.Stderr = appLog

	log.Println("Starting app in the background...")

	if err := appCmd.Start(); err != nil {
		return err
	}

	appPID = appCmd.Process.Pid

	log.Println("Waiting for app to be ready...")

	for {
		_, err := util.DoHTTPRequest(context.Background(), "http://localhost:8000/health/liveness", nil)
		if err == nil {
			break
		}

		time.Sleep(100 * time.Millisecond)
	}

	log.Println("App ready")

	return nil
}

func (suite *RPCTestSuite) TestShouldAllowFairTxUseRpc() {
	var logLevelStr, _ = gocore.Config().Get("logLevel", "ERROR")
	logger := ulogger.New("e2eTestRun", ulogger.WithLevel(logLevelStr))

	ctx := context.Background()
	t := suite.T()
	url := "http://localhost:8090"

	blockchainClient, err := blockchain.NewClient(ctx, logger, "test")
	require.NoError(t, err)

	err = blockchainClient.Run(ctx, "test")
	require.NoError(t, err, "Failed to create Blockchain client")

	txDistributor, err := distributor.NewDistributor(ctx, logger,
		distributor.WithBackoffDuration(200*time.Millisecond),
		distributor.WithRetryAttempts(3),
		distributor.WithFailureTolerance(0),
	)
	require.NoError(t, err, "Failed to create distributor client")

	_, err = helper.CallRPC(ubsv1RPCEndpoint, "generate", []interface{}{101})
	if err != nil {
		t.Errorf("Failed to generate initial blocks: %v", err)
	}

	coinbaseClient, _ := coinbase.NewClient(ctx, logger)
	utxoBalanceBefore, _, _ := coinbaseClient.GetBalance(ctx)
	t.Logf("utxoBalanceBefore: %d\n", utxoBalanceBefore)

	coinbasePrivKey, _ := gocore.Config().Get("coinbase_wallet_private_key")
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

	resp, err := helper.CallRPC(ubsv1RPCEndpoint, "sendrawtransaction", []interface{}{txBytes})
	require.NoError(t, err, "Failed to send new tx with rpc")
	t.Logf("Transaction sent with RPC: %s\n", resp)

	delay, _ := gocore.Config().GetInt("double_spend_window_millis", 2000)
	if delay != 0 {
		t.Logf("Waiting %dms [block assembly has delay processing txs to catch double spends]\n", delay)
		time.Sleep(time.Duration(delay) * time.Millisecond)
	}

	height, _ := helper.GetBlockHeight(url)
	fmt.Printf("Block height: %d\n", height)

	utxoBalanceAfter, _, _ := coinbaseClient.GetBalance(ctx)
	logger.Infof("utxoBalanceBefore: %d, utxoBalanceAfter: %d\n", utxoBalanceBefore, utxoBalanceAfter)

	blockStoreURL, err, found := gocore.Config().GetURL("blockstore.dev.system.test")
	require.NoError(t, err, "Error getting blockstore url")

	if !found {
		t.Errorf("Error finding blockstore")
	}

	t.Logf("blockStoreURL: %s", blockStoreURL.String())

	blockStore, err := blob.NewStore(logger, blockStoreURL)
	require.NoError(t, err, "Error creating blockstore")

	bl := false

	targetHeight := height + 1

	_, err = helper.CallRPC(ubsv1RPCEndpoint, "generate", []interface{}{1})
	if err != nil {
		t.Errorf("Failed to generate blocks: %v", err)
	}

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
		// _, err = helper.MineBlock(ctx, *baClient, logger)
		_, err = helper.CallRPC(ubsv1RPCEndpoint, "generate", []interface{}{1})
		require.NoError(t, err, "Failed to generate blocks")

		if err != nil {
			t.Errorf("Failed to mine block: %v", err)
		}
	}

	assert.Equal(t, true, bl, "Test Tx not found in block")
}

func (suite *RPCTestSuite) TestRPCInvalidateBlock() {
	var bestBlockHash BestBlockHashResp

	var respInvalidateBlock InvalidBlockResp

	t := suite.T()

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)

	defer cancel()

	blockchainClient, err := blockchain.NewClient(ctx, ulogger.TestLogger{}, "test")
	require.NoError(t, err)

	err = blockchainClient.Run(ctx, "test")
	require.NoError(t, err, "Blockchain client failed to start")

	// Generate blocks
	_, err = helper.CallRPC(ubsv1RPCEndpoint, "generate", []interface{}{101})
	require.NoError(t, err, "Failed to generate blocks")

	resp, err := helper.CallRPC(ubsv1RPCEndpoint, "getbestblockhash", []interface{}{})
	require.NoError(t, err, "Failed to get block hash")

	errJSON := json.Unmarshal([]byte(resp), &bestBlockHash)
	if errJSON != nil {
		t.Errorf("JSON decoding error: %v", errJSON)
		return
	}

	t.Logf("Best block hash: %s", bestBlockHash.Result)

	respInv, errInv := helper.CallRPC(ubsv1RPCEndpoint, "invalidateblock", []interface{}{bestBlockHash.Result})

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
	var bestBlockHash BestBlockHashResp

	var respReconsiderBlock InvalidBlockResp

	t := suite.T()

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)

	defer cancel()

	blockchainClient, err := blockchain.NewClient(ctx, ulogger.TestLogger{}, "test")
	require.NoError(t, err)

	err = blockchainClient.Run(ctx, "test")
	require.NoError(t, err, "Blockchain client failed to start")

	// Generate blocks
	_, err = helper.CallRPC(ubsv1RPCEndpoint, "generate", []interface{}{101})
	require.NoError(t, err, "Failed to generate blocks")

	resp, err := helper.CallRPC(ubsv1RPCEndpoint, "getbestblockhash", []interface{}{})
	require.NoError(t, err, "Failed to get block hash")

	errJSON := json.Unmarshal([]byte(resp), &bestBlockHash)
	if errJSON != nil {
		t.Errorf("JSON decoding error: %v", errJSON)
		return
	}

	t.Logf("Best block hash: %s", bestBlockHash.Result)

	respInv, errInv := helper.CallRPC(ubsv1RPCEndpoint, "reconsiderblock", []interface{}{bestBlockHash.Result})

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
	var logLevelStr, _ = gocore.Config().Get("logLevel", "ERROR")
	logger := ulogger.New("e2eTestRun", ulogger.WithLevel(logLevelStr))

	ctx := context.Background()
	t := suite.T()
	// url := "http://localhost:8090"

	blockchainClient, err := blockchain.NewClient(ctx, logger, "test")
	require.NoError(t, err)

	err = blockchainClient.Run(ctx, "test")
	require.NoError(t, err, "Failed to create Blockchain client")
	time.Sleep(20 * time.Second)

	txDistributor, err := distributor.NewDistributor(ctx, logger,
		distributor.WithBackoffDuration(200*time.Millisecond),
		distributor.WithRetryAttempts(3),
		distributor.WithFailureTolerance(0),
	)
	require.NoError(t, err, "Failed to create distributor client")

	_, err = helper.CallRPC(ubsv1RPCEndpoint, "generate", []interface{}{101})
	if err != nil {
		t.Errorf("Failed to generate initial blocks: %v", err)
	}

	coinbaseClient, _ := coinbase.NewClient(ctx, logger)
	utxoBalanceBefore, _, _ := coinbaseClient.GetBalance(ctx)
	t.Logf("utxoBalanceBefore: %d\n", utxoBalanceBefore)

	coinbasePrivKey, _ := gocore.Config().Get("coinbase_wallet_private_key")
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
	resp, err := helper.CallRPC(ubsv1RPCEndpoint, "createrawtransaction", []interface{}{inputs, outputs})
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

	_, err = helper.CallRPC(ubsv1RPCEndpoint, "sendrawtransaction", []interface{}{txHex})
	require.NoError(t, err, "Failed to send transaction")

	// Mine a block to include our transaction
	_, err = helper.CallRPC(ubsv1RPCEndpoint, "generate", []interface{}{1})
	require.NoError(t, err, "Failed to generate block")
}

func TestRPCTestSuite(t *testing.T) {
	suite.Run(t, new(RPCTestSuite))
}

func stopKafka() {
	log.Println("Stopping Kafka...")

	cmd := exec.Command("docker", "stop", "kafka-server")

	if err := cmd.Run(); err != nil {
		log.Printf("Failed to stop Kafka: %v\n", err)
	} else {
		log.Println("Kafka stopped successfully")
	}
}

func stopUbsv() {
	isGitHubActions := os.Getenv("GITHUB_ACTIONS") == stringTrue

	log.Println("Stopping UBSV...")

	cmd := exec.Command("pkill", "-f", "ubsv")

	if err := cmd.Run(); err != nil {
		log.Printf("Failed to stop UBSV: %v\n", err)
	} else {
		log.Println("UBSV stopped successfully")
	}

	_ = helper.RemoveDataDirectory("./data", isGitHubActions)
}

func newHashFromStr(hexStr string) *chainhash.Hash {
	hash, err := chainhash.NewHashFromStr(hexStr)
	if err != nil {
		panic(err)
	}

	return hash
}
