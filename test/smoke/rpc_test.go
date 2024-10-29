////go:build rpc

// go test -v -run "^TestRPCTestSuite$/TestRPCGetDifficulty$" -tags rpc

package test

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/bitcoin-sv/ubsv/services/blockchain"
	arrange "github.com/bitcoin-sv/ubsv/test/fixtures"
	helper "github.com/bitcoin-sv/ubsv/test/utils"
	"github.com/bitcoin-sv/ubsv/ulogger"
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

	// Ensure Kafka has time to initialize
	time.Sleep(5 * time.Second)

	// Start the app
	if err := startApp("app.log"); err != nil {
		log.Fatalf("Failed to start app: %v", err)
	}

	// Ensure the app has time to initialize
	time.Sleep(5 * time.Second)
}

func (suite *RPCTestSuite) TearDownTest() {
	stopKafka()
	stopUbsv()
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
	time.Sleep(1 * time.Second)

	// Generate blocks
	_, err = helper.CallRPC(ubsv1RPCEndpoint, "generate", []interface{}{"[5]"})
	require.NoError(t, err, "Failed to generate blocks")
	time.Sleep(5 * time.Second)

	resp, err := helper.CallRPC(ubsv1RPCEndpoint, "getblockhash", []interface{}{block})

	if err != nil {
		t.Errorf("Error CallRPC: %v", err)
	}

	errJSON := json.Unmarshal([]byte(resp), &getBlockHash)
	if err != nil {
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
	time.Sleep(1 * time.Second)

	// Generate blocks
	_, err = helper.CallRPC(ubsv1RPCEndpoint, "generate", []interface{}{"[101]"})
	require.NoError(t, err, "Failed to generate blocks")
	time.Sleep(5 * time.Second)

	resp, err := helper.CallRPC(ubsv1RPCEndpoint, "getblockbyheight", []interface{}{height})

	if err != nil {
		t.Errorf("Error CallRPC: %v", err)
	}

	errJSON := json.Unmarshal([]byte(resp), &getBlockByHeightResp)
	if err != nil {
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

func TestRPCTestSuite(t *testing.T) {
	suite.Run(t, new(RPCTestSuite))
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
	appCmd := exec.Command("go", "run", "../../.")

	appCmd.Env = append(os.Environ(), "SETTINGS_CONTEXT=dev.system.test")

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

	// Wait for the app to be ready (consider implementing a health check here)
	time.Sleep(30 * time.Second) // Adjust this as needed for your app's startup time

	return nil
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
