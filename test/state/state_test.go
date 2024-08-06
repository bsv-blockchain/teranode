////go:build e2eTest

// How to run this test:
// $ unzip data.zip
// $ cd test/state/
// $ `SETTINGS_CONTEXT=docker.ci.tc1.run go test -run TestNodeCatchUpState`

package test

import (
	"context"
	"fmt"
	"github.com/bitcoin-sv/ubsv/stores/blob/options"
	"google.golang.org/protobuf/types/known/emptypb"
	"io"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/bitcoin-sv/ubsv/services/blockchain/blockchain_api"
	tf "github.com/bitcoin-sv/ubsv/test/test_framework"
	helper "github.com/bitcoin-sv/ubsv/test/utils"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/ordishs/gocore"
	"github.com/stretchr/testify/assert"
)

var (
	framework   *tf.BitcoinTestFramework
	settingsMap map[string]string
)

var logLevelStr, _ = gocore.Config().Get("logLevel", "INFO")
var logger = ulogger.New("testRun", ulogger.WithLevel(logLevelStr))

func TestMain(m *testing.M) {
	setupBitcoinTestFramework()
	defer tearDownBitcoinTestFramework()

	m.Run()

	// os.Exit(0)
}

func setupBitcoinTestFramework() {
	framework = tf.NewBitcoinTestFramework([]string{"../../docker-compose.yml", "../../docker-compose.aerospike.override.yml", "../../docker-compose.e2etest.override.yml"})
	settingsMap = map[string]string{
		"SETTINGS_CONTEXT_1": "docker.ci.ubsv1.tc1",
		"SETTINGS_CONTEXT_2": "docker.ci.ubsv2.tc1",
		"SETTINGS_CONTEXT_3": "docker.ci.ubsv3.tc1",
	}
	if err := framework.SetupNodes(settingsMap); err != nil {
		fmt.Printf("Error setting up nodes: %v\n", err)
		os.Exit(1)
	}
}

func tearDownBitcoinTestFramework() {
	if err := framework.StopNodesWithRmVolume(); err != nil {
		fmt.Printf("Error stopping nodes: %v\n", err)
	}
	err := os.RemoveAll("../../data")
	if err != nil {
		fmt.Printf("Error removing data directory: %v\n", err)
	}
}

func TestNodeCatchUpState_WithStartAndStopNodes(t *testing.T) {
	ctx := context.Background()
	blockchainNode0 := framework.Nodes[0].BlockchainClient
	blockchainNode1 := framework.Nodes[1].BlockchainClient
	var (
		states    []blockchain_api.FSMStateType
		lastState *blockchain_api.FSMStateType
		mu        sync.Mutex
		wg        sync.WaitGroup
		done      = make(chan struct{})
	)

	var logLevelStr, _ = gocore.Config().Get("logLevel", "INFO")
	logger := ulogger.New("testRun", ulogger.WithLevel(logLevelStr))

	err := framework.StopNode("ubsv-2")
	if err != nil {
		t.Errorf("Failed to stop node: %v", err)
	}

	for i := 0; i < 5; i++ {
		hashes, err := helper.CreateAndSendRawTxs(ctx, framework.Nodes[0], 10)
		if err != nil {
			t.Errorf("Failed to create and send raw txs: %v", err)
		}
		fmt.Printf("Hashes: %v\n", hashes)

		baClient := framework.Nodes[0].BlockassemblyClient
		_, err = helper.MineBlock(ctx, baClient, logger)
		if err != nil {
			t.Errorf("Failed to mine block: %v", err)
		}
	}

	err = framework.StartNode("ubsv-2")
	if err != nil {
		t.Errorf("Failed to start node: %v", err)
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-done:
				return
			default:
				response := blockchainNode1.GetFSMCurrentState()
				if err == nil && (lastState == nil || response != *lastState) {
					mu.Lock()
					states = append(states, response)
					lastState = &response
					mu.Unlock()
				}
				// time.Sleep(10 * time.Millisecond) // Adjust the interval as needed
			}
		}
	}()

	time.Sleep(120 * time.Second)
	close(done)
	wg.Wait()

	stateFound := false
	for _, state := range states {
		if state == blockchain_api.FSMStateType(3) {
			stateFound = true
			break
		}
	}
	assert.True(t, stateFound, "State 3 was not captured")
	fmt.Printf("Captured states: %v\n", states)

	headerNode1, _, _ := blockchainNode1.GetBestBlockHeader(ctx)
	headerNode0, _, _ := blockchainNode0.GetBestBlockHeader(ctx)
	assert.Equal(t, headerNode0.Hash(), headerNode1.Hash(), "Best block headers are not equal")
}

func TestNodeCatchUpState_WithP2PSwitch(t *testing.T) {
	blockchainNode0 := framework.Nodes[0].BlockchainClient
	blockchainNode1 := framework.Nodes[1].BlockchainClient

	var (
		states    []blockchain_api.FSMStateType
		lastState *blockchain_api.FSMStateType
		mu        sync.Mutex
		wg        sync.WaitGroup
		done      = make(chan struct{})
	)

	settingsMap["SETTINGS_CONTEXT_2"] = "docker.ci.ubsv2.tc3"
	if err := framework.RestartNodes(settingsMap); err != nil {
		t.Errorf("Failed to restart nodes: %v", err)
	}
	ctx := context.Background()

	var logLevelStr, _ = gocore.Config().Get("logLevel", "INFO")
	logger := ulogger.New("testRun", ulogger.WithLevel(logLevelStr))

	for i := 0; i < 5; i++ {
		hashes, err := helper.CreateAndSendRawTxs(ctx, framework.Nodes[0], 10)
		if err != nil {
			t.Errorf("Failed to create and send raw txs: %v", err)
		}
		fmt.Printf("Hashes: %v\n", hashes)

		baClient := framework.Nodes[0].BlockassemblyClient
		_, err = helper.MineBlock(ctx, baClient, logger)
		if err != nil {
			t.Errorf("Failed to mine block: %v", err)
		}
	}

	settingsMap["SETTINGS_CONTEXT_2"] = "docker.ci.ubsv2.tc1"
	if err := framework.RestartNodes(settingsMap); err != nil {
		t.Errorf("Failed to restart nodes: %v", err)
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-done:
				return
			default:
				response := blockchainNode1.GetFSMCurrentState()
				if lastState == nil || response != *lastState {
					mu.Lock()
					states = append(states, response)
					lastState = &response
					mu.Unlock()
				}
				// time.Sleep(10 * time.Millisecond)
			}
		}
	}()

	time.Sleep(120 * time.Second)
	close(done)
	wg.Wait()

	stateFound := false
	for _, state := range states {
		if state == blockchain_api.FSMStateType(3) {
			stateFound = true
			break
		}
	}
	assert.True(t, stateFound, "State 3 was not captured")
	fmt.Printf("Captured states: %v\n", states)

	headerNode1, _, _ := blockchainNode1.GetBestBlockHeader(ctx)
	headerNode0, _, _ := blockchainNode0.GetBestBlockHeader(ctx)
	assert.Equal(t, headerNode0.Hash(), headerNode1.Hash(), "Best block headers are not equal")
}

func TestTXCatchUpState_SendTXsToNode0(t *testing.T) {

	url := "http://localhost:18090"
	blockchainNode0 := framework.Nodes[0].BlockchainClient
	blockAssemblyNode0 := framework.Nodes[0].BlockassemblyClient

	// Set CatchUpTransactions State
	_, err := blockchainNode0.CatchUpTransactions(framework.Context, &emptypb.Empty{})
	if err != nil {
		t.Fatalf("Failed to set state: %v", err)
	}
	time.Sleep(5 * time.Second)
	fsmState := blockchainNode0.GetFSMCurrentStateForE2ETestMode()
	assert.Equal(t, fsmState, blockchain_api.FSMStateType(4), "FSM state is not equal to 4")

	state, err := blockAssemblyNode0.GetBlockAssemblyState(framework.Context)
	if err != nil {
		t.Fatalf("Failed to get block assembly state: %v", err)
	}
	txCountBefore := state.GetTxCount()

	metricsBefore, err := helper.QueryPrometheusMetric("http://localhost:19090", "validator_processed_transactions")
	if err != nil {
		t.Fatalf("Failed to query prometheus metric: %v", err)
	}

	hashesNode0, err := helper.CreateAndSendRawTxs(framework.Context, framework.Nodes[0], 20)
	if err != nil {
		t.Fatalf("Failed to create and send raw txs: %v", err)
	}
	// for i := 0; i < 5; i++ {
	// 	hashesNode0, err = helper.CreateAndSendRawTxs(framework.Context, framework.Nodes[0], 10)
	// 	if err != nil {
	// 		t.Fatalf("Failed to create and send raw txs: %v", err)
	// 	}
	// 	fmt.Printf("Hashes: %v\n", hashesNode0)
	// }

	state, err = blockAssemblyNode0.GetBlockAssemblyState(framework.Context)
	if err != nil {
		t.Fatalf("Failed to get block assembly state: %v", err)
	}
	txCountAfter := state.GetTxCount()

	fmt.Println("Tx count before: ", txCountBefore)
	fmt.Println("Tx count after: ", txCountAfter)

	assert.LessOrEqual(t, txCountAfter, uint64(10), "Tx count mismatch")

	metricsAfter, err := helper.QueryPrometheusMetric("http://localhost:19090", "validator_processed_transactions")
	if err != nil {
		t.Fatalf("Failed to query prometheus metric: %v", err)
	}

	fmt.Printf("Metrics: %v\n", metricsBefore)
	fmt.Printf("Metrics: %v\n", metricsAfter)
	assert.LessOrEqual(t, metricsAfter, float64(10), "Tx count mismatch")

	var o []options.Options
	o = append(o, options.WithFileExtension("block"))
	bestBlock, _, _ := blockchainNode0.GetBestBlockHeader(framework.Context)
	blockStore := framework.Nodes[0].Blockstore
	r, err := blockStore.GetIoReader(framework.Context, bestBlock.Hash()[:], o...)
	if err != nil {
		t.Errorf("error getting block reader: %v", err)
	}
	if err == nil {
		if bl, err := helper.ReadFile(framework.Context, "block", logger, r, hashesNode0[5], framework.Nodes[0].BlockstoreUrl); err != nil {
			t.Errorf("error reading block: %v", err)
		} else {
			fmt.Printf("Block at height (%d): was tested for the test Tx\n", *bestBlock.Hash())
			assert.Equal(t, false, bl, "Test Tx not found in block")
		}
	}

	bestBlock, _, _ = blockchainNode0.GetBestBlockHeader(framework.Context)
	// Set Mining State
	_, err = blockchainNode0.Mine(framework.Context, &emptypb.Empty{})
	if err != nil {
		t.Fatalf("Failed to set state: %v", err)
	}
	time.Sleep(5 * time.Second)
	fsmState = blockchainNode0.GetFSMCurrentStateForE2ETestMode()
	assert.Equal(t, fsmState, blockchain_api.FSMStateType(2), "FSM state is not equal to 2")

	var newHeight int
	var bl bool
	height, _ := helper.GetBlockHeight(url)
	fmt.Println("Height before: ", height)
	for i := 0; i < 180; i++ {
		newHeight, _ = helper.GetBlockHeight(url)
		if newHeight > height {
			height = newHeight
			fmt.Println("Testing at height: ", height)
			mBlock, _ := blockchainNode0.GetBlockByHeight(framework.Context, uint32(newHeight))
			r, err = blockStore.GetIoReader(framework.Context, mBlock.Hash()[:], o...)
			if err != nil {
				t.Errorf("error getting block reader: %v", err)
			}
			if err == nil {
				if bl, err = helper.ReadFile(framework.Context, "block", logger, r, hashesNode0[5], framework.Nodes[0].BlockstoreUrl); err != nil {
					t.Errorf("error reading block: %v", err)
				} else {
					fmt.Printf("Block at height (%d): was tested for the test Tx\n", *bestBlock.Hash())
					if bl {
						break
					}
					continue
				}
			}
		}
		time.Sleep(2 * time.Second)
	}
	assert.Equal(t, true, bl, "Test Tx not found in block")
}

func TestTXCatchUpState_SendTXsToNode0_and_Node1(t *testing.T) {

	url := "http://localhost:18090"
	blockchainNode0 := framework.Nodes[0].BlockchainClient

	// Set CatchUpTransactions State
	_, err := blockchainNode0.CatchUpTransactions(framework.Context, &emptypb.Empty{})
	if err != nil {
		t.Fatalf("Failed to set state: %v", err)
	}
	time.Sleep(5 * time.Second)
	fsmState := blockchainNode0.GetFSMCurrentStateForE2ETestMode()
	assert.Equal(t, fsmState, blockchain_api.FSMStateType(4), "FSM state is not equal to 4")

	metricsBefore, err := helper.QueryPrometheusMetric("http://localhost:19090", "validator_processed_transactions")
	if err != nil {
		t.Fatalf("Failed to query prometheus metric: %v", err)
	}

	hashesNode0, err := helper.CreateAndSendRawTxs(framework.Context, framework.Nodes[0], 20)
	if err != nil {
		t.Fatalf("Failed to create and send raw txs: %v", err)
	}
	hashesNode1, err := helper.CreateAndSendRawTxs(framework.Context, framework.Nodes[1], 20)
	if err != nil {
		t.Fatalf("Failed to create and send raw txs: %v", err)
	}

	metricsAfter, err := helper.QueryPrometheusMetric("http://localhost:19090", "validator_processed_transactions")
	if err != nil {
		t.Fatalf("Failed to query prometheus metric: %v", err)
	}

	fmt.Printf("Metrics: %v\n", metricsBefore)
	fmt.Printf("Metrics: %v\n", metricsAfter)
	assert.LessOrEqual(t, metricsAfter, float64(10), "Tx count mismatch")

	var o []options.Options
	var r io.ReadCloser
	o = append(o, options.WithFileExtension("block"))
	bestBlock, _, _ := blockchainNode0.GetBestBlockHeader(framework.Context)
	blockStore := framework.Nodes[0].Blockstore

	var newHeight int
	var bl bool
	height, _ := helper.GetBlockHeight(url)
	fmt.Println("Height before: ", height)
	for i := 0; i < 180; i++ {
		newHeight, _ = helper.GetBlockHeight(url)
		if newHeight > height {
			height = newHeight
			fmt.Println("Testing at height: ", height)
			mBlock, _ := blockchainNode0.GetBlockByHeight(framework.Context, uint32(newHeight))
			r, err = blockStore.GetIoReader(framework.Context, mBlock.Hash()[:], o...)
			if err != nil {
				t.Errorf("error getting block reader: %v", err)
			}
			if err == nil {
				if bl, err = helper.ReadFile(framework.Context, "block", logger, r, hashesNode1[5], framework.Nodes[0].BlockstoreUrl); err != nil {
					t.Errorf("error reading block: %v", err)
				} else {
					fmt.Printf("Block at height (%d): was tested for the test Tx\n", *bestBlock.Hash())
					if bl {
						break
					}
					continue
				}
			}
		}
		time.Sleep(2 * time.Second)
	}
	assert.Equal(t, true, bl, "Test Tx not found in block")

	// Set Mining State
	_, err = blockchainNode0.Mine(framework.Context, &emptypb.Empty{})
	if err != nil {
		t.Fatalf("Failed to set state: %v", err)
	}
	time.Sleep(5 * time.Second)
	fsmState = blockchainNode0.GetFSMCurrentStateForE2ETestMode()
	assert.Equal(t, fsmState, blockchain_api.FSMStateType(2), "FSM state is not equal to 2")

	height, _ = helper.GetBlockHeight(url)
	fmt.Println("Height before: ", height)
	for i := 0; i < 180; i++ {
		newHeight, _ = helper.GetBlockHeight(url)
		if newHeight > height {
			height = newHeight
			fmt.Println("Testing at height: ", height)
			mBlock, _ := blockchainNode0.GetBlockByHeight(framework.Context, uint32(newHeight))
			r, err = blockStore.GetIoReader(framework.Context, mBlock.Hash()[:], o...)
			if err != nil {
				t.Errorf("error getting block reader: %v", err)
			}
			if err == nil {
				if bl, err = helper.ReadFile(framework.Context, "block", logger, r, hashesNode0[5], framework.Nodes[0].BlockstoreUrl); err != nil {
					t.Errorf("error reading block: %v", err)
				} else {
					fmt.Printf("Block at height (%d): was tested for the test Tx\n", *bestBlock.Hash())
					if bl {
						break
					}
					continue
				}
			}
		}
		time.Sleep(2 * time.Second)
	}
	assert.Equal(t, true, bl, "Test Tx not found in block")
}
