//go:build functional

// How to run this test:
// $ unzip data.zip
// $ cd test/state/
// $ `SETTINGS_CONTEXT=docker.ci.tc1.run go test -run TestNodeCatchUpState`

package test

import (
	"context"
	"fmt"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/bitcoin-sv/ubsv/services/blockchain/blockchain_api"
	"github.com/bitcoin-sv/ubsv/stores/blob/options"
	"github.com/bitcoin-sv/ubsv/test/setup"
	helper "github.com/bitcoin-sv/ubsv/test/utils"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/ordishs/gocore"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"google.golang.org/protobuf/types/known/emptypb"
)

type FsmTestSuite struct {
	setup.BitcoinTestSuite
}

func (suite *FsmTestSuite) TestSendTxs() {

	url := "http://localhost:18090"
	ctx := context.Background()
	t := suite.T()
	framework := suite.Framework
	blockchainNode0 := framework.Nodes[0].BlockchainClient
	blockchainNode1 := framework.Nodes[1].BlockchainClient

	var logLevelStr, _ = gocore.Config().Get("logLevel", "INFO")
	logger := ulogger.New("testRun", ulogger.WithLevel(logLevelStr))

	for i := 0; i < 2; i++ {
		hashes, err := helper.CreateAndSendRawTxs(ctx, framework.Nodes[0], 10)
		if err != nil {
			t.Errorf("Failed to create and send raw txs: %v", err)
		}

		height, _ := helper.GetBlockHeight(url)
		baClient := framework.Nodes[0].BlockassemblyClient
		_, err = helper.MineBlock(ctx, baClient, logger)
		if err != nil {
			t.Errorf("Failed to mine block: %v", err)
		}

		time.Sleep(120 * time.Second)

		var o []options.Options
		o = append(o, options.WithFileExtension("block"))
		blockStore := framework.Nodes[0].Blockstore

		var newHeight int

		for i := 0; i < 1; i++ {
			newHeight, _ = helper.GetBlockHeight(url)
			if newHeight > height {
				height = newHeight
				fmt.Println("Testing at height: ", height)
				mBlock, _ := blockchainNode0.GetBlockByHeight(framework.Context, uint32(newHeight))
				r, err := blockStore.GetIoReader(framework.Context, mBlock.Hash()[:], o...)

				if err != nil {
					t.Errorf("error getting block reader: %v", err)
				}
				if err == nil {

					allTransactionsIncluded := true

					for _, txHash := range hashes {
						bl, err := helper.ReadFile(framework.Context, "block", framework.Logger, r, txHash, framework.Nodes[0].BlockstoreUrl)
						if err != nil {
							t.Errorf("error reading block for transaction hash %v: %v", txHash, err)
							allTransactionsIncluded = false

							break
						}

						if !bl {
							t.Errorf("Transaction hash %v not found in block at height %d", txHash, newHeight)
							allTransactionsIncluded = false

							break
						}
					}

					if !allTransactionsIncluded {
						t.Errorf("All transactions not included in the block")
					}
				}
			}
		}
	}

	// assert.True(t, bl, "All transactions not included in the block")
	headerNode1, _, _ := blockchainNode1.GetBestBlockHeader(ctx)
	headerNode0, _, _ := blockchainNode0.GetBestBlockHeader(ctx)
	assert.Equal(t, headerNode0.Hash(), headerNode1.Hash(), "Best block headers are not equal")
}

func (suite *FsmTestSuite) TestNodeCatchUpState_WithStartAndStopNodes() {
	ctx := context.Background()
	t := suite.T()
	framework := suite.Framework
	blockchainNode0 := framework.Nodes[0].BlockchainClient
	blockchainNode1 := framework.Nodes[1].BlockchainClient

	var (
		mu   sync.Mutex
		wg   sync.WaitGroup
		done = make(chan struct{})
	)

	stateSet := make(map[blockchain_api.FSMStateType]struct{})
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
				response := blockchainNode1.GetFSMCurrentStateForE2ETestMode()

				mu.Lock()
				if _, exists := stateSet[response]; !exists {

					framework.Logger.Infof("New unique state: %v", response)
					stateSet[response] = struct{}{} // Add the state to the set
				}
				mu.Unlock()
			}
		}
	}()

	time.Sleep(120 * time.Second)
	close(done)
	wg.Wait()

	stateFound := false
	for state := range stateSet {
		if state == blockchain_api.FSMStateType(3) {
			stateFound = true
			break
		}
	}
	assert.True(t, stateFound, "State 3 was not captured")

	headerNode1, _, _ := blockchainNode1.GetBestBlockHeader(ctx)
	headerNode0, _, _ := blockchainNode0.GetBestBlockHeader(ctx)
	assert.Equal(t, headerNode0.Hash(), headerNode1.Hash(), "Best block headers are not equal")
}

func (suite *FsmTestSuite) TestNodeCatchUpState_WithP2PSwitch() {
	t := suite.T()
	framework := suite.Framework
	settingsMap := suite.SettingsMap
	blockchainNode0 := framework.Nodes[0].BlockchainClient
	blockchainNode1 := framework.Nodes[1].BlockchainClient

	var (
		states []blockchain_api.FSMStateType
		mu     sync.Mutex
		wg     sync.WaitGroup
		done   = make(chan struct{})
	)

	stateSet := make(map[blockchain_api.FSMStateType]struct{})
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
				response := blockchainNode1.GetFSMCurrentStateForE2ETestMode()
				mu.Lock()
				if _, exists := stateSet[response]; !exists {
					framework.Logger.Infof("New unique state: %v", response)
					stateSet[response] = struct{}{} // Add the state to the set
				}
				mu.Unlock()
				// time.Sleep(10 * time.Millisecond)
			}
		}
	}()

	time.Sleep(120 * time.Second)
	close(done)
	wg.Wait()

	stateFound := false

	for state := range stateSet {
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
func (suite *FsmTestSuite) TestTXCatchUpState_SendTXsToNode0() {
	t := suite.T()
	framework := suite.Framework
	url := "http://localhost:18090"
	blockchainNode0 := framework.Nodes[0].BlockchainClient
	blockAssemblyNode0 := framework.Nodes[0].BlockassemblyClient

	// Set CatchUpTransactions State
	_, err := blockchainNode0.CatchUpTransactions(framework.Context, &emptypb.Empty{})
	if err != nil {
		t.Errorf("Failed to set state: %v", err)
	}

	time.Sleep(5 * time.Second)
	fsmState := blockchainNode0.GetFSMCurrentStateForE2ETestMode()
	assert.Equal(t, fsmState, blockchain_api.FSMStateType(4), "FSM state is not equal to 4")

	state, err := blockAssemblyNode0.GetBlockAssemblyState(framework.Context)
	if err != nil {
		t.Errorf("Failed to get block assembly state: %v", err)
	}
	txCountBefore := state.GetTxCount()

	metricsBefore, err := helper.QueryPrometheusMetric("http://localhost:19090", "validator_processed_transactions")
	if err != nil {
		t.Errorf("Failed to query prometheus metric: %v", err)
	}

	hashesNode0, err := helper.CreateAndSendRawTxs(framework.Context, framework.Nodes[0], 20)
	if err != nil {
		t.Errorf("Failed to create and send raw txs: %v", err)
	}
	// for i := 0; i < 5; i++ {
	// 	hashesNode0, err = helper.CreateAndSendRawTxs(framework.Context, framework.Nodes[0], 10)
	// 	if err != nil {
	// 		t.Errorf("Failed to create and send raw txs: %v", err)
	// 	}
	// 	fmt.Printf("Hashes: %v\n", hashesNode0)
	// }

	state, err = blockAssemblyNode0.GetBlockAssemblyState(framework.Context)
	if err != nil {
		t.Errorf("Failed to get block assembly state: %v", err)
	}
	txCountAfter := state.GetTxCount()

	fmt.Println("Tx count before: ", txCountBefore)
	fmt.Println("Tx count after: ", txCountAfter)

	assert.LessOrEqual(t, txCountAfter, uint64(10), "Tx count mismatch")

	metricsAfter, err := helper.QueryPrometheusMetric("http://localhost:19090", "validator_processed_transactions")
	if err != nil {
		t.Errorf("Failed to query prometheus metric: %v", err)
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
		if bl, err := helper.ReadFile(framework.Context, "block", framework.Logger, r, hashesNode0[5], framework.Nodes[0].BlockstoreUrl); err != nil {
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

		t.Errorf("Failed to set state: %v", err)

	}
	time.Sleep(5 * time.Second)
	fsmState = blockchainNode0.GetFSMCurrentStateForE2ETestMode()
	assert.Equal(t, fsmState, blockchain_api.FSMStateType(2), "FSM state is not equal to 2")

	var newHeight int

	var bl bool
	height, _ := helper.GetBlockHeight(url)

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
				if bl, err = helper.ReadFile(framework.Context, "block", framework.Logger, r, hashesNode0[5], framework.Nodes[0].BlockstoreUrl); err != nil {
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

func (suite *FsmTestSuite) TestTXCatchUpState_SendTXsToNode0_and_Node1() {

	t := suite.T()
	framework := suite.Framework
	url := "http://localhost:18090"
	blockchainNode0 := framework.Nodes[0].BlockchainClient

	// Set CatchUpTransactions State
	_, err := blockchainNode0.CatchUpTransactions(framework.Context, &emptypb.Empty{})
	if err != nil {
		t.Errorf("Failed to set state: %v", err)
	}
	time.Sleep(5 * time.Second)
	fsmState := blockchainNode0.GetFSMCurrentStateForE2ETestMode()
	assert.Equal(t, fsmState, blockchain_api.FSMStateType(4), "FSM state is not equal to 4")

	metricsBefore, err := helper.QueryPrometheusMetric("http://localhost:19090", "validator_processed_transactions")
	if err != nil {
		t.Errorf("Failed to query prometheus metric: %v", err)
	}

	hashesNode0, err := helper.CreateAndSendRawTxs(framework.Context, framework.Nodes[0], 20)
	if err != nil {
		t.Errorf("Failed to create and send raw txs: %v", err)
	}
	hashesNode1, err := helper.CreateAndSendRawTxs(framework.Context, framework.Nodes[1], 20)
	if err != nil {
		t.Errorf("Failed to create and send raw txs: %v", err)
	}

	metricsAfter, err := helper.QueryPrometheusMetric("http://localhost:19090", "validator_processed_transactions")
	if err != nil {
		t.Errorf("Failed to query prometheus metric: %v", err)
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
				if bl, err = helper.ReadFile(framework.Context, "block", framework.Logger, r, hashesNode1[5], framework.Nodes[0].BlockstoreUrl); err != nil {
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
		t.Errorf("Failed to set state: %v", err)
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
				if bl, err = helper.ReadFile(framework.Context, "block", framework.Logger, r, hashesNode0[5], framework.Nodes[0].BlockstoreUrl); err != nil {
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

func TestFsmTestSuite(t *testing.T) {
	suite.Run(t, new(FsmTestSuite))
}
