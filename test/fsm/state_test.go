//go:build functional

// How to run this test:
// $ cd test/fsm/
// $ SETTINGS_CONTEXT=docker.ci.tc1.run go test -v -run "^TestFsmTestSuite$/TestNodeCatchUpState_WithStartAndStopNodes$" -tags functional
// $ SETTINGS_CONTEXT=docker.ci.tc1.run go test -v -run "^TestFsmTestSuite$/TestNodeCatchUpState_WithP2PSwitch$" -tags functional
// $ SETTINGS_CONTEXT=docker.ci.tc1.run go test -v -run "^TestFsmTestSuite$/TestTXCatchUpState_SendTXsToNode0$" -tags functional

package test

import (
	"context"
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
)

type FsmTestSuite struct {
	setup.BitcoinTestSuite
}

/* Description */
// This test suite is used to test the FSM states of the blockchain node.
// Start the chain of 3 nodes
// Stop the 2nd node
// Send transactions to the 1st node and mine blocks for 5 times
// Start the 2nd node
// Check if the 2nd node catches up with the 1st node
// The test captures the intermediate states of the 2nd node and checks if the node wa in the catch-up state
func (suite *FsmTestSuite) TestNodeCatchUpState_WithStartAndStopNodes() {
	ctx := context.Background()
	t := suite.T()
	framework := suite.Framework
	logger := framework.Logger
	blockchainNode0 := framework.Nodes[0].BlockchainClient
	blockchainNode1 := framework.Nodes[1].BlockchainClient

	var (
		mu   sync.Mutex
		wg   sync.WaitGroup
		done = make(chan struct{})
	)

	stateSet := make(map[blockchain_api.FSMStateType]struct{})

	err := framework.StopNode("ubsv-2")
	if err != nil {
		t.Errorf("Failed to stop node: %v", err)
	}

	for i := 0; i < 5; i++ {
		hashes, err := helper.CreateAndSendRawTxs(ctx, framework.Nodes[0], 10)
		if err != nil {
			t.Errorf("Failed to create and send raw txs: %v", err)
		}
		logger.Infof("Hashes: %v", hashes)


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

/* Description */
// This test suite is used to test the FSM states of the blockchain node.
// Start the chain of 3 nodes, 2nd node starts with p2p off startP2P.docker.ci.ubsv2.tc3=false
// Send transactions to the 1st node and mine blocks for 5 times
// Re-Start the 2nd node with the p2p on startP2P.docker.ci.ubsv2.tc1=true
// Check if the 2nd node catches up with the 1st node
// The test captures the intermediate states of the 2nd node and checks if the node wa in the catch-up state
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
		logger.Infof("Hashes: %v", hashes)

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
	logger.Infof("Captured states: %v", states)

	headerNode1, _, _ := blockchainNode1.GetBestBlockHeader(ctx)
	headerNode0, _, _ := blockchainNode0.GetBestBlockHeader(ctx)
	assert.Equal(t, headerNode0.Hash(), headerNode1.Hash(), "Best block headers are not equal")
}

/* Description */
// This test suite is used to test the CatchUpTransactions State of the blockchain node.
// Start the chain of 3 nodes
// Set the CatchUpTransactions State for the 1st node
// Take a count of the transactions in block assembly before sending transactions
// Send transactions to the 1st node
// Take a count of the transactions in block assembly after sending transactions
// Check if the transactions count after is less than or equal to before (in catch-up state no new transactions should be accepted)
// Additionally, verify that the transactions are not included in the block
// Set the Running State for the 1st node
// Check if the transactions are included in the block (should be included this time)
// TODO - The prometheus metrics that were introduced earlier are not working as expected. Need to fix this.
func (suite *FsmTestSuite) TestTXCatchUpState_SendTXsToNode0() {
	t := suite.T()
	framework := suite.Framework
	url := "http://localhost:10090"
	logger := framework.Logger
	blockchainNode0 := framework.Nodes[0].BlockchainClient
	blockAssemblyNode0 := framework.Nodes[0].BlockassemblyClient

	// Set CatchUpTransactions State
	err := blockchainNode0.CatchUpTransactions(framework.Context)
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
	logger.Infof("Tx count before: %v", txCountBefore)

	// metricsBefore, err := helper.QueryPrometheusMetric("http://localhost:16090", "validator_processed_transactions")
	// if err != nil {
	// 	t.Errorf("Failed to query prometheus metric: %v", err)
	// }

	hashesNode0, err := helper.CreateAndSendRawTxs(framework.Context, framework.Nodes[0], 20)
	if err != nil {
		t.Errorf("Failed to create and send raw txs: %v", err)
	}

	state, err = blockAssemblyNode0.GetBlockAssemblyState(framework.Context)
	if err != nil {
		t.Errorf("Failed to get block assembly state: %v", err)
	}

	txCountAfter := state.GetTxCount()
	logger.Infof("Tx count after: %v", txCountAfter)


	assert.LessOrEqual(t, txCountAfter, uint64(10), "Tx count mismatch")

	// metricsAfter, err := helper.QueryPrometheusMetric("http://localhost:16090", "validator_processed_transactions")
	// if err != nil {
	// 	t.Errorf("Failed to query prometheus metric: %v", err)
	// }

	// assert.LessOrEqual(t, metricsAfter, float64(10), "Tx count mismatch")

	var o []options.FileOption
	o = append(o, options.WithFileExtension("block"))
	bestBlock, _, _ := blockchainNode0.GetBestBlockHeader(framework.Context)
	blockStore := framework.Nodes[0].Blockstore

	r, err := blockStore.GetIoReader(framework.Context, bestBlock.Hash()[:], o...)
	if err != nil {
		t.Errorf("error getting block reader: %v", err)
	}

	if err == nil {
		if bl, err := helper.ReadFile(framework.Context, "block", framework.Logger, r, hashesNode0[5], framework.Nodes[0].BlockstoreURL); err != nil {
			t.Errorf("error reading block: %v", err)
		} else {
			logger.Infof("Block at height (%d): was tested for the test Tx\n", *bestBlock.Hash())
			assert.Equal(t, false, bl, "Test Tx found in block, was not expecting to see it in the block while in catch-up state")
		}
	}

	bestBlock, _, _ = blockchainNode0.GetBestBlockHeader(framework.Context)

	// Set Running State
	err = blockchainNode0.Run(framework.Context)
	if err != nil {
		t.Errorf("Failed to set state: %v", err)
	}

	time.Sleep(5 * time.Second)

	fsmState = blockchainNode0.GetFSMCurrentStateForE2ETestMode()
	assert.Equal(t, fsmState, blockchain_api.FSMStateType(1), "FSM state is not equal to 1")

	var newHeight uint32

	var bl bool

	height, _ := helper.GetBlockHeight(url)

	for i := 0; i < 180; i++ {
		newHeight, _ = helper.GetBlockHeight(url)
		if newHeight > height {
			height = newHeight
			logger.Infof("Testing at height: %v", height)
			mBlock, _ := blockchainNode0.GetBlockByHeight(framework.Context, uint32(newHeight))

			r, err = blockStore.GetIoReader(framework.Context, mBlock.Hash()[:], o...)
			if err != nil {
				t.Errorf("error getting block reader: %v", err)
			}

			if err == nil {
				if bl, err = helper.ReadFile(framework.Context, "block", framework.Logger, r, hashesNode0[5], framework.Nodes[0].BlockstoreURL); err != nil {
					t.Errorf("error reading block: %v", err)
				} else {
					logger.Infof("Block at height (%d): was tested for the test Tx\n", newHeight)

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
