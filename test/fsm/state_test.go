//go:build functional

// How to run this test:
// $ cd test/fsm/
// $ go test -v -run "^TestFsmTestSuite$/TestNodeCatchUpState_WithStartAndStopNodes$" -tags functional
// $ go test -v -run "^TestFsmTestSuite$/TestNodeCatchUpState_WithP2PSwitch$" -tags functional
// $ go test -v -run "^TestFsmTestSuite$/TestTXCatchUpState_SendTXsToNode0$" -tags functional

package test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/bitcoin-sv/ubsv/services/blockchain/blockchain_api"
	"github.com/bitcoin-sv/ubsv/stores/blob/options"
	arrange "github.com/bitcoin-sv/ubsv/test/fixtures"
	helper "github.com/bitcoin-sv/ubsv/test/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type FsmTestSuite struct {
	arrange.TeranodeTestSuite
}

func (suite *FsmTestSuite) TearDownTest() {
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
	framework := suite.TeranodeTestEnv
	logger := framework.Logger
	blockchainNode0 := framework.Nodes[0].BlockchainClient
	blockchainNode1 := framework.Nodes[1].BlockchainClient

	var (
		mu   sync.Mutex
		wg   sync.WaitGroup
		done = make(chan struct{})
	)

	stateSet := make(map[blockchain_api.FSMStateType]struct{})

	err := framework.StopNode("ubsv2")
	if err != nil {
		t.Errorf("Failed to stop node: %v", err)
	}

	for i := 0; i < 5; i++ {
		hashes, err := helper.CreateAndSendTxs(ctx, framework.Nodes[0], 10)

		if err != nil {
			t.Errorf("Failed to create and send raw txs: %v", err)
		}

		logger.Infof("Hashes: %v", hashes)

		baClient := framework.Nodes[0].BlockassemblyClient

		_, err = helper.MineBlock(ctx, baClient, logger)
		require.NoError(t, err)
	}

	err = framework.StartNode("ubsv2")
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

	headerNode1, _, _ := blockchainNode1.GetBestBlockHeader(ctx)
	headerNode0, _, _ := blockchainNode0.GetBestBlockHeader(ctx)

	assert.Equal(t, headerNode0.Hash(), headerNode1.Hash(), "Best block headers are not equal")
	assert.True(t, stateFound, "State 3 was not captured")
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
	framework := suite.TeranodeTestEnv
	settingsMap := suite.SettingsMap
	logger := framework.Logger
	blockchainNode0 := framework.Nodes[0].BlockchainClient
	blockchainNode1 := framework.Nodes[1].BlockchainClient
	ctx := framework.Context

	var (
		states []blockchain_api.FSMStateType
		mu     sync.Mutex
		wg     sync.WaitGroup
		done   = make(chan struct{})
	)

	stateSet := make(map[blockchain_api.FSMStateType]struct{})

	settingsMap["SETTINGS_CONTEXT_2"] = "docker.ubsv2.test.stopP2P"
	if err := framework.RestartDockerNodes(settingsMap); err != nil {
		t.Errorf("Failed to restart nodes: %v", err)
	}

	err := framework.Nodes[0].BlockchainClient.Run(ctx, "ubsv1")
	if err != nil {
		suite.T().Fatal(err)
	}

	err = framework.Nodes[1].BlockchainClient.Run(ctx, "ubsv2")
	if err != nil {
		suite.T().Fatal(err)
	}

	err = framework.Nodes[2].BlockchainClient.Run(ctx, "ubsv3")
	if err != nil {
		suite.T().Fatal(err)
	}

	for i := 0; i < 5; i++ {
		hashes, err := helper.CreateAndSendTxs(ctx, framework.Nodes[0], 1)
		if err != nil {
			t.Errorf("Failed to create and send raw txs: %v", err)
		}

		logger.Infof("Hashes: %v", hashes)

		_, err = helper.MineBlockWithRPC(ctx, framework.Nodes[0], logger)
		require.NoError(t, err)

		if err != nil {
			t.Errorf("Failed to mine block: %v", err)
		}
	}

	settingsMap["SETTINGS_CONTEXT_2"] = "docker.ubsv2.test"
	if err := framework.RestartDockerNodes(settingsMap); err != nil {
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
				response, _ := blockchainNode1.GetFSMCurrentState(framework.Context)

				mu.Lock()
				if _, exists := stateSet[*response]; !exists {
					logger.Infof("New unique state: %v", response)

					stateSet[*response] = struct{}{} // Add the state to the set
				}
				logger.Infof("Current states: %v", stateSet)
				mu.Unlock()
				time.Sleep(1 * time.Second)
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

	headerNode1, _, _ := blockchainNode1.GetBestBlockHeader(ctx)
	headerNode0, _, _ := blockchainNode0.GetBestBlockHeader(ctx)
	assert.Equal(t, headerNode0.Hash(), headerNode1.Hash(), "Best block headers are not equal")

	logger.Infof("Captured states: %v", states)
	assert.True(t, stateFound, "State 3 was not captured")
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
// Also, verify that the transactions are included in the block of the 2nd node
// Set the Running State for the 1st node
// Check if the transactions are included in the block (should be included this time)
// TODO - The prometheus metrics that were introduced earlier are not working as expected. Need to fix this.
func (suite *FsmTestSuite) TestTXCatchUpState_SendTXsToNode0() {
	t := suite.T()
	framework := suite.TeranodeTestEnv
	url := "http://localhost:10090"
	logger := framework.Logger
	blockchainNode0 := framework.Nodes[0].BlockchainClient
	blockchainNode1 := framework.Nodes[1].BlockchainClient
	blockAssemblyNode0 := framework.Nodes[0].BlockassemblyClient

	// blockchainNode1 := framework.Nodes[1].BlockchainClient
	blockAssemblyNode1 := framework.Nodes[1].BlockassemblyClient

	// Set CatchUpTransactions State
	err := blockchainNode0.CatchUpTransactions(framework.Context)
	if err != nil {
		t.Errorf("Failed to set state: %v", err)
	}

	time.Sleep(10 * time.Second)

	fsmState, _ := blockchainNode0.GetFSMCurrentState(framework.Context)
	t.Logf("FSM state after sending CatchupTx: %v", fsmState)
	t.Logf("Expected FSM state name after sending CatchupTx: %v", blockchain_api.FSMStateType_name[4])
	assert.Equal(t, "CATCHINGTXS", fsmState.String(), "FSM state is not equal to 4")

	state0, err := blockAssemblyNode0.GetBlockAssemblyState(framework.Context)
	if err != nil {
		t.Errorf("Failed to get block assembly state: %v", err)
	}

	state1, err := blockAssemblyNode1.GetBlockAssemblyState(framework.Context)
	if err != nil {
		t.Errorf("Failed to get block assembly state: %v", err)
	}

	txCountBefore0 := state0.GetTxCount()
	logger.Infof("Node 0 Tx count before: %v", txCountBefore0)

	txCountBefore1 := state1.GetTxCount()
	logger.Infof("Node 1 Tx count before: %v", txCountBefore1)

	// metricsBefore, err := helper.QueryPrometheusMetric("http://localhost:16090", "validator_processed_transactions")
	// if err != nil {
	// 	t.Errorf("Failed to query prometheus metric: %v", err)
	// }

	// nodes := framework.Nodes

	// Uncommenting and using either of the below methods will send transactions to all the nodes.
	// This will result with test fail, in line 332 and 333.
	// hashesNode0, err := helper.CreateAndSendTxsToASliceOfNodes(framework.Context, nodes, 20)
	// hashesNode0, err := helper.CreateAndSendTxsToASliceOfNodes(framework.Context, framework.Nodes, 20)

	hashesNode0, err := helper.CreateAndSendTxs(framework.Context, framework.Nodes[0], 20)
	if err != nil {
		t.Errorf("Failed to create and send raw txs: %v", err)
	}

	for i := 0; i < 20; i++ {
		logger.Infof("Hashes: %v", hashesNode0[i])
	}

	state0, err = blockAssemblyNode0.GetBlockAssemblyState(framework.Context)
	if err != nil {
		t.Errorf("Failed to get block assembly state for Node 0: %v", err)
	}

	state1, err = blockAssemblyNode1.GetBlockAssemblyState(framework.Context)
	if err != nil {
		t.Errorf("Failed to get block assembly state for Node 1: %v", err)
	}

	txCountAfter0 := state0.GetTxCount()
	logger.Infof("Node 0 Tx count after sending transactions: %v", txCountAfter0)

	txCountAfter1 := state1.GetTxCount()
	logger.Infof("Node 1 Tx count after sending transactions: %v", txCountAfter1)

	assert.LessOrEqual(t, txCountAfter0, uint64(10), "Tx count mismatch")
	assert.LessOrEqual(t, txCountAfter1, uint64(10), "Tx count mismatch")

	// metricsAfter, err := helper.QueryPrometheusMetric("http://localhost:16090", "validator_processed_transactions")
	// if err != nil {
	// 	t.Errorf("Failed to query prometheus metric: %v", err)
	// }

	// assert.LessOrEqual(t, metricsAfter, float64(10), "Tx count mismatch")

	//mine a block
	_, err = helper.MineBlockWithRPC(framework.Context, framework.Nodes[0], logger)
	require.NoError(t, err)
	time.Sleep(5 * time.Second)

	var o []options.FileOption
	o = append(o, options.WithFileExtension("block"))
	bestBlock, _, _ := blockchainNode0.GetBestBlockHeader(framework.Context)
	blockStoreA := framework.Nodes[0].Blockstore
	blockStoreB := framework.Nodes[1].Blockstore

	r, err := blockStoreA.GetIoReader(framework.Context, bestBlock.Hash()[:], o...)
	if err != nil {
		t.Errorf("error getting block reader: %v", err)
	}

	fmt.Println("Initial block read")
	if err == nil {
		if bl, err := helper.ReadFile(framework.Context, "block", framework.Logger, r, hashesNode0[1], framework.Nodes[0].BlockstoreURL); err != nil {
			t.Errorf("error reading block: %v", err)
		} else {
			logger.Infof("Block at height (%d): was tested for the test Tx\n", *bestBlock.Hash())
			assert.Equal(t, false, bl, "Test Tx found in block, was not expecting to see it in the block while in catch-up state")
		}
	}

	// return
	_, meta, _ := blockchainNode0.GetBestBlockHeader(framework.Context)
	t.Logf("Best block height of node0: %v", meta.Height)

	_, meta, _ = blockchainNode1.GetBestBlockHeader(framework.Context)
	t.Logf("Best block height of node1: %v", meta.Height)

	// Set Running State
	err = blockchainNode0.Run(framework.Context, "ubsv1")
	if err != nil {
		t.Errorf("Failed to set state: %v", err)
	}

	time.Sleep(10 * time.Second)

	fsmState, _ = blockchainNode0.GetFSMCurrentState(framework.Context)
	t.Logf("FSM state after sending run: %v", fsmState)
	assert.Equal(t, "RUNNING", fsmState.String(), "FSM state is not equal to 1")

	height, _ := helper.GetBlockHeight(url)
	t.Logf("Block height before mining: %d\n", height)

	// Check newer blocks that arrived and check if the test tx is included in the block

	_, err = helper.MineBlockWithRPC(framework.Context, framework.Nodes[0], logger)
	require.NoError(t, err)

	if err != nil {
		t.Errorf("Failed to mine block: %v", err)
	}

	blA := false
	blB := false
	targetHeight := height + 1

	for i := 0; i < 10; i++ {
		err := helper.WaitForBlockHeight(url, targetHeight, 30)
		if err != nil {
			t.Errorf("Failed to wait for block height: %v", err)
		}

		header, meta, err := blockchainNode0.GetBlockHeadersFromHeight(framework.Context, targetHeight, 1)
		if err != nil {
			t.Errorf("Failed to get block headers: %v", err)
		}

		t.Logf("Testing on Best block height: %v", meta[0].Height)
		t.Logf("Testing on Test Tx: %v", hashesNode0[10])
		blA, err = helper.CheckIfTxExistsInBlock(framework.Context, blockStoreA, framework.Nodes[0].BlockstoreURL, header[0].Hash()[:], meta[0].Height, hashesNode0[10], logger)

		if err != nil {
			t.Errorf("error checking if tx exists in block: %v", err)
		}

		blB, err = helper.CheckIfTxExistsInBlock(framework.Context, blockStoreB, framework.Nodes[1].BlockstoreURL, header[0].Hash()[:], meta[0].Height, hashesNode0[10], logger)

		if err != nil {
			t.Errorf("error checking if tx exists in block: %v", err)
		}

		if blA && blB {
			break
		}

		targetHeight++
		_, err = helper.MineBlockWithRPC(framework.Context, framework.Nodes[0], logger)
		require.NoError(t, err)

		if err != nil {
			t.Errorf("Failed to mine block: %v", err)
		}
	}

	assert.Equal(t, true, blA, "Test Tx not found in block")
	assert.Equal(t, true, blB, "Test Tx not found in block")
}

func TestFsmTestSuite(t *testing.T) {
	suite.Run(t, new(FsmTestSuite))
}
