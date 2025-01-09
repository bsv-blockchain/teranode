//go:build test_all || test_fsm || test_functional

// How to run this test:
// $ cd test/fsm/
// $ go test -v -run "^TestFsmTestSuite$/TestNodeCatchUpState_WithStartAndStopNodes$" -tags test_functional
// $ go test -v -run "^TestFsmTestSuite$/TestNodeCatchUpStateSingleNode_WithP2PSwitch$" -tags test_functional
// $ go test -v -run "^TestFsmTestSuite$/TestNodeCatchUpStateMultipleNodes_WithP2PSwitch$" -tags test_functional
// $ go test -v -run "^TestFsmTestSuite$/TestNodeDoesNotSendMiningCandidate_CatchUpState_WithStartAndStopNodes$" -tags test_functional

package test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/model"
	"github.com/bitcoin-sv/teranode/services/blockchain/blockchain_api"
	helper "github.com/bitcoin-sv/teranode/test/utils"
	"github.com/docker/go-connections/nat"
	"github.com/ordishs/gocore"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type FsmTestSuite struct {
	helper.TeranodeTestSuite
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
		mu sync.Mutex
	)

	err := framework.StopNode("teranode2")
	if err != nil {
		t.Errorf("Failed to stop node: %v", err)
	}

	for i := 0; i < 5; i++ {
		hashes, err := helper.CreateAndSendTxs(ctx, framework.Nodes[0], 5)

		if err != nil {
			t.Errorf("Failed to create and send raw txs: %v", err)
		}

		logger.Infof("Hashes: %v", hashes)

		_, err = helper.MineBlockWithRPC(ctx, framework.Nodes[0], logger)
		require.NoError(t, err)
	}

	err = framework.StartNode("teranode2")
	if err != nil {
		t.Errorf("Failed to start node: %v", err)
	}

	err = framework.InitializeTeranodeTestClients()
	if err != nil {
		t.Errorf("Failed to initialize teranode test clients: %v", err)
	}
	time.Sleep(10 * time.Second)

	port, ok := gocore.Config().GetInt("health_check_port", 8000)
	if !ok {
		suite.T().Fatalf("health_check_port not set in config")
	}

	ports := []int{port, port, port}

	for index, port := range ports {
		mappedPort, err := suite.TeranodeTestEnv.GetMappedPort(fmt.Sprintf("teranode%d", index+1), nat.Port(fmt.Sprintf("%d/tcp", port)))
		if err != nil {
			suite.T().Fatal(err)
		}

			suite.T().Logf("Waiting for node %d to be ready", index)

			err = helper.WaitForHealthLiveness(mappedPort.Int(), 30*time.Second)
			if err != nil {
				suite.T().Fatal(err)
			}
		}

	blockchainNode1 = framework.Nodes[1].BlockchainClient
	stateSet := make(map[blockchain_api.FSMStateType]struct{})

	// wait for node 2 block validator to catch up
	wait := func() {
		// trigger a new block to wake up node 2 and start catch up process
		_, err = helper.MineBlockWithRPC(ctx, framework.Nodes[0], logger)
		require.NoError(t, err)

		timeout := time.After(30 * time.Second)

		for {
			select {
			case <-timeout:
				return
			default:
				// We are trying to capture all the states as block validation runs in the background
				// However, it is very possible that block validation switches from RUNNING to CATCHINGUP
				// and then back to RUNNING before our test manages to capture the CATCHINGUP state
				// This is making this test flaky
				// To attempt to counter this, the Blockchain server uses a delay to ensure that the state change is captured
				// This is configured using the fsm_state_change_delay setting
				state := blockchainNode1.GetFSMCurrentStateForE2ETestMode()

				mu.Lock()
				if _, exists := stateSet[state]; !exists {
					framework.Logger.Infof("New unique state: %v", state)

					stateSet[state] = struct{}{}
				}
				mu.Unlock()

				if state == blockchain_api.FSMStateType_RUNNING {
					if _, exists := stateSet[blockchain_api.FSMStateType_CATCHINGBLOCKS]; exists {
						// take a note of the best block header of node 0 (teranode1)
						blockHeaderNode0, _, err := blockchainNode0.GetBestBlockHeader(ctx)
						require.NoError(t, err)

						// check if the block of node 0 (teranode1) is synced to node 1 (teranode2)
						_, err = blockchainNode1.GetBlockExists(ctx, blockHeaderNode0.Hash())
						if !errors.Is(err, errors.ErrBlockNotFound) {
							return
						}
					}
				}

				time.Sleep(10 * time.Millisecond) // this is increasing of NOT capturing the CATCHINGUP state if block validation is fast
			}
		}
	}
	wait()

	stateFound := false

	for state := range stateSet {
		if state == blockchain_api.FSMStateType_CATCHINGBLOCKS {
			stateFound = true
			break
		}
	}

	headerNode1, _, _ := blockchainNode1.GetBestBlockHeader(ctx)
	headerNode0, _, _ := blockchainNode0.GetBestBlockHeader(ctx)

	assert.Equal(t, headerNode0.Hash(), headerNode1.Hash(), "Best block headers are not equal")
	assert.True(t, stateFound, "State 3 (CATCHINGBLOCKS) was not captured")
}

/* Description */
// This test suite is used to test the FSM states of the blockchain node.
// Start the chain of 3 nodes, 2nd node starts with p2p off startP2P.docker.ci.teranode2.tc3=false
// Send transactions to the 1st node and mine blocks for 5 times
// Re-Start the 2nd node with the p2p on startP2P.docker.ci.teranode2.tc1=true
// Check if the 2nd node catches up with the 1st node
// The test captures the intermediate states of the 2nd node and checks if the node wa in the catch-up state
func (suite *FsmTestSuite) TestNodeCatchUpStateSingleNode_WithP2PSwitch() {
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
	)

	stateSet := make(map[blockchain_api.FSMStateType]struct{})

	settingsMap["SETTINGS_CONTEXT_2"] = "docker.teranode2.test.stopP2P"
	if err := framework.RestartDockerNodes(settingsMap); err != nil {
		t.Errorf("Failed to restart nodes: %v", err)
	}

	err := framework.InitializeTeranodeTestClients()
	if err != nil {
		t.Errorf("Failed to initialize teranode test clients: %v", err)
	}
	time.Sleep(10 * time.Second)

	port, ok := gocore.Config().GetInt("health_check_port", 8000)
	if !ok {
		suite.T().Fatalf("health_check_port not set in config")
	}

	ports := []int{port, port, port}

	for index, port := range ports {
		mappedPort, err := suite.TeranodeTestEnv.GetMappedPort(fmt.Sprintf("teranode%d", index+1), nat.Port(fmt.Sprintf("%d/tcp", port)))
		if err != nil {
			suite.T().Fatal(err)
		}

			suite.T().Logf("Waiting for node %d to be ready", index)

			err = helper.WaitForHealthLiveness(mappedPort.Int(), 30*time.Second)
			if err != nil {
				suite.T().Fatal(err)
			}
		}

	// wait for all blockchain nodes to be ready
	for index, node := range suite.TeranodeTestEnv.Nodes {
		suite.T().Logf("Sending initial RUN event to Blockchain %d", index)

		err = helper.SendEventRun(suite.TeranodeTestEnv.Context, node.BlockchainClient, suite.TeranodeTestEnv.Logger)
		if err != nil {
			suite.T().Fatal(err)
		}
	}

	suite.T().Log("All nodes ready")

	blockchainNode0 = framework.Nodes[0].BlockchainClient
	blockchainNode1 = framework.Nodes[1].BlockchainClient

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

	settingsMap["SETTINGS_CONTEXT_2"] = "docker.teranode2.test"
	if err := framework.RestartDockerNodes(settingsMap); err != nil {
		t.Errorf("Failed to restart nodes: %v", err)
	}

	err = framework.InitializeTeranodeTestClients()
	if err != nil {
		t.Errorf("Failed to initialize teranode test clients: %v", err)
	}
	time.Sleep(10 * time.Second)

	for index, port := range ports {
		mappedPort, err := suite.TeranodeTestEnv.GetMappedPort(fmt.Sprintf("teranode%d", index+1), nat.Port(fmt.Sprintf("%d/tcp", port)))
		if err != nil {
			suite.T().Fatal(err)
		}

			suite.T().Logf("Waiting for node %d to be ready", index)

			err = helper.WaitForHealthLiveness(mappedPort.Int(), 30*time.Second)
			if err != nil {
				suite.T().Fatal(err)
			}
		}

	// wait for all blockchain nodes to be ready
	for index, node := range suite.TeranodeTestEnv.Nodes {
		suite.T().Logf("Sending initial RUN event to Blockchain %d", index)

		err = helper.SendEventRun(suite.TeranodeTestEnv.Context, node.BlockchainClient, suite.TeranodeTestEnv.Logger)
		if err != nil {
			suite.T().Fatal(err)
		}
	}

	suite.T().Log("All nodes ready")

	blockchainNode0 = framework.Nodes[0].BlockchainClient
	blockchainNode1 = framework.Nodes[1].BlockchainClient

	// wait for node 2 block validator to catch up
	wait := func() {
		// trigger a new block to wake up node 2 and start catch up process
		_, err = helper.MineBlockWithRPC(ctx, framework.Nodes[0], logger)
		require.NoError(t, err)

		timeout := time.After(30 * time.Second)
		for {
			select {
			case <-timeout:
				return
			default:
				// We are trying to capture all the states as block validation runs in the background
				// However, it is very possible that block validation switches from RUNNING to CATCHINGUP
				// and then back to RUNNING before our test manages to capture the CATCHINGUP state
				// This is making this test flaky
				// To attempt to counter this, the Blockchain server uses a delay to ensure that the state change is captured
				// This is configured using the fsm_state_change_delay setting
				state := blockchainNode1.GetFSMCurrentStateForE2ETestMode()

				mu.Lock()
				if _, exists := stateSet[state]; !exists {
					framework.Logger.Infof("New unique state: %v", state)

					stateSet[state] = struct{}{}
				}
				mu.Unlock()

				if state == blockchain_api.FSMStateType_RUNNING {
					if _, exists := stateSet[blockchain_api.FSMStateType_CATCHINGBLOCKS]; exists {
						// take a note of the best block header of node 0 (teranode1)
						blockHeaderNode0, _, err := blockchainNode0.GetBestBlockHeader(ctx)
						require.NoError(t, err)

						// check if the block of node 0 (teranode1) is synced to node 1 (teranode2)
						_, err = blockchainNode1.GetBlockExists(ctx, blockHeaderNode0.Hash())
						if !errors.Is(err, errors.ErrBlockNotFound) {
							return
						}
					}
				}

				time.Sleep(10 * time.Millisecond) // this is increasing of NOT capturing the CATCHINGUP state if block validation is fast
			}
		}
	}
	wait()

	stateFound := false

	for state := range stateSet {
		if state == blockchain_api.FSMStateType_CATCHINGBLOCKS {
			stateFound = true
			break
		}
	}

	headerNode1, _, _ := blockchainNode1.GetBestBlockHeader(ctx)
	headerNode0, _, _ := blockchainNode0.GetBestBlockHeader(ctx)
	assert.Equal(t, headerNode0.Hash(), headerNode1.Hash(), "Best block headers are not equal")

	logger.Infof("Captured states: %v", states)
	assert.True(t, stateFound, "State 3 (CATCHINGBLOCKS) was not captured")
}

/* Description */
// This test suite is used to test the FSM states of the blockchain node with p2p initially off.
// 1. Start the chain of 3 nodes with p2p off for teranode2 and teranode3
// 2. Send transactions concurrently to all nodes in batches
// 3. Turn on p2p for all nodes
// 4. Check if nodes catch up and have matching best block headers
func (suite *FsmTestSuite) TestNodeCatchUpStateMultipleNodes_WithP2PSwitch() {
	t := suite.T()
	framework := suite.TeranodeTestEnv
	settingsMap := suite.SettingsMap
	logger := framework.Logger
	ctx := framework.Context

	// Get blockchain clients for verification
	blockchainNode0 := framework.Nodes[0].BlockchainClient
	blockchainNode1 := framework.Nodes[1].BlockchainClient
	blockchainNode2 := framework.Nodes[2].BlockchainClient

	var (
		mu            sync.Mutex
		stateSetNode1 = make(map[blockchain_api.FSMStateType]struct{})
		stateSetNode2 = make(map[blockchain_api.FSMStateType]struct{})
	)

	// Start nodes with p2p off for teranode2 and teranode3
	settingsMap["SETTINGS_CONTEXT_2"] = "docker.teranode2.test.stopP2P"
	settingsMap["SETTINGS_CONTEXT_3"] = "docker.teranode3.test.stopP2P"
	if err := framework.RestartDockerNodes(settingsMap); err != nil {
		t.Errorf("Failed to restart nodes: %v", err)
	}

	err := framework.InitializeTeranodeTestClients()
	if err != nil {
		t.Errorf("Failed to initialize teranode test clients: %v", err)
	}
	time.Sleep(10 * time.Second)

	port, ok := gocore.Config().GetInt("health_check_port", 8000)
	if !ok {
		suite.T().Fatalf("health_check_port not set in config")
	}

	ports := []int{port, port, port}

	for index, port := range ports {
		mappedPort, err := suite.TeranodeTestEnv.GetMappedPort(fmt.Sprintf("teranode%d", index+1), nat.Port(fmt.Sprintf("%d/tcp", port)))
		if err != nil {
			suite.T().Fatal(err)
		}

			suite.T().Logf("Waiting for node %d to be ready", index)

			err = helper.WaitForHealthLiveness(mappedPort.Int(), 30*time.Second)
			if err != nil {
				suite.T().Fatal(err)
			}
		}

	// wait for all blockchain nodes to be ready
	for index, node := range suite.TeranodeTestEnv.Nodes {
		suite.T().Logf("Sending initial RUN event to Blockchain %d", index)

		err = helper.SendEventRun(suite.TeranodeTestEnv.Context, node.BlockchainClient, suite.TeranodeTestEnv.Logger)
		if err != nil {
			suite.T().Fatal(err)
		}
	}

	blockchainNode0 = framework.Nodes[0].BlockchainClient
	blockchainNode1 = framework.Nodes[1].BlockchainClient
	blockchainNode2 = framework.Nodes[2].BlockchainClient

	// Wait for all blockchain nodes to be ready
	for index, node := range suite.TeranodeTestEnv.Nodes {
		suite.T().Logf("Sending initial RUN event to Blockchain %d", index)
		err := helper.SendEventRun(suite.TeranodeTestEnv.Context, node.BlockchainClient, suite.TeranodeTestEnv.Logger)
		if err != nil {
			suite.T().Fatal(err)
		}
	}

	suite.T().Log("All nodes ready")

	// Send transactions concurrently to all nodes
	var wg sync.WaitGroup
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func(nodeIndex int) {
			defer wg.Done()
			hashes, err := helper.CreateAndSendTxs(ctx, framework.Nodes[nodeIndex], 10)

			if err != nil {
				t.Errorf("Failed to create and send raw txs to node %d: %v", nodeIndex, err)
				return
			}
			logger.Infof("Hashes for node %d: %v", nodeIndex, hashes)

			// Generate multiple blocks for each node
			resp, err := helper.GenerateBlocks(ctx, framework.Nodes[nodeIndex], 500, logger)
			if err != nil {
				t.Errorf("Failed to generate blocks on node %d: %v", nodeIndex, err)
			}
			logger.Infof("Generated blocks for node %d: %v", nodeIndex, resp)
		}(i)
	}
	wg.Wait()

	// Turn on p2p for all nodes by resetting to default settings
	settingsMap["SETTINGS_CONTEXT_2"] = "docker.teranode2.test"
	settingsMap["SETTINGS_CONTEXT_3"] = "docker.teranode3.test"
	if err := framework.RestartDockerNodes(settingsMap); err != nil {
		t.Errorf("Failed to restart nodes with p2p on: %v", err)
	}

	err = framework.InitializeTeranodeTestClients()
	if err != nil {
		t.Errorf("Failed to initialize teranode test clients: %v", err)
	}
	time.Sleep(10 * time.Second)

	for index, port := range ports {
		mappedPort, err := suite.TeranodeTestEnv.GetMappedPort(fmt.Sprintf("teranode%d", index+1), nat.Port(fmt.Sprintf("%d/tcp", port)))
		if err != nil {
			suite.T().Fatal(err)
		}

			suite.T().Logf("Waiting for node %d to be ready", index)

			err = helper.WaitForHealthLiveness(mappedPort.Int(), 30*time.Second)
			if err != nil {
				suite.T().Fatal(err)
			}
		}
		blockchainNode0 = framework.Nodes[0].BlockchainClient
		blockchainNode1 = framework.Nodes[1].BlockchainClient
		blockchainNode2 = framework.Nodes[2].BlockchainClient

		// Wait for all blockchain nodes to be ready
		for index, node := range suite.TeranodeTestEnv.Nodes {
			suite.T().Logf("Sending initial RUN event to Blockchain %d", index)
			err := helper.SendEventRun(suite.TeranodeTestEnv.Context, node.BlockchainClient, suite.TeranodeTestEnv.Logger)
			if err != nil {
				suite.T().Fatal(err)
			}
		}

		suite.T().Log("All nodes ready")

	// Monitor state changes and wait for catch up
	wait := func() {
		// Trigger a new block to wake up nodes and start catch up process
		resp, err := helper.GenerateBlocks(ctx, framework.Nodes[0], 1, logger)
		require.NoError(t, err)
		logger.Infof("Generated trigger block: %v", resp)

		timeout := time.After(30 * time.Second)
		for {
			select {
			case <-timeout:
				return
			default:
				// Monitor Node 1 states
				responseNode1 := blockchainNode1.GetFSMCurrentStateForE2ETestMode()
				mu.Lock()
				if _, exists := stateSetNode1[responseNode1]; !exists {
					framework.Logger.Infof("Node 1 new state: %v", responseNode1)
					stateSetNode1[responseNode1] = struct{}{}
				}
				mu.Unlock()

				// Monitor Node 2 states
				responseNode2 := blockchainNode2.GetFSMCurrentStateForE2ETestMode()
				mu.Lock()
				if _, exists := stateSetNode2[responseNode2]; !exists {
					framework.Logger.Infof("Node 2 new state: %v", responseNode2)
					stateSetNode2[responseNode2] = struct{}{}
				}
				mu.Unlock()

				// Check if both nodes have caught up
				_, blockMetaNode0, err := blockchainNode0.GetBestBlockHeader(ctx)
				require.NoError(t, err)

				_, err1 := blockchainNode1.GetBlockByHeight(ctx, blockMetaNode0.Height)
				_, err2 := blockchainNode2.GetBlockByHeight(ctx, blockMetaNode0.Height)
				if !errors.Is(err1, errors.ErrBlockNotFound) && !errors.Is(err2, errors.ErrBlockNotFound) {
					return
				}

				time.Sleep(10 * time.Millisecond)
			}
		}
	}
	wait()

	// Verify both nodes went through catch-up state (FSMStateType 3 is CATCHINGUP)
	stateCatchupFound1 := false
	stateCatchupFound2 := false

	for state := range stateSetNode1 {
		if state == blockchain_api.FSMStateType_CATCHINGBLOCKS {
			stateCatchupFound1 = true
			break
		}
	}

	for state := range stateSetNode2 {
		if state == blockchain_api.FSMStateType_CATCHINGBLOCKS {
			stateCatchupFound2 = true
			break
		}
	}

	// Get final block headers from all nodes
	headerNode0, _, err := blockchainNode0.GetBestBlockHeader(ctx)
	require.NoError(t, err)
	headerNode1, _, err := blockchainNode1.GetBestBlockHeader(ctx)
	require.NoError(t, err)
	headerNode2, _, err := blockchainNode2.GetBestBlockHeader(ctx)
	require.NoError(t, err)

	// Verify all nodes have caught up and have the same best block header
	assert.Equal(t, headerNode0.Hash(), headerNode1.Hash(), "Best block headers of node 0 and 1 are not equal")
	assert.Equal(t, headerNode0.Hash(), headerNode2.Hash(), "Best block headers of node 0 and 2 are not equal")
	assert.True(t, stateCatchupFound1, "Node 1 did not enter catch-up state")
	assert.True(t, stateCatchupFound2, "Node 2 did not enter catch-up state")
}

func (suite *FsmTestSuite) TestNodeDoesNotSendMiningCandidate_CatchUpState_WithStartAndStopNodes() {
	ctx := context.Background()
	t := suite.T()
	framework := suite.TeranodeTestEnv
	logger := framework.Logger
	blockchainNode0 := framework.Nodes[0].BlockchainClient
	blockchainNode1 := framework.Nodes[1].BlockchainClient

	var (
		mu sync.Mutex
	)

	err := framework.StopNode("teranode2")
	if err != nil {
		t.Errorf("Failed to stop node: %v", err)
	}

	for i := 0; i < 50; i++ {
		hashes, err := helper.CreateAndSendTxs(ctx, framework.Nodes[0], 1)

		if err != nil {
			t.Errorf("Failed to create and send raw txs: %v", err)
		}

		logger.Infof("Hashes: %v", hashes)

		_, err = helper.MineBlockWithRPC(ctx, framework.Nodes[0], logger)
		require.NoError(t, err)
	}

	err = framework.StartNode("teranode2")
	if err != nil {
		t.Errorf("Failed to start node: %v", err)
	}

	err = framework.InitializeTeranodeTestClients()
	if err != nil {
		t.Errorf("Failed to initialize teranode test clients: %v", err)
	}
	time.Sleep(10 * time.Second)

	port, ok := gocore.Config().GetInt("health_check_port", 8000)
	if !ok {
		suite.T().Fatalf("health_check_port not set in config")
	}

	ports := []int{port, port, port}

	for index, port := range ports {
		mappedPort, err := suite.TeranodeTestEnv.GetMappedPort(fmt.Sprintf("teranode%d", index+1), nat.Port(fmt.Sprintf("%d/tcp", port)))
		if err != nil {
			suite.T().Fatal(err)
		}

			suite.T().Logf("Waiting for node %d to be ready", index)

			err = helper.WaitForHealthLiveness(mappedPort.Int(), 30*time.Second)
			if err != nil {
				suite.T().Fatal(err)
			}
		}

	blockchainNode1 = framework.Nodes[1].BlockchainClient
	stateSet := make(map[blockchain_api.FSMStateType]struct{})

	// wait for node 2 block validator to catch up
	wait := func() {
		// trigger a new block to wake up node 2 and start catch up process
		_, err = helper.MineBlockWithRPC(ctx, framework.Nodes[0], logger)
		require.NoError(t, err)

		timeout := time.After(30 * time.Second)

		for {
			select {
			case <-timeout:
				return
			default:
				// We are trying to capture all the states as block validation runs in the background
				// However, it is very possible that block validation switches from RUNNING to CATCHINGUP
				// and then back to RUNNING before our test manages to capture the CATCHINGUP state
				// This is making this test flaky
				// To attempt to counter this, the Blockchain server uses a delay to ensure that the state change is captured
				// This is configured using the fsm_state_change_delay setting
				state := blockchainNode1.GetFSMCurrentStateForE2ETestMode()

				mu.Lock()
				if _, exists := stateSet[state]; !exists {
					framework.Logger.Infof("New unique state: %v", state)

					stateSet[state] = struct{}{}
				}
				mu.Unlock()

				if state == blockchain_api.FSMStateType_RUNNING {
					if _, exists := stateSet[blockchain_api.FSMStateType_CATCHINGBLOCKS]; exists {
						// take a note of the best block header of node 0 (teranode1)
						blockHeaderNode0, _, err := blockchainNode0.GetBestBlockHeader(ctx)
						require.NoError(t, err)

						// check if the block of node 0 (teranode1) is synced to node 1 (teranode2)
						_, err = blockchainNode1.GetBlockExists(ctx, blockHeaderNode0.Hash())
						if !errors.Is(err, errors.ErrBlockNotFound) {
							return
						}
					}
					return
				}

				// try to request mining candidate
				_, err = framework.Nodes[1].BlockassemblyClient.GetMiningCandidate(ctx)
				require.ErrorContains(t, err, "cannot get mining candidate when FSM is not in RUNNING state")

				// while not in running state, send a bunch of txs to node 1
				for i := 0; i < 10; i++ {
					_, err = helper.CreateAndSendTxs(ctx, framework.Nodes[1], 1)
					require.NoError(t, err)
				}

				// check for blockchain notifications
				blockchainSubscription, err := blockchainNode0.Subscribe(ctx, "state-test")
				require.NoError(t, err)

				// Use select with timeout to check notifications
				select {
				case <-ctx.Done():
					return
				case notification := <-blockchainSubscription:
					if notification.Type == model.NotificationType_Subtree {
						t.Errorf("unexpected notification: %v", notification)
					}
				case <-time.After(10 * time.Second):
					// Timeout without receiving unwanted notifications is the happy path
				}

				time.Sleep(10 * time.Millisecond) // this is increasing of NOT capturing the CATCHINGUP state if block validation is fast
			}
		}
	}
	wait()

	stateFound := false

	for state := range stateSet {
		if state == blockchain_api.FSMStateType_CATCHINGBLOCKS {
			stateFound = true
			break
		}
	}

	headerNode1, _, _ := blockchainNode1.GetBestBlockHeader(ctx)
	headerNode0, _, _ := blockchainNode0.GetBestBlockHeader(ctx)

	assert.Equal(t, headerNode0.Hash(), headerNode1.Hash(), "Best block headers are not equal")
	assert.True(t, stateFound, "State 3 (CATCHINGBLOCKS) was not captured")
}

func TestFsmTestSuite(t *testing.T) {
	suite.Run(t, new(FsmTestSuite))
}
