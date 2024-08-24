//go:build tnjtests

// How to run this test manually:
//
// This test has complex  intermediate setup and tear down steps. It is recommended to run this test using the test script.
// make smoketests test=fork.TestRejectLongerChainWithDoubleSpend
// OR ------ If no need to build the image again, use the following command -------
// make smoketests no-build=1 test=fork.TestRejectLongerChainWithDoubleSpend

package tnj

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/bitcoin-sv/ubsv/services/blockchain/blockchain_api"
	setup "github.com/bitcoin-sv/ubsv/test/setup"
	tf "github.com/bitcoin-sv/ubsv/test/test_framework"
	helper "github.com/bitcoin-sv/ubsv/test/utils"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/ordishs/gocore"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

type TNJDoubleSpendTestSuite struct {
	setup.BitcoinTestSuite
}

func (suite *TNJDoubleSpendTestSuite) TestRejectLongerChainWithDoubleSpend() {
	t := suite.T()
	framework := suite.Framework
	settingsMap := suite.SettingsMap
	blockchainNode0 := framework.Nodes[0].BlockchainClient
	blockchainNode1 := framework.Nodes[1].BlockchainClient

	var (
		states    []*blockchain_api.FSMStateType
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
		hashes, err := helper.CreateAndSendRawTxs(ctx, framework.Nodes[0], 1)
		if err != nil {
			t.Errorf("Failed to create and send raw txs: %v", err)
		}
		fmt.Printf("Hashes: %v\n", hashes)

		baClient := framework.Nodes[0].BlockassemblyClient
		_, err = helper.MineBlock(ctx, baClient, logger)

		if err != nil {

			t.Errorf("Failed to mine block: %v", err)

		}
		time.Sleep(5 * time.Second)
	}

	// Send a double spend transaction
	arrayOfNodes := []tf.BitcoinNode{framework.Nodes[0], framework.Nodes[2]}
	_, err := helper.CreateAndSendDoubleSpendTx(ctx, arrayOfNodes)
	if err != nil {
		t.Errorf("Failed to create and send double spend tx: %v", err)
	}
	baClient := arrayOfNodes[0].BlockassemblyClient
	_, err = helper.MineBlock(ctx, baClient, logger)
	if err != nil {
		t.Errorf("Failed to mine block: %v", err)
	}
	baClient = arrayOfNodes[1].BlockassemblyClient
	_, err = helper.MineBlock(ctx, baClient, logger)
	if err != nil {
		t.Errorf("Failed to mine block: %v", err)
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
				response, err := blockchainNode1.GetFSMCurrentState(framework.Context)
				if err == nil && (lastState == nil || response != lastState) {
					mu.Lock()
					states = append(states, response)
					lastState = response
					mu.Unlock()

				}
				time.Sleep(10 * time.Millisecond)
			}
		}
	}()

	time.Sleep(60 * time.Second)
	close(done)
	wg.Wait()

	stateFound := false
	for _, state := range states {

		if *state == blockchain_api.FSMStateType(3) {
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

// func (suite *TNJTestSuite) TestRejectDoubleSpend() {
// 	t := suite.T()
// 	framework := suite.Framework
// 	settingsMap := suite.SettingsMap
// 	blockchainNode0 := framework.Nodes[0].BlockchainClient
// 	blockchainNode1 := framework.Nodes[1].BlockchainClient

// 	var (
// 		states    []blockchain_api.FSMStateType
// 		lastState *blockchain_api.FSMStateType
// 		mu        sync.Mutex
// 		wg        sync.WaitGroup
// 		done      = make(chan struct{})
// 	)

// 	settingsMap["SETTINGS_CONTEXT_2"] = "docker.ci.ubsv2.tc3"
// 	if err := framework.RestartNodes(settingsMap); err != nil {
// 		t.Errorf("Failed to restart nodes: %v", err)
// 	}
// 	ctx := context.Background()

// 	var logLevelStr, _ = gocore.Config().Get("logLevel", "INFO")
// 	logger := ulogger.New("testRun", ulogger.WithLevel(logLevelStr))

// 	for i := 0; i < 5; i++ {
// 		hashes, err := helper.CreateAndSendRawTxs(ctx, framework.Nodes[0], 1)
// 		if err != nil {
// 			t.Errorf("Failed to create and send raw txs: %v", err)
// 		}
// 		fmt.Printf("Hashes: %v\n", hashes)

// 		baClient := framework.Nodes[0].BlockassemblyClient
// 		_, err = helper.MineBlock(ctx, baClient, logger)
// 		if err != nil {
// 			t.Errorf("Failed to mine block: %v", err)
// 		}
// 	}

// 	// Send a double spend transaction
// 	arrayOfNodes := []tf.BitcoinNode{framework.Nodes[0], framework.Nodes[1]}
// 	_, err := helper.CreateAndSendDoubleSpendTx(ctx, arrayOfNodes)
// 	if err != nil {
// 		t.Errorf("Failed to create and send double spend tx: %v", err)
// 	}
// 	baClient := framework.Nodes[0].BlockassemblyClient
// 	_, err = helper.MineBlock(ctx, baClient, logger)
// 	if err != nil {
// 		t.Errorf("Failed to mine block: %v", err)
// 	}
// 	baClient = framework.Nodes[1].BlockassemblyClient
// 	_, err = helper.MineBlock(ctx, baClient, logger)
// 	if err != nil {
// 		t.Errorf("Failed to mine block: %v", err)
// 	}

// 	settingsMap["SETTINGS_CONTEXT_2"] = "docker.ci.ubsv2.tc1"
// 	if err := framework.RestartNodes(settingsMap); err != nil {
// 		t.Errorf("Failed to restart nodes: %v", err)
// 	}

// 	wg.Add(1)
// 	go func() {
// 		defer wg.Done()
// 		for {
// 			select {
// 			case <-done:
// 				return
// 			default:
// 				response := blockchainNode1.GetFSMCurrentState()
// 				if err == nil && (lastState == nil || response != *lastState) {
// 					mu.Lock()
// 					states = append(states, response)
// 					lastState = &response
// 					mu.Unlock()
// 				}
// 				time.Sleep(10 * time.Millisecond)
// 			}
// 		}
// 	}()

// 	time.Sleep(120 * time.Second)
// 	close(done)
// 	wg.Wait()

// 	stateFound := false
// 	for _, state := range states {
// 		if state == blockchain_api.FSMStateType(3) {
// 			stateFound = true
// 			break
// 		}
// 	}
// 	assert.True(t, stateFound, "State 3 was not captured")
// 	fmt.Printf("Captured states: %v\n", states)

// 	headerNode1, _, _ := blockchainNode1.GetBestBlockHeader(ctx)
// 	headerNode0, _, _ := blockchainNode0.GetBestBlockHeader(ctx)
// 	assert.Equal(t, headerNod//e0.Hash(), headerNode1.Hash(), "Best block headers are not equal")
// }

func TestTNJDoubleSpendTestSuite(t *testing.T) {
	suite.Run(t, new(TNJDoubleSpendTestSuite))
}
