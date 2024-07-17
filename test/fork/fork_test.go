////go:build e2eTest

// How to run this test manually:
//
// This test has complex  intermediate setup and tear down steps. It is recommended to run this test using the test script.
// make smoketests test=fork.TestRejectLongerChainWithDoubleSpend_01
// OR ------ If no need to build the image again, use the following command -------
// make smoketests no-build=1 test=fork.TestRejectLongerChainWithDoubleSpend_01

package test

import (
	"context"
	"fmt"
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

func TestMain(m *testing.M) {
	setupBitcoinTestFramework()
	defer tearDownBitcoinTestFramework()

	m.Run()

	// os.Exit(exitCode)
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
	if err := framework.StopNodes(); err != nil {
		fmt.Printf("Error stopping nodes: %v\n", err)
	}
	_ = os.RemoveAll("../../data")
}

func TestRejectLongerChainWithDoubleSpend(t *testing.T) {
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
				response, err := blockchainNode1.GetFSMCurrentState(context.Background())
				if err == nil && (lastState == nil || *response != *lastState) {
					mu.Lock()
					states = append(states, *response)
					lastState = response
					mu.Unlock()
				}
				time.Sleep(10 * time.Millisecond)
			}
		}
	}()

	time.Sleep(180 * time.Second)
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

func TestRejectChainWithDoubleSpend(t *testing.T) {
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

	// Send a double spend transaction
	arrayOfNodes := []tf.BitcoinNode{framework.Nodes[0], framework.Nodes[1]}
	_, err := helper.CreateAndSendDoubleSpendTx(ctx, arrayOfNodes)
	if err != nil {
		t.Errorf("Failed to create and send double spend tx: %v", err)
	}
	baClient := framework.Nodes[0].BlockassemblyClient
	_, err = helper.MineBlock(ctx, baClient, logger)
	if err != nil {
		t.Errorf("Failed to mine block: %v", err)
	}
	baClient = framework.Nodes[1].BlockassemblyClient
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
				response, err := blockchainNode1.GetFSMCurrentState(context.Background())
				if err == nil && (lastState == nil || *response != *lastState) {
					mu.Lock()
					states = append(states, *response)
					lastState = response
					mu.Unlock()
				}
				time.Sleep(10 * time.Millisecond)
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
