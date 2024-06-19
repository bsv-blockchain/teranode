////go:build e2eTest

// How to run this test:
// $ unzip data.zip
// $ cd test/state/
// $ `SETTINGS_CONTEXT=docker.ci.tc1.run go test -run TestNodeCatchUpState`

package test

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/bitcoin-sv/ubsv/services/blockchain/blockchain_api"
	tf "github.com/bitcoin-sv/ubsv/test/test_framework"
	helper "github.com/bitcoin-sv/ubsv/test/utils"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/ordishs/gocore"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
	framework = tf.NewBitcoinTestFramework([]string{"../../docker-compose.yml", "../../docker-compose.aerospike.override.yml", "../../docker-compose.e2etest.override.yml", "../../docker-compose.p2p.down.yml"})
	settingsMap := map[string]string{
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
}

func TestNodeCatchUpState(t *testing.T) {
	ctx := context.Background()

	var logLevelStr, _ = gocore.Config().Get("logLevel", "INFO")
	logger := ulogger.New("txblast", ulogger.WithLevel(logLevelStr))

	for i := 0; i < 5; i++ {
		hashes, err := helper.CreateAndSendRawTxs(ctx, framework.Nodes[0], 10)
		if err != nil {
			t.Fatalf("Failed to create and send raw txs: %v", err)
		}
		fmt.Printf("Hashes: %v\n", hashes)

		baClient := framework.Nodes[0].BlockassemblyClient
		_, err = helper.MineBlock(ctx, baClient, logger)
		if err != nil {
			t.Fatalf("Failed to mine block: %v", err)
		}
	}

	framework.ComposeFilePaths = []string{"../../docker-compose.yml", "../../docker-compose.aerospike.override.yml", "../../docker-compose.e2etest.override.yml"}
	if err := framework.RestartNodes(settingsMap); err != nil {
		t.Fatalf("Failed to restart nodes: %v", err)
	}

	blockchain := framework.Nodes[1].BlockchainClient
	response, err := blockchain.GetFSMCurrentState(context.Background())
	require.NoError(t, err)
	require.NotNil(t, response)
	assert.Equal(t, blockchain_api.FSMStateType(3), *response, "Expected FSM state did not match")
	header, _, _ := blockchain.GetBestBlockHeader(ctx)
	fmt.Printf("Best block header: %v\n", header.Hash())
}
