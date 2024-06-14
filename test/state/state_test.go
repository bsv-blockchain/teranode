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
	"time"

	ba "github.com/bitcoin-sv/ubsv/services/blockassembly"
	"github.com/bitcoin-sv/ubsv/services/blockchain"
	"github.com/bitcoin-sv/ubsv/services/blockchain/blockchain_api"
	tf "github.com/bitcoin-sv/ubsv/test/test_framework"
	helper "github.com/bitcoin-sv/ubsv/test/utils"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/ordishs/gocore"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	framework *tf.BitcoinTestFramework
)

func TestMain(m *testing.M) {
	setupBitcoinTestFramework()
	defer tearDownBitcoinTestFramework()

	m.Run()

	// os.Exit(exitCode)
}

func setupBitcoinTestFramework() {
	framework = tf.NewBitcoinTestFramework([]string{"../../docker-compose.yml", "../../docker-compose.aerospike.override.yml", "../../docker-compose.e2etest.override.yml"})
	m := map[string]string{
		"SETTINGS_CONTEXT_1": "docker.ci.ubsv1.tc1",
		"SETTINGS_CONTEXT_2": "docker.ci.ubsv2.tc1",
		"SETTINGS_CONTEXT_3": "docker.ci.ubsv3.tc1",
	}
	if err := framework.SetupNodes(m); err != nil {
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

	err := framework.StopNode("ubsv-2")
	if err != nil {
		t.Fatalf("Failed to stop node: %v", err)
	}
	for i := 0; i < 500; i++ {
		// hashes, err := helper.CreateAndSendRawTxs(ctx, 10, logger)
		// if err != nil {
		// 	t.Fatalf("Failed to create and send raw txs: %v", err)
		// }
		// fmt.Printf("Hashes: %v\n", hashes)

		baClient := ba.NewClient(ctx, logger)
		_, err = helper.MineBlock(ctx, *baClient, logger)
		if err != nil {
			t.Fatalf("Failed to mine block: %v", err)
		}
	}

	err = framework.StartNode("ubsv-2")
	time.Sleep(5 * time.Second)
	if err != nil {
		t.Fatalf("Failed to start node: %v", err)
	}
	blockchain, err := blockchain.NewClientWithAddress(ctx, logger, "localhost:28087")
	if err != nil {
		t.Errorf("error creating blockchain client: %v", err)
	}
	response, err := blockchain.GetFSMCurrentState(context.Background())
	require.NoError(t, err)
	require.NotNil(t, response)
	assert.Equal(t, blockchain_api.FSMStateType(3), *response, "Expected FSM state did not match")
	header, _, _ := blockchain.GetBestBlockHeader(ctx)
	fmt.Printf("Best block header: %v\n", header.Hash())
}
