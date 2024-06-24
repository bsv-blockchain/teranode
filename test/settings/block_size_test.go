////go:build e2eTest

// How to run this test manually:
//
// 1. For resetting data:
//    - Delete existing data: rm -rf data/
//    - Restore data from zip: unzip data.zip
//
// 2. Add the following settings in settings_local.conf (DO NOT COMMIT THIS CHANGE TO GIT):
//    - excessiveblocksize.docker.ci.ubsv2=1000
//
// 3. Start another terminal and run the following script:
//    - ./scripts/bestblock-docker.sh
//
// 4. Bring up Docker containers:
//    - docker compose -f docker-compose.yml -f docker-compose.aerospike.override.yml up -d
//    - wait for initial 300 blocks to be mined
//
// 5. Navigate to the test settings directory:
//    - cd test/settings/
//
// 6. Execute the test in dev mode:
//    - test_run_mode=dev go test -run TestShouldRejectExcessiveBlockSize
//
// 7. Expected result:
//    - The ubsv-2 node should reject the blocks and be out of sync.
//    - ubsv-1 and ubsv-3 should remain in sync.
// 8. To clean up:
//    - docker compose -f docker-compose.yml -f docker-compose.aerospike.override.yml down

package test

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/bitcoin-sv/ubsv/stores/blob/options"
	tf "github.com/bitcoin-sv/ubsv/test/test_framework"
	helper "github.com/bitcoin-sv/ubsv/test/utils"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/ordishs/gocore"
	"github.com/stretchr/testify/assert"
)

var (
	cluster *tf.BitcoinTestFramework
)
var logLevelStr, _ = gocore.Config().Get("logLevel", "INFO")
var logger = ulogger.New("txblast", ulogger.WithLevel(logLevelStr))

func TestMain(m *testing.M) {
	setupBitcoinTestFramework()
	defer tearDownBitcoinTestFramework()

	m.Run()

	// os.Exit(exitCode)
}

func setupBitcoinTestFramework() {
	cluster = tf.NewBitcoinTestFramework([]string{"../../docker-compose.yml", "../../docker-compose.aerospike.override.yml", "../../docker-compose.e2etest.override.yml"})
	m := map[string]string{
		"SETTINGS_CONTEXT_1": "docker.ci.ubsv1.tc1",
		"SETTINGS_CONTEXT_2": "docker.ci.ubsv2.tc2",
		"SETTINGS_CONTEXT_3": "docker.ci.ubsv3.tc1",
	}
	if err := cluster.SetupNodes(m); err != nil {
		fmt.Printf("Error setting up nodes: %v\n", err)
		os.Exit(1)
	}
}

func tearDownBitcoinTestFramework() {
	if err := cluster.StopNodes(); err != nil {
		fmt.Printf("Error stopping nodes: %v\n", err)
	}
}

func TestShouldRejectExcessiveBlockSize(t *testing.T) {
	ctx := context.Background()
	url := "http://localhost:18090"

	hashes, err := helper.CreateAndSendRawTxs(ctx, cluster.Nodes[0], 100)
	if err != nil {
		t.Fatalf("Failed to create and send raw txs: %v", err)
	}
	fmt.Printf("Hashes: %v\n", hashes)

	height, _ := helper.GetBlockHeight(url)

	baClient := cluster.Nodes[0].BlockassemblyClient
	_, err = helper.MineBlock(ctx, baClient, logger)
	if err != nil {
		t.Fatalf("Failed to mine block: %v", err)
	}

	for {
		newHeight, _ := helper.GetBlockHeight(url)
		if newHeight > height {
			break
		}
		time.Sleep(1 * time.Second)
	}

	blockStore := cluster.Nodes[1].Blockstore
	var o []options.Options
	o = append(o, options.WithFileExtension("block"))
	//wait
	time.Sleep(10 * time.Second)

	blockchain := cluster.Nodes[1].BlockchainClient
	header, meta, _ := blockchain.GetBestBlockHeader(ctx)
	fmt.Printf("Best block header: %v\n", header.Hash())

	r, err := blockStore.GetIoReader(ctx, header.Hash()[:], o...)
	// t.Errorf("error getting block reader: %v", err)
	if err != nil {
		t.Errorf("error getting block reader: %v", err)
	}
	if err == nil {
		if bl, err := helper.ReadFile(ctx, "block", logger, r, hashes[90], ""); err != nil {
			t.Errorf("error reading block: %v", err)
		} else {
			fmt.Printf("Block at height (%d): was tested for the test Tx\n", meta.Height)
			assert.Equal(t, false, bl, "Test Tx not found in block")
		}
	}

}
