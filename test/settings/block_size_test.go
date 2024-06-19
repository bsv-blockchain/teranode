////go:build e2eTest

// How to run this test:
// $ unzip data.zip
// $ cd test/settings/
// $ `SETTINGS_CONTEXT=docker.ci.tc1.run go test -run TestShouldAllowMaxBlockSize`

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
	"github.com/libsv/go-bt/v2/chainhash"
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

func TestShouldAllowMaxBlockSize(t *testing.T) {
	ctx := context.Background()
	url := "http://localhost:18090"

	hashes, err := helper.CreateAndSendRawTxs(ctx, cluster.Nodes[0], 10)
	if err != nil {
		t.Fatalf("Failed to create and send raw txs: %v", err)
	}
	fmt.Printf("Hashes: %v\n", hashes)

	height, _ := helper.GetBlockHeight(url)

	baClient := cluster.Nodes[0].BlockassemblyClient
	blockHash, err := helper.MineBlock(ctx, baClient, logger)
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

	blockchainDB := cluster.Nodes[0].BlockChainDB
	b, _, _ := blockchainDB.GetBlock(ctx, (*chainhash.Hash)(blockHash))
	fmt.Printf("Block: %v\n", b)

	blockStore := cluster.Nodes[0].Blockstore
	var o []options.Options
	o = append(o, options.WithFileExtension("block"))
	//wait
	time.Sleep(10 * time.Second)

	blockchain := cluster.Nodes[0].BlockchainClient
	header, meta, _ := blockchain.GetBestBlockHeader(ctx)
	fmt.Printf("Best block header: %v\n", header.Hash())

	r, err := blockStore.GetIoReader(ctx, header.Hash()[:], o...)
	// t.Errorf("error getting block reader: %v", err)
	if err != nil {
		t.Errorf("error getting block reader: %v", err)
	}
	if err == nil {
		if bl, err := helper.ReadFile(ctx, "block", logger, r, hashes[5], ""); err != nil {
			t.Errorf("error reading block: %v", err)
		} else {
			fmt.Printf("Block at height (%d): was tested for the test Tx\n", meta.Height)
			assert.Equal(t, true, bl, "Test Tx not found in block")
		}
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
	blockHash, err := helper.MineBlock(ctx, baClient, logger)
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

	blockchainDB := cluster.Nodes[1].BlockChainDB
	b, _, _ := blockchainDB.GetBlock(ctx, (*chainhash.Hash)(blockHash))
	fmt.Printf("Block: %v\n", b)

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
