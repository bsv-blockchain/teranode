//go:build e2eTest

// How to run this test:
// $ unzip data.zip
// $ cd test/functional/
// $ `SETTINGS_CONTEXT=docker.ci.tc1.run go test -run TestShouldAllowFairTx`

package test

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	ba "github.com/bitcoin-sv/ubsv/services/blockassembly"
	"github.com/bitcoin-sv/ubsv/services/blockchain"
	"github.com/bitcoin-sv/ubsv/stores/blob/options"
	blockchain_store "github.com/bitcoin-sv/ubsv/stores/blockchain"
	tf "github.com/bitcoin-sv/ubsv/test/test_framework"
	helper "github.com/bitcoin-sv/ubsv/test/utils"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/ordishs/gocore"
	"github.com/stretchr/testify/assert"
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

func TestShouldAllowMaxBlockSize(t *testing.T) {
	ctx := context.Background()
	url := "http://localhost:18090"

	var logLevelStr, _ = gocore.Config().Get("logLevel", "INFO")
	logger := ulogger.New("txblast", ulogger.WithLevel(logLevelStr))

	hashes, err := helper.CreateAndSendRawTxs(ctx, 10, logger)
	if err != nil {
		t.Fatalf("Failed to create and send raw txs: %v", err)
	}
	fmt.Printf("Hashes: %v\n", hashes)

	height, _ := helper.GetBlockHeight(url)

	baClient := ba.NewClient(ctx, logger)
	blockHash, err := helper.MineBlock(ctx, *baClient, logger)
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

	blockchainStoreURL, _, found := gocore.Config().GetURL("blockchain_store.docker.ci.chainintegrity.ubsv1")
	blockchainDB, err := blockchain_store.NewStore(logger, blockchainStoreURL)
	if err != nil {
		panic(err.Error())
	}
	if !found {
		panic("no blockchain_store setting found")
	}
	b, _, _ := blockchainDB.GetBlock(ctx, (*chainhash.Hash)(blockHash))
	fmt.Printf("Block: %v\n", b)

	blockStore := helper.GetBlockStore(logger)
	var o []options.Options
	o = append(o, options.WithFileExtension("block"))
	//wait
	time.Sleep(10 * time.Second)
	blockchain, err := blockchain.NewClient(ctx, logger)
	if err != nil {
		t.Errorf("error creating blockchain client: %v", err)
	}
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
