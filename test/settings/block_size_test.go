//go:build settingstest

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
	"testing"
	"time"

	"github.com/bitcoin-sv/ubsv/stores/blob/options"
	"github.com/bitcoin-sv/ubsv/test/setup"
	helper "github.com/bitcoin-sv/ubsv/test/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

type SettingsTestSuite struct {
	setup.BitcoinTestSuite
}

func (suite *SettingsTestSuite) InitSuite() {
	suite.SettingsMap = map[string]string{
		"SETTINGS_CONTEXT_1": "docker.ci.ubsv1.tc1",
		"SETTINGS_CONTEXT_2": "docker.ci.ubsv2.tc2",
		"SETTINGS_CONTEXT_3": "docker.ci.ubsv3.tc1",
	}
}

func (suite *SettingsTestSuite) SetupTest() {
	suite.InitSuite()
	suite.BitcoinTestSuite.SetupTestWithCustomSettings(suite.SettingsMap)
}
func (suite *SettingsTestSuite) TestShouldRejectExcessiveBlockSize() {
	t := suite.T()
	cluster := suite.Framework
	logger := cluster.Logger
	defer func() {
		if r := recover(); r != nil {
			t.Errorf("Recovered from panic: %v", r)
			_ = cluster.Compose.Down(cluster.Context)
		}
	}()
	ctx := context.Background()
	url := "http://localhost:18090"

	hashes, err := helper.CreateAndSendRawTxs(ctx, cluster.Nodes[0], 100)
	if err != nil {
		t.Errorf("Failed to create and send raw txs: %v", err)
	}
	fmt.Printf("Hashes in created block: %v\n", hashes)

	height, _ := helper.GetBlockHeight(url)

	baClient := cluster.Nodes[0].BlockassemblyClient
	_, err = helper.MineBlock(ctx, baClient, logger)
	if err != nil {
		t.Errorf("Failed to mine block: %v", err)
	}

	for {
		newHeight, _ := helper.GetBlockHeight(url)
		if newHeight > height {
			break
		}
		time.Sleep(1 * time.Second)
	}

	time.Sleep(10 * time.Second)

	blockchain := cluster.Nodes[0].BlockchainClient
	header, meta, _ := blockchain.GetBestBlockHeader(ctx)
	fmt.Printf("Best block header: %v\n", header.Hash())
	blockchain1 := cluster.Nodes[1].BlockchainClient
	header1, meta1, _ := blockchain1.GetBestBlockHeader(ctx)
	fmt.Printf("Best block header1: %v\n", header1.Hash())

	assert.NotEqual(t, header.Hash(), header1.Hash(), "Blocks are equal")

	var o []options.Options
	o = append(o, options.WithFileExtension("block"))

	blockStore := cluster.Nodes[1].Blockstore
	r, err := blockStore.GetIoReader(ctx, header1.Hash()[:], o...)
	if err != nil {
		t.Errorf("error getting block reader: %v", err)
	}
	fmt.Printf("Block reader: %v\n", cluster.Nodes[1].BlockstoreUrl)
	if err == nil {
		if bl, err := helper.ReadFile(ctx, "block", logger, r, hashes[90], cluster.Nodes[1].BlockstoreUrl); err != nil {
			t.Errorf("error reading block: %v", err)
		} else {
			fmt.Printf("Block at height (%d): was tested for the test Tx\n", meta1.Height)
			assert.Equal(t, false, bl, "Test Tx not found in block")
		}
	}

	blockStore = cluster.Nodes[0].Blockstore
	r, err = blockStore.GetIoReader(ctx, header.Hash()[:], o...)
	if err != nil {
		t.Errorf("error getting block reader: %v", err)
	}
	fmt.Printf("Block reader: %v\n", cluster.Nodes[0].BlockstoreUrl)
	if err == nil {
		if bl, err := helper.ReadFile(ctx, "block", logger, r, hashes[90], cluster.Nodes[0].BlockstoreUrl); err != nil {
			t.Errorf("error reading block: %v", err)
		} else {
			fmt.Printf("Block at height (%d): was tested for the test Tx\n", meta.Height)
			assert.Equal(t, true, bl, "Test Tx not found in block")
		}
	}

}

func TestSettingsTestSuite(t *testing.T) {
	suite.Run(t, new(SettingsTestSuite))
}
