//go:build tnftests

// How to run this test:
// $ cd test/tnf/
// $ SETTINGS_CONTEXT=docker.ci.tc1.run go test -v -run "^TestTNFTestSuite$/TestInvalidateBlock$" --tags tnftests

package tnf

import (
	"fmt"
	"testing"
	"time"

	"github.com/bitcoin-sv/ubsv/test/setup"
	helper "github.com/bitcoin-sv/ubsv/test/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

type TNFTestSuite struct {
	setup.BitcoinTestSuite
}

func (suite *TNFTestSuite) InitSuite() {
	suite.SettingsMap = map[string]string{
		"SETTINGS_CONTEXT_1": "docker.ci.ubsv1.tnf6",
		"SETTINGS_CONTEXT_2": "docker.ci.ubsv2.tnf6",
		"SETTINGS_CONTEXT_3": "docker.ci.ubsv3.tnf6",
	}
}

const (
	NodeURL1 = "http://localhost:18090"
	NodeURL2 = "http://localhost:28090"
	NodeURL3 = "http://localhost:38090"
)

func (suite *TNFTestSuite) SetupTest() {
	suite.InitSuite()
	suite.BitcoinTestSuite.SetupTestWithCustomSettingsDoNotReset(suite.SettingsMap)
}

func (suite *TNFTestSuite) TearDownTest() {
	// suite.BitcoinTestSuite.TearDownTest()
}

func (suite *TNFTestSuite) TestInvalidateBlock() {
	t := suite.T()
	cluster := suite.Framework
	logger := cluster.Logger

	defer func() {
		if r := recover(); r != nil {
			t.Errorf("Recovered from panic: %v", r)

			_ = cluster.Compose.Down(cluster.Context)
		}
	}()

	ctx := cluster.Context

	time.Sleep(10 * time.Second)

	blockchain := cluster.Nodes[0].BlockchainClient
	header, _, _ := blockchain.GetBestBlockHeader(ctx)
	fmt.Printf("Best block header: %v\n", header.Hash())
	err := blockchain.InvalidateBlock(ctx, header.Hash())

	if err != nil {
		t.Errorf("Failed to invalidate block: %v", err)
	}

	header1, _, _ := cluster.Nodes[2].BlockchainClient.GetBestBlockHeader(ctx)
	blockchain1 := cluster.Nodes[2].BlockchainClient
	err = blockchain1.InvalidateBlock(ctx, header1.Hash())

	if err != nil {
		t.Errorf("Failed to invalidate block: %v", err)
	}

	time.Sleep(20 * time.Second)

	_, errMine0 := helper.MineBlock(ctx, cluster.Nodes[0].BlockassemblyClient, logger)
	if errMine0 != nil {
		t.Errorf("Failed to mine block: %v", errMine0)
	}

	header1, _, _ = blockchain1.GetBestBlockHeader(ctx)
	fmt.Printf("Best block header1: %v\n", header1.Hash())

	header, _, _ = blockchain.GetBestBlockHeader(ctx)
	fmt.Printf("Best block header: %v\n", header.Hash())

	assert.NotEqual(t, header.Hash(), header1.Hash(), "Blocks are equal")
}

func TestTNFTestSuite(t *testing.T) {
	suite.Run(t, new(TNFTestSuite))
}
