//go:build tnatests

package tna

import (
	"context"
	"testing"

	"github.com/bitcoin-sv/ubsv/test/setup"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/stretchr/testify/suite"
)

type TNA6TestSuite struct {
	setup.BitcoinTestSuite
}

func (suite *TNA6TestSuite) InitSuite() {
	suite.SettingsMap = map[string]string{
		"SETTINGS_CONTEXT_1": "docker.ci.ubsv1.tna1Test",
		"SETTINGS_CONTEXT_2": "docker.ci.ubsv2.tna1Test",
		"SETTINGS_CONTEXT_3": "docker.ci.ubsv3.tna1Test",
	}
}

func (suite *TNA6TestSuite) SetupTest() {
	suite.InitSuite()
	suite.BitcoinTestSuite.SetupTestWithCustomSettings(suite.SettingsMap)
}
func (suite *TNA6TestSuite) TestAcceptanceNextBlock() {
	ctx := context.Background()
	t := suite.T()
	framework := suite.Framework
	ba := framework.Nodes[0].BlockassemblyClient
	bc := framework.Nodes[0].BlockchainClient
	miningCandidate, err := ba.GetMiningCandidate(ctx)

	if err != nil {
		t.Errorf("error getting mining candidate: %v", err)
	}

	bestBlockheader, _, errBbh := bc.GetBestBlockHeader(ctx)
	if errBbh != nil {
		t.Errorf("error getting best block header: %v", errBbh)
	}

	prevHash, errHash := chainhash.NewHash(miningCandidate.PreviousHash)

	if errHash != nil {
		t.Errorf("error getting previous hash: %v", errHash)
	}

	if bestBlockheader.Hash().String() != prevHash.String() {
		t.Errorf("Error comparing hashes")
	}
}

func TestTNA6TestSuite(t *testing.T) {
	suite.Run(t, new(TNA6TestSuite))
}
