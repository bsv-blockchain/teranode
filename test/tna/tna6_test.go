//go:build test_all || test_tna

package tna

import (
	"testing"

	helper "github.com/bitcoin-sv/ubsv/test/utils"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/stretchr/testify/suite"
)

type TNA6TestSuite struct {
	helper.TeranodeTestSuite
}

func (suite *TNA6TestSuite) InitSuite() {
	suite.SettingsMap = map[string]string{
		"SETTINGS_CONTEXT_1": "docker.ubsv1.test.tna1Test",
		"SETTINGS_CONTEXT_2": "docker.ubsv2.test.tna1Test",
		"SETTINGS_CONTEXT_3": "docker.ubsv3.test.tna1Test",
	}
}

func (suite *TNA6TestSuite) SetupTest() {
	suite.InitSuite()
	suite.SetupTestEnv(suite.SettingsMap, suite.DefaultComposeFiles(), false)
}

func (suite *TNA6TestSuite) TestAcceptanceNextBlock() {
	testEnv := suite.TeranodeTestEnv
	ctx := testEnv.Context
	t := suite.T()
	ba := testEnv.Nodes[0].BlockassemblyClient
	bc := testEnv.Nodes[0].BlockchainClient
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
