//go:build system

// How to run each test:
// Clean up docker containers before running the test manually
// $ cd test/system/
// $ go test -v -run "^TestBlockassemblyTestSuite$/TestBlockchainSubsriptionWhenUnavilable$" -tags system

package test

import (
	"testing"
	"time"

	arrange "github.com/bitcoin-sv/ubsv/test/fixtures"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

type BlockassemblyTestSuite struct {
	arrange.TeranodeTestSuite
}

// func (suite *SanityTestSuite) TearDownTest() {
// }

func (suite *BlockassemblyTestSuite) TestBlockchainSubsriptionWhenUnavilable() {
	t := suite.T()
	testEnv := suite.TeranodeTestEnv
	ctx := testEnv.Context

	//subscribe to blockchain service
	blockchainSubscription, err := testEnv.Nodes[0].BlockchainClient.Subscribe(ctx, "system-test")

	if err != nil {
		t.Errorf("error subscribing to blockchain service: %v", err)
		return
	}


	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case notification := <-blockchainSubscription:
				t.Logf(notification.Type.String())
				assert.NotEmpty(t, notification.Type, "expected notification")
			}
		}
	}()

	suite.SettingsMap["SETTINGS_CONTEXT_1"] = "docker.ubsv1.test.stopBlockchain"
	_ = testEnv.RestartDockerNodes(suite.SettingsMap)

	time.Sleep(120 * time.Second)

}

func TestBlockassemblyTestSuite(t *testing.T) {
	suite.Run(t, new(BlockassemblyTestSuite))
}
