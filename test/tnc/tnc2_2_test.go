//go:build test_all || test_tnc

// TNC-2.2 Test Suite
// Requirement: Teranode must temporarily store all candidate blocks for which a solution
// is being sought by the mining software.
//
// How to run these tests:
//
// 1. Run all TNC-2.2 tests:
//    $ cd test/tnc
//    $ go test -v -run "^TestTNC2_2TestSuite$" -tags test_tnc
//
// 2. Run specific test cases:
//    $ cd test/tnc
//    $ go test -v -run "^TestTNC2_2TestSuite$/TestCandidateBlockStorage$" -tags test_tnc
//    $ go test -v -run "^TestTNC2_2TestSuite$/TestCandidateBlockRetention$" -tags test_tnc
//    $ go test -v -run "^TestTNC2_2TestSuite$/TestConcurrentCandidateStorage$" -tags test_tnc
//
// Prerequisites:
// - Go 1.19 or later
// - Docker and Docker Compose
// - Access to required Docker images
//
// For more details, see README.md in the same directory.

package tnc

import (
	"testing"

	helper "github.com/bitcoin-sv/teranode/test/utils"
	"github.com/bitcoin-sv/teranode/test/utils/tconfig"
	"github.com/stretchr/testify/suite"
)

// TNC2_2TestSuite is a test suite for TNC-2.2 requirement:
// Teranode must temporarily store all candidate blocks for which a solution
// is being sought by the mining software.
type TNC2_2TestSuite struct {
	helper.TeranodeTestSuite
}

func (suite *TNC2_2TestSuite) InitSuite() {
	suite.TConfig = tconfig.LoadTConfig(
		map[string]any{
			tconfig.KeyTeranodeContexts: []string{
				"docker.teranode1.test.tnc2_2Test",
				"docker.teranode2.test.tnc2_2Test",
				"docker.teranode3.test.tnc2_2Test",
			},
		},
	)
}

// func (suite *TNC2_2TestSuite) SetupTest() {
// 	suite.InitSuite()
// 	suite.SetupTestEnv(false)
// }

// func (suite *TNC2_2TestSuite) TearDownTest() {
// }

// TestCandidateBlockStorage verifies that Teranode properly stores candidate blocks
// and allows retrieving them by their unique identifiers
func (suite *TNC2_2TestSuite) TestCandidateBlockStorage() {
	// TODO
}

// TestCandidateBlockRetention verifies that Teranode retains candidate blocks
// until they are no longer needed
func (suite *TNC2_2TestSuite) TestCandidateBlockRetention() {
	// TODO
}

// TestConcurrentCandidateStorage verifies that Teranode properly handles
// multiple candidate blocks being stored and retrieved concurrently
func (suite *TNC2_2TestSuite) TestConcurrentCandidateStorage() {
	// TODO
}

func TestTNC2_2TestSuite(t *testing.T) {
	suite.Run(t, new(TNC2_2TestSuite))
}
