package blockvalidation

import (
	"testing"

	"github.com/bsv-blockchain/teranode/services/blockvalidation/testhelpers"
	"github.com/stretchr/testify/require"
)

// TestQuickValidateBlockRejectsOutdatedVersion proves that the quick-validation catchup path
// enforces the BIP34/66/65 version floor. A below-floor block that reaches the checkpoint
// fast path must be rejected with bad-version before any body/coinbase inspection, matching svnode
// ContextualCheckBlockHeader. Height 200_000_000 is at/after BIP34 on every network, so a v1 block
// there is always below the floor regardless of the test network's chaincfg params.
func TestQuickValidateBlockRejectsOutdatedVersion(t *testing.T) {
	t.Run("quickValidateBlock", func(t *testing.T) {
		suite := NewCatchupTestSuite(t)
		defer suite.Cleanup()

		block := testhelpers.CreateTestBlocks(t, 1)[0]
		block.Height = 200_000_000
		block.Header.Version = 1

		err := suite.Server.blockValidation.quickValidateBlock(suite.Ctx, block, "test", "", nil)
		require.Error(t, err)
		require.Contains(t, err.Error(), "bad-version")
	})

	t.Run("quickValidateBlockAsync", func(t *testing.T) {
		suite := NewCatchupTestSuite(t)
		defer suite.Cleanup()

		block := testhelpers.CreateTestBlocks(t, 1)[0]
		block.Height = 200_000_000
		block.Header.Version = 1

		writeJobsChan := make(chan *SubtreeWriteJob, 1)
		err := suite.Server.blockValidation.quickValidateBlockAsync(suite.Ctx, block, "test", "", writeJobsChan)
		require.Error(t, err)
		require.Contains(t, err.Error(), "bad-version")
	})
}
