package block_assembly

import (
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/daemon"
	"github.com/bsv-blockchain/teranode/services/blockassembly/blockassembly_api"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/test"
	"github.com/stretchr/testify/require"
)

// TestGenerateAfterInvalidateBlock covers issue 764: generate/generatetoaddress
// failed when it arrived before block assembly had processed the reorg that
// invalidateblock had already applied to the blockchain store.
//
// The generate call is made as soon as the blockchain reports the lower tip,
// which is exactly what an operator sees from getinfo, and must succeed on the
// first attempt rather than after an unpredictable retry window.
func TestGenerateAfterInvalidateBlock(t *testing.T) {
	td := daemon.NewTestDaemon(t, daemon.TestOptions{
		SettingsOverrideFunc: test.ComposeSettings(
			test.SystemTestSettings(),
			func(s *settings.Settings) {
				s.ChainCfgParams.CoinbaseMaturity = 2
			},
		),
	})
	defer td.Stop(t)

	require.NoError(t, td.BlockchainClient.Run(td.Ctx, "test"))

	require.NoError(t, td.BlockAssemblyClient.GenerateBlocks(td.Ctx,
		&blockassembly_api.GenerateBlocksRequest{Count: 3}))

	block3, err := td.BlockchainClient.GetBlockByHeight(td.Ctx, 3)
	require.NoError(t, err)

	td.WaitForBlockHeight(t, block3, blockWait, true)

	_, err = td.BlockchainClient.InvalidateBlock(td.Ctx, block3.Header.Hash())
	require.NoError(t, err)

	// Wait only for the blockchain store to report the new tip, mirroring an
	// operator polling getinfo. Block assembly may still be behind at this point,
	// which is the race being guarded.
	require.Eventually(t, func() bool {
		_, meta, err := td.BlockchainClient.GetBestBlockHeader(td.Ctx)
		return err == nil && meta.Height == 2
	}, 15*time.Second, 50*time.Millisecond, "blockchain tip should drop to height 2")

	require.NoError(t, td.BlockAssemblyClient.GenerateBlocks(td.Ctx,
		&blockassembly_api.GenerateBlocksRequest{Count: 1}),
		"generate must succeed immediately after invalidateblock")

	newTip, meta, err := td.BlockchainClient.GetBestBlockHeader(td.Ctx)
	require.NoError(t, err)
	require.Equal(t, uint32(3), meta.Height)

	// Assert the property the fix is about, not a symptom of it. Height alone
	// catches the regression only incidentally: a candidate built on the
	// invalidated parent would land at height 4, or fail earlier and trip the
	// require.NoError above. Pinning the parent says what "generate must build
	// on the post-invalidate tip" actually means, and survives a change in how
	// a bad candidate fails.
	require.True(t, newTip.HashPrevBlock.IsEqual(block3.Header.HashPrevBlock),
		"the generated block should extend the parent of the invalidated block")
}
