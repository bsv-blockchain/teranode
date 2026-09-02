package settings

import (
	"testing"

	"github.com/ordishs/gocore"
	"github.com/stretchr/testify/require"
)

// TestLegacyBlockScheduler_Defaults guards the loader entries for the three
// settings that spread block downloads over several peers. A struct tag on its
// own loads nothing: without the matching lines in NewSettings() the master
// switch would arrive false and both budgets zero, which is the shape of a
// feature that is permanently unreachable however an operator configures it.
func TestLegacyBlockScheduler_Defaults(t *testing.T) {
	tSettings := NewSettings()

	require.NotNil(t, tSettings)
	require.True(t, tSettings.Legacy.MultiPeerBlockDownload,
		"the scheduler ships on; false restores the single-sync-peer download path")
	require.Equal(t, 16, tSettings.Legacy.MaxBlocksInTransitPerPeer,
		"default must be svnode's MAX_BLOCKS_IN_TRANSIT_PER_PEER of 16")
	require.Equal(t, 1024, tSettings.Legacy.BlockDownloadWindow,
		"default must be svnode's DEFAULT_BLOCK_DOWNLOAD_WINDOW of 1024")
	require.Zero(t, tSettings.Legacy.BlockDownloadLowerWindow,
		"the lookahead limit ships off, which is what svnode does when pruning is not enabled")
}

// TestLegacyBlockScheduler_LoaderReadsOverrides catches the field-exists-but-the-
// loader-never-reads-it mistake: a distinctive configured value must come back
// out of the loaded settings.
func TestLegacyBlockScheduler_LoaderReadsOverrides(t *testing.T) {
	gocore.Config().Set("legacy_multiPeerBlockDownload", "false")
	gocore.Config().Set("legacy_maxBlocksInTransitPerPeer", "7")
	gocore.Config().Set("legacy_blockDownloadWindow", "33")
	gocore.Config().Set("legacy_blockDownloadLowerWindow", "9")

	t.Cleanup(func() {
		gocore.Config().Set("legacy_multiPeerBlockDownload", "")
		gocore.Config().Set("legacy_maxBlocksInTransitPerPeer", "")
		gocore.Config().Set("legacy_blockDownloadWindow", "")
		gocore.Config().Set("legacy_blockDownloadLowerWindow", "")
	})

	tSettings := NewSettings()

	require.False(t, tSettings.Legacy.MultiPeerBlockDownload)
	require.Equal(t, 7, tSettings.Legacy.MaxBlocksInTransitPerPeer)
	require.Equal(t, 33, tSettings.Legacy.BlockDownloadWindow)
	require.Equal(t, 9, tSettings.Legacy.BlockDownloadLowerWindow)
}
