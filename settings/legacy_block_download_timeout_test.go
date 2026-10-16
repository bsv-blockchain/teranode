package settings

import (
	"testing"

	"github.com/ordishs/gocore"
	"github.com/stretchr/testify/require"
)

// TestLegacyBlockDownloadTimeout_Defaults guards the loader entries for the
// three percentages that decide how long a single block transfer may stay in
// flight before the peer is disconnected. A struct tag on its own loads nothing,
// so without the matching lines in NewSettings() all three would arrive as zero,
// the total would be zero, and blockDownloadBudget's misconfiguration fallback
// would quietly put every peer on the flat MaxBlockDownloadTime with nothing
// failing anywhere.
func TestLegacyBlockDownloadTimeout_Defaults(t *testing.T) {
	tSettings := NewSettings()

	require.NotNil(t, tSettings)
	require.Equal(t, int64(100), tSettings.Legacy.BlockDownloadTimeoutBasePercent,
		"default must be 100%% of the block interval at the tip, matching svnode")
	require.Equal(t, int64(600), tSettings.Legacy.BlockDownloadTimeoutBaseIBDPercent,
		"default must be 600%% while catching up, matching svnode")
	require.Equal(t, int64(50), tSettings.Legacy.BlockDownloadTimeoutPerPeerPercent,
		"default must be 50%% per other peer downloading, matching svnode")
}

// TestLegacyBlockDownloadTimeout_LoaderReadsOverrides catches the
// field-exists-but-the-loader-never-reads-it mistake: a distinctive configured
// value must come back out of the loaded settings.
func TestLegacyBlockDownloadTimeout_LoaderReadsOverrides(t *testing.T) {
	gocore.Config().Set("legacy_blockDownloadTimeoutBasePercent", "137")
	gocore.Config().Set("legacy_blockDownloadTimeoutBaseIBDPercent", "911")
	gocore.Config().Set("legacy_blockDownloadTimeoutPerPeerPercent", "73")

	t.Cleanup(func() {
		gocore.Config().Set("legacy_blockDownloadTimeoutBasePercent", "")
		gocore.Config().Set("legacy_blockDownloadTimeoutBaseIBDPercent", "")
		gocore.Config().Set("legacy_blockDownloadTimeoutPerPeerPercent", "")
	})

	tSettings := NewSettings()

	require.Equal(t, int64(137), tSettings.Legacy.BlockDownloadTimeoutBasePercent)
	require.Equal(t, int64(911), tSettings.Legacy.BlockDownloadTimeoutBaseIBDPercent)
	require.Equal(t, int64(73), tSettings.Legacy.BlockDownloadTimeoutPerPeerPercent)
}
