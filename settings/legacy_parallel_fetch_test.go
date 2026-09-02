package settings

import (
	"testing"
	"time"

	"github.com/ordishs/gocore"
	"github.com/stretchr/testify/require"
)

// TestLegacyParallelFetch_Defaults guards the loader entries for the two
// settings that control asking a second peer for a stalled block. A struct tag
// on its own loads nothing, so without the matching lines in NewSettings() both
// fields would arrive as zero — and zero means "off" for each of them, leaving
// the feature permanently unreachable no matter what an operator configures.
func TestLegacyParallelFetch_Defaults(t *testing.T) {
	tSettings := NewSettings()

	require.NotNil(t, tSettings)
	require.Equal(t, 2, tSettings.Legacy.MaxBlockParallelFetch,
		"default must be 2 (the peer that owes us the block plus one more); 0 or 1 disables racing")
	require.Equal(t, 20*time.Second, tSettings.Legacy.BlockSlowFetchTimeout,
		"default must be 20s; a zero duration disables racing")
	require.Less(t, tSettings.Legacy.BlockSlowFetchTimeout, 180*time.Second,
		"must stay well below the 180s sync-peer rotation window, or the peer is dropped before a race could ever start")
}

// TestLegacyParallelFetch_LoaderReadsOverrides catches the field-exists-but-the-
// loader-never-reads-it mistake: a distinctive configured value must come back
// out of the loaded settings.
func TestLegacyParallelFetch_LoaderReadsOverrides(t *testing.T) {
	gocore.Config().Set("legacy_maxBlockParallelFetch", "5")
	gocore.Config().Set("legacy_blockSlowFetchTimeout", "45s")

	t.Cleanup(func() {
		gocore.Config().Set("legacy_maxBlockParallelFetch", "")
		gocore.Config().Set("legacy_blockSlowFetchTimeout", "")
	})

	tSettings := NewSettings()

	require.Equal(t, 5, tSettings.Legacy.MaxBlockParallelFetch)
	require.Equal(t, 45*time.Second, tSettings.Legacy.BlockSlowFetchTimeout)
}
