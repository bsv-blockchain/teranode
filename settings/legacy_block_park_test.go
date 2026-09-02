package settings

import (
	"testing"
	"time"

	"github.com/ordishs/gocore"
	"github.com/stretchr/testify/require"
)

// TestLegacyBlockPark_Defaults guards the loader entries for the three settings
// that control keeping an out-of-order block instead of throwing it away. A
// struct tag on its own loads nothing: without the matching lines in
// NewSettings() all three fields arrive zero, and zero means "off" for each of
// them, so the park would be permanently unreachable no matter what an operator
// configures. This repo has shipped that mistake before.
func TestLegacyBlockPark_Defaults(t *testing.T) {
	tSettings := NewSettings()

	require.NotNil(t, tSettings)
	require.True(t, tSettings.Legacy.ParkOutOfOrderBlocks,
		"default must be on; false restores the old discard-the-block behaviour")
	require.Equal(t, int64(4*1024*1024*1024), tSettings.Legacy.ParkMaxBytes,
		"default must be 4 GiB of disk; 0 is the second, independent kill switch")
	require.Equal(t, 10*time.Second, tSettings.Legacy.ParkStoreTimeout,
		"default must be 10s, well under the blob store's own 25s permit deadline")
	require.Less(t, tSettings.Legacy.ParkStoreTimeout, 25*time.Second,
		"a caller deadline can only shorten the store's 25s permit wait, so above 25s this setting does nothing")
}

// TestLegacyBlockPark_LoaderReadsOverrides catches the field-exists-but-the-
// loader-never-reads-it mistake: distinctive configured values must come back
// out of the loaded settings.
func TestLegacyBlockPark_LoaderReadsOverrides(t *testing.T) {
	gocore.Config().Set("legacy_parkOutOfOrderBlocks", "false")
	gocore.Config().Set("legacy_parkMaxBytes", "123456789")
	gocore.Config().Set("legacy_parkStoreTimeout", "45s")

	t.Cleanup(func() {
		gocore.Config().Set("legacy_parkOutOfOrderBlocks", "")
		gocore.Config().Set("legacy_parkMaxBytes", "")
		gocore.Config().Set("legacy_parkStoreTimeout", "")
	})

	tSettings := NewSettings()

	require.False(t, tSettings.Legacy.ParkOutOfOrderBlocks)
	require.Equal(t, int64(123456789), tSettings.Legacy.ParkMaxBytes)
	require.Equal(t, 45*time.Second, tSettings.Legacy.ParkStoreTimeout)
}
