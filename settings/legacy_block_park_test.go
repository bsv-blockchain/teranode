package settings

import (
	"testing"
	"time"

	"github.com/ordishs/gocore"
	"github.com/stretchr/testify/require"
)

// TestLegacyBlockPark_Defaults guards the loader entries for the two settings
// that control keeping an out-of-order block instead of throwing it away. A
// struct tag on its own loads nothing: without the matching lines in
// NewSettings() both fields arrive zero, and zero means "off" for each of them,
// so the park would be permanently unreachable no matter what an operator
// configures. This repo has shipped that mistake before.
//
// Two, and deliberately not a third bounding the park in bytes. A byte ceiling
// could only be checked after the block had been downloaded and decoded, so it
// would never save any bandwidth, and a park filled above a hole would refuse
// the one block that would have drained it. The park is bounded by how many
// blocks it holds, and the disk by the download walk's read-ahead depth, which
// is in blocks.
func TestLegacyBlockPark_Defaults(t *testing.T) {
	tSettings := NewSettings()

	require.NotNil(t, tSettings)
	require.Equal(t, 10*time.Second, tSettings.Legacy.ParkStoreTimeout,
		"default must be 10s, well under the blob store's own 25s permit deadline")
	require.Less(t, tSettings.Legacy.ParkStoreTimeout, 25*time.Second,
		"a caller deadline can only shorten the store's 25s permit wait, so above 25s this setting does nothing")
}

// TestLegacyBlockPark_LoaderReadsOverrides catches the field-exists-but-the-
// loader-never-reads-it mistake: distinctive configured values must come back
// out of the loaded settings.
func TestLegacyBlockPark_LoaderReadsOverrides(t *testing.T) {
	gocore.Config().Set("legacy_parkStoreTimeout", "45s")

	t.Cleanup(func() {
		gocore.Config().Set("legacy_parkStoreTimeout", "")
	})

	tSettings := NewSettings()

	require.Equal(t, 45*time.Second, tSettings.Legacy.ParkStoreTimeout)
}
