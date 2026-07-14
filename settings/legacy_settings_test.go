package settings

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLegacyDiskSyncDefaults(t *testing.T) {
	s := NewSettings()
	require.Equal(t, "", s.Legacy.DiskSyncDir)
	require.Equal(t, uint32(0), s.Legacy.DiskSyncStopAtHeight)
	require.False(t, s.Legacy.DiskSyncForceFullValidation)
}
