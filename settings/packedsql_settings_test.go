package settings

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPackedSQLSettingsDefaults(t *testing.T) {
	tSettings := NewSettings()

	require.Equal(t, 64, tSettings.UtxoStore.PackedSQLPartitions)
	require.Equal(t, 64, tSettings.UtxoStore.PackedSQLPageSize)
	require.Equal(t, 8, tSettings.UtxoStore.PackedSQLSpendWorkers)
}
