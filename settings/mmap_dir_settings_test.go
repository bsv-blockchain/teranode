package settings

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// These three settings back the mmap-backed-subtree / disk-backed TxMap
// block-RAM-reduction feature. Defaults stay empty (feature off) but
// operators must be able to reach the fields via env/config, or the feature
// can never be turned on regardless of what the consumers do with it.

func TestBlockAssemblySubtreeMmapDir_Default(t *testing.T) {
	// Explicitly override to "" so a developer's local settings_local.conf
	// (which may legitimately enable this feature for manual testing) cannot
	// leak into the "default is off" assertion; os.LookupEnv wins over the
	// config file in gocore's precedence.
	t.Setenv("blockassembly_subtreeMmapDir", "")
	tSettings := NewSettings()
	require.Equal(t, "", tSettings.BlockAssembly.SubtreeMmapDir)
}

func TestBlockAssemblySubtreeMmapDir_EnvOverride(t *testing.T) {
	t.Setenv("blockassembly_subtreeMmapDir", "/mnt/nvme0/subtree-mmap")
	tSettings := NewSettings()
	require.Equal(t, "/mnt/nvme0/subtree-mmap", tSettings.BlockAssembly.SubtreeMmapDir)
}

func TestBlockAssemblyTxMapDirs_Default(t *testing.T) {
	t.Setenv("blockassembly_txMapDirs", "")
	tSettings := NewSettings()
	require.Empty(t, tSettings.BlockAssembly.TxMapDirs)
}

func TestBlockAssemblyTxMapDirs_EnvOverride(t *testing.T) {
	t.Setenv("blockassembly_txMapDirs", "/mnt/nvme0/txmap|/mnt/nvme1/txmap")
	tSettings := NewSettings()
	require.Equal(t, []string{"/mnt/nvme0/txmap", "/mnt/nvme1/txmap"}, tSettings.BlockAssembly.TxMapDirs)
}

func TestBlockValidationSubtreeMmapDir_Default(t *testing.T) {
	t.Setenv("blockvalidation_subtreeMmapDir", "")
	tSettings := NewSettings()
	require.Equal(t, "", tSettings.BlockValidation.SubtreeMmapDir)
}

func TestBlockValidationSubtreeMmapDir_EnvOverride(t *testing.T) {
	t.Setenv("blockvalidation_subtreeMmapDir", "/mnt/nvme0/subtree-mmap")
	tSettings := NewSettings()
	require.Equal(t, "/mnt/nvme0/subtree-mmap", tSettings.BlockValidation.SubtreeMmapDir)
}
