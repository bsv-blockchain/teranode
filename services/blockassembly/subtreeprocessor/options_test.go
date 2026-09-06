package subtreeprocessor

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestWithMmapDir confirms the functional option actually sets mmapDir on the
// processor, so a configured blockassembly_subtreeMmapDir setting that reaches
// this option is not silently dropped.
func TestWithMmapDir(t *testing.T) {
	stp := &SubtreeProcessor{}

	WithMmapDir("/some/dir")(stp)

	require.Equal(t, "/some/dir", stp.mmapDir)
}

// TestWithTxMapDirs confirms the functional option actually sets txMapDirs on
// the processor, so a configured blockassembly_txMapDirs setting that reaches
// this option is not silently dropped.
func TestWithTxMapDirs(t *testing.T) {
	stp := &SubtreeProcessor{}

	dirs := []string{"/some/dir1", "/some/dir2"}
	WithTxMapDirs(dirs)(stp)

	require.Equal(t, dirs, stp.txMapDirs)
}
