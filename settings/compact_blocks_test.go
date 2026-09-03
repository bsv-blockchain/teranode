package settings

import (
	"testing"

	"github.com/ordishs/gocore"
	"github.com/stretchr/testify/require"
)

// TestCompactBlocks_LoaderReadsKey guards legacy_compactBlocks against the
// field-exists-but-loader-never-reads-it bug: a `key:` tag is documentation
// only, so without a getBool call in NewSettings the field stays at the Go
// zero value.
func TestCompactBlocks_LoaderReadsKey(t *testing.T) {
	const key = "legacy_compactBlocks"

	require.False(t, NewSettings().Legacy.CompactBlocks,
		"%s must default to off", key)

	setForTest(t, key, "true")

	require.True(t, NewSettings().Legacy.CompactBlocks,
		"loader must read %s under context %q", key, gocore.Config().GetContext())
}

// TestCompactBlocksRecentTxs_LoaderReadsKey guards legacy_compactBlocksRecentTxs
// the same way, and checks the <= 0 guard falls back to the default capacity
// rather than passing a non-positive ring size on to the recent-tx index.
func TestCompactBlocksRecentTxs_LoaderReadsKey(t *testing.T) {
	const key = "legacy_compactBlocksRecentTxs"
	const defaultCapacity = 5000000

	require.Equal(t, defaultCapacity, NewSettings().Legacy.CompactBlocksRecentTxs,
		"%s must default to %d", key, defaultCapacity)

	setForTest(t, key, "1000")

	require.Equal(t, 1000, NewSettings().Legacy.CompactBlocksRecentTxs,
		"loader must read %s under context %q", key, gocore.Config().GetContext())
}

func TestCompactBlocksRecentTxs_NonPositiveFallsBackToDefault(t *testing.T) {
	const key = "legacy_compactBlocksRecentTxs"
	const defaultCapacity = 5000000

	setForTest(t, key, "0")

	require.Equal(t, defaultCapacity, NewSettings().Legacy.CompactBlocksRecentTxs,
		"%s=0 must fall back to the default capacity", key)

	setForTest(t, key, "-5")

	require.Equal(t, defaultCapacity, NewSettings().Legacy.CompactBlocksRecentTxs,
		"%s<0 must fall back to the default capacity", key)
}
