package state

import (
	"testing"

	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

// TestGetLastBlockHeightEmptyFile validates behavior when reading from an empty state file
func TestGetLastBlockHeightEmptyFile(t *testing.T) {
	store := New(ulogger.NewVerboseTestLogger(t), t.TempDir()+"/blocks.dat")

	height, err := store.GetLastPersistedBlockHeight()
	require.NoError(t, err)
	require.Zero(t, height)
}

// TestGetLastBlockHeight validates the block height tracking functionality
func TestGetLastBlockHeight(t *testing.T) {
	store := New(ulogger.NewVerboseTestLogger(t), t.TempDir()+"/blocks.dat")

	err := store.AddBlock(1, "0000000000000000000000000000000000000000000000000000000000000001")
	require.NoError(t, err)

	height, err := store.GetLastPersistedBlockHeight()
	require.NoError(t, err)
	require.Equal(t, uint32(1), height)

	err = store.AddBlock(2, "0000000000000000000000000000000000000000000000000000000000000002")
	require.NoError(t, err)

	err = store.AddBlock(3, "0000000000000000000000000000000000000000000000000000000000000003")
	require.NoError(t, err)

	height, err = store.GetLastPersistedBlockHeight()
	require.NoError(t, err)
	require.Equal(t, uint32(3), height)
}
