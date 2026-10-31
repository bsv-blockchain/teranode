package blockchain

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/stretchr/testify/require"
)

// TestMockStore_GetLastNBlocksNonPositiveN: a count of zero or less returns no blocks
// instead of panicking on a negative slice capacity.
func TestMockStore_GetLastNBlocksNonPositiveN(t *testing.T) {
	m := NewMockStore()

	for _, n := range []int64{0, -1} {
		infos, err := m.GetLastNBlocks(context.Background(), n, true, 0)
		require.NoError(t, err)
		require.Empty(t, infos)
	}
}

// TestMockStore_State: state reads what was written, and a key never written is not found,
// as in the SQL store.
func TestMockStore_State(t *testing.T) {
	m := NewMockStore()

	_, err := m.GetState(context.Background(), "k")
	require.ErrorIs(t, err, errors.ErrNotFound)

	require.NoError(t, m.SetState(context.Background(), "k", []byte{1, 2}))

	data, err := m.GetState(context.Background(), "k")
	require.NoError(t, err)
	require.Equal(t, []byte{1, 2}, data)
}
