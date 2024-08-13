package utxopersister

import (
	"context"
	"testing"

	"github.com/bitcoin-sv/ubsv/stores/blob/memory"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestReadWriteHeight(t *testing.T) {
	ctx := context.Background()

	store := memory.New()

	// Create a new UTXO persister
	s := New(ctx, ulogger.TestLogger{}, store, nil)

	oldHeight, err := s.readLastHeight(ctx)
	require.NoError(t, err)
	assert.Equal(t, uint32(1), oldHeight)

	// Write the height
	err = s.writeLastHeight(ctx, 100_000)
	require.NoError(t, err)

	height, err := s.readLastHeight(ctx)
	require.NoError(t, err)
	assert.Equal(t, uint32(100_000), height)

	// Write the old height back
	err = s.writeLastHeight(ctx, oldHeight)
	require.NoError(t, err)
}
