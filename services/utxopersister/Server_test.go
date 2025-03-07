// Package utxopersister provides functionality for managing UTXO (Unspent Transaction Output) persistence.
package utxopersister

import (
	"context"
	"testing"

	"github.com/bitcoin-sv/teranode/stores/blob/memory"
	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/bitcoin-sv/teranode/util/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestReadWriteHeight(t *testing.T) {
	ctx := context.Background()

	store := memory.New()

	// Create a new UTXO persister
	tSettings := test.CreateBaseTestSettings()
	s := New(ctx, ulogger.TestLogger{}, tSettings, store, nil)

	oldHeight, err := s.readLastHeight(ctx)
	require.NoError(t, err)
	assert.Equal(t, uint32(0), oldHeight)

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
