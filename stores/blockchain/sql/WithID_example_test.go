package sql

import (
	"context"
	"net/url"
	"testing"

	"github.com/bitcoin-sv/teranode/stores/blockchain/options"
	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/bitcoin-sv/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// TestExampleUsage demonstrates how to use the WithID option with StoreBlock

func TestExampleUsage(t *testing.T) {
	// This test verifies the example works
	tSettings := test.CreateBaseTestSettings(t)
	storeURL, err := url.Parse("sqlitememory:///")
	require.NoError(t, err)

	s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
	require.NoError(t, err)
	defer s.Close()

	ctx := context.Background()

	// Example 1: Store with custom ID
	customID := uint64(1000)
	blockID, _, err := s.StoreBlock(ctx, block1, "", options.WithID(customID))
	require.NoError(t, err)
	require.Equal(t, customID, blockID)

	// Example 2: Store with auto-increment (no WithID option)
	blockID2, _, err := s.StoreBlock(ctx, block2, "")
	require.NoError(t, err)
	require.Greater(t, blockID2, uint64(0))

	// Example 3: Store with WithID(0) - should use auto-increment
	blockID3, _, err := s.StoreBlock(ctx, block3, "", options.WithID(0))
	require.NoError(t, err)
	require.Greater(t, blockID3, uint64(0))

	// Example 4: Combine WithID with other options
	customID4 := uint64(5000)
	_, _, err = s.StoreBlock(ctx, block1, "", // Note: using block1 again but with different settings
		options.WithID(customID4),
		options.WithMinedSet(true),
		options.WithInvalid(true),
	)
	// This should fail because block1 hash already exists
	require.Error(t, err)

	t.Log("All examples completed successfully")
}
