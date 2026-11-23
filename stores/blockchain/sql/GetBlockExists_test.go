package sql

import (
	"context"
	"fmt"
	"net/url"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSQLGetBlockExists(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)

	t.Run("check non-existent block", func(t *testing.T) {
		storeURL, err := url.Parse("sqlitememory:///")
		require.NoError(t, err)

		s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
		require.NoError(t, err)

		exists, err := s.GetBlockExists(context.Background(), block1.Hash())
		require.NoError(t, err)
		assert.False(t, exists)
	})

	t.Run("check existing block", func(t *testing.T) {
		storeURL, err := url.Parse("sqlitememory:///")
		require.NoError(t, err)

		s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
		require.NoError(t, err)

		// Store block1
		_, _, err = s.StoreBlock(context.Background(), block1, "")
		require.NoError(t, err)

		exists, err := s.GetBlockExists(context.Background(), block1.Hash())
		require.NoError(t, err)
		assert.True(t, exists)
	})
}

// TestSQLGetBlockExists_NeverCachesNotFound: StoreBlock clears the response cache only after
// its commit, so a cached "not found" can still be served for a block that is already stored.
// Block validation reads GetBlockExists to decide whether a child's parent is missing and the
// child goes to catchup, so a stale "not found" there costs a whole catchup. Only "found" is
// cached.
func TestSQLGetBlockExists_NeverCachesNotFound(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)

	storeURL, err := url.Parse("sqlitememory:///")
	require.NoError(t, err)

	s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
	require.NoError(t, err)

	cacheID := chainhash.HashH([]byte(fmt.Sprintf("GetBlockExists-%s", block1.Hash().String())))

	exists, err := s.GetBlockExists(context.Background(), block1.Hash())
	require.NoError(t, err)
	require.False(t, exists)
	require.Nil(t, s.responseCache.NewOp(cacheID).Get(), "a not-found result must not be cached")

	_, _, err = s.StoreBlock(context.Background(), block1, "")
	require.NoError(t, err)

	exists, err = s.GetBlockExists(context.Background(), block1.Hash())
	require.NoError(t, err)
	require.True(t, exists)
	require.NotNil(t, s.responseCache.NewOp(cacheID).Get(), "a found result is still cached")
}
