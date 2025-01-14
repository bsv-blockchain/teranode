package localttl

import (
	"bytes"
	"context"
	"io"
	"testing"
	"time"

	"github.com/bitcoin-sv/teranode/stores/blob/memory"
	"github.com/bitcoin-sv/teranode/stores/blob/options"
	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// setupTest creates fresh stores and returns them along with a LocalTTL instance
func setupTest(t *testing.T, opts ...options.StoreOption) (*LocalTTL, blobStore, blobStore) {
	ttlStore := memory.New(opts...)
	blobStore := memory.New(opts...)
	store, err := New(ulogger.TestLogger{}, ttlStore, blobStore, opts...)
	require.NoError(t, err)

	return store, ttlStore, blobStore
}

func TestLocalTTL_Basic(t *testing.T) {
	ctx := context.Background()
	key := []byte("test-key")
	value := []byte("test-value")

	t.Run("Set and Get without TTL", func(t *testing.T) {
		store, ttlStore, blobStore := setupTest(t)

		err := store.Set(ctx, key, value)
		require.NoError(t, err)

		// Should be in blob store
		got, err := store.Get(ctx, key)
		require.NoError(t, err)
		assert.Equal(t, value, got)

		// Verify it's in the blob store
		exists, err := blobStore.Exists(ctx, key)
		require.NoError(t, err)
		assert.True(t, exists)

		// Verify it's not in the TTL store
		exists, err = ttlStore.Exists(ctx, key)
		require.NoError(t, err)
		assert.False(t, exists)
	})

	t.Run("Set and Get with TTL", func(t *testing.T) {
		store, ttlStore, blobStore := setupTest(t)

		ttl := time.Hour
		err := store.Set(ctx, key, value, options.WithTTL(ttl))
		require.NoError(t, err)

		// Should be in TTL store (and not blob store since it's a new item with TTL)
		got, err := store.Get(ctx, key)
		require.NoError(t, err)
		assert.Equal(t, value, got)

		// Verify it's in TTL store
		exists, err := ttlStore.Exists(ctx, key)
		require.NoError(t, err)
		assert.True(t, exists)

		// Verify it's not in the blob store
		exists, err = blobStore.Exists(ctx, key)
		require.NoError(t, err)
		assert.False(t, exists)
	})
}

func TestLocalTTL_TTLOperations(t *testing.T) {
	ctx := context.Background()
	key := []byte("test-key")
	value := []byte("test-value")

	t.Run("Set TTL on existing item", func(t *testing.T) {
		store, ttlStore, blobStore := setupTest(t)

		// First set without TTL
		err := store.Set(ctx, key, value)
		require.NoError(t, err)

		// Now set TTL
		ttl := time.Hour
		err = store.SetTTL(ctx, key, ttl)
		require.NoError(t, err)

		// Should be copied to TTL store while remaining in blob store
		exists, err := ttlStore.Exists(ctx, key)
		require.NoError(t, err)
		assert.True(t, exists)

		// Should still be in blob store
		exists, err = blobStore.Exists(ctx, key)
		require.NoError(t, err)
		assert.True(t, exists)

		// Verify TTL
		gotTTL, err := store.GetTTL(ctx, key)
		require.NoError(t, err)
		assert.Equal(t, ttl, gotTTL)
	})

	t.Run("Remove TTL from item", func(t *testing.T) {
		store, ttlStore, blobStore := setupTest(t)

		// First set with TTL
		err := store.Set(ctx, key, value, options.WithTTL(time.Hour))
		require.NoError(t, err)

		// Now remove TTL
		err = store.SetTTL(ctx, key, 0)
		require.NoError(t, err)

		// Should still be in blob store
		exists, err := blobStore.Exists(ctx, key)
		require.NoError(t, err)
		assert.True(t, exists)

		// Should be removed from TTL store
		exists, err = ttlStore.Exists(ctx, key)
		require.NoError(t, err)
		assert.False(t, exists)

		// Verify no TTL
		gotTTL, err := store.GetTTL(ctx, key)
		require.NoError(t, err)
		assert.Equal(t, time.Duration(0), gotTTL)
	})
}

func TestLocalTTL_Delete(t *testing.T) {
	ctx := context.Background()
	key := []byte("test-key")
	value := []byte("test-value")

	t.Run("Delete from both stores", func(t *testing.T) {
		store, ttlStore, blobStore := setupTest(t)

		// Set in both stores
		err := store.Set(ctx, key, value) // blob store
		require.NoError(t, err)
		err = store.Set(ctx, append(key, '-', '2'), value, options.WithTTL(time.Hour)) // ttl store only since new with TTL
		require.NoError(t, err)

		// Delete
		err = store.Del(ctx, key)
		require.NoError(t, err)

		// Verify deleted from both
		exists, err := blobStore.Exists(ctx, key)
		require.NoError(t, err)
		assert.False(t, exists)

		exists, err = ttlStore.Exists(ctx, key)
		require.NoError(t, err)
		assert.False(t, exists)
	})
}

func TestLocalTTL_IoOperations(t *testing.T) {
	ctx := context.Background()
	key := []byte("test-key")
	value := []byte("test-value")

	t.Run("SetFromReader and GetIoReader without TTL", func(t *testing.T) {
		store, _, blobStore := setupTest(t)

		reader := io.NopCloser(bytes.NewReader(value))
		err := store.SetFromReader(ctx, key, reader)
		require.NoError(t, err)

		// Read using GetIoReader
		gotReader, err := store.GetIoReader(ctx, key)
		require.NoError(t, err)

		gotValue, err := io.ReadAll(gotReader)
		require.NoError(t, err)
		assert.Equal(t, value, gotValue)

		// Verify it's only in blob store
		exists, err := blobStore.Exists(ctx, key)
		require.NoError(t, err)
		assert.True(t, exists)
	})

	t.Run("SetFromReader and GetIoReader with TTL", func(t *testing.T) {
		store, ttlStore, blobStore := setupTest(t)

		reader := io.NopCloser(bytes.NewReader(value))
		err := store.SetFromReader(ctx, key, reader, options.WithTTL(time.Hour))
		require.NoError(t, err)

		// Read using GetIoReader
		gotReader, err := store.GetIoReader(ctx, key)
		require.NoError(t, err)

		gotValue, err := io.ReadAll(gotReader)
		require.NoError(t, err)
		assert.Equal(t, value, gotValue)

		// Verify it's in TTL store only (new item with TTL)
		exists, err := ttlStore.Exists(ctx, key)
		require.NoError(t, err)
		assert.True(t, exists)

		// Verify it's not in blob store since it was just created with TTL
		exists, err = blobStore.Exists(ctx, key)
		require.NoError(t, err)
		assert.False(t, exists)
	})
}

func TestLocalTTL_HeaderFooter(t *testing.T) {
	ctx := context.Background()
	key := []byte("test-key")
	value := []byte("test-value")

	t.Run("Get/Set with header and footer", func(t *testing.T) {
		store, _, _ := setupTest(t,
			options.WithHeader([]byte("header-")),
			options.WithFooter(options.NewFooter(len([]byte("-footer")), []byte("-footer"), nil)),
		)

		err := store.Set(ctx, key, value)
		require.NoError(t, err)

		got, err := store.Get(ctx, key)
		require.NoError(t, err)
		assert.Equal(t, value, got)

		gotHeader, err := store.GetHeader(ctx, key)
		require.NoError(t, err)
		assert.Equal(t, []byte("header-"), gotHeader)

		gotFooter, err := store.GetFooterMetaData(ctx, key)
		require.NoError(t, err)
		assert.Equal(t, []byte{}, gotFooter) // -footer is the eofmarker, the footer metadata is empty
	})
}

func TestLocalTTL_Health(t *testing.T) {
	t.Run("Health check", func(t *testing.T) {
		store, _, _ := setupTest(t)

		status, _, err := store.Health(context.Background(), true)
		require.NoError(t, err)
		assert.Equal(t, 200, status)
	})
}
