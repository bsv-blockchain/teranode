package localdah

import (
	"bytes"
	"context"
	"io"
	"testing"

	"github.com/bitcoin-sv/teranode/stores/blob/memory"
	"github.com/bitcoin-sv/teranode/stores/blob/options"
	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// setupTest creates fresh stores and returns them along with a LocalDAH instance
func setupTest(t *testing.T, opts ...options.StoreOption) (*LocalDAH, blobStore, blobStore) {
	dahStore := memory.New(opts...)
	blobStore := memory.New(opts...)
	store, err := New(ulogger.TestLogger{}, dahStore, blobStore, opts...)
	require.NoError(t, err)

	return store, dahStore, blobStore
}

func TestLocalDAH_Basic(t *testing.T) {
	ctx := context.Background()
	key := []byte("test-key")
	value := []byte("test-value")

	t.Run("Set and Get without DAH", func(t *testing.T) {
		store, dahStore, blobStore := setupTest(t)

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

		// Verify it's not in the DAH store
		exists, err = dahStore.Exists(ctx, key)
		require.NoError(t, err)
		assert.False(t, exists)
	})

	t.Run("Set and Get with DAH", func(t *testing.T) {
		store, dahStore, blobStore := setupTest(t)

		dah := uint32(5)
		err := store.Set(ctx, key, value, options.WithDeleteAt(dah))
		require.NoError(t, err)

		// Should be in DAH store (and not blob store since it's a new item with DAH)
		got, err := store.Get(ctx, key)
		require.NoError(t, err)
		assert.Equal(t, value, got)

		// Verify it's in DAH store
		exists, err := dahStore.Exists(ctx, key)
		require.NoError(t, err)
		assert.True(t, exists)

		// Verify it's not in the blob store
		exists, err = blobStore.Exists(ctx, key)
		require.NoError(t, err)
		assert.False(t, exists)
	})
}

func TestLocalDAH_DAHOperations(t *testing.T) {
	ctx := context.Background()
	key := []byte("test-key")
	value := []byte("test-value")

	t.Run("Set DAH on existing item", func(t *testing.T) {
		store, dahStore, blobStore := setupTest(t)

		// First set without DAH
		err := store.Set(ctx, key, value)
		require.NoError(t, err)

		// Now set DAH
		dah := uint32(5)
		err = store.SetDAH(ctx, key, dah)
		require.NoError(t, err)

		// Should be copied to DAH store while remaining in blob store
		exists, err := dahStore.Exists(ctx, key)
		require.NoError(t, err)
		assert.True(t, exists)

		// Should still be in blob store
		exists, err = blobStore.Exists(ctx, key)
		require.NoError(t, err)
		assert.True(t, exists)

		// Verify DAH
		gotDAH, err := store.GetDAH(ctx, key)
		require.NoError(t, err)
		assert.Equal(t, dah, gotDAH)
	})

	t.Run("Remove DAH from item", func(t *testing.T) {
		store, dahStore, blobStore := setupTest(t)

		// First set with DAH
		err := store.Set(ctx, key, value, options.WithDeleteAt(5))
		require.NoError(t, err)

		// Now remove DAH
		err = store.SetDAH(ctx, key, 0)
		require.NoError(t, err)

		// Should still be in blob store
		exists, err := blobStore.Exists(ctx, key)
		require.NoError(t, err)
		assert.True(t, exists)

		// Should be removed from DAH store
		exists, err = dahStore.Exists(ctx, key)
		require.NoError(t, err)
		assert.False(t, exists)

		// Verify no DAH
		gotDAH, err := store.GetDAH(ctx, key)
		require.NoError(t, err)
		assert.Equal(t, uint32(0), gotDAH)
	})
}

func TestLocalDAH_Delete(t *testing.T) {
	ctx := context.Background()
	key := []byte("test-key")
	value := []byte("test-value")

	t.Run("Delete from both stores", func(t *testing.T) {
		store, dahStore, blobStore := setupTest(t)

		// Set in both stores
		err := store.Set(ctx, key, value) // blob store
		require.NoError(t, err)
		err = store.Set(ctx, append(key, '-', '2'), value, options.WithDeleteAt(5))
		require.NoError(t, err)

		// Delete
		err = store.Del(ctx, key)
		require.NoError(t, err)

		// Verify deleted from both
		exists, err := blobStore.Exists(ctx, key)
		require.NoError(t, err)
		assert.False(t, exists)

		exists, err = dahStore.Exists(ctx, key)
		require.NoError(t, err)
		assert.False(t, exists)
	})
}

func TestLocalDAH_IoOperations(t *testing.T) {
	ctx := context.Background()
	key := []byte("test-key")
	value := []byte("test-value")

	t.Run("SetFromReader and GetIoReader without DAH", func(t *testing.T) {
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

	t.Run("SetFromReader and GetIoReader with DAH", func(t *testing.T) {
		store, dahStore, blobStore := setupTest(t)

		reader := io.NopCloser(bytes.NewReader(value))
		err := store.SetFromReader(ctx, key, reader, options.WithDeleteAt(5))
		require.NoError(t, err)

		// Read using GetIoReader
		gotReader, err := store.GetIoReader(ctx, key)
		require.NoError(t, err)

		gotValue, err := io.ReadAll(gotReader)
		require.NoError(t, err)
		assert.Equal(t, value, gotValue)

		// Verify it's in DAH store only (new item with DAH)
		exists, err := dahStore.Exists(ctx, key)
		require.NoError(t, err)
		assert.True(t, exists)

		// Verify it's not in blob store since it was just created with DAH
		exists, err = blobStore.Exists(ctx, key)
		require.NoError(t, err)
		assert.False(t, exists)
	})
}

func TestLocalDAH_HeaderFooter(t *testing.T) {
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

func TestLocalDAH_Health(t *testing.T) {
	t.Run("Health check", func(t *testing.T) {
		store, _, _ := setupTest(t)

		status, _, err := store.Health(context.Background(), true)
		require.NoError(t, err)
		assert.Equal(t, 200, status)
	})
}
