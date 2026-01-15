package tempstore

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBadgerTempStore_BasicOperations(t *testing.T) {
	store, err := New(Options{Prefix: "test"})
	require.NoError(t, err)
	defer store.Close()

	// Test Put and Get
	key := []byte("testkey")
	value := []byte("testvalue")

	err = store.Put(key, value)
	require.NoError(t, err)

	result, err := store.Get(key)
	require.NoError(t, err)
	assert.Equal(t, value, result)

	// Test non-existent key
	result, err = store.Get([]byte("nonexistent"))
	require.NoError(t, err)
	assert.Nil(t, result)
}

func TestBadgerTempStore_WriteBatch(t *testing.T) {
	store, err := New(Options{Prefix: "test-batch"})
	require.NoError(t, err)
	defer store.Close()

	batch := store.NewWriteBatch()

	// Write multiple entries
	for i := 0; i < 100; i++ {
		key := []byte{byte(i)}
		value := []byte{byte(i * 2)}
		err = batch.Set(key, value)
		require.NoError(t, err)
	}

	assert.Equal(t, int64(100), batch.Count())

	err = batch.Flush()
	require.NoError(t, err)

	assert.Equal(t, int64(0), batch.Count())

	// Verify all entries are readable
	for i := 0; i < 100; i++ {
		key := []byte{byte(i)}
		result, err := store.Get(key)
		require.NoError(t, err)
		assert.Equal(t, []byte{byte(i * 2)}, result)
	}
}

func TestBadgerTempStore_Iterate(t *testing.T) {
	store, err := New(Options{Prefix: "test-iterate"})
	require.NoError(t, err)
	defer store.Close()

	// Insert entries in reverse order to verify sorting
	batch := store.NewWriteBatch()
	for i := 9; i >= 0; i-- {
		key := []byte{byte(i)}
		value := []byte{byte(i * 10)}
		err = batch.Set(key, value)
		require.NoError(t, err)
	}
	err = batch.Flush()
	require.NoError(t, err)

	// Iterate and verify lexicographic order
	var keys []byte
	err = store.Iterate(func(key, value []byte) error {
		keys = append(keys, key[0])
		return nil
	})
	require.NoError(t, err)

	// Should be in ascending order
	assert.Equal(t, []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, keys)
}

func TestBadgerTempStore_IterateKeys(t *testing.T) {
	store, err := New(Options{Prefix: "test-iterate-keys"})
	require.NoError(t, err)
	defer store.Close()

	batch := store.NewWriteBatch()
	for i := 0; i < 5; i++ {
		key := []byte{byte(i)}
		value := []byte("largevalue12345678901234567890")
		err = batch.Set(key, value)
		require.NoError(t, err)
	}
	err = batch.Flush()
	require.NoError(t, err)

	var count int
	err = store.IterateKeys(func(key []byte) error {
		count++
		return nil
	})
	require.NoError(t, err)
	assert.Equal(t, 5, count)
}

func TestBadgerTempStore_Count(t *testing.T) {
	store, err := New(Options{Prefix: "test-count"})
	require.NoError(t, err)
	defer store.Close()

	batch := store.NewWriteBatch()
	for i := 0; i < 50; i++ {
		err = batch.Set([]byte{byte(i)}, []byte{byte(i)})
		require.NoError(t, err)
	}
	err = batch.Flush()
	require.NoError(t, err)

	count := store.Count()
	assert.Equal(t, int64(50), count)
}

func TestBadgerTempStore_Path(t *testing.T) {
	store, err := New(Options{Prefix: "test-path"})
	require.NoError(t, err)
	defer store.Close()

	path := store.Path()
	assert.NotEmpty(t, path)
	assert.Contains(t, path, "test-path")
}

func TestBadgerTempStore_WriteBatchCancel(t *testing.T) {
	store, err := New(Options{Prefix: "test-cancel"})
	require.NoError(t, err)
	defer store.Close()

	batch := store.NewWriteBatch()

	err = batch.Set([]byte("key1"), []byte("value1"))
	require.NoError(t, err)

	batch.Cancel()

	// After cancel, the entry should not exist
	result, err := store.Get([]byte("key1"))
	require.NoError(t, err)
	assert.Nil(t, result)
}

func TestBadgerTempStore_EmptyBatchFlush(t *testing.T) {
	store, err := New(Options{Prefix: "test-empty-flush"})
	require.NoError(t, err)
	defer store.Close()

	batch := store.NewWriteBatch()

	// Flushing an empty batch should be a no-op
	err = batch.Flush()
	require.NoError(t, err)
}
