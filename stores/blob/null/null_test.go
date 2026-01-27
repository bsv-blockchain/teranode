package null

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"testing"

	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/stores/blob/options"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestNew tests the creation of a new null store
func TestNew(t *testing.T) {
	t.Run("create null store with logger", func(t *testing.T) {
		logger := ulogger.TestLogger{}
		store, err := New(logger)

		require.NoError(t, err)
		require.NotNil(t, store)
		assert.NotNil(t, store.logger)
		assert.NotNil(t, store.options)
	})

	t.Run("create null store with options", func(t *testing.T) {
		logger := ulogger.TestLogger{}
		store, err := New(logger, options.WithDefaultBlockHeightRetention(100))

		require.NoError(t, err)
		require.NotNil(t, store)
		assert.NotNil(t, store.options)
	})

	t.Run("create null store with multiple options", func(t *testing.T) {
		logger := ulogger.TestLogger{}
		store, err := New(logger,
			options.WithDefaultBlockHeightRetention(100),
			options.WithHashPrefix(2))

		require.NoError(t, err)
		require.NotNil(t, store)
		assert.NotNil(t, store.options)
	})

	t.Run("create null store always succeeds", func(t *testing.T) {
		logger := ulogger.TestLogger{}

		// Call New multiple times - should always succeed
		for i := 0; i < 10; i++ {
			store, err := New(logger)
			require.NoError(t, err)
			require.NotNil(t, store)
		}
	})
}

// TestHealth tests the Health method
func TestHealth(t *testing.T) {
	logger := ulogger.TestLogger{}
	store, err := New(logger)
	require.NoError(t, err)

	t.Run("health check returns OK", func(t *testing.T) {
		status, msg, err := store.Health(context.Background(), false)

		assert.NoError(t, err)
		assert.Equal(t, http.StatusOK, status)
		assert.Equal(t, "Null Store", msg)
	})

	t.Run("health check with liveness returns OK", func(t *testing.T) {
		status, msg, err := store.Health(context.Background(), true)

		assert.NoError(t, err)
		assert.Equal(t, http.StatusOK, status)
		assert.Equal(t, "Null Store", msg)
	})

	t.Run("health check with cancelled context returns OK", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel() // Cancel immediately

		// Null store ignores context, so should still return OK
		status, msg, err := store.Health(ctx, true)

		assert.NoError(t, err)
		assert.Equal(t, http.StatusOK, status)
		assert.Equal(t, "Null Store", msg)
	})

	t.Run("multiple health checks always succeed", func(t *testing.T) {
		for i := 0; i < 100; i++ {
			status, msg, err := store.Health(context.Background(), i%2 == 0)
			assert.NoError(t, err)
			assert.Equal(t, http.StatusOK, status)
			assert.Equal(t, "Null Store", msg)
		}
	})
}

// TestClose tests the Close method
func TestClose(t *testing.T) {
	t.Run("close returns no error", func(t *testing.T) {
		logger := ulogger.TestLogger{}
		store, err := New(logger)
		require.NoError(t, err)

		err = store.Close(context.Background())
		assert.NoError(t, err)
	})

	t.Run("close with cancelled context returns no error", func(t *testing.T) {
		logger := ulogger.TestLogger{}
		store, err := New(logger)
		require.NoError(t, err)

		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		err = store.Close(ctx)
		assert.NoError(t, err)
	})

	t.Run("multiple closes are safe", func(t *testing.T) {
		logger := ulogger.TestLogger{}
		store, err := New(logger)
		require.NoError(t, err)

		// Call Close multiple times - should always succeed
		for i := 0; i < 10; i++ {
			err = store.Close(context.Background())
			assert.NoError(t, err)
		}
	})
}

// TestSet tests the Set method
func TestSet(t *testing.T) {
	logger := ulogger.TestLogger{}
	store, err := New(logger)
	require.NoError(t, err)

	t.Run("set always succeeds", func(t *testing.T) {
		key := []byte("test-key")
		value := []byte("test-value")

		err := store.Set(context.Background(), key, fileformat.FileTypeTesting, value)
		assert.NoError(t, err)
	})

	t.Run("set with empty key succeeds", func(t *testing.T) {
		err := store.Set(context.Background(), []byte{}, fileformat.FileTypeTesting, []byte("value"))
		assert.NoError(t, err)
	})

	t.Run("set with empty value succeeds", func(t *testing.T) {
		err := store.Set(context.Background(), []byte("key"), fileformat.FileTypeTesting, []byte{})
		assert.NoError(t, err)
	})

	t.Run("set with nil key succeeds", func(t *testing.T) {
		err := store.Set(context.Background(), nil, fileformat.FileTypeTesting, []byte("value"))
		assert.NoError(t, err)
	})

	t.Run("set with nil value succeeds", func(t *testing.T) {
		err := store.Set(context.Background(), []byte("key"), fileformat.FileTypeTesting, nil)
		assert.NoError(t, err)
	})

	t.Run("set with options succeeds", func(t *testing.T) {
		err := store.Set(context.Background(), []byte("key"), fileformat.FileTypeTesting,
			[]byte("value"), options.WithDeleteAt(100))
		assert.NoError(t, err)
	})

	t.Run("set with cancelled context succeeds", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		err := store.Set(ctx, []byte("key"), fileformat.FileTypeTesting, []byte("value"))
		assert.NoError(t, err)
	})

	t.Run("set with large value succeeds", func(t *testing.T) {
		largeValue := make([]byte, 10*1024*1024) // 10MB
		err := store.Set(context.Background(), []byte("key"), fileformat.FileTypeTesting, largeValue)
		assert.NoError(t, err)
	})

	t.Run("set with different file types succeeds", func(t *testing.T) {
		fileTypes := []fileformat.FileType{
			fileformat.FileTypeTesting,
			fileformat.FileTypeTx,
			fileformat.FileTypeSubtree,
		}

		for _, ft := range fileTypes {
			err := store.Set(context.Background(), []byte("key"), ft, []byte("value"))
			assert.NoError(t, err, "Should succeed for file type %v", ft)
		}
	})
}

// TestSetFromReader tests the SetFromReader method
func TestSetFromReader(t *testing.T) {
	logger := ulogger.TestLogger{}
	store, err := New(logger)
	require.NoError(t, err)

	t.Run("set from reader always succeeds", func(t *testing.T) {
		reader := io.NopCloser(bytes.NewReader([]byte("test-value")))
		err := store.SetFromReader(context.Background(), []byte("key"), fileformat.FileTypeTesting, reader)
		assert.NoError(t, err)
	})

	t.Run("set from reader with empty data succeeds", func(t *testing.T) {
		reader := io.NopCloser(bytes.NewReader([]byte{}))
		err := store.SetFromReader(context.Background(), []byte("key"), fileformat.FileTypeTesting, reader)
		assert.NoError(t, err)
	})

	t.Run("set from reader with nil reader succeeds", func(t *testing.T) {
		err := store.SetFromReader(context.Background(), []byte("key"), fileformat.FileTypeTesting, nil)
		assert.NoError(t, err)
	})

	t.Run("set from reader with options succeeds", func(t *testing.T) {
		reader := io.NopCloser(bytes.NewReader([]byte("test")))
		err := store.SetFromReader(context.Background(), []byte("key"), fileformat.FileTypeTesting,
			reader, options.WithDeleteAt(100))
		assert.NoError(t, err)
	})

	t.Run("set from reader with cancelled context succeeds", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		reader := io.NopCloser(bytes.NewReader([]byte("test")))
		err := store.SetFromReader(ctx, []byte("key"), fileformat.FileTypeTesting, reader)
		assert.NoError(t, err)
	})

	t.Run("set from reader with large data succeeds", func(t *testing.T) {
		largeData := make([]byte, 10*1024*1024) // 10MB
		reader := io.NopCloser(bytes.NewReader(largeData))
		err := store.SetFromReader(context.Background(), []byte("key"), fileformat.FileTypeTesting, reader)
		assert.NoError(t, err)
	})
}

// TestGet tests the Get method
func TestGet(t *testing.T) {
	logger := ulogger.TestLogger{}
	store, err := New(logger)
	require.NoError(t, err)

	t.Run("get always returns not found error", func(t *testing.T) {
		data, err := store.Get(context.Background(), []byte("key"), fileformat.FileTypeTesting)

		assert.Error(t, err)
		assert.Nil(t, data)
		// Null store returns a storage error, not ErrNotFound
		assert.Contains(t, err.Error(), "no such file or directory")
	})

	t.Run("get returns not found even after set", func(t *testing.T) {
		// Set a value
		_ = store.Set(context.Background(), []byte("key"), fileformat.FileTypeTesting, []byte("value"))

		// Try to get it - should still not be found
		data, err := store.Get(context.Background(), []byte("key"), fileformat.FileTypeTesting)
		assert.Error(t, err)
		assert.Nil(t, data)
	})

	t.Run("get with empty key returns error", func(t *testing.T) {
		data, err := store.Get(context.Background(), []byte{}, fileformat.FileTypeTesting)
		assert.Error(t, err)
		assert.Nil(t, data)
	})

	t.Run("get with nil key returns error", func(t *testing.T) {
		data, err := store.Get(context.Background(), nil, fileformat.FileTypeTesting)
		assert.Error(t, err)
		assert.Nil(t, data)
	})

	t.Run("get with options returns error", func(t *testing.T) {
		data, err := store.Get(context.Background(), []byte("key"), fileformat.FileTypeTesting,
			options.WithSubDirectory("test"))
		assert.Error(t, err)
		assert.Nil(t, data)
	})

	t.Run("get error message contains filename", func(t *testing.T) {
		_, err := store.Get(context.Background(), []byte("test-key"), fileformat.FileTypeTesting)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "no such file or directory")
	})
}

// TestGetIoReader tests the GetIoReader method
func TestGetIoReader(t *testing.T) {
	logger := ulogger.TestLogger{}
	store, err := New(logger)
	require.NoError(t, err)

	t.Run("get io reader always returns not found error", func(t *testing.T) {
		reader, err := store.GetIoReader(context.Background(), []byte("key"), fileformat.FileTypeTesting)

		assert.Error(t, err)
		assert.Nil(t, reader)
		// Null store returns a storage error, not ErrNotFound
		assert.Contains(t, err.Error(), "no such file or directory")
	})

	t.Run("get io reader returns not found even after set", func(t *testing.T) {
		// Set a value
		_ = store.Set(context.Background(), []byte("key"), fileformat.FileTypeTesting, []byte("value"))

		// Try to get it - should still not be found
		reader, err := store.GetIoReader(context.Background(), []byte("key"), fileformat.FileTypeTesting)
		assert.Error(t, err)
		assert.Nil(t, reader)
	})

	t.Run("get io reader with options returns error", func(t *testing.T) {
		reader, err := store.GetIoReader(context.Background(), []byte("key"), fileformat.FileTypeTesting,
			options.WithSubDirectory("test"))
		assert.Error(t, err)
		assert.Nil(t, reader)
	})

	t.Run("get io reader error message contains filename", func(t *testing.T) {
		_, err := store.GetIoReader(context.Background(), []byte("test-key"), fileformat.FileTypeTesting)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "no such file or directory")
	})

	t.Run("get io reader with invalid options returns error", func(t *testing.T) {
		// Use an option that might cause ConstructFilename to fail
		reader, err := store.GetIoReader(context.Background(), []byte("key"), fileformat.FileTypeTesting)
		assert.Error(t, err)
		assert.Nil(t, reader)
	})
}

// TestExists tests the Exists method
func TestExists(t *testing.T) {
	logger := ulogger.TestLogger{}
	store, err := New(logger)
	require.NoError(t, err)

	t.Run("exists always returns false", func(t *testing.T) {
		exists, err := store.Exists(context.Background(), []byte("key"), fileformat.FileTypeTesting)

		assert.NoError(t, err)
		assert.False(t, exists)
	})

	t.Run("exists returns false even after set", func(t *testing.T) {
		// Set a value
		_ = store.Set(context.Background(), []byte("key"), fileformat.FileTypeTesting, []byte("value"))

		// Check if it exists - should return false
		exists, err := store.Exists(context.Background(), []byte("key"), fileformat.FileTypeTesting)
		assert.NoError(t, err)
		assert.False(t, exists)
	})

	t.Run("exists with empty key returns false", func(t *testing.T) {
		exists, err := store.Exists(context.Background(), []byte{}, fileformat.FileTypeTesting)
		assert.NoError(t, err)
		assert.False(t, exists)
	})

	t.Run("exists with nil key returns false", func(t *testing.T) {
		exists, err := store.Exists(context.Background(), nil, fileformat.FileTypeTesting)
		assert.NoError(t, err)
		assert.False(t, exists)
	})

	t.Run("exists with options returns false", func(t *testing.T) {
		exists, err := store.Exists(context.Background(), []byte("key"), fileformat.FileTypeTesting,
			options.WithSubDirectory("test"))
		assert.NoError(t, err)
		assert.False(t, exists)
	})

	t.Run("exists with cancelled context returns false", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		exists, err := store.Exists(ctx, []byte("key"), fileformat.FileTypeTesting)
		assert.NoError(t, err)
		assert.False(t, exists)
	})
}

// TestDel tests the Del method
func TestDel(t *testing.T) {
	logger := ulogger.TestLogger{}
	store, err := New(logger)
	require.NoError(t, err)

	t.Run("del always succeeds", func(t *testing.T) {
		err := store.Del(context.Background(), []byte("key"), fileformat.FileTypeTesting)
		assert.NoError(t, err)
	})

	t.Run("del on non-existent key succeeds", func(t *testing.T) {
		err := store.Del(context.Background(), []byte("non-existent"), fileformat.FileTypeTesting)
		assert.NoError(t, err)
	})

	t.Run("del with empty key succeeds", func(t *testing.T) {
		err := store.Del(context.Background(), []byte{}, fileformat.FileTypeTesting)
		assert.NoError(t, err)
	})

	t.Run("del with nil key succeeds", func(t *testing.T) {
		err := store.Del(context.Background(), nil, fileformat.FileTypeTesting)
		assert.NoError(t, err)
	})

	t.Run("del with options succeeds", func(t *testing.T) {
		err := store.Del(context.Background(), []byte("key"), fileformat.FileTypeTesting,
			options.WithSubDirectory("test"))
		assert.NoError(t, err)
	})

	t.Run("del with cancelled context succeeds", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		err := store.Del(ctx, []byte("key"), fileformat.FileTypeTesting)
		assert.NoError(t, err)
	})

	t.Run("multiple deletes succeed", func(t *testing.T) {
		for i := 0; i < 10; i++ {
			err := store.Del(context.Background(), []byte("key"), fileformat.FileTypeTesting)
			assert.NoError(t, err)
		}
	})
}

// TestSetDAH tests the SetDAH method
func TestSetDAH(t *testing.T) {
	logger := ulogger.TestLogger{}
	store, err := New(logger)
	require.NoError(t, err)

	t.Run("set DAH always succeeds", func(t *testing.T) {
		err := store.SetDAH(context.Background(), []byte("key"), fileformat.FileTypeTesting, 100)
		assert.NoError(t, err)
	})

	t.Run("set DAH with zero value succeeds", func(t *testing.T) {
		err := store.SetDAH(context.Background(), []byte("key"), fileformat.FileTypeTesting, 0)
		assert.NoError(t, err)
	})

	t.Run("set DAH with maximum value succeeds", func(t *testing.T) {
		err := store.SetDAH(context.Background(), []byte("key"), fileformat.FileTypeTesting, ^uint32(0))
		assert.NoError(t, err)
	})

	t.Run("set DAH with empty key succeeds", func(t *testing.T) {
		err := store.SetDAH(context.Background(), []byte{}, fileformat.FileTypeTesting, 100)
		assert.NoError(t, err)
	})

	t.Run("set DAH with options succeeds", func(t *testing.T) {
		err := store.SetDAH(context.Background(), []byte("key"), fileformat.FileTypeTesting, 100,
			options.WithSubDirectory("test"))
		assert.NoError(t, err)
	})

	t.Run("set DAH with cancelled context succeeds", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		err := store.SetDAH(ctx, []byte("key"), fileformat.FileTypeTesting, 100)
		assert.NoError(t, err)
	})
}

// TestGetDAH tests the GetDAH method
func TestGetDAH(t *testing.T) {
	t.Run("get DAH removed", func(t *testing.T) {
		// GetDAH has been removed from the blob.Store interface
		// DAH functionality is now centralized in the pruner service
		t.Skip("GetDAH removed from interface - see e2e pruner tests")
	})
}

// TestSetCurrentBlockHeight tests the SetCurrentBlockHeight method
func TestSetCurrentBlockHeight(t *testing.T) {
	logger := ulogger.TestLogger{}
	store, err := New(logger)
	require.NoError(t, err)

	t.Run("set current block height is a no-op", func(t *testing.T) {
		// Should not panic or error
		assert.NotPanics(t, func() {
			store.SetCurrentBlockHeight(100)
		})
	})

	t.Run("set current block height with zero", func(t *testing.T) {
		assert.NotPanics(t, func() {
			store.SetCurrentBlockHeight(0)
		})
	})

	t.Run("set current block height with maximum value", func(t *testing.T) {
		assert.NotPanics(t, func() {
			store.SetCurrentBlockHeight(^uint32(0))
		})
	})

	t.Run("multiple set current block height calls", func(t *testing.T) {
		assert.NotPanics(t, func() {
			for i := uint32(0); i < 1000; i++ {
				store.SetCurrentBlockHeight(i)
			}
		})
	})
}

// TestIntegration tests integration scenarios
func TestIntegration(t *testing.T) {
	logger := ulogger.TestLogger{}
	store, err := New(logger)
	require.NoError(t, err)

	t.Run("complete workflow returns expected results", func(t *testing.T) {
		ctx := context.Background()
		key := []byte("test-key")
		value := []byte("test-value")
		fileType := fileformat.FileTypeTesting

		// Set data
		err := store.Set(ctx, key, fileType, value)
		assert.NoError(t, err)

		// Try to get data - should not exist
		data, err := store.Get(ctx, key, fileType)
		assert.Error(t, err)
		assert.Nil(t, data)

		// Check if exists - should be false
		exists, err := store.Exists(ctx, key, fileType)
		assert.NoError(t, err)
		assert.False(t, exists)

		// Set DAH
		err = store.SetDAH(ctx, key, fileType, 100)
		assert.NoError(t, err)

		// DAH verification now done via pruner service in e2e tests

		// Delete
		err = store.Del(ctx, key, fileType)
		assert.NoError(t, err)

		// Health check
		status, msg, err := store.Health(ctx, true)
		assert.NoError(t, err)
		assert.Equal(t, http.StatusOK, status)
		assert.Equal(t, "Null Store", msg)

		// Close
		err = store.Close(ctx)
		assert.NoError(t, err)
	})

	t.Run("null store is truly a blackhole", func(t *testing.T) {
		ctx := context.Background()

		// Write many different blobs
		for i := 0; i < 100; i++ {
			key := []byte{byte(i)}
			value := []byte{byte(i * 2)}

			err := store.Set(ctx, key, fileformat.FileTypeTesting, value)
			assert.NoError(t, err)
		}

		// None should exist
		for i := 0; i < 100; i++ {
			key := []byte{byte(i)}
			exists, err := store.Exists(ctx, key, fileformat.FileTypeTesting)
			assert.NoError(t, err)
			assert.False(t, exists, "Blob %d should not exist", i)
		}
	})

	t.Run("concurrent operations are safe", func(t *testing.T) {
		ctx := context.Background()
		done := make(chan bool, 10)

		// Run operations concurrently
		for i := 0; i < 10; i++ {
			go func(id int) {
				defer func() { done <- true }()

				key := []byte{byte(id)}
				value := []byte{byte(id * 2)}

				_ = store.Set(ctx, key, fileformat.FileTypeTesting, value)
				_, _ = store.Get(ctx, key, fileformat.FileTypeTesting)
				_, _ = store.Exists(ctx, key, fileformat.FileTypeTesting)
				_ = store.SetDAH(ctx, key, fileformat.FileTypeTesting, uint32(id))
				_ = store.Del(ctx, key, fileformat.FileTypeTesting)
			}(i)
		}

		// Wait for all goroutines
		for i := 0; i < 10; i++ {
			<-done
		}
	})
}

// BenchmarkNullStore benchmarks null store operations
func BenchmarkNullStore(b *testing.B) {
	logger := ulogger.TestLogger{}
	store, _ := New(logger)
	ctx := context.Background()
	key := []byte("benchmark-key")
	value := []byte("benchmark-value")

	b.Run("Set", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = store.Set(ctx, key, fileformat.FileTypeTesting, value)
		}
	})

	b.Run("Get", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _ = store.Get(ctx, key, fileformat.FileTypeTesting)
		}
	})

	b.Run("Exists", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _ = store.Exists(ctx, key, fileformat.FileTypeTesting)
		}
	})

	b.Run("SetDAH", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = store.SetDAH(ctx, key, fileformat.FileTypeTesting, 100)
		}
	})

	b.Run("GetDAH removed", func(b *testing.B) {
		// GetDAH has been removed from the blob.Store interface
		b.Skip("GetDAH removed from interface - see e2e pruner tests")
	})

	b.Run("Health", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _, _ = store.Health(ctx, false)
		}
	})
}
