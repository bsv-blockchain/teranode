// Package blob provides blob storage functionality with various storage backend implementations.
package blob

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/stores/blob/http"
	"github.com/bsv-blockchain/teranode/stores/blob/options"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestServerOperations(t *testing.T) {
	// Create a temporary directory for the file store
	tempDir, err := os.MkdirTemp("", "test")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	// Create a logger
	logger := ulogger.New("blob-server-test")

	// Add a unique parameter to ensure a new DAH cleaner is started for this test
	serverStoreURL, err := url.Parse(fmt.Sprintf("file://%s?testId=%d", tempDir, time.Now().UnixNano()))
	require.NoError(t, err)

	blobServer, err := NewHTTPBlobServer(
		logger,
		serverStoreURL,
		options.WithDefaultSubDirectory("sub"),
	)
	require.NoError(t, err)

	serverAddr := "localhost:7979"
	go func() {
		err := blobServer.Start(context.Background(), serverAddr)
		if err != nil {
			t.Logf("Server stopped: %v", err)
		}
	}()

	// Wait for the server to start
	time.Sleep(100 * time.Millisecond)

	clientStoreURL, err := url.Parse("http://localhost:7979")
	require.NoError(t, err)

	client, err := http.New(logger, clientStoreURL)
	require.NoError(t, err)

	t.Run("SetAndGet", func(t *testing.T) {
		key := []byte("testKey1")
		value := []byte("testValue1")

		err := client.Set(context.Background(), key, fileformat.FileTypeTesting, value)
		require.NoError(t, err)

		retrievedValue, err := client.Get(context.Background(), key, fileformat.FileTypeTesting)
		require.NoError(t, err)

		assert.Equal(t, value, retrievedValue)

		err = client.Del(context.Background(), key, fileformat.FileTypeTesting)
		require.NoError(t, err)
	})

	// SetDAH test removed - automatic DAH cleanup now handled by pruner service, not file store

	t.Run("Exists", func(t *testing.T) {
		key := []byte("testKey3")
		value := []byte("testValue3")

		err := client.Set(context.Background(), key, fileformat.FileTypeTesting, value)
		require.NoError(t, err)

		exists, err := client.Exists(context.Background(), key, fileformat.FileTypeTesting)
		require.NoError(t, err)
		assert.True(t, exists)

		err = client.Del(context.Background(), key, fileformat.FileTypeTesting)
		require.NoError(t, err)

		exists, err = client.Exists(context.Background(), key, fileformat.FileTypeTesting)
		require.NoError(t, err)
		assert.False(t, exists)
	})

	t.Run("SetFromReader", func(t *testing.T) {
		key := []byte("testKey4")

		largeData := make([]byte, 10*1024*1024) // 10 MB of data
		for i := range largeData {
			largeData[i] = byte(i % 256)
		}

		reader := bytes.NewReader(largeData)

		err := client.SetFromReader(context.Background(), key, fileformat.FileTypeTesting, io.NopCloser(reader))
		require.NoError(t, err)

		// Retrieve the data
		retrievedReader, err := client.GetIoReader(context.Background(), key, fileformat.FileTypeTesting)
		require.NoError(t, err)
		defer retrievedReader.Close()

		retrievedData, err := io.ReadAll(retrievedReader)
		require.NoError(t, err)

		assert.Equal(t, largeData, retrievedData)

		// Clean up
		err = client.Del(context.Background(), key, fileformat.FileTypeTesting)
		require.NoError(t, err)
	})

	t.Run("WithFilename", func(t *testing.T) {
		key := []byte("testKey5")
		value := []byte("testValue5")

		err := client.Set(context.Background(), key, fileformat.FileTypeTesting, value, options.WithFilename("testFilename"))
		require.NoError(t, err)

		exists, err := client.Exists(context.Background(), key, fileformat.FileTypeTesting)
		require.NoError(t, err)
		assert.False(t, exists)

		exists, err = client.Exists(context.Background(), key, fileformat.FileTypeTesting, options.WithFilename("testFilename"))
		require.NoError(t, err)
		assert.True(t, exists)

		err = client.Del(context.Background(), key, fileformat.FileTypeTesting, options.WithFilename("testFilename"))
		require.NoError(t, err)
	})

	t.Run("WithExtension", func(t *testing.T) {
		key := []byte("testKey5")
		value := []byte("testValue5")

		err := client.Set(context.Background(), key, fileformat.FileTypeTesting, value)
		require.NoError(t, err)

		exists, err := client.Exists(context.Background(), key, fileformat.FileTypeTesting)
		require.NoError(t, err)
		assert.True(t, exists)

		err = client.Del(context.Background(), key, fileformat.FileTypeTesting)
		require.NoError(t, err)
	})
}
