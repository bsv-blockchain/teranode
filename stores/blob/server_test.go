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

	"github.com/bitcoin-sv/ubsv/stores/blob/http"
	"github.com/bitcoin-sv/ubsv/stores/blob/options"
	"github.com/bitcoin-sv/ubsv/ulogger"
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

	serverStoreURL, err := url.Parse(fmt.Sprintf("file://%s?ttlCleanerInterval=100ms", tempDir))
	require.NoError(t, err)

	blobServer, err := NewHTTPBlobServer(
		logger,
		serverStoreURL,
		options.WithSubDirectory("sub"),
	)
	require.NoError(t, err)

	serverAddr := "localhost:8080"
	go func() {
		err := blobServer.Start(context.Background(), serverAddr)
		if err != nil {
			t.Logf("Server stopped: %v", err)
		}
	}()

	// Wait for the server to start
	time.Sleep(100 * time.Millisecond)

	clientStoreURL, err := url.Parse("http://localhost:8080")
	require.NoError(t, err)

	client, err := http.New(logger, clientStoreURL)
	require.NoError(t, err)

	t.Run("SetAndGet", func(t *testing.T) {
		key := []byte("testKey1")
		value := []byte("testValue1")

		err := client.Set(context.Background(), key, value)
		require.NoError(t, err)

		retrievedValue, err := client.Get(context.Background(), key)
		require.NoError(t, err)

		assert.Equal(t, value, retrievedValue)

		err = client.Del(context.Background(), key)
		require.NoError(t, err)
	})

	t.Run("SetTTL", func(t *testing.T) {
		key := []byte("testKey2")
		value := []byte("testValue2")

		err := client.Set(context.Background(), key, value)
		require.NoError(t, err)

		err = client.SetTTL(context.Background(), key, 1*time.Millisecond)
		require.NoError(t, err)

		time.Sleep(200 * time.Millisecond)

		_, err = client.Get(context.Background(), key)
		assert.Error(t, err)
	})

	t.Run("Exists", func(t *testing.T) {
		key := []byte("testKey3")
		value := []byte("testValue3")

		err := client.Set(context.Background(), key, value)
		require.NoError(t, err)

		exists, err := client.Exists(context.Background(), key)
		require.NoError(t, err)
		assert.True(t, exists)

		err = client.Del(context.Background(), key)
		require.NoError(t, err)

		exists, err = client.Exists(context.Background(), key)
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

		err := client.SetFromReader(context.Background(), key, io.NopCloser(reader))
		require.NoError(t, err)

		// Retrieve the data
		retrievedReader, err := client.GetIoReader(context.Background(), key)
		require.NoError(t, err)
		defer retrievedReader.Close()

		retrievedData, err := io.ReadAll(retrievedReader)
		require.NoError(t, err)

		assert.Equal(t, largeData, retrievedData)

		// Clean up
		err = client.Del(context.Background(), key)
		require.NoError(t, err)
	})

	t.Run("WithFilename", func(t *testing.T) {
		key := []byte("testKey5")
		value := []byte("testValue5")

		err := client.Set(context.Background(), key, value, options.WithFilename("testFilename"))
		require.NoError(t, err)

		exists, err := client.Exists(context.Background(), key)
		require.NoError(t, err)
		assert.False(t, exists)

		exists, err = client.Exists(context.Background(), key, options.WithFilename("testFilename"))
		require.NoError(t, err)
		assert.True(t, exists)

		err = client.Del(context.Background(), key, options.WithFilename("testFilename"))
		require.NoError(t, err)
	})

	t.Run("WithExtension", func(t *testing.T) {
		key := []byte("testKey5")
		value := []byte("testValue5")

		err := client.Set(context.Background(), key, value, options.WithFileExtension("ext"))
		require.NoError(t, err)

		exists, err := client.Exists(context.Background(), key)
		require.NoError(t, err)
		assert.False(t, exists)

		exists, err = client.Exists(context.Background(), key, options.WithFileExtension("ext"))
		require.NoError(t, err)
		assert.True(t, exists)

		err = client.Del(context.Background(), key, options.WithFileExtension("ext"))
		require.NoError(t, err)
	})
}
