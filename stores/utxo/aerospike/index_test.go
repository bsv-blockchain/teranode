package aerospike

import (
	"context"
	"testing"
	"time"

	"github.com/aerospike/aerospike-client-go/v8"
	"github.com/bsv-blockchain/teranode/stores/utxo/aerospike/pruner"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/uaerospike"
	aeroTest "github.com/bsv-blockchain/testcontainers-aerospike-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCreateIndexIfNotExists(t *testing.T) {
	logger := ulogger.NewVerboseTestLogger(t)
	ctx := context.Background()

	container, err := aeroTest.RunContainer(ctx)
	require.NoError(t, err)

	t.Cleanup(func() {
		err = container.Terminate(ctx)
		require.NoError(t, err)
	})

	host, err := container.Host(ctx)
	require.NoError(t, err)

	port, err := container.ServicePort(ctx)
	require.NoError(t, err)

	client, err := uaerospike.NewClient(host, port)
	require.NoError(t, err)

	// Create a minimal Store instance for testing
	store := &Store{
		logger:    logger,
		client:    client,
		namespace: "test",
		setName:   "test",
	}

	t.Run("Create index and verify existence", func(t *testing.T) {
		// Check that index doesn't exist initially
		exists, err := store.indexExists("test_index")
		require.NoError(t, err)
		assert.False(t, exists)

		// Create the index
		err = store.CreateIndexIfNotExists(t.Context(), "test_index", "test-bin", aerospike.NUMERIC)
		require.NoError(t, err)

		// Wait for the index to be built
		err = store.waitForIndexReady(t.Context(), "test_index")
		require.NoError(t, err)

		// Verify the index exists
		exists, err = store.indexExists("test_index")
		require.NoError(t, err)
		assert.True(t, exists)
	})

	t.Run("Create index that already exists", func(t *testing.T) {
		// Create the index again (should not error)
		err = store.CreateIndexIfNotExists(t.Context(), "test_index", "test-bin", aerospike.NUMERIC)
		require.NoError(t, err)
	})
}

func TestWaitForIndexReady(t *testing.T) {
	logger := ulogger.NewVerboseTestLogger(t)
	ctx := context.Background()

	container, err := aeroTest.RunContainer(ctx)
	require.NoError(t, err)

	t.Cleanup(func() {
		err = container.Terminate(ctx)
		require.NoError(t, err)
	})

	host, err := container.Host(ctx)
	require.NoError(t, err)

	port, err := container.ServicePort(ctx)
	require.NoError(t, err)

	client, err := uaerospike.NewClient(host, port)
	require.NoError(t, err)

	// Create a minimal Store instance for testing
	store := &Store{
		logger:    logger,
		client:    client,
		namespace: "test",
		setName:   "test",
	}

	t.Run("Wait for index times out", func(t *testing.T) {
		// Create a context with a short timeout
		ctx2, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
		defer cancel()

		// Try to wait for a non-existent index
		err = store.waitForIndexReady(ctx2, "doesnotexist")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "context deadline exceeded")
	})

	t.Run("Wait for index succeeds", func(t *testing.T) {
		// Create an index
		indexName := "test_wait_index"
		writePolicy := aerospike.NewWritePolicy(0, 0)
		_, err := client.CreateIndex(writePolicy, "test", "test", indexName, fields.DeleteAtHeight.String(), aerospike.NUMERIC)
		require.NoError(t, err)

		// Wait for the index to be built with a reasonable timeout
		ctx2, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		if err := store.waitForIndexReady(ctx2, indexName); err != nil {
			t.Fatalf("Failed to wait for index: %v", err)
		}
	})
}

func TestIndexWaiterAdapter(t *testing.T) {
	logger := ulogger.NewVerboseTestLogger(t)
	ctx := context.Background()

	container, err := aeroTest.RunContainer(ctx)
	require.NoError(t, err)

	t.Cleanup(func() {
		err = container.Terminate(ctx)
		require.NoError(t, err)
	})

	host, err := container.Host(ctx)
	require.NoError(t, err)

	port, err := container.ServicePort(ctx)
	require.NoError(t, err)

	client, err := uaerospike.NewClient(host, port)
	require.NoError(t, err)

	// Create a minimal Store instance for testing
	store := &Store{
		logger:    logger,
		client:    client,
		namespace: "test",
		setName:   "test",
	}

	// Verify the Store implements the IndexWaiter interface
	var indexWaiter pruner.IndexWaiter = store

	assert.NotNil(t, indexWaiter)

	t.Run("IndexWaiter interface implementation", func(t *testing.T) {
		// Create an index
		indexName := "test_adapter_index"
		writePolicy := aerospike.NewWritePolicy(0, 0)
		_, err := client.CreateIndex(writePolicy, "test", "test", indexName, fields.DeleteAtHeight.String(), aerospike.NUMERIC)
		require.NoError(t, err)

		// Use the interface method to wait for the index
		ctx2, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		if err := indexWaiter.WaitForIndexReady(ctx2, indexName); err != nil {
			t.Fatalf("Failed to wait for index using interface: %v", err)
		}
	})
}
