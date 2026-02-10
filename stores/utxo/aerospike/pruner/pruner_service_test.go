package pruner

import (
	"context"
	"testing"
	"time"

	"github.com/aerospike/aerospike-client-go/v8"
	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/blob/memory"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/uaerospike"
	aeroTest "github.com/bsv-blockchain/testcontainers-aerospike-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// createTestSettings creates default settings for testing
func createTestSettings() *settings.Settings {
	return &settings.Settings{
		UtxoStore: settings.UtxoStoreSettings{
			UtxoBatchSize: 128,
		},
		Pruner: settings.PrunerSettings{
			UTXODefensiveEnabled:       false,
			UTXODefensiveBatchReadSize: 10000,
			UTXOChunkGroupLimit:        10,
			UTXOProgressLogInterval:    30 * time.Second,
		},
	}
}

func TestCleanupServiceLogicWithoutProcessor(t *testing.T) {
	logger := ulogger.NewErrorTestLogger(t, func() {})
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

	// Create a mock index waiter that actually creates the index
	mockIndexWaiter := &MockIndexWaiter{
		Client:    client,
		Namespace: "test",
		Set:       "test",
	}

	opts := Options{
		Logger:        logger,
		Client:        client,
		ExternalStore: memory.New(),
		Namespace:     "test",
		Set:           "test",
		IndexWaiter:   mockIndexWaiter,
	}

	t.Run("Valid block height", func(t *testing.T) {
		service, err := NewService(createTestSettings(), opts)
		require.NoError(t, err)

		service.Start(ctx)

		// Wait for index to be ready
		time.Sleep(2 * time.Second)

		pruneCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		recordsProcessed, err := service.Prune(pruneCtx, 1, "<test-hash>")
		require.NoError(t, err)
		require.GreaterOrEqual(t, recordsProcessed, int64(0))
	})

	t.Run("Multiple block heights", func(t *testing.T) {
		service, err := NewService(createTestSettings(), opts)
		require.NoError(t, err)

		service.Start(ctx)

		// Wait for index to be ready
		time.Sleep(2 * time.Second)

		pruneCtx1, cancel1 := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel1()
		recordsProcessed, err := service.Prune(pruneCtx1, 1, "<test-hash>")
		require.NoError(t, err)
		require.GreaterOrEqual(t, recordsProcessed, int64(0))

		pruneCtx2, cancel2 := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel2()
		recordsProcessed, err = service.Prune(pruneCtx2, 2, "<test-hash>")
		require.NoError(t, err)
		require.GreaterOrEqual(t, recordsProcessed, int64(0))
	})

	t.Run("Sequential pruning operations", func(t *testing.T) {
		service, err := NewService(createTestSettings(), opts)
		require.NoError(t, err)

		service.Start(ctx)

		// Wait for index to be ready
		time.Sleep(2 * time.Second)

		// Prune at multiple heights sequentially
		for height := uint32(1); height <= 4; height++ {
			pruneCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			recordsProcessed, err := service.Prune(pruneCtx, height, "<test-hash>")
			cancel()
			require.NoError(t, err)
			require.GreaterOrEqual(t, recordsProcessed, int64(0))
		}
	})
}

func TestNewServiceValidation(t *testing.T) {
	logger := ulogger.NewErrorTestLogger(t, func() {})
	client := &uaerospike.Client{}

	mockIndexWaiter := &MockIndexWaiter{
		Client:    client,
		Namespace: "test",
		Set:       "test",
	}

	t.Run("Missing logger", func(t *testing.T) {
		opts := Options{
			Client:        client,
			ExternalStore: memory.New(),
			Namespace:     "test",
			Set:           "test",
			IndexWaiter:   mockIndexWaiter,
		}

		service, err := NewService(createTestSettings(), opts)
		assert.Error(t, err)
		assert.Nil(t, service)
	})

	t.Run("Missing client", func(t *testing.T) {
		opts := Options{
			Logger:        logger,
			ExternalStore: memory.New(),
			Namespace:     "test",
			Set:           "test",
			IndexWaiter:   mockIndexWaiter,
		}

		service, err := NewService(createTestSettings(), opts)
		assert.Error(t, err)
		assert.Nil(t, service)
	})

	t.Run("Missing external store", func(t *testing.T) {
		opts := Options{
			Logger:      logger,
			Client:      client,
			Namespace:   "test",
			Set:         "test",
			IndexWaiter: mockIndexWaiter,
		}

		service, err := NewService(createTestSettings(), opts)
		assert.Error(t, err)
		assert.Nil(t, service)
	})

	t.Run("Missing namespace", func(t *testing.T) {
		opts := Options{
			Logger:        logger,
			Client:        client,
			ExternalStore: memory.New(),
			Set:           "test",
			IndexWaiter:   mockIndexWaiter,
		}

		service, err := NewService(createTestSettings(), opts)
		assert.Error(t, err)
		assert.Nil(t, service)
	})

	t.Run("Missing set", func(t *testing.T) {
		opts := Options{
			Logger:        logger,
			Client:        client,
			ExternalStore: memory.New(),
			Namespace:     "test",
			IndexWaiter:   mockIndexWaiter,
		}

		service, err := NewService(createTestSettings(), opts)
		assert.Error(t, err)
		assert.Nil(t, service)
	})

	t.Run("Missing IndexWaiter", func(t *testing.T) {
		opts := Options{
			Logger:        logger,
			Client:        client,
			ExternalStore: memory.New(),
			Namespace:     "test",
			Set:           "test",
		}

		service, err := NewService(createTestSettings(), opts)
		assert.Error(t, err)
		assert.Nil(t, service)
	})
}

func TestServiceStartStop(t *testing.T) {
	logger := ulogger.NewErrorTestLogger(t, func() {})
	ctx := context.Background()

	container, err := aeroTest.RunContainer(ctx)
	require.NoError(t, err)

	host, err := container.Host(ctx)
	require.NoError(t, err)

	port, err := container.ServicePort(ctx)
	require.NoError(t, err)

	client, err := uaerospike.NewClient(host, port)
	require.NoError(t, err)

	mockIndexWaiter := &MockIndexWaiter{
		Client:    client,
		Namespace: "test",
		Set:       "test",
	}

	service, err := NewService(createTestSettings(), Options{
		Logger:        logger,
		Client:        client,
		ExternalStore: memory.New(),
		Namespace:     "test",
		Set:           "test",
		IndexWaiter:   mockIndexWaiter,
	})
	require.NoError(t, err)

	// Create a context with cancel
	ctx, cancel := context.WithCancel(context.Background())

	// Start the service (this will create the index in a goroutine)
	service.Start(ctx)

	// Wait a bit for the index to be ready
	time.Sleep(100 * time.Millisecond)

	// Cancel the context
	cancel()

	// The service doesn't have a Stop method anymore - cancelling the context is sufficient
}

func TestDeleteAtHeight(t *testing.T) {
	logger := ulogger.New("test")
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

	// Create a test namespace and set
	namespace := "test"
	set := "test"

	// Create a mock index waiter that actually creates the index
	mockIndexWaiter := &MockIndexWaiter{
		Client:    client,
		Namespace: namespace,
		Set:       set,
	}

	// Create a cleanup service
	service, err := NewService(createTestSettings(), Options{
		Logger:        logger,
		Client:        client,
		ExternalStore: memory.New(),
		Namespace:     namespace,
		Set:           set,
		IndexWaiter:   mockIndexWaiter,
	})
	require.NoError(t, err)

	// Start the service (this will create the index and start the job manager)
	service.Start(ctx)

	// Wait for index to be ready
	time.Sleep(2 * time.Second)

	// Create some test records
	writePolicy := aerospike.NewWritePolicy(0, 0)

	txIDParent := chainhash.HashH([]byte("parent"))
	keySourceParent := uaerospike.CalculateKeySource(&txIDParent, 0, 128)
	keyParent, _ := aerospike.NewKey(namespace, set, keySourceParent)

	txID1 := chainhash.HashH([]byte("test1"))
	key1, _ := aerospike.NewKey(namespace, set, txID1[:])

	txID2Parent := chainhash.HashH([]byte("parent2"))
	keySourceParent2 := uaerospike.CalculateKeySource(&txID2Parent, 0, 128)
	keyParent2, _ := aerospike.NewKey(namespace, set, keySourceParent2)

	txID2 := chainhash.HashH([]byte("test2"))
	key2, _ := aerospike.NewKey(namespace, set, txID2[:])

	input1 := &bt.Input{
		PreviousTxOutIndex: 0,
		PreviousTxSatoshis: 100,
	}
	_ = input1.PreviousTxIDAdd(&txIDParent)

	input2 := &bt.Input{
		PreviousTxOutIndex: 0,
		PreviousTxSatoshis: 200,
	}
	_ = input2.PreviousTxIDAdd(&txID2Parent)

	// create parent record that should be marked before deletion of child
	err = client.Put(writePolicy, keyParent, aerospike.BinMap{
		fields.TxID.String():           txIDParent.CloneBytes(),
		fields.DeleteAtHeight.String(): 0,
	})
	require.NoError(t, err)

	// Create record 1 with deleteAtHeight = 0 (not to be deleted)
	err = client.Put(writePolicy, key1, aerospike.BinMap{
		fields.TxID.String():           txID1.CloneBytes(),
		fields.Inputs.String():         []interface{}{input1.Bytes(true)},
		fields.DeleteAtHeight.String(): 0,
	})
	require.NoError(t, err)

	// Create record 2 with deleteAtHeight = 0 (not to be deleted)
	err = client.Put(writePolicy, key2, aerospike.BinMap{
		fields.TxID.String():           txID2.CloneBytes(),
		fields.Inputs.String():         []interface{}{input2.Bytes(true)},
		fields.DeleteAtHeight.String(): 0,
	})
	require.NoError(t, err)

	// Verify the records were created
	record, err := client.Get(nil, key1)
	require.NoError(t, err)
	assert.NotNil(t, record)

	record, err = client.Get(nil, key2)
	require.NoError(t, err)
	assert.NotNil(t, record)

	// Run prune synchronously
	ctx1, cancel1 := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel1()

	recordsProcessed, err := service.Prune(ctx1, 1, "<test-hash>")
	require.NoError(t, err)
	require.GreaterOrEqual(t, recordsProcessed, int64(0))

	// Verify the record was not deleted
	record, err = client.Get(nil, key1)
	assert.NoError(t, err)
	assert.NotNil(t, record)

	// Update record 1 with deleteAtHeight = 3
	err = client.Put(writePolicy, key1, aerospike.BinMap{
		fields.DeleteAtHeight.String(): 3,
	})
	require.NoError(t, err)

	// Update record 2 with deleteAtHeight = 4
	err = client.Put(writePolicy, key2, aerospike.BinMap{
		fields.DeleteAtHeight.String(): 4,
	})
	require.NoError(t, err)

	record, err = client.Get(nil, key1)
	require.NoError(t, err)
	assert.NotNil(t, record)
	assert.Equal(t, 3, record.Bins[fields.DeleteAtHeight.String()])

	record, err = client.Get(nil, key2)
	require.NoError(t, err)
	assert.NotNil(t, record)
	assert.Equal(t, 4, record.Bins[fields.DeleteAtHeight.String()])

	// Run prune synchronously for height 2
	ctx2, cancel2 := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel2()

	recordsProcessed, err = service.Prune(ctx2, 2, "<test-hash>")
	require.NoError(t, err)
	require.GreaterOrEqual(t, recordsProcessed, int64(0))

	// Verify the record was not deleted
	record, err = client.Get(nil, key1)
	assert.NoError(t, err)
	assert.NotNil(t, record)

	// Run prune synchronously for height 3
	ctx3, cancel3 := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel3()

	recordsProcessed, err = service.Prune(ctx3, 3, "<test-hash>")
	require.NoError(t, err)
	require.GreaterOrEqual(t, recordsProcessed, int64(0))

	// Verify the record1 was deleted
	record, err = client.Get(nil, key1)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not found")
	assert.Nil(t, record)

	// Verify the record2 was not deleted
	record, err = client.Get(nil, key2)
	require.NoError(t, err)
	assert.NotNil(t, record)

	// verify that the parent record was marked
	record, err = client.Get(nil, keyParent)
	require.NoError(t, err)
	assert.NotNil(t, record)
	assert.Equal(t, map[interface{}]interface{}{
		txID1.String(): true,
	}, record.Bins[fields.DeletedChildren.String()])

	// Run prune synchronously for height 4
	ctx4, cancel4 := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel4()

	recordsProcessed, err = service.Prune(ctx4, 4, "<test-hash>")
	require.NoError(t, err)
	require.GreaterOrEqual(t, recordsProcessed, int64(0))

	// Verify the record2 was deleted
	record, err = client.Get(nil, key2)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not found")
	assert.Nil(t, record)

	// verify that the parent2 record was not created
	record, err = client.Get(nil, keyParent2)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not found")
	assert.Nil(t, record)
}

func TestOptionsSimple(t *testing.T) {
	logger := ulogger.NewVerboseTestLogger(t)
	client := &uaerospike.Client{} // dummy client
	mockIndexWaiter := &MockIndexWaiter{
		Client:    client,
		Namespace: "test",
		Set:       "test",
	}

	t.Run("Default options struct fields", func(t *testing.T) {
		opts := Options{}
		assert.Nil(t, opts.Logger)
		assert.Nil(t, opts.Client)
		assert.Nil(t, opts.IndexWaiter)
		assert.Equal(t, "", opts.Namespace)
		assert.Equal(t, "", opts.Set)
	})

	t.Run("Populated options struct fields", func(t *testing.T) {
		opts := Options{
			Logger:        logger,
			Client:        client,
			ExternalStore: memory.New(),
			IndexWaiter:   mockIndexWaiter,
			Namespace:     "ns",
			Set:           "set",
		}
		assert.Equal(t, logger, opts.Logger)
		assert.Equal(t, client, opts.Client)
		assert.Equal(t, mockIndexWaiter, opts.IndexWaiter)
		assert.Equal(t, "ns", opts.Namespace)
		assert.Equal(t, "set", opts.Set)
	})
}

func TestServiceSimple(t *testing.T) {
	logger := ulogger.NewVerboseTestLogger(t)
	client := &uaerospike.Client{} // dummy client
	mockIndexWaiter := &MockIndexWaiter{
		Client:    client,
		Namespace: "test",
		Set:       "test",
	}

	t.Run("Service creation with valid options", func(t *testing.T) {
		opts := Options{
			Logger:        logger,
			Client:        client,
			ExternalStore: memory.New(),
			IndexWaiter:   mockIndexWaiter,
			Namespace:     "ns",
			Set:           "set",
		}

		service, err := NewService(createTestSettings(), opts)
		assert.NoError(t, err)
		assert.NotNil(t, service)
		assert.Equal(t, logger, service.logger)
		assert.Equal(t, client, service.client)
		assert.Equal(t, "ns", service.namespace)
		assert.Equal(t, "set", service.set)
	})

	t.Run("Service creation fails with missing required options", func(t *testing.T) {
		opts := Options{}
		service, err := NewService(createTestSettings(), opts)
		assert.Error(t, err)
		assert.Nil(t, service)
	})
}

// TestCleanupWithBlockPersisterCoordination tests cleanup coordination with block persister
func TestCleanupWithBlockPersisterCoordination(t *testing.T) {
	t.Run("BlockPersisterBehind_LimitsCleanup", func(t *testing.T) {
		logger := ulogger.NewErrorTestLogger(t, func() {})
		ctx := context.Background()

		container, err := aeroTest.RunContainer(ctx)
		require.NoError(t, err)
		defer func() {
			_ = container.Terminate(ctx)
		}()

		host, err := container.Host(ctx)
		require.NoError(t, err)

		port, err := container.ServicePort(ctx)
		require.NoError(t, err)

		client, err := uaerospike.NewClient(host, port)
		require.NoError(t, err)
		defer client.Close()

		tSettings := createTestSettings()
		tSettings.GlobalBlockHeightRetention = 100

		// Simulate block persister at height 50
		persistedHeight := uint32(50)
		getPersistedHeight := func() uint32 {
			return persistedHeight
		}

		indexWaiter := &MockIndexWaiter{
			Client:    client,
			Namespace: "test",
			Set:       "transactions",
		}
		external := memory.New()

		service, err := NewService(tSettings, Options{
			Logger:             logger,
			Ctx:                ctx,
			IndexWaiter:        indexWaiter,
			Client:             client,
			ExternalStore:      external,
			Namespace:          "test",
			Set:                "transactions",
			GetPersistedHeight: getPersistedHeight,
		})
		require.NoError(t, err)

		// Trigger cleanup at height 200
		// Expected: Limited to 50 + 100 = 150 (not 200)
		service.Start(ctx)

		// Wait for index to be ready
		time.Sleep(2 * time.Second)

		// Add logging to verify safe height calculation
		pruneCtx, pruneCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer pruneCancel()

		recordsProcessed, err := service.Prune(pruneCtx, 200, "<test-hash>")
		require.NoError(t, err)
		assert.GreaterOrEqual(t, recordsProcessed, int64(0))

		// Note: Actual verification would require checking logs for "Limiting cleanup" message
		// or querying which records were actually deleted
	})

	t.Run("BlockPersisterCaughtUp_NoLimitation", func(t *testing.T) {
		logger := ulogger.NewErrorTestLogger(t, func() {})
		ctx := context.Background()

		container, err := aeroTest.RunContainer(ctx)
		require.NoError(t, err)
		defer func() {
			_ = container.Terminate(ctx)
		}()

		host, err := container.Host(ctx)
		require.NoError(t, err)

		port, err := container.ServicePort(ctx)
		require.NoError(t, err)

		client, err := uaerospike.NewClient(host, port)
		require.NoError(t, err)
		defer client.Close()

		tSettings := createTestSettings()
		tSettings.GlobalBlockHeightRetention = 100

		// Block persister caught up at height 150
		getPersistedHeight := func() uint32 {
			return uint32(150)
		}

		indexWaiter := &MockIndexWaiter{
			Client:    client,
			Namespace: "test",
			Set:       "transactions",
		}
		external := memory.New()

		service, err := NewService(tSettings, Options{
			Logger:             logger,
			Ctx:                ctx,
			IndexWaiter:        indexWaiter,
			Client:             client,
			ExternalStore:      external,
			Namespace:          "test",
			Set:                "transactions",
			GetPersistedHeight: getPersistedHeight,
		})
		require.NoError(t, err)

		// Trigger cleanup at height 200
		// Expected: No limitation (150 + 100 = 250 > 200)
		service.Start(ctx)

		// Wait for index to be ready
		time.Sleep(2 * time.Second)

		pruneCtx, pruneCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer pruneCancel()

		recordsProcessed, err := service.Prune(pruneCtx, 200, "<test-hash>")
		require.NoError(t, err)
		assert.GreaterOrEqual(t, recordsProcessed, int64(0))
	})

	t.Run("BlockPersisterNotRunning_HeightZero", func(t *testing.T) {
		logger := ulogger.NewErrorTestLogger(t, func() {})
		ctx := context.Background()

		container, err := aeroTest.RunContainer(ctx)
		require.NoError(t, err)
		defer func() {
			_ = container.Terminate(ctx)
		}()

		host, err := container.Host(ctx)
		require.NoError(t, err)

		port, err := container.ServicePort(ctx)
		require.NoError(t, err)

		client, err := uaerospike.NewClient(host, port)
		require.NoError(t, err)
		defer client.Close()

		tSettings := createTestSettings()

		// Block persister not running - returns 0
		getPersistedHeight := func() uint32 {
			return uint32(0)
		}

		indexWaiter := &MockIndexWaiter{
			Client:    client,
			Namespace: "test",
			Set:       "transactions",
		}
		external := memory.New()

		service, err := NewService(tSettings, Options{
			Logger:             logger,
			Ctx:                ctx,
			IndexWaiter:        indexWaiter,
			Client:             client,
			ExternalStore:      external,
			Namespace:          "test",
			Set:                "transactions",
			GetPersistedHeight: getPersistedHeight,
		})
		require.NoError(t, err)

		// Cleanup should proceed normally (no limitation when height = 0)
		service.Start(ctx)

		// Wait for index to be ready
		time.Sleep(2 * time.Second)

		pruneCtx, pruneCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer pruneCancel()

		recordsProcessed, err := service.Prune(pruneCtx, 100, "<test-hash>")
		require.NoError(t, err)
		assert.GreaterOrEqual(t, recordsProcessed, int64(0))
	})

	t.Run("NoGetPersistedHeightFunction_NormalCleanup", func(t *testing.T) {
		logger := ulogger.NewErrorTestLogger(t, func() {})
		ctx := context.Background()

		container, err := aeroTest.RunContainer(ctx)
		require.NoError(t, err)
		defer func() {
			_ = container.Terminate(ctx)
		}()

		host, err := container.Host(ctx)
		require.NoError(t, err)

		port, err := container.ServicePort(ctx)
		require.NoError(t, err)

		client, err := uaerospike.NewClient(host, port)
		require.NoError(t, err)
		defer client.Close()

		tSettings := createTestSettings()

		indexWaiter := &MockIndexWaiter{
			Client:    client,
			Namespace: "test",
			Set:       "transactions",
		}
		external := memory.New()

		// Create service WITHOUT getPersistedHeight
		service, err := NewService(tSettings, Options{
			Logger:             logger,
			Ctx:                ctx,
			IndexWaiter:        indexWaiter,
			Client:             client,
			ExternalStore:      external,
			Namespace:          "test",
			Set:                "transactions",
			GetPersistedHeight: nil, // Not set
		})
		require.NoError(t, err)

		// Cleanup should proceed normally
		service.Start(ctx)

		// Wait for index to be ready
		time.Sleep(2 * time.Second)

		pruneCtx, pruneCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer pruneCancel()

		recordsProcessed, err := service.Prune(pruneCtx, 100, "<test-hash>")
		require.NoError(t, err)
		assert.GreaterOrEqual(t, recordsProcessed, int64(0))
	})
}

// TestSetPersistedHeightGetter tests the setter method
func TestSetPersistedHeightGetter(t *testing.T) {
	logger := ulogger.NewErrorTestLogger(t, func() {})
	ctx := context.Background()

	container, err := aeroTest.RunContainer(ctx)
	require.NoError(t, err)
	defer func() {
		_ = container.Terminate(ctx)
	}()

	host, err := container.Host(ctx)
	require.NoError(t, err)

	port, err := container.ServicePort(ctx)
	require.NoError(t, err)

	client, err := uaerospike.NewClient(host, port)
	require.NoError(t, err)
	defer client.Close()

	tSettings := createTestSettings()
	tSettings.GlobalBlockHeightRetention = 50

	indexWaiter := &MockIndexWaiter{
		Client:    client,
		Namespace: "test",
		Set:       "transactions",
	}
	external := memory.New()

	// Create service without getter initially
	service, err := NewService(tSettings, Options{
		Logger:        logger,
		Ctx:           ctx,
		IndexWaiter:   indexWaiter,
		Client:        client,
		ExternalStore: external,
		Namespace:     "test",
		Set:           "transactions",
	})
	require.NoError(t, err)

	// Set the getter after creation
	persistedHeight := uint32(100)
	service.SetPersistedHeightGetter(func() uint32 {
		return persistedHeight
	})

	// Verify it's used (cleanup at 200 should be limited to 100+50=150)
	service.Start(ctx)

	// Wait for index to be ready
	time.Sleep(2 * time.Second)

	pruneCtx, pruneCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer pruneCancel()

	recordsProcessed, err := service.Prune(pruneCtx, 200, "<test-hash>")
	require.NoError(t, err)
	assert.GreaterOrEqual(t, recordsProcessed, int64(0))
}
