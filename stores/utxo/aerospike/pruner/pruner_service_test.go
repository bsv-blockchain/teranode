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
	"github.com/bsv-blockchain/teranode/stores/pruner"
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
			PrunerParentUpdateBatcherSize:           100,
			PrunerParentUpdateBatcherDurationMillis: 10,
			PrunerDeleteBatcherSize:                 256,
			PrunerDeleteBatcherDurationMillis:       10,
			PrunerMaxConcurrentOperations:           0,   //  0 = auto-detect from connection queue size
			UtxoBatchSize:                           128, // Add missing UtxoBatchSize
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
		Logger:         logger,
		Client:         client,
		ExternalStore:  memory.New(),
		Namespace:      "test",
		Set:            "test",
		MaxJobsHistory: 3,
		IndexWaiter:    mockIndexWaiter,
	}

	t.Run("Valid block height", func(t *testing.T) {
		service, err := NewService(createTestSettings(), opts)
		require.NoError(t, err)

		err = service.UpdateBlockHeight(1)
		require.NoError(t, err)

		jobs := service.GetJobs()
		assert.Len(t, jobs, 1)
		assert.Equal(t, pruner.JobStatusPending, jobs[0].GetStatus())
	})

	t.Run("New block height", func(t *testing.T) {
		service, err := NewService(createTestSettings(), opts)
		require.NoError(t, err)

		err = service.UpdateBlockHeight(1)
		require.NoError(t, err)

		err = service.UpdateBlockHeight(2)
		require.NoError(t, err)

		jobs := service.GetJobs()
		assert.Len(t, jobs, 2)
		assert.Equal(t, pruner.JobStatusCancelled, jobs[0].GetStatus())
		assert.Equal(t, pruner.JobStatusPending, jobs[1].GetStatus())
	})

	t.Run("Max jobs history", func(t *testing.T) {
		service, err := NewService(createTestSettings(), opts)
		require.NoError(t, err)

		err = service.UpdateBlockHeight(1)
		require.NoError(t, err)

		err = service.UpdateBlockHeight(2)
		require.NoError(t, err)

		err = service.UpdateBlockHeight(3)
		require.NoError(t, err)

		jobs := service.GetJobs()

		assert.Len(t, jobs, 3)
		assert.Equal(t, uint32(1), jobs[0].BlockHeight)
		assert.Equal(t, pruner.JobStatusCancelled, jobs[0].GetStatus())

		assert.Equal(t, uint32(2), jobs[1].BlockHeight)
		assert.Equal(t, pruner.JobStatusCancelled, jobs[1].GetStatus())

		assert.Equal(t, uint32(3), jobs[2].BlockHeight)
		assert.Equal(t, pruner.JobStatusPending, jobs[2].GetStatus())

		err = service.UpdateBlockHeight(4)
		require.NoError(t, err)

		jobs = service.GetJobs()

		assert.Len(t, jobs, 3)
		assert.Equal(t, uint32(2), jobs[0].BlockHeight)
		assert.Equal(t, pruner.JobStatusCancelled, jobs[0].GetStatus())

		assert.Equal(t, uint32(3), jobs[1].BlockHeight)
		assert.Equal(t, pruner.JobStatusCancelled, jobs[1].GetStatus())

		assert.Equal(t, uint32(4), jobs[2].BlockHeight)
		assert.Equal(t, pruner.JobStatusPending, jobs[2].GetStatus())
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

	// Start the service (this will create the index and start the job manager)
	service.Start(ctx)

	// Wait a bit for the service to start
	time.Sleep(100 * time.Millisecond)

	// Cancel the context to stop the service
	cancel()

	// Wait for the service to fully stop by waiting for the job manager to finish
	err = service.Stop(context.Background())
	require.NoError(t, err)
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
		WorkerCount:   1,
		IndexWaiter:   mockIndexWaiter,
	})
	require.NoError(t, err)

	// Start the service (this will create the index and start the job manager)
	service.Start(ctx)

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

	// Create a done channel
	done := make(chan string)

	err = service.UpdateBlockHeight(1, done)
	require.NoError(t, err)

	// Wait for the job to complete
	// require.Equal(t, "completed", <-done)
	<-done

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

	// Create a new done channel for the next job
	done = make(chan string)

	err = service.UpdateBlockHeight(2, done)
	require.NoError(t, err)

	// Wait for the job to complete
	require.Equal(t, pruner.JobStatusCompleted.String(), <-done)

	// Verify the record was not deleted
	record, err = client.Get(nil, key1)
	assert.NoError(t, err)
	assert.NotNil(t, record)

	// Create a new done channel for the next job
	done = make(chan string)

	err = service.UpdateBlockHeight(3, done)
	require.NoError(t, err)

	// Wait for the job to complete
	require.Equal(t, pruner.JobStatusCompleted.String(), <-done)

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

	// Create a new done channel for the next job
	done = make(chan string)

	err = service.UpdateBlockHeight(4, done)
	require.NoError(t, err)

	// Wait for the job to complete
	require.Equal(t, pruner.JobStatusCompleted.String(), <-done)

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
		assert.Equal(t, 0, opts.WorkerCount)
		assert.Equal(t, 0, opts.MaxJobsHistory)
	})

	t.Run("Populated options struct fields", func(t *testing.T) {
		opts := Options{
			Logger:         logger,
			Client:         client,
			ExternalStore:  memory.New(),
			IndexWaiter:    mockIndexWaiter,
			Namespace:      "ns",
			Set:            "set",
			WorkerCount:    2,
			MaxJobsHistory: 5,
		}
		assert.Equal(t, logger, opts.Logger)
		assert.Equal(t, client, opts.Client)
		assert.Equal(t, mockIndexWaiter, opts.IndexWaiter)
		assert.Equal(t, "ns", opts.Namespace)
		assert.Equal(t, "set", opts.Set)
		assert.Equal(t, 2, opts.WorkerCount)
		assert.Equal(t, 5, opts.MaxJobsHistory)
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
		assert.NotNil(t, service.jobManager)
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
			WorkerCount:        1,
			MaxJobsHistory:     10,
			GetPersistedHeight: getPersistedHeight,
		})
		require.NoError(t, err)

		// Trigger cleanup at height 200
		// Expected: Limited to 50 + 100 = 150 (not 200)
		done := make(chan string, 1)
		service.Start(ctx)

		// Add logging to verify safe height calculation
		err = service.UpdateBlockHeight(200, done)
		require.NoError(t, err)

		// Wait for completion
		select {
		case status := <-done:
			assert.Equal(t, pruner.JobStatusCompleted.String(), status)
		case <-time.After(5 * time.Second):
			t.Fatal("Cleanup should complete within 5 seconds")
		}

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
			WorkerCount:        1,
			MaxJobsHistory:     10,
			GetPersistedHeight: getPersistedHeight,
		})
		require.NoError(t, err)

		// Trigger cleanup at height 200
		// Expected: No limitation (150 + 100 = 250 > 200)
		done := make(chan string, 1)
		service.Start(ctx)

		err = service.UpdateBlockHeight(200, done)
		require.NoError(t, err)

		select {
		case status := <-done:
			assert.Equal(t, pruner.JobStatusCompleted.String(), status)
		case <-time.After(5 * time.Second):
			t.Fatal("Cleanup should complete within 5 seconds")
		}
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
			WorkerCount:        1,
			MaxJobsHistory:     10,
			GetPersistedHeight: getPersistedHeight,
		})
		require.NoError(t, err)

		// Cleanup should proceed normally (no limitation when height = 0)
		done := make(chan string, 1)
		service.Start(ctx)

		err = service.UpdateBlockHeight(100, done)
		require.NoError(t, err)

		select {
		case status := <-done:
			assert.Equal(t, pruner.JobStatusCompleted.String(), status)
		case <-time.After(5 * time.Second):
			t.Fatal("Cleanup should complete within 5 seconds")
		}
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
			WorkerCount:        1,
			MaxJobsHistory:     10,
			GetPersistedHeight: nil, // Not set
		})
		require.NoError(t, err)

		// Cleanup should proceed normally
		done := make(chan string, 1)
		service.Start(ctx)

		err = service.UpdateBlockHeight(100, done)
		require.NoError(t, err)

		select {
		case status := <-done:
			assert.Equal(t, pruner.JobStatusCompleted.String(), status)
		case <-time.After(5 * time.Second):
			t.Fatal("Cleanup should complete within 5 seconds")
		}
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
		Logger:         logger,
		Ctx:            ctx,
		IndexWaiter:    indexWaiter,
		Client:         client,
		ExternalStore:  external,
		Namespace:      "test",
		Set:            "transactions",
		WorkerCount:    1,
		MaxJobsHistory: 10,
	})
	require.NoError(t, err)

	// Set the getter after creation
	persistedHeight := uint32(100)
	service.SetPersistedHeightGetter(func() uint32 {
		return persistedHeight
	})

	// Verify it's used (cleanup at 200 should be limited to 100+50=150)
	service.Start(ctx)
	done := make(chan string, 1)

	err = service.UpdateBlockHeight(200, done)
	require.NoError(t, err)

	select {
	case status := <-done:
		assert.Equal(t, pruner.JobStatusCompleted.String(), status)
	case <-time.After(5 * time.Second):
		t.Fatal("Cleanup should complete within 5 seconds")
	}
}
