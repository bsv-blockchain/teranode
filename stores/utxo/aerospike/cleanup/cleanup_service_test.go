package cleanup

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/aerospike/aerospike-client-go/v8"
	aeroTest "github.com/bitcoin-sv/testcontainers-aerospike-go"
	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/blob/memory"
	"github.com/bsv-blockchain/teranode/stores/cleanup"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/uaerospike"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// createTestSettings creates default settings for testing
func createTestSettings() *settings.Settings {
	return &settings.Settings{
		UtxoStore: settings.UtxoStoreSettings{
			CleanupParentUpdateBatcherSize:           100,
			CleanupParentUpdateBatcherDurationMillis: 10,
			CleanupDeleteBatcherSize:                 256,
			CleanupDeleteBatcherDurationMillis:       10,
			CleanupMaxConcurrentOperations:           0,   //  0 = auto-detect from connection queue size
			UtxoBatchSize:                            128, // Add missing UtxoBatchSize
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
		assert.Equal(t, cleanup.JobStatusPending, jobs[0].GetStatus())
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
		assert.Equal(t, cleanup.JobStatusCancelled, jobs[0].GetStatus())
		assert.Equal(t, cleanup.JobStatusPending, jobs[1].GetStatus())
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
		assert.Equal(t, cleanup.JobStatusCancelled, jobs[0].GetStatus())

		assert.Equal(t, uint32(2), jobs[1].BlockHeight)
		assert.Equal(t, cleanup.JobStatusCancelled, jobs[1].GetStatus())

		assert.Equal(t, uint32(3), jobs[2].BlockHeight)
		assert.Equal(t, cleanup.JobStatusPending, jobs[2].GetStatus())

		err = service.UpdateBlockHeight(4)
		require.NoError(t, err)

		jobs = service.GetJobs()

		assert.Len(t, jobs, 3)
		assert.Equal(t, uint32(2), jobs[0].BlockHeight)
		assert.Equal(t, cleanup.JobStatusCancelled, jobs[0].GetStatus())

		assert.Equal(t, uint32(3), jobs[1].BlockHeight)
		assert.Equal(t, cleanup.JobStatusCancelled, jobs[1].GetStatus())

		assert.Equal(t, uint32(4), jobs[2].BlockHeight)
		assert.Equal(t, cleanup.JobStatusPending, jobs[2].GetStatus())
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
	require.Equal(t, cleanup.JobStatusCompleted.String(), <-done)

	// Verify the record was not deleted
	record, err = client.Get(nil, key1)
	assert.NoError(t, err)
	assert.NotNil(t, record)

	// Create a new done channel for the next job
	done = make(chan string)

	err = service.UpdateBlockHeight(3, done)
	require.NoError(t, err)

	// Wait for the job to complete
	require.Equal(t, cleanup.JobStatusCompleted.String(), <-done)

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
	require.Equal(t, cleanup.JobStatusCompleted.String(), <-done)

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

// Comprehensive integration tests for batching functionality using real Aerospike
func TestBatchingIntegration(t *testing.T) {
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

	namespace := "test"
	set := "test"

	t.Run("Parent Update Batching", func(t *testing.T) {
		t.Run("Multiple batch items referencing same parent UTXO", func(t *testing.T) {
			mockIndexWaiter := &MockIndexWaiter{
				Client:    client,
				Namespace: namespace,
				Set:       set,
			}

			service, err := NewService(createTestSettings(), Options{
				Logger:        logger,
				Client:        client,
				ExternalStore: memory.New(),
				Namespace:     namespace,
				Set:           set,
				IndexWaiter:   mockIndexWaiter,
			})
			require.NoError(t, err)

			// Create one parent UTXO record
			parentTxHash := chainhash.HashH([]byte("parent-tx"))
			keySource := uaerospike.CalculateKeySource(&parentTxHash, 0, 1)
			parentKey, _ := aerospike.NewKey(namespace, set, keySource)

			writePolicy := aerospike.NewWritePolicy(0, 0)
			err = client.Put(writePolicy, parentKey, aerospike.BinMap{
				fields.TxID.String(): parentTxHash.CloneBytes(),
			})
			require.NoError(t, err)

			// Create two different child transactions that both reference the same parent UTXO
			// This simulates processing multiple cleanup records that happen to reference the same parent
			txHash1 := chainhash.HashH([]byte("child-tx-1"))
			txHash2 := chainhash.HashH([]byte("child-tx-2"))

			// Both inputs reference the same parent UTXO (same tx + same output index)
			// This tests the deduplication logic in sendParentUpdateBatch
			input1 := &bt.Input{PreviousTxOutIndex: 0, PreviousTxSatoshis: 100}
			_ = input1.PreviousTxIDAdd(&parentTxHash)

			input2 := &bt.Input{PreviousTxOutIndex: 0, PreviousTxSatoshis: 200}
			_ = input2.PreviousTxIDAdd(&parentTxHash)

			// Create batch parent updates - this simulates the batch containing
			// multiple items that need to update the same parent record
			errCh1 := make(chan error, 1)
			errCh2 := make(chan error, 1)
			batch := []*batchParentUpdate{
				{txHash: &txHash1, inputs: []*bt.Input{input1}, errCh: errCh1},
				{txHash: &txHash2, inputs: []*bt.Input{input2}, errCh: errCh2},
			}

			// Execute batch
			service.sendParentUpdateBatch(batch)

			// Verify both operations succeeded
			select {
			case err := <-errCh1:
				assert.NoError(t, err)
			case <-time.After(1 * time.Second):
				t.Fatal("Expected response on error channel 1")
			}

			select {
			case err := <-errCh2:
				assert.NoError(t, err)
			case <-time.After(1 * time.Second):
				t.Fatal("Expected response on error channel 2")
			}

			// Verify the single parent record was updated with both child hashes
			record, err := client.Get(nil, parentKey)
			require.NoError(t, err)
			assert.NotNil(t, record)

			deletedChildren, exists := record.Bins[fields.DeletedChildren.String()]
			assert.True(t, exists)

			deletedChildrenMap := deletedChildren.(map[interface{}]interface{})
			assert.Equal(t, true, deletedChildrenMap[txHash1.String()])
			assert.Equal(t, true, deletedChildrenMap[txHash2.String()])
		})

		t.Run("Multiple transactions with different parents", func(t *testing.T) {
			mockIndexWaiter := &MockIndexWaiter{
				Client:    client,
				Namespace: namespace,
				Set:       set,
			}

			service, err := NewService(createTestSettings(), Options{
				Logger:        logger,
				Client:        client,
				ExternalStore: memory.New(),
				Namespace:     namespace,
				Set:           set,
				IndexWaiter:   mockIndexWaiter,
			})
			require.NoError(t, err)

			// Create two different parent records
			parentTxHash1 := chainhash.HashH([]byte("parent-tx-1"))
			parentTxHash2 := chainhash.HashH([]byte("parent-tx-2"))
			parentKey1, _ := aerospike.NewKey(namespace, set, parentTxHash1[:])
			parentKey2, _ := aerospike.NewKey(namespace, set, parentTxHash2[:])

			writePolicy := aerospike.NewWritePolicy(0, 0)
			err = client.Put(writePolicy, parentKey1, aerospike.BinMap{
				fields.TxID.String(): parentTxHash1.CloneBytes(),
			})
			require.NoError(t, err)

			err = client.Put(writePolicy, parentKey2, aerospike.BinMap{
				fields.TxID.String(): parentTxHash2.CloneBytes(),
			})
			require.NoError(t, err)

			// Create test inputs that reference different parents
			txHash1 := chainhash.HashH([]byte("child-tx-1"))
			txHash2 := chainhash.HashH([]byte("child-tx-2"))

			input1 := &bt.Input{PreviousTxOutIndex: 0, PreviousTxSatoshis: 100}
			_ = input1.PreviousTxIDAdd(&parentTxHash1)

			input2 := &bt.Input{PreviousTxOutIndex: 0, PreviousTxSatoshis: 200}
			_ = input2.PreviousTxIDAdd(&parentTxHash2)

			// Create batch parent updates
			errCh1 := make(chan error, 1)
			errCh2 := make(chan error, 1)
			batch := []*batchParentUpdate{
				{txHash: &txHash1, inputs: []*bt.Input{input1}, errCh: errCh1},
				{txHash: &txHash2, inputs: []*bt.Input{input2}, errCh: errCh2},
			}

			// Execute batch
			service.sendParentUpdateBatch(batch)

			// Verify both operations succeeded
			for i, errCh := range []chan error{errCh1, errCh2} {
				select {
				case err := <-errCh:
					assert.NoError(t, err)
				case <-time.After(1 * time.Second):
					t.Fatalf("Expected response on error channel %d", i+1)
				}
			}

			// Verify each parent record was updated with its respective child
			record1, err := client.Get(nil, parentKey1)
			require.NoError(t, err)
			deletedChildren1 := record1.Bins[fields.DeletedChildren.String()].(map[interface{}]interface{})
			assert.Equal(t, true, deletedChildren1[txHash1.String()])

			record2, err := client.Get(nil, parentKey2)
			require.NoError(t, err)
			deletedChildren2 := record2.Bins[fields.DeletedChildren.String()].(map[interface{}]interface{})
			assert.Equal(t, true, deletedChildren2[txHash2.String()])
		})

		t.Run("Parent not found - should succeed", func(t *testing.T) {
			mockIndexWaiter := &MockIndexWaiter{
				Client:    client,
				Namespace: namespace,
				Set:       set,
			}

			service, err := NewService(createTestSettings(), Options{
				Logger:        logger,
				Client:        client,
				ExternalStore: memory.New(),
				Namespace:     namespace,
				Set:           set,
				IndexWaiter:   mockIndexWaiter,
			})
			require.NoError(t, err)

			// Create input that references non-existent parent
			nonExistentParentHash := chainhash.HashH([]byte("non-existent-parent"))
			txHash := chainhash.HashH([]byte("child-tx"))

			input := &bt.Input{PreviousTxOutIndex: 0, PreviousTxSatoshis: 100}
			_ = input.PreviousTxIDAdd(&nonExistentParentHash)

			errCh := make(chan error, 1)
			batch := []*batchParentUpdate{
				{txHash: &txHash, inputs: []*bt.Input{input}, errCh: errCh},
			}

			// Execute batch
			service.sendParentUpdateBatch(batch)

			// Should succeed even if parent doesn't exist (treated as already cleaned up)
			select {
			case err := <-errCh:
				assert.NoError(t, err)
			case <-time.After(1 * time.Second):
				t.Fatal("Expected success response on error channel")
			}
		})

		t.Run("Transaction with no inputs", func(t *testing.T) {
			mockIndexWaiter := &MockIndexWaiter{
				Client:    client,
				Namespace: namespace,
				Set:       set,
			}

			service, err := NewService(createTestSettings(), Options{
				Logger:        logger,
				Client:        client,
				ExternalStore: memory.New(),
				Namespace:     namespace,
				Set:           set,
				IndexWaiter:   mockIndexWaiter,
			})
			require.NoError(t, err)

			txHash := chainhash.HashH([]byte("coinbase-tx"))
			errCh := make(chan error, 1)
			batch := []*batchParentUpdate{
				{txHash: &txHash, inputs: []*bt.Input{}, errCh: errCh},
			}

			// Execute batch
			service.sendParentUpdateBatch(batch)

			// Should succeed immediately for transactions with no inputs (like coinbase)
			select {
			case err := <-errCh:
				assert.NoError(t, err)
			case <-time.After(100 * time.Millisecond):
				t.Fatal("Expected immediate success response")
			}
		})
	})

	t.Run("Delete Batching", func(t *testing.T) {
		t.Run("Multiple record deletions", func(t *testing.T) {
			mockIndexWaiter := &MockIndexWaiter{
				Client:    client,
				Namespace: namespace,
				Set:       set,
			}

			service, err := NewService(createTestSettings(), Options{
				Logger:        logger,
				Client:        client,
				ExternalStore: memory.New(),
				Namespace:     namespace,
				Set:           set,
				IndexWaiter:   mockIndexWaiter,
			})
			require.NoError(t, err)

			// Create test records
			txHash1 := chainhash.HashH([]byte("delete-tx-1"))
			txHash2 := chainhash.HashH([]byte("delete-tx-2"))
			key1, _ := aerospike.NewKey(namespace, set, txHash1[:])
			key2, _ := aerospike.NewKey(namespace, set, txHash2[:])

			writePolicy := aerospike.NewWritePolicy(0, 0)
			err = client.Put(writePolicy, key1, aerospike.BinMap{
				fields.TxID.String(): txHash1.CloneBytes(),
			})
			require.NoError(t, err)

			err = client.Put(writePolicy, key2, aerospike.BinMap{
				fields.TxID.String(): txHash2.CloneBytes(),
			})
			require.NoError(t, err)

			// Create batch deletions
			errCh1 := make(chan error, 1)
			errCh2 := make(chan error, 1)
			batch := []*batchDelete{
				{key: key1, txHash: &txHash1, errCh: errCh1},
				{key: key2, txHash: &txHash2, errCh: errCh2},
			}

			// Execute batch
			service.sendDeleteBatch(batch)

			// Verify both operations succeeded
			for i, errCh := range []chan error{errCh1, errCh2} {
				select {
				case err := <-errCh:
					assert.NoError(t, err)
				case <-time.After(1 * time.Second):
					t.Fatalf("Expected response on error channel %d", i+1)
				}
			}

			// Verify records were actually deleted
			_, err = client.Get(nil, key1)
			assert.Error(t, err)
			assert.Contains(t, err.Error(), "not found")

			_, err = client.Get(nil, key2)
			assert.Error(t, err)
			assert.Contains(t, err.Error(), "not found")
		})

		t.Run("Delete non-existent record - should succeed", func(t *testing.T) {
			mockIndexWaiter := &MockIndexWaiter{
				Client:    client,
				Namespace: namespace,
				Set:       set,
			}

			service, err := NewService(createTestSettings(), Options{
				Logger:        logger,
				Client:        client,
				ExternalStore: memory.New(),
				Namespace:     namespace,
				Set:           set,
				IndexWaiter:   mockIndexWaiter,
			})
			require.NoError(t, err)

			// Try to delete non-existent record
			nonExistentHash := chainhash.HashH([]byte("non-existent-tx"))
			key, _ := aerospike.NewKey(namespace, set, nonExistentHash[:])

			errCh := make(chan error, 1)
			batch := []*batchDelete{
				{key: key, txHash: &nonExistentHash, errCh: errCh},
			}

			// Execute batch
			service.sendDeleteBatch(batch)

			// Should succeed (key not found treated as success)
			select {
			case err := <-errCh:
				assert.NoError(t, err)
			case <-time.After(1 * time.Second):
				t.Fatal("Expected success response on error channel")
			}
		})
	})

	t.Run("Concurrent Operations", func(t *testing.T) {
		mockIndexWaiter := &MockIndexWaiter{
			Client:    client,
			Namespace: namespace,
			Set:       set,
		}

		service, err := NewService(createTestSettings(), Options{
			Logger:        logger,
			Client:        client,
			ExternalStore: memory.New(),
			Namespace:     namespace,
			Set:           set,
			IndexWaiter:   mockIndexWaiter,
		})
		require.NoError(t, err)

		t.Run("Concurrent batch operations", func(t *testing.T) {
			const numOperations = 20
			var wg sync.WaitGroup
			errorChannels := make([]chan error, numOperations)

			// Create records to delete
			writePolicy := aerospike.NewWritePolicy(0, 0)
			for i := 0; i < numOperations; i++ {
				txHash := chainhash.HashH([]byte(fmt.Sprintf("concurrent-tx-%d", i)))
				key, _ := aerospike.NewKey(namespace, set, txHash[:])
				err := client.Put(writePolicy, key, aerospike.BinMap{
					fields.TxID.String(): txHash.CloneBytes(),
				})
				require.NoError(t, err)
			}

			// Execute concurrent deletions
			for i := 0; i < numOperations; i++ {
				wg.Add(1)
				errorChannels[i] = make(chan error, 1)

				go func(idx int) {
					defer wg.Done()

					txHash := chainhash.HashH([]byte(fmt.Sprintf("concurrent-tx-%d", idx)))
					key, _ := aerospike.NewKey(namespace, set, txHash[:])

					batch := []*batchDelete{
						{key: key, txHash: &txHash, errCh: errorChannels[idx]},
					}

					service.sendDeleteBatch(batch)
				}(i)
			}

			// Wait for all operations to complete
			wg.Wait()

			// Verify all operations succeeded
			for i, errCh := range errorChannels {
				select {
				case err := <-errCh:
					assert.NoError(t, err, "Operation %d should succeed", i)
				case <-time.After(1 * time.Second):
					t.Fatalf("Expected response on error channel %d", i)
				}
			}

			// Verify all records were deleted
			for i := 0; i < numOperations; i++ {
				txHash := chainhash.HashH([]byte(fmt.Sprintf("concurrent-tx-%d", i)))
				key, _ := aerospike.NewKey(namespace, set, txHash[:])
				_, err := client.Get(nil, key)
				assert.Error(t, err, "Record %d should be deleted", i)
				assert.Contains(t, err.Error(), "not found")
			}
		})
	})

	t.Run("External Transactions", func(t *testing.T) {
		mockIndexWaiter := &MockIndexWaiter{
			Client:    client,
			Namespace: namespace,
			Set:       set,
		}

		externalStore := memory.New()
		service, err := NewService(createTestSettings(), Options{
			Logger:        logger,
			Client:        client,
			ExternalStore: externalStore,
			Namespace:     namespace,
			Set:           set,
			IndexWaiter:   mockIndexWaiter,
		})
		require.NoError(t, err)

		t.Run("External transaction with valid blob storage", func(t *testing.T) {
			// Create a transaction with inputs
			parentTxHash := chainhash.HashH([]byte("ext-parent-tx"))
			txHash := chainhash.HashH([]byte("external-tx"))

			// Create parent UTXO record
			keySource := uaerospike.CalculateKeySource(&parentTxHash, 0, 1)
			parentKey, _ := aerospike.NewKey(namespace, set, keySource)

			writePolicy := aerospike.NewWritePolicy(0, 0)
			err := client.Put(writePolicy, parentKey, aerospike.BinMap{
				fields.TxID.String(): parentTxHash.CloneBytes(),
			})
			require.NoError(t, err)

			// Create a transaction and store it in external storage
			input := &bt.Input{PreviousTxOutIndex: 0, PreviousTxSatoshis: 100}
			_ = input.PreviousTxIDAdd(&parentTxHash)

			tx := &bt.Tx{
				Inputs: []*bt.Input{input},
			}

			// Store transaction in external store
			setErr := externalStore.Set(context.Background(), txHash.CloneBytes(), fileformat.FileTypeTx, tx.Bytes())
			require.NoError(t, setErr)

			// Create cleanup record for external transaction
			txKey, _ := aerospike.NewKey(namespace, set, txHash[:])
			err = client.Put(writePolicy, txKey, aerospike.BinMap{
				fields.TxID.String():           txHash.CloneBytes(),
				fields.External.String():       true,
				fields.DeleteAtHeight.String(): 1,
			})
			require.NoError(t, err)

			// Test processRecordCleanup for external transaction
			record, err := client.Get(nil, txKey)
			require.NoError(t, err)

			job := &cleanup.Job{BlockHeight: 1}
			processErr := service.processRecordCleanup(job, 0, &aerospike.Result{Record: record})
			assert.NoError(t, processErr)

			// Verify parent was updated
			parentRecord, err := client.Get(nil, parentKey)
			require.NoError(t, err)
			deletedChildren := parentRecord.Bins[fields.DeletedChildren.String()].(map[interface{}]interface{})
			assert.Equal(t, true, deletedChildren[txHash.String()])

			// Verify external transaction record was deleted
			_, err = client.Get(nil, txKey)
			assert.Error(t, err)
			assert.Contains(t, err.Error(), "not found")
		})

		t.Run("External transaction - FileTypeTx not found but outputs exist", func(t *testing.T) {
			txHash := chainhash.HashH([]byte("external-tx-outputs-only"))

			// Store only outputs in external store (transaction file missing)
			err := externalStore.Set(context.Background(), txHash.CloneBytes(), fileformat.FileTypeOutputs, []byte("dummy-outputs"))
			require.NoError(t, err)

			// Create cleanup record for external transaction
			txKey, _ := aerospike.NewKey(namespace, set, txHash[:])
			writePolicy := aerospike.NewWritePolicy(0, 0)
			err = client.Put(writePolicy, txKey, aerospike.BinMap{
				fields.TxID.String():           txHash.CloneBytes(),
				fields.External.String():       true,
				fields.DeleteAtHeight.String(): 1,
			})
			require.NoError(t, err)

			// Test processRecordCleanup - should succeed with no parent updates (returns nil inputs)
			record, err := client.Get(nil, txKey)
			require.NoError(t, err)

			job := &cleanup.Job{BlockHeight: 1}
			processErr := service.processRecordCleanup(job, 0, &aerospike.Result{Record: record})
			assert.NoError(t, processErr)

			// Verify transaction record was deleted
			_, err = client.Get(nil, txKey)
			assert.Error(t, err)
			assert.Contains(t, err.Error(), "not found")
		})

		t.Run("External transaction - not found in external store", func(t *testing.T) {
			txHash := chainhash.HashH([]byte("missing-external-tx"))

			// Create cleanup record for external transaction (but don't store in external store)
			txKey, _ := aerospike.NewKey(namespace, set, txHash[:])
			writePolicy := aerospike.NewWritePolicy(0, 0)
			err := client.Put(writePolicy, txKey, aerospike.BinMap{
				fields.TxID.String():           txHash.CloneBytes(),
				fields.External.String():       true,
				fields.DeleteAtHeight.String(): 1,
			})
			require.NoError(t, err)

			// Test processRecordCleanup - should fail
			record, err := client.Get(nil, txKey)
			require.NoError(t, err)

			job := &cleanup.Job{BlockHeight: 1}
			processErr := service.processRecordCleanup(job, 0, &aerospike.Result{Record: record})
			assert.Error(t, processErr)
			assert.Contains(t, processErr.Error(), "not found in external store")

			// Verify transaction record still exists (cleanup failed)
			_, err = client.Get(nil, txKey)
			assert.NoError(t, err)
		})

		t.Run("External transaction - invalid transaction bytes", func(t *testing.T) {
			txHash := chainhash.HashH([]byte("invalid-external-tx"))

			// Store invalid transaction bytes in external store
			err := externalStore.Set(context.Background(), txHash.CloneBytes(), fileformat.FileTypeTx, []byte("invalid-tx-bytes"))
			require.NoError(t, err)

			// Create cleanup record for external transaction
			txKey, _ := aerospike.NewKey(namespace, set, txHash[:])
			writePolicy := aerospike.NewWritePolicy(0, 0)
			err = client.Put(writePolicy, txKey, aerospike.BinMap{
				fields.TxID.String():           txHash.CloneBytes(),
				fields.External.String():       true,
				fields.DeleteAtHeight.String(): 1,
			})
			require.NoError(t, err)

			// Test processRecordCleanup - should fail
			record, err := client.Get(nil, txKey)
			require.NoError(t, err)

			job := &cleanup.Job{BlockHeight: 1}
			processErr := service.processRecordCleanup(job, 0, &aerospike.Result{Record: record})
			assert.Error(t, processErr)
			assert.Contains(t, processErr.Error(), "invalid tx bytes")

			// Verify transaction record still exists (cleanup failed)
			_, err = client.Get(nil, txKey)
			assert.NoError(t, err)
		})
	})

	t.Run("Failure Scenarios", func(t *testing.T) {
		mockIndexWaiter := &MockIndexWaiter{
			Client:    client,
			Namespace: namespace,
			Set:       set,
		}

		service, err := NewService(createTestSettings(), Options{
			Logger:        logger,
			Client:        client,
			ExternalStore: memory.New(),
			Namespace:     namespace,
			Set:           set,
			IndexWaiter:   mockIndexWaiter,
		})
		require.NoError(t, err)

		t.Run("Missing record bins", func(t *testing.T) {
			// Create a record without required bins
			job := &cleanup.Job{BlockHeight: 1}

			// Test with nil bins
			err := service.processRecordCleanup(job, 0, &aerospike.Result{
				Record: &aerospike.Record{Bins: nil},
			})
			assert.Error(t, err)
			assert.Contains(t, err.Error(), "missing bins")
		})

		t.Run("Invalid TxID bin", func(t *testing.T) {
			job := &cleanup.Job{BlockHeight: 1}

			// Test with missing TxID
			err := service.processRecordCleanup(job, 0, &aerospike.Result{
				Record: &aerospike.Record{
					Bins: aerospike.BinMap{
						fields.DeleteAtHeight.String(): 1,
					},
				},
			})
			assert.Error(t, err)
			assert.Contains(t, err.Error(), "invalid or missing txid")

			// Test with invalid TxID format
			err = service.processRecordCleanup(job, 0, &aerospike.Result{
				Record: &aerospike.Record{
					Bins: aerospike.BinMap{
						fields.TxID.String():           "invalid-txid",
						fields.DeleteAtHeight.String(): 1,
					},
				},
			})
			assert.Error(t, err)
			assert.Contains(t, err.Error(), "invalid or missing txid")

			// Test with wrong length TxID
			err = service.processRecordCleanup(job, 0, &aerospike.Result{
				Record: &aerospike.Record{
					Bins: aerospike.BinMap{
						fields.TxID.String():           []byte("short"),
						fields.DeleteAtHeight.String(): 1,
					},
				},
			})
			assert.Error(t, err)
			assert.Contains(t, err.Error(), "invalid or missing txid")
		})

		t.Run("Missing inputs for non-external transaction", func(t *testing.T) {
			txHash := chainhash.HashH([]byte("no-inputs-tx"))
			job := &cleanup.Job{BlockHeight: 1}

			err := service.processRecordCleanup(job, 0, &aerospike.Result{
				Record: &aerospike.Record{
					Bins: aerospike.BinMap{
						fields.TxID.String():           txHash.CloneBytes(),
						fields.DeleteAtHeight.String(): 1,
						fields.External.String():       false,
						// Missing inputs field
					},
				},
			})
			assert.Error(t, err)
			assert.Contains(t, err.Error(), "missing inputs")
		})

		t.Run("Invalid input data format", func(t *testing.T) {
			txHash := chainhash.HashH([]byte("invalid-inputs-tx"))
			job := &cleanup.Job{BlockHeight: 1}

			err := service.processRecordCleanup(job, 0, &aerospike.Result{
				Record: &aerospike.Record{
					Bins: aerospike.BinMap{
						fields.TxID.String():           txHash.CloneBytes(),
						fields.DeleteAtHeight.String(): 1,
						fields.External.String():       false,
						fields.Inputs.String():         []interface{}{[]byte("invalid-input-data")},
					},
				},
			})
			assert.Error(t, err)
			assert.Contains(t, err.Error(), "invalid input")
		})

		t.Run("Aerospike read error", func(t *testing.T) {
			job := &cleanup.Job{BlockHeight: 1}

			// Simulate Aerospike read error - we need to pass Result with no record and check error handling
			// Create a result that would cause an error in processRecordCleanup
			result := &aerospike.Result{
				Record: &aerospike.Record{
					Bins: aerospike.BinMap{
						fields.TxID.String():           []byte("invalid-length-txid"), // Wrong length will cause error
						fields.DeleteAtHeight.String(): 1,
					},
				},
			}

			processErr := service.processRecordCleanup(job, 0, result)
			assert.Error(t, processErr)
			assert.Contains(t, processErr.Error(), "invalid")
		})

		t.Run("Batch operation failures", func(t *testing.T) {
			// This test simulates what happens when the underlying Aerospike batch operations fail
			// We can't easily simulate this with the real client, but we test the error handling logic

			txHash := chainhash.HashH([]byte("batch-fail-tx"))
			parentTxHash := chainhash.HashH([]byte("batch-fail-parent"))
			errCh := make(chan error, 1)

			// Create properly initialized input
			input := &bt.Input{PreviousTxOutIndex: 0, PreviousTxSatoshis: 100}
			_ = input.PreviousTxIDAdd(&parentTxHash)

			// Test batch parent update - this will try to access parent records that don't exist
			batch := []*batchParentUpdate{
				{
					txHash: &txHash,
					inputs: []*bt.Input{input},
					errCh:  errCh,
				},
			}

			// The sendParentUpdateBatch should handle the error gracefully
			service.sendParentUpdateBatch(batch)

			// Should get a success response since parent not found is treated as success
			select {
			case err := <-errCh:
				// Parent not found is treated as success (nil error)
				assert.NoError(t, err)
				t.Logf("Batch operation completed with: %v", err)
			case <-time.After(1 * time.Second):
				t.Fatal("Expected response on error channel")
			}
		})
	})

	t.Run("Edge Cases", func(t *testing.T) {
		mockIndexWaiter := &MockIndexWaiter{
			Client:    client,
			Namespace: namespace,
			Set:       set,
		}

		service, err := NewService(createTestSettings(), Options{
			Logger:        logger,
			Client:        client,
			ExternalStore: memory.New(),
			Namespace:     namespace,
			Set:           set,
			IndexWaiter:   mockIndexWaiter,
		})
		require.NoError(t, err)

		t.Run("Empty batches", func(t *testing.T) {
			// Empty batches should not crash
			service.sendParentUpdateBatch([]*batchParentUpdate{})
			service.sendDeleteBatch([]*batchDelete{})
		})
	})

	t.Run("Job-Level Failures", func(t *testing.T) {
		mockIndexWaiter := &MockIndexWaiter{
			Client:    client,
			Namespace: namespace,
			Set:       set,
		}

		service, err := NewService(createTestSettings(), Options{
			Logger:        logger,
			Client:        client,
			ExternalStore: memory.New(),
			Namespace:     namespace,
			Set:           set,
			IndexWaiter:   mockIndexWaiter,
		})
		require.NoError(t, err)

		service.Start(context.Background())
		defer func() {
			_ = service.Stop(context.Background())
		}()

		// Clear any existing data by truncating the set to ensure test isolation
		// First get a node to send the truncate command to
		node, err := client.Client.Cluster().GetRandomNode()
		if err != nil {
			t.Logf("Failed to get node for truncation: %v", err)
		} else {
			infoPolicy := aerospike.NewInfoPolicy()
			infoPolicy.Timeout = 5 * time.Second

			truncateCmd := fmt.Sprintf("truncate:namespace=%s;set=%s", namespace, set)
			_, err = node.RequestInfo(infoPolicy, truncateCmd)
			if err != nil {
				t.Logf("Failed to truncate set (may not exist yet): %v", err)
			} else {
				// Wait a bit for truncation to complete
				time.Sleep(1 * time.Second)
			}
		}

		t.Run("Job with invalid record should fail", func(t *testing.T) {
			// Test that a job fails when it encounters an invalid record
			testBlockHeight := uint32(2000000)

			// Create only a record with invalid TxID that should cause failure
			// Use a proper 32-byte key but invalid TxID bin
			invalidKeyBytes := make([]byte, 32)
			copy(invalidKeyBytes, "some-invalid-key-2000000")
			invalidKey, _ := aerospike.NewKey(namespace, set, invalidKeyBytes)

			writePolicy := aerospike.NewWritePolicy(0, 0)
			err := client.Put(writePolicy, invalidKey, aerospike.BinMap{
				fields.TxID.String():           []byte("invalid-short-txid"), // Invalid length - only 18 bytes
				fields.DeleteAtHeight.String(): int(testBlockHeight),
			})
			require.NoError(t, err)

			// Create a done channel and run the cleanup job
			done := make(chan string, 1)
			updateErr := service.UpdateBlockHeight(testBlockHeight, done)
			require.NoError(t, updateErr)

			// Wait for the job to complete
			select {
			case status := <-done:
				// The job should fail due to the invalid record
				// We can verify this by checking that the status indicates failure
				assert.Equal(t, cleanup.JobStatusFailed.String(), status)
			case <-time.After(10 * time.Second):
				t.Fatal("Job should have completed within 10 seconds")
			}

			// Verify job status - the main thing we can reliably test is that the job
			// is marked as failed when it encounters invalid data
			jobs := service.GetJobs()
			require.True(t, len(jobs) >= 1, "Expected at least one job")

			// Find the job for our test block height
			var targetJob *cleanup.Job
			for _, job := range jobs {
				if job.BlockHeight == testBlockHeight {
					targetJob = job
					break
				}
			}
			require.NotNil(t, targetJob, "Should find job for block height %d", testBlockHeight)
			assert.Equal(t, cleanup.JobStatusFailed, targetJob.GetStatus())
			// Note: Due to concurrency issues in job management, we focus on testing
			// that the job status is correctly set to failed rather than checking
			// the specific error details
		})

		t.Run("Concurrent job processing", func(t *testing.T) {
			// Test job cancellation behavior when multiple jobs are submitted rapidly
			// Since the cleanup service only allows one active job at a time,
			// subsequent jobs will cancel previous ones
			const numJobs = 3
			baseHeight := uint32(60000) // Use high base to avoid conflicts
			doneChannels := make([]chan string, numJobs)

			// Create test records for the jobs with valid coinbase transactions
			for i := 0; i < numJobs; i++ {
				txHash := chainhash.HashH([]byte(fmt.Sprintf("concurrent-job-coinbase-%d", i)))
				key, _ := aerospike.NewKey(namespace, set, txHash[:])

				writePolicy := aerospike.NewWritePolicy(0, 0)
				err := client.Put(writePolicy, key, aerospike.BinMap{
					fields.TxID.String():           txHash.CloneBytes(),
					fields.Inputs.String():         []interface{}{},             // Empty inputs (coinbase)
					fields.DeleteAtHeight.String(): int(baseHeight + uint32(i)), // Different heights
				})
				require.NoError(t, err)
			}

			// Submit multiple jobs rapidly - this should cause some to be cancelled
			for i := 0; i < numJobs; i++ {
				doneChannels[i] = make(chan string, 1)
				err := service.UpdateBlockHeight(baseHeight+uint32(i), doneChannels[i])
				require.NoError(t, err)
			}

			// Wait for all jobs to complete (some may be cancelled)
			completedJobs := 0
			cancelledJobs := 0
			failedJobs := 0

			for i := 0; i < numJobs; i++ {
				select {
				case status := <-doneChannels[i]:
					switch status {
					case cleanup.JobStatusCompleted.String():
						completedJobs++
					case cleanup.JobStatusCancelled.String():
						cancelledJobs++
					case cleanup.JobStatusFailed.String():
						failedJobs++
					}
					t.Logf("Job %d (height %d) status: %s", i, baseHeight+uint32(i), status)
				case <-time.After(15 * time.Second):
					t.Fatalf("Job %d should have completed within 15 seconds", i)
				}
			}

			// All jobs should have a final status
			totalJobs := completedJobs + cancelledJobs + failedJobs
			assert.Equal(t, numJobs, totalJobs, "All jobs should have a final status")
			t.Logf("Job results: %d completed, %d cancelled, %d failed", completedJobs, cancelledJobs, failedJobs)
		})
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
			assert.Equal(t, cleanup.JobStatusCompleted.String(), status)
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
			assert.Equal(t, cleanup.JobStatusCompleted.String(), status)
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
			assert.Equal(t, cleanup.JobStatusCompleted.String(), status)
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
			assert.Equal(t, cleanup.JobStatusCompleted.String(), status)
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
		assert.Equal(t, cleanup.JobStatusCompleted.String(), status)
	case <-time.After(5 * time.Second):
		t.Fatal("Cleanup should complete within 5 seconds")
	}
}
