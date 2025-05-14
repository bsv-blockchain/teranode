package cleanup

import (
	"context"
	"testing"
	"time"

	"github.com/aerospike/aerospike-client-go/v8"
	"github.com/bitcoin-sv/teranode/stores/cleanup"
	"github.com/bitcoin-sv/teranode/stores/utxo/fields"
	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/bitcoin-sv/teranode/util/uaerospike"
	aeroTest "github.com/bitcoin-sv/testcontainers-aerospike-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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
		Namespace:      "test",
		Set:            "test",
		MaxJobsHistory: 3,
		IndexWaiter:    mockIndexWaiter,
	}

	t.Run("Valid block height", func(t *testing.T) {
		service, err := NewService(opts)
		require.NoError(t, err)

		err = service.UpdateBlockHeight(1)
		require.NoError(t, err)

		jobs := service.GetJobs()
		assert.Len(t, jobs, 1)
		assert.Equal(t, cleanup.JobStatusPending, jobs[0].GetStatus())
	})

	t.Run("New block height", func(t *testing.T) {
		service, err := NewService(opts)
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
		service, err := NewService(opts)
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
			Client:      client,
			Namespace:   "test",
			Set:         "test",
			IndexWaiter: mockIndexWaiter,
		}

		service, err := NewService(opts)
		assert.Error(t, err)
		assert.Nil(t, service)
	})

	t.Run("Missing client", func(t *testing.T) {
		opts := Options{
			Logger:      logger,
			Namespace:   "test",
			Set:         "test",
			IndexWaiter: mockIndexWaiter,
		}

		service, err := NewService(opts)
		assert.Error(t, err)
		assert.Nil(t, service)
	})

	t.Run("Missing namespace", func(t *testing.T) {
		opts := Options{
			Logger:      logger,
			Client:      client,
			Set:         "test",
			IndexWaiter: mockIndexWaiter,
		}

		service, err := NewService(opts)
		assert.Error(t, err)
		assert.Nil(t, service)
	})

	t.Run("Missing set", func(t *testing.T) {
		opts := Options{
			Logger:      logger,
			Client:      client,
			Namespace:   "test",
			IndexWaiter: mockIndexWaiter,
		}

		service, err := NewService(opts)
		assert.Error(t, err)
		assert.Nil(t, service)
	})

	t.Run("Missing IndexWaiter", func(t *testing.T) {
		opts := Options{
			Logger:    logger,
			Client:    client,
			Namespace: "test",
			Set:       "test",
		}

		service, err := NewService(opts)
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

	service, err := NewService(Options{
		Logger:      logger,
		Client:      client,
		Namespace:   "test",
		Set:         "test",
		IndexWaiter: mockIndexWaiter,
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
	service, err := NewService(Options{
		Logger:      logger,
		Client:      client,
		Namespace:   namespace,
		Set:         set,
		WorkerCount: 1,
		IndexWaiter: mockIndexWaiter,
	})
	require.NoError(t, err)

	// Start the service (this will create the index and start the job manager)
	service.Start(ctx)

	// Create some test records
	writePolicy := aerospike.NewWritePolicy(0, 0)
	key1, _ := aerospike.NewKey(namespace, set, "test1")
	key2, _ := aerospike.NewKey(namespace, set, "test2")

	// Create record 1 with deleteAtHeight = 0 (not to be deleted)
	err = client.Put(writePolicy, key1, aerospike.BinMap{
		fields.DeleteAtHeight.String(): 0,
	})
	require.NoError(t, err)

	// Create record 2 with deleteAtHeight = 0 (not to be deleted)
	err = client.Put(writePolicy, key2, aerospike.BinMap{
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
	// require.Equal(t, "completed", <-done)
	<-done

	// Verify the record was not deleted
	record, err = client.Get(nil, key1)
	assert.NoError(t, err)
	assert.NotNil(t, record)

	// Create a new done channel for the next job
	done = make(chan string)

	err = service.UpdateBlockHeight(3, done)
	require.NoError(t, err)

	// Wait for the job to complete
	// require.Equal(t, "completed", <-done)
	<-done

	// Verify the record1 was deleted
	_, err = client.Get(nil, key1)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not found")

	// Verify the record2 was not deleted
	record, err = client.Get(nil, key2)
	assert.NoError(t, err)
	assert.NotNil(t, record)
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
			Logger:      logger,
			Client:      client,
			IndexWaiter: mockIndexWaiter,
			Namespace:   "ns",
			Set:         "set",
		}

		service, err := NewService(opts)
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
		service, err := NewService(opts)
		assert.Error(t, err)
		assert.Nil(t, service)
	})
}
