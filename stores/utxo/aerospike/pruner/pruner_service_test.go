package pruner

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/bsv-blockchain/aerospike-client-go/v8"
	"github.com/bsv-blockchain/aerospike-client-go/v8/types"
	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/blob/memory"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/bsv-blockchain/teranode/util/uaerospike"
	aeroTest "github.com/bsv-blockchain/testcontainers-aerospike-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
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
	test.SkipIfContainerUnavailable(t, err)

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
	test.SkipIfContainerUnavailable(t, err)

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
	test.SkipIfContainerUnavailable(t, err)

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

	// create parent record that should be marked before deletion of child. Its
	// output 0 records child 1 as the spender: a marker is only written for the
	// child an output really names (keepSpendHolders).
	err = client.Put(writePolicy, keyParent, aerospike.BinMap{
		fields.TxID.String():           txIDParent.CloneBytes(),
		fields.Utxos.String():          []interface{}{spentUtxoElement(&txID1)},
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

// TestCollectDeletedChildren: a parent's deletedChildren bin adds every child it
// names to the chunk-wide set, and a bin of the wrong shape, or a key that is
// not a string, adds nothing rather than failing the chunk.
func TestCollectDeletedChildren(t *testing.T) {
	s := &Service{fieldDeletedChildren: "deletedChildren", logger: ulogger.TestLogger{}}

	for _, tc := range []struct {
		name string
		bins aerospike.BinMap
		want map[string]bool
	}{
		{"no bin", aerospike.BinMap{}, map[string]bool{}},
		{"wrong type", aerospike.BinMap{"deletedChildren": []interface{}{"a"}}, map[string]bool{}},
		{"string and non-string keys", aerospike.BinMap{"deletedChildren": map[interface{}]interface{}{"a": true, 7: true}}, map[string]bool{"a": true}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			into := map[string]bool{}
			s.collectDeletedChildren(tc.bins, into)
			require.Equal(t, tc.want, into)
		})
	}
}

// TestExtractSpendingChildren: only a spent utxo entry, 32 bytes of utxo hash
// and 36 of spending data with a non-zero txid, names a child. A bin that is
// not a list, an entry that is not bytes or is short, and an unspent entry name
// none.
func TestExtractSpendingChildren(t *testing.T) {
	child := chainhash.HashH([]byte("extract-child"))

	spent := make([]byte, 68)
	copy(spent[32:64], child[:])

	unspent := make([]byte, 68)

	require.Nil(t, extractSpendingChildren("not a list", map[string][]byte{}))

	unique := map[string][]byte{}
	got := extractSpendingChildren([]interface{}{spent, unspent, spent[:40], "x"}, unique)

	require.Equal(t, []string{child.String()}, got)
	require.Equal(t, child[:], unique[child.String()])
	require.True(t, isZeroBytes(unspent[32:64]))
	require.False(t, isZeroBytes(spent[32:64]))
}

// TestAddParentUpdatesForInput covers what the pruner asks Aerospike to write for
// one spent outpoint: which parent records get a marker, and how many copies of
// the child hash each one receives.
func TestAddParentUpdatesForInput(t *testing.T) {
	var parent chainhash.Hash

	parent[0] = 0xAA

	var child chainhash.Hash

	child[0] = 0xBB

	newService := func(defensive bool) *Service {
		return &Service{namespace: "test", set: "utxo", utxoBatchSize: 128, defensiveEnabled: defensive}
	}

	masterKey := string(parent.CloneBytes())
	pageKey := string(uaerospike.CalculateKeySource(&parent, 300, 128))

	require.NotEqual(t, masterKey, pageKey, "fixture must use a vout past the first page")

	t.Run("either mode writes only the page the spend path reads", func(t *testing.T) {
		for _, defensive := range []bool{false, true} {
			updates := map[string]*parentUpdateInfo{}
			require.NoError(t, newService(defensive).addParentUpdatesForInput(updates, &parent, 300, &child))
			require.Len(t, updates, 1)
			require.Contains(t, updates, pageKey)
			require.NotContains(t, updates, masterKey,
				"the master holds outputs 0..batchSize-1 only, so a marker for a higher output is read by nothing there and grows the record without bound")
		}
	})

	t.Run("first page needs one write in either mode", func(t *testing.T) {
		for _, defensive := range []bool{false, true} {
			updates := map[string]*parentUpdateInfo{}
			require.NoError(t, newService(defensive).addParentUpdatesForInput(updates, &parent, 7, &child))
			require.Len(t, updates, 1)
			require.Contains(t, updates, masterKey, "vout 0..127 lives on the master record")
		}
	})

	t.Run("a consolidation spend queues one child hash, not one per input", func(t *testing.T) {
		// 10k outputs of the same paginated parent, spread across pages: every
		// input asks for the same (parent, child) marker. Without dedup this
		// produced a 10,000-element list of the identical 64-char hex string in
		// one batch record.
		s := newService(true)
		updates := map[string]*parentUpdateInfo{}

		for vout := uint32(0); vout < 10_000; vout++ {
			require.NoError(t, s.addParentUpdatesForInput(updates, &parent, vout, &child))
		}

		require.Len(t, updates, 79, "one entry per output page (page 0 is the master)")

		for source, info := range updates {
			require.Lenf(t, info.childHashes, 1, "parent record %x queued %d copies of the same child", source, len(info.childHashes))
		}
	})
}

// TestPartitionWorker_CancelledWorkerStillDrainsTheCarry: each chunk's deletes
// ride the next chunk's batch, so when the worker stops the carry still holds
// children whose markers have landed. The worker's deferred drain runs detached
// from ctx. Here ctx is cancelled straight after the first batch, which writes
// the first chunk's marker. A drain that ran on ctx would send nothing, and the
// child would stay present behind its own marker, so its next re-spend would be
// refused as a pruned replay. The end state required: no child is both marked
// and still present, and at least one marker landed.
func TestPartitionWorker_CancelledWorkerStillDrainsTheCarry(t *testing.T) {
	s, client := newFlushTestService(t)

	s.chunkSize = 1
	s.chunkGroupLimit = 1

	require.NoError(t, s.indexWaiter.WaitForIndexReady(context.Background(), IndexName))

	type pair struct {
		child               chainhash.Hash
		parentKey, childKey *aerospike.Key
	}

	pairs := make([]pair, 0, 2)

	for n := byte(1); n <= 2; n++ {
		parent := chainhash.HashH([]byte{0xD0, n, 1})
		child := chainhash.HashH([]byte{0xD0, n, 2})

		input := &bt.Input{PreviousTxOutIndex: 0, PreviousTxSatoshis: 100}
		require.NoError(t, input.PreviousTxIDAdd(&parent))

		parentKey := putRecord(t, client, &parent, spentBy(&child))
		childKey := putRecord(t, client, &child, aerospike.BinMap{
			fields.TxID.String():           child.CloneBytes(),
			fields.Inputs.String():         []interface{}{input.Bytes(true)},
			fields.DeleteAtHeight.String(): 5,
		})

		pairs = append(pairs, pair{child: child, parentKey: parentKey, childKey: childKey})
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	calls := 0
	s.batchOperateFn = func(policy *aerospike.BatchPolicy, records []aerospike.BatchRecordIfc) error {
		err := client.BatchOperate(policy, records)

		calls++
		if calls == 1 {
			cancel()
		}

		return err
	}

	_, _ = s.PruneWithPartitions(ctx, 10, "canceled-worker", 1)

	require.NotZero(t, calls, "precondition: the first chunk's marker batch was sent")

	marked := 0

	for _, p := range pairs {
		if !hasMarker(t, client, p.parentKey, &p.child) {
			continue
		}

		marked++

		exists, err := client.Exists(nil, p.childKey)
		require.NoError(t, err)
		require.False(t, exists, "child %s carries a marker and must have been deleted by the detached drain", p.child.String())
	}

	require.NotZero(t, marked, "precondition: a marker landed before the cancel")
}

// TestStageParentUpdate_ReusesTheQueuedKey: a parent already queued for the
// chunk keeps its key when a later record stages a marker on it, so the key
// digest runs once per distinct parent in a chunk, not once per child.
func TestStageParentUpdate_ReusesTheQueuedKey(t *testing.T) {
	s := newTestService(t)

	parent := chainhash.HashH([]byte("shared parent"))
	first := chainhash.HashH([]byte("first child"))
	second := chainhash.HashH([]byte("second child"))

	queued := make(map[string]*parentUpdateInfo)
	require.NoError(t, s.stageParentUpdatesForInput(queued, nil, &parent, 0, &first))

	staged := make(map[string]*parentUpdateInfo)
	require.NoError(t, s.stageParentUpdatesForInput(staged, queued, &parent, 1, &second))

	source := string(uaerospike.CalculateKeySource(&parent, 1, s.utxoBatchSize))
	require.Contains(t, staged, source)
	require.Same(t, queued[source].key, staged[source].key, "the staged record reuses the queued parent's key")
	require.Equal(t, []uint32{1}, staged[source].offsets[second], "staging still records the child's own offset")
}

// markerBenchChunk queues the marker writes of one chunk: children children,
// each spending inputs outputs. Every input names output 0 of its own parent,
// drawn from a pool of parents parents, so with parents < children*inputs the
// same parent is spent by several children of the chunk, as a busy parent is.
func markerBenchChunk(b *testing.B, s *Service, children, inputs, parents int, reuseKeys bool) map[string]*parentUpdateInfo {
	b.Helper()

	all := make(map[string]*parentUpdateInfo, children*inputs)

	for c := 0; c < children; c++ {
		child := chainhash.HashH([]byte{byte(c), byte(c >> 8), 0xC1})
		record := make(map[string]*parentUpdateInfo, inputs)

		queued := all
		if !reuseKeys {
			queued = nil
		}

		for i := 0; i < inputs; i++ {
			n := (c*inputs + i) % parents
			parent := chainhash.HashH([]byte{byte(n), byte(n >> 8), byte(n >> 16), 0xB1})

			if err := s.stageParentUpdatesForInput(record, queued, &parent, 0, &child); err != nil {
				b.Fatal(err)
			}
		}

		for source, info := range record {
			if existing, ok := all[source]; ok {
				existing.merge(info)
			} else {
				all[source] = info
			}
		}
	}

	return all
}

// BenchmarkStageChunkParentUpdates measures queueing one chunk's marker
// writes, as processRecordChunkCarry does: 1024 children, 1, 4 or 8 inputs
// each, over 256 parents. "reuse" looks the parent up in the chunk's queue
// before computing its key; "per-child" computes a key for every child that
// spends the parent, which is what the branch did before.
func BenchmarkStageChunkParentUpdates(b *testing.B) {
	s := &Service{namespace: "test", set: "test", utxoBatchSize: 128}

	for _, inputs := range []int{1, 4, 8} {
		for _, mode := range []struct {
			name  string
			reuse bool
		}{{"reuse", true}, {"per-child", false}} {
			b.Run(fmt.Sprintf("inputs=%d/%s", inputs, mode.name), func(b *testing.B) {
				b.ReportAllocs()

				for i := 0; i < b.N; i++ {
					_ = markerBenchChunk(b, s, 1024, inputs, 256, mode.reuse)
				}
			})
		}
	}
}

// BenchmarkBuildVerifiedMarkerRecords measures building one chunk's marker
// batch records on the client: 1024 children with 1, 4 or 8 inputs each, every
// input on its own parent record. "filtered" is the MapPutItems write with its
// filter expression, the fallback; "native" is the sub-op 15 write that
// native-op deployments use, built by testNativeMarkerRecord, which encodes
// the same payload and policy as the store's prunerVerifiedMarkerRecord. The
// expression's wire packing happens later, in BatchOperate, and is not
// measured here.
func BenchmarkBuildVerifiedMarkerRecords(b *testing.B) {
	for _, inputs := range []int{1, 4, 8} {
		for _, mode := range []string{"filtered", "native"} {
			b.Run(fmt.Sprintf("inputs=%d/%s", inputs, mode), func(b *testing.B) {
				s := &Service{namespace: "test", set: "test", utxoBatchSize: 128, fieldUtxos: fields.Utxos.String(), fieldDeletedChildren: fields.DeletedChildren.String()}
				if mode == "native" {
					s.buildVerifiedMarkerRecord = testNativeMarkerRecord
				}

				updates := markerBenchChunk(b, s, 1024, inputs, 1024*inputs, true)

				b.ReportAllocs()
				b.ResetTimer()

				for i := 0; i < b.N; i++ {
					records, _, _ := s.buildVerifiedMarkerRecords(updates)
					if len(records) != len(updates) {
						b.Fatalf("built %d records for %d parents", len(records), len(updates))
					}
				}
			})
		}
	}
}

// TestFinishPartitionWorker pins the end of a partition worker, which runs on every path: it waits
// for the chunks still in flight, because each adds its deletes to the carry when it finishes, then
// drains the carry and adds the result to the worker's counts. A drain that fails is returned only
// when the worker has no error of its own.
func TestFinishPartitionWorker(t *testing.T) {
	newWorker := func(t *testing.T) (*Service, *deleteCarry, chainhash.Hash, *[][]string) {
		t.Helper()

		s := newTestService(t)
		s.settings.Pruner.SkipDeletions = false
		s.drainFlushTimeout = 200 * time.Millisecond

		child := chainhash.HashH([]byte("finish partition worker child"))

		masterKey, err := aerospike.NewKey(s.namespace, s.set, uaerospike.CalculateKeySourceInternal(&child, 0))
		require.NoError(t, err)

		sent := &[][]string{}

		s.batchOperateFn = func(_ *aerospike.BatchPolicy, records []aerospike.BatchRecordIfc) error {
			batch := make([]string, 0, len(records))

			for _, rec := range records {
				batch = append(batch, string(rec.BatchRec().Key.Digest()))
				rec.BatchRec().ResultCode = types.OK
			}

			*sent = append(*sent, batch)

			return nil
		}

		carry := &deleteCarry{blockHeight: testPruneHeight}
		carry.add(&deleteCarry{
			deletions: []*pendingDeletion{{txHash: &child, keys: []*aerospike.Key{masterKey}}},
			updates:   []map[string]*parentUpdateInfo{{}},
		})

		return s, carry, child, sent
	}

	t.Run("it waits for a chunk still in flight before it drains", func(t *testing.T) {
		s, _, child, sent := newWorker(t)

		// The chunk owes its delete to the carry only once it finishes.
		inFlight := &deleteCarry{blockHeight: testPruneHeight}

		masterKey, err := aerospike.NewKey(s.namespace, s.set, uaerospike.CalculateKeySourceInternal(&child, 0))
		require.NoError(t, err)

		release := make(chan struct{})

		var group errgroup.Group

		group.Go(func() error {
			<-release
			inFlight.add(&deleteCarry{
				deletions: []*pendingDeletion{{txHash: &child, keys: []*aerospike.Key{masterKey}}},
				updates:   []map[string]*parentUpdateInfo{{}},
			})

			return nil
		})

		time.AfterFunc(150*time.Millisecond, func() { close(release) })

		var processed, skipped int64

		var finishErr error

		s.finishPartitionWorker(context.Background(), &group, inFlight, &processed, &skipped, &finishErr)

		require.NoError(t, finishErr)
		require.Equal(t, int64(1), processed, "the delete the chunk added when it finished is drained, not left behind its marker")
		require.Len(t, *sent, 1)
	})

	t.Run("a drain error is returned when the worker has none of its own", func(t *testing.T) {
		s, carry, _, _ := newWorker(t)
		s.batchOperateFn = func(_ *aerospike.BatchPolicy, _ []aerospike.BatchRecordIfc) error {
			return &aerospike.AerospikeError{ResultCode: types.TIMEOUT}
		}

		var processed, skipped int64

		var err error

		s.finishPartitionWorker(context.Background(), &errgroup.Group{}, carry, &processed, &skipped, &err)

		require.Error(t, err, "a drain that could not send what the worker owes must not read as success")
		require.Zero(t, processed)
	})

	t.Run("the worker's own error wins over the drain's", func(t *testing.T) {
		s, carry, _, _ := newWorker(t)
		s.batchOperateFn = func(_ *aerospike.BatchPolicy, _ []aerospike.BatchRecordIfc) error {
			return &aerospike.AerospikeError{ResultCode: types.TIMEOUT}
		}

		own := errors.NewProcessingError("the worker's own failure")

		var processed, skipped int64

		err := error(own)

		s.finishPartitionWorker(context.Background(), &errgroup.Group{}, carry, &processed, &skipped, &err)

		require.Same(t, own, err)
	})
}
