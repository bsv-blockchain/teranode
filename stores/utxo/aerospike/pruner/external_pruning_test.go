package pruner

import (
	"context"
	"testing"

	"github.com/aerospike/aerospike-client-go/v8"
	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/stores/blob/memory"
	"github.com/bsv-blockchain/teranode/stores/pruner"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/uaerospike"
	aeroTest "github.com/bsv-blockchain/testcontainers-aerospike-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestExternalTransactionPruning tests that external transaction files are deleted during pruning
func TestExternalTransactionPruning(t *testing.T) {
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

	namespace := "test"
	set := "test"

	mockIndexWaiter := &MockIndexWaiter{
		Client:    client,
		Namespace: namespace,
		Set:       set,
	}

	// Create an in-memory external store to track file operations
	externalStore := memory.New()

	service, err := NewService(createTestSettings(), Options{
		Logger:        logger,
		Client:        client,
		ExternalStore: externalStore,
		Namespace:     namespace,
		Set:           set,
		WorkerCount:   1,
		IndexWaiter:   mockIndexWaiter,
	})
	require.NoError(t, err)

	service.Start(ctx)

	writePolicy := aerospike.NewWritePolicy(0, 0)

	// Create an external transaction with a .outputs file in blob store
	// (using .outputs instead of .tx avoids parsing requirements in pruner)
	txID := chainhash.HashH([]byte("external-outputs"))
	key, _ := aerospike.NewKey(namespace, set, txID[:])

	// Store fake outputs data in external blob store
	fakeOutputsData := []byte("fake-outputs-data")
	err = externalStore.Set(ctx, txID[:], fileformat.FileTypeOutputs, fakeOutputsData)
	require.NoError(t, err)

	// Verify the file exists
	exists, err := externalStore.Exists(ctx, txID[:], fileformat.FileTypeOutputs)
	require.NoError(t, err)
	require.True(t, exists)

	// Create Aerospike record with external=true and deleteAtHeight=5
	err = client.Put(writePolicy, key, aerospike.BinMap{
		fields.TxID.String():           txID.CloneBytes(),
		fields.External.String():       true,
		fields.DeleteAtHeight.String(): 5,
	})
	require.NoError(t, err)

	// Verify the record was created
	record, err := client.Get(nil, key)
	require.NoError(t, err)
	assert.NotNil(t, record)
	assert.True(t, record.Bins[fields.External.String()].(bool))

	// Trigger cleanup at height 5
	done := make(chan string)
	err = service.UpdateBlockHeight(5, done)
	require.NoError(t, err)

	// Wait for completion
	status := <-done
	require.Equal(t, pruner.JobStatusCompleted.String(), status)

	// Verify the Aerospike record was deleted
	record, err = client.Get(nil, key)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not found")

	// Verify the external file was deleted
	exists, err = externalStore.Exists(ctx, txID[:], fileformat.FileTypeOutputs)
	require.NoError(t, err)
	assert.False(t, exists, "External .outputs file should be deleted")
}

// TestExternalTransactionOutputsOnlyPruning tests pruning of outputs-only external transactions
func TestExternalTransactionOutputsOnlyPruning(t *testing.T) {
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

	namespace := "test"
	set := "test"

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
		WorkerCount:   1,
		IndexWaiter:   mockIndexWaiter,
	})
	require.NoError(t, err)

	service.Start(ctx)

	writePolicy := aerospike.NewWritePolicy(0, 0)

	// Create an external transaction with .outputs file (no inputs)
	txID := chainhash.HashH([]byte("external-outputs"))
	key, _ := aerospike.NewKey(namespace, set, txID[:])

	// Store outputs in external blob store
	fakeOutputsData := []byte("fake-outputs-data")
	err = externalStore.Set(ctx, txID[:], fileformat.FileTypeOutputs, fakeOutputsData)
	require.NoError(t, err)

	// Verify the file exists
	exists, err := externalStore.Exists(ctx, txID[:], fileformat.FileTypeOutputs)
	require.NoError(t, err)
	require.True(t, exists)

	// Create Aerospike record with external=true, no inputs, and deleteAtHeight=3
	err = client.Put(writePolicy, key, aerospike.BinMap{
		fields.TxID.String():           txID.CloneBytes(),
		fields.External.String():       true,
		fields.DeleteAtHeight.String(): 3,
	})
	require.NoError(t, err)

	// Trigger cleanup at height 3
	done := make(chan string)
	err = service.UpdateBlockHeight(3, done)
	require.NoError(t, err)

	// Wait for completion
	status := <-done
	require.Equal(t, pruner.JobStatusCompleted.String(), status)

	// Verify the Aerospike record was deleted
	_, err = client.Get(nil, key)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not found")

	// Verify the external .outputs file was deleted
	exists, err = externalStore.Exists(ctx, txID[:], fileformat.FileTypeOutputs)
	require.NoError(t, err)
	assert.False(t, exists, "External .outputs file should be deleted")
}

// TestMultiRecordExternalTransactionPruning tests that all child records are deleted
func TestMultiRecordExternalTransactionPruning(t *testing.T) {
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

	namespace := "test"
	set := "test"

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
		WorkerCount:   1,
		IndexWaiter:   mockIndexWaiter,
	})
	require.NoError(t, err)

	service.Start(ctx)

	writePolicy := aerospike.NewWritePolicy(0, 0)

	// Create a multi-record external transaction (master + 2 child records)
	txID := chainhash.HashH([]byte("multi-record-tx"))

	// Create master record key (txid_0 / txid)
	keyMaster, _ := aerospike.NewKey(namespace, set, txID[:])

	// Create child record keys (txid_1, txid_2)
	keyChild1, _ := aerospike.NewKey(namespace, set, uaerospike.CalculateKeySourceInternal(&txID, 1))
	keyChild2, _ := aerospike.NewKey(namespace, set, uaerospike.CalculateKeySourceInternal(&txID, 2))

	// Store fake outputs data in external blob store
	fakeOutputsData := []byte("fake-multi-record-outputs-data")
	err = externalStore.Set(ctx, txID[:], fileformat.FileTypeOutputs, fakeOutputsData)
	require.NoError(t, err)

	// Create master record with totalExtraRecs=2
	// Note: Using .outputs file avoids parsing requirements
	err = client.Put(writePolicy, keyMaster, aerospike.BinMap{
		fields.TxID.String():           txID.CloneBytes(),
		fields.External.String():       true,
		fields.TotalExtraRecs.String(): 2,
		fields.DeleteAtHeight.String(): 10,
	})
	require.NoError(t, err)

	// Create child record 1
	err = client.Put(writePolicy, keyChild1, aerospike.BinMap{
		fields.TxID.String():           txID.CloneBytes(),
		fields.RecordUtxos.String():    50,
		fields.SpentUtxos.String():     50,
		fields.DeleteAtHeight.String(): 10,
	})
	require.NoError(t, err)

	// Create child record 2
	err = client.Put(writePolicy, keyChild2, aerospike.BinMap{
		fields.TxID.String():           txID.CloneBytes(),
		fields.RecordUtxos.String():    30,
		fields.SpentUtxos.String():     30,
		fields.DeleteAtHeight.String(): 10,
	})
	require.NoError(t, err)

	// Verify all records were created
	record, err := client.Get(nil, keyMaster)
	require.NoError(t, err)
	assert.NotNil(t, record)
	assert.Equal(t, 2, record.Bins[fields.TotalExtraRecs.String()])

	record, err = client.Get(nil, keyChild1)
	require.NoError(t, err)
	assert.NotNil(t, record)

	record, err = client.Get(nil, keyChild2)
	require.NoError(t, err)
	assert.NotNil(t, record)

	// Trigger cleanup at height 10
	done := make(chan string)
	err = service.UpdateBlockHeight(10, done)
	require.NoError(t, err)

	// Wait for completion
	status := <-done
	require.Equal(t, pruner.JobStatusCompleted.String(), status)

	// Verify ALL records were deleted (master + 2 children)
	record, err = client.Get(nil, keyMaster)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not found", "Master record should be deleted")

	record, err = client.Get(nil, keyChild1)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not found", "Child record 1 should be deleted")

	record, err = client.Get(nil, keyChild2)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not found", "Child record 2 should be deleted")

	// Verify the external file was deleted
	exists, err := externalStore.Exists(ctx, txID[:], fileformat.FileTypeOutputs)
	require.NoError(t, err)
	assert.False(t, exists, "External .outputs file should be deleted")
}

// TestExternalFileAlreadyDeleted tests graceful handling when external file is already gone
func TestExternalFileAlreadyDeleted(t *testing.T) {
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

	namespace := "test"
	set := "test"

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
		WorkerCount:   1,
		IndexWaiter:   mockIndexWaiter,
	})
	require.NoError(t, err)

	service.Start(ctx)

	writePolicy := aerospike.NewWritePolicy(0, 0)

	// Create an external transaction record WITHOUT creating the actual file
	// (simulating the file was already deleted by LocalDAH cleanup)
	txID := chainhash.HashH([]byte("missing-file"))
	key, _ := aerospike.NewKey(namespace, set, txID[:])

	err = client.Put(writePolicy, key, aerospike.BinMap{
		fields.TxID.String():           txID.CloneBytes(),
		fields.External.String():       true,
		fields.DeleteAtHeight.String(): 7,
	})
	require.NoError(t, err)

	// Verify the file does NOT exist
	exists, err := externalStore.Exists(ctx, txID[:], fileformat.FileTypeTx)
	require.NoError(t, err)
	require.False(t, exists)

	// Trigger cleanup - should handle missing file gracefully
	done := make(chan string)
	err = service.UpdateBlockHeight(7, done)
	require.NoError(t, err)

	// Wait for completion
	status := <-done
	require.Equal(t, pruner.JobStatusCompleted.String(), status)

	// Verify the Aerospike record was still deleted
	_, err = client.Get(nil, key)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not found")
}

// TestMixedExternalAndNormalTransactions tests pruning of both types in one batch
func TestMixedExternalAndNormalTransactions(t *testing.T) {
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

	namespace := "test"
	set := "test"

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
		WorkerCount:   1,
		IndexWaiter:   mockIndexWaiter,
	})
	require.NoError(t, err)

	service.Start(ctx)

	writePolicy := aerospike.NewWritePolicy(0, 0)

	// Create parent for inputs
	txIDParent := chainhash.HashH([]byte("parent-mixed"))
	keySourceParent := uaerospike.CalculateKeySource(&txIDParent, 0, 128)
	keyParent, _ := aerospike.NewKey(namespace, set, keySourceParent)

	err = client.Put(writePolicy, keyParent, aerospike.BinMap{
		fields.TxID.String():           txIDParent.CloneBytes(),
		fields.DeleteAtHeight.String(): 0,
	})
	require.NoError(t, err)

	input := &bt.Input{
		PreviousTxOutIndex: 0,
		PreviousTxSatoshis: 100,
	}
	_ = input.PreviousTxIDAdd(&txIDParent)

	// Create 1 external transaction (using .outputs to avoid parsing)
	txIDExternal := chainhash.HashH([]byte("external-mixed"))
	keyExternal, _ := aerospike.NewKey(namespace, set, txIDExternal[:])

	// Store fake outputs data in external blob store
	fakeOutputsData := []byte("fake-mixed-outputs-data")
	err = externalStore.Set(ctx, txIDExternal[:], fileformat.FileTypeOutputs, fakeOutputsData)
	require.NoError(t, err)

	err = client.Put(writePolicy, keyExternal, aerospike.BinMap{
		fields.TxID.String():           txIDExternal.CloneBytes(),
		fields.External.String():       true,
		fields.DeleteAtHeight.String(): 15,
	})
	require.NoError(t, err)

	// Create 1 normal transaction (stored inline)
	txIDNormal := chainhash.HashH([]byte("normal-mixed"))
	keyNormal, _ := aerospike.NewKey(namespace, set, txIDNormal[:])

	err = client.Put(writePolicy, keyNormal, aerospike.BinMap{
		fields.TxID.String():           txIDNormal.CloneBytes(),
		fields.Inputs.String():         []interface{}{input.Bytes(true)},
		fields.External.String():       false,
		fields.DeleteAtHeight.String(): 15,
	})
	require.NoError(t, err)

	// Trigger cleanup at height 15
	done := make(chan string)
	err = service.UpdateBlockHeight(15, done)
	require.NoError(t, err)

	// Wait for completion
	status := <-done
	require.Equal(t, pruner.JobStatusCompleted.String(), status)

	// Verify both Aerospike records were deleted
	_, err = client.Get(nil, keyExternal)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not found")

	_, err = client.Get(nil, keyNormal)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not found")

	// Verify ONLY the external file was deleted (normal tx has no file)
	exists, err := externalStore.Exists(ctx, txIDExternal[:], fileformat.FileTypeOutputs)
	require.NoError(t, err)
	assert.False(t, exists, "External .outputs file should be deleted")

	// Verify parent was marked with the deleted normal child (external has no inputs)
	parentRecord, err := client.Get(nil, keyParent)
	require.NoError(t, err)
	assert.NotNil(t, parentRecord)
	deletedChildren := parentRecord.Bins[fields.DeletedChildren.String()].(map[interface{}]interface{})
	assert.True(t, deletedChildren[txIDNormal.String()].(bool), "Normal tx with inputs should update parent")
}
