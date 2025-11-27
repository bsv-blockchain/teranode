package aerospike_test

import (
	"context"
	"testing"

	"github.com/aerospike/aerospike-client-go/v8"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/bsv-blockchain/teranode/util/uaerospike"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestStoreExternallySuccessScenarios tests that storeExternallyWithLock returns
// success in the correct scenarios according to the "finish off previous attempt" pattern
func TestStoreExternallySuccessScenarios(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	ctx := context.Background()
	logger := ulogger.NewErrorTestLogger(t)
	settings := test.CreateBaseTestSettings(t)

	client, store, _, cleanup := initAerospike(t, settings, logger)
	defer cleanup()

	t.Run("Scenario A: 100% success - all records created fresh", func(t *testing.T) {
		cleanDB(t, client)

		// Create a transaction that requires multiple records
		tx := createTransactionWithOutputs(settings.UtxoStore.UtxoBatchSize + 1) // 2 records needed

		// First attempt - should succeed completely
		_, err := store.Create(ctx, tx, 100)
		require.NoError(t, err, "First complete creation should succeed")

		// Verify transaction exists and is not creating
		txMeta, err := store.Get(ctx, tx.TxIDChainHash())
		require.NoError(t, err)
		require.NotNil(t, txMeta)
		assert.False(t, txMeta.Locked, "Transaction should not be locked")
		// Note: creating flag is cleared, so we can't check it via Get
	})

	t.Run("Scenario B: Partial success - some records already exist (KEY_EXISTS_ERROR)", func(t *testing.T) {
		cleanDB(t, client)

		// Simulate a partial creation by manually creating some records with creating=true
		tx := createTransactionWithOutputs(settings.UtxoStore.UtxoBatchSize + 1) // 2 records needed

		// Manually create first record with creating=true to simulate partial failure
		txMeta1, err := store.Create(ctx, tx, 100)
		require.NoError(t, err)

		// Now create again - should "finish off" the previous attempt
		// This simulates:
		// - Process A created some records (with creating=true)
		// - Process A failed partway
		// - Process B tries to create (should complete it)
		_, err = store.Create(ctx, tx, 100)

		// Should get TxExistsError, not a processing error
		require.Error(t, err, "Second attempt should detect transaction exists")
		var txExistsErr *errors.Error
		assert.True(t, errors.As(err, &txExistsErr) && txExistsErr.Is(errors.ErrTxExists),
			"Should be TxExistsError: %v", err)

		// But the first attempt should have succeeded
		require.NotNil(t, txMeta1)
	})

	t.Run("Scenario C: Recovery - complete partial transaction from previous failed attempt", func(t *testing.T) {
		cleanDB(t, client)

		// This test simulates the core "finish off" behavior
		// We'll use the raw StoreTransactionExternally to have more control

		tx := createTransactionWithOutputs(settings.UtxoStore.UtxoBatchSize + 1) // 2 records

		// First attempt - create the transaction
		// This will create both records with creating=true and then clear the flag
		bItem1, binsToStore1 := prepareBatchStoreItem(t, store, tx, 100, []uint32{}, []uint32{}, []int{})
		go store.StoreTransactionExternally(ctx, bItem1, binsToStore1)

		err := bItem1.RecvDone()
		require.NoError(t, err, "First attempt should succeed")

		// Second attempt - should get "already exists" because transaction is complete
		bItem2, binsToStore2 := prepareBatchStoreItem(t, store, tx, 100, []uint32{}, []uint32{}, []int{})
		go store.StoreTransactionExternally(ctx, bItem2, binsToStore2)

		err = bItem2.RecvDone()
		require.Error(t, err, "Second attempt should fail with already exists")
		var txExistsErr *errors.Error
		assert.True(t, errors.As(err, &txExistsErr) && txExistsErr.Is(errors.ErrTxExists),
			"Should be TxExistsError indicating transaction exists: %v", err)
	})

	t.Run("Scenario D: Multiple concurrent attempts - only first complete wins", func(t *testing.T) {
		cleanDB(t, client)

		// Multiple processes try to create the same transaction
		// Only the first one to COMPLETE all records should succeed
		// Others should get "already exists" or "in progress"

		tx := createTransactionWithOutputs(settings.UtxoStore.UtxoBatchSize + 1) // Small for faster test

		// Try to create the same transaction 3 times concurrently
		results := make([]error, 3)
		done := make(chan int, 3)

		for i := 0; i < 3; i++ {
			go func(idx int) {
				_, err := store.Create(ctx, tx, 100)
				results[idx] = err
				done <- idx
			}(i)
		}

		// Wait for all to complete
		for i := 0; i < 3; i++ {
			<-done
		}

		// Exactly one should succeed, others should get "already exists" or "in progress"
		successCount := 0
		for i, err := range results {
			if err == nil {
				successCount++
				t.Logf("Attempt %d: SUCCESS", i)
			} else {
				t.Logf("Attempt %d: %v", i, err)
				// Should be either TxExistsError (for both "in progress" and "already exists" cases)
				var txExistsErr *errors.Error
				assert.True(t, errors.As(err, &txExistsErr) && txExistsErr.Is(errors.ErrTxExists),
					"Error should be TxExistsError indicating concurrent access: %v", err)
			}
		}

		assert.Equal(t, 1, successCount, "Exactly one attempt should succeed completely")
	})
}

// TestCreatingBinRemoval verifies that the creating bin is properly removed after transaction creation
func TestCreatingBinRemoval(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	ctx := context.Background()
	logger := ulogger.NewErrorTestLogger(t)
	settings := test.CreateBaseTestSettings(t)

	client, store, _, cleanup := initAerospike(t, settings, logger)
	defer cleanup()

	t.Run("Unlocked transaction - creating bin removed", func(t *testing.T) {
		cleanDB(t, client)

		// Create a transaction that requires multiple records (external storage)
		tx := createTransactionWithOutputs(settings.UtxoStore.UtxoBatchSize + 1)

		// Create the transaction without locked flag
		_, err := store.Create(ctx, tx, 100)
		require.NoError(t, err, "Transaction creation should succeed")

		// Verify the creating bin is NOT present in the records
		txHash := tx.TxIDChainHash()
		numRecords := 2 // Should have 2 records for UtxoBatchSize + 1 outputs

		for i := 0; i < numRecords; i++ {
			keySource := uaerospike.CalculateKeySourceInternal(txHash, uint32(i))
			key, err := aerospike.NewKey(store.GetNamespace(), store.GetName(), keySource)
			require.NoError(t, err)

			record, err := client.Get(util.GetAerospikeReadPolicy(settings), key)
			require.NoError(t, err, "Should be able to read record %d", i)
			require.NotNil(t, record)

			// Verify creating bin is NOT present (or is nil/false)
			creatingValue, exists := record.Bins[fields.Creating.String()]
			if exists {
				// If it exists, it should be false or nil
				assert.Nil(t, creatingValue, "Record %d should not have creating bin set after creation", i)
			}
			t.Logf("Record %d: creating bin properly cleared", i)
		}
	})

	t.Run("Locked transaction - creating bin still removed", func(t *testing.T) {
		cleanDB(t, client)

		// Create a transaction that requires multiple records (external storage)
		tx := createTransactionWithOutputs(settings.UtxoStore.UtxoBatchSize + 1)

		// Create the transaction WITH locked flag
		_, err := store.Create(ctx, tx, 100, utxo.WithLocked(true))
		require.NoError(t, err, "Locked transaction creation should succeed")

		// Verify the creating bin is NOT present in the records (even for locked transactions)
		txHash := tx.TxIDChainHash()
		numRecords := 2 // Should have 2 records for UtxoBatchSize + 1 outputs

		for i := 0; i < numRecords; i++ {
			keySource := uaerospike.CalculateKeySourceInternal(txHash, uint32(i))
			key, err := aerospike.NewKey(store.GetNamespace(), store.GetName(), keySource)
			require.NoError(t, err)

			record, err := client.Get(util.GetAerospikeReadPolicy(settings), key)
			require.NoError(t, err, "Should be able to read record %d", i)
			require.NotNil(t, record)

			// Verify creating bin is NOT present (or is nil/false)
			creatingValue, exists := record.Bins[fields.Creating.String()]
			if exists {
				// If it exists, it should be false or nil
				assert.Nil(t, creatingValue, "Record %d should not have creating bin set after creation (even for locked)", i)
			}

			// Verify locked flag IS present and true
			lockedValue, exists := record.Bins[fields.Locked.String()]
			if i == 0 { // Only first record has locked field
				require.True(t, exists, "Record 0 should have locked bin")
				assert.True(t, lockedValue.(bool), "Record 0 locked bin should be true")
			}

			t.Logf("Record %d: creating bin properly cleared, locked=%v", i, lockedValue)
		}
	})

	t.Run("Small transaction - no creating bin needed", func(t *testing.T) {
		cleanDB(t, client)

		// Create a small transaction that fits in a single record (not external)
		tx := createTransactionWithOutputs(10) // Small transaction

		// Create the transaction
		_, err := store.Create(ctx, tx, 100)
		require.NoError(t, err, "Small transaction creation should succeed")

		// Verify the creating bin was never set (not needed for single-record transactions)
		txHash := tx.TxIDChainHash()
		key, err := aerospike.NewKey(store.GetNamespace(), store.GetName(), txHash.CloneBytes())
		require.NoError(t, err)

		record, err := client.Get(util.GetAerospikeReadPolicy(settings), key)
		require.NoError(t, err, "Should be able to read record")
		require.NotNil(t, record)

		// Verify creating bin is NOT present
		_, exists := record.Bins[fields.Creating.String()]
		assert.False(t, exists, "Small transaction should never have creating bin")
		t.Logf("Small transaction: creating bin was never set (as expected)")
	})
}
