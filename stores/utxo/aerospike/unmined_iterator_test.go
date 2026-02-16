package aerospike

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_extractLocked(t *testing.T) {
	it := &unminedTxIterator{}

	locked, err := it.extractLocked(map[string]interface{}{
		fields.Locked.String(): true,
	})
	require.NoError(t, err)
	assert.True(t, locked)

	locked, err = it.extractLocked(map[string]interface{}{
		fields.Locked.String(): false,
	})
	require.NoError(t, err)
	assert.False(t, locked)

	// missing field should default to false
	locked, err = it.extractLocked(map[string]interface{}{})
	require.NoError(t, err)
	assert.False(t, locked)
}

// Test_toUint64 tests the toUint64 utility function
func Test_toUint64(t *testing.T) {
	tests := []struct {
		name     string
		input    interface{}
		expected uint64
		hasError bool
	}{
		{"int", int(42), 42, false},
		{"int64", int64(42), 42, false},
		{"uint64", uint64(42), 42, false},
		{"uint32", uint32(42), 42, false},
		{"float64", float64(42.5), 42, false},
		{"float32", float32(42.5), 42, false},
		{"nil", nil, 0, false},
		{"string", "invalid", 0, true},
		{"bool", true, 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := toUint64(tt.input)
			if tt.hasError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expected, result)
			}
		})
	}
}

// Test_extractCreatedAt tests the extractCreatedAt method
func Test_extractCreatedAt(t *testing.T) {
	it := &unminedTxIterator{}

	t.Run("ValidCreatedAt", func(t *testing.T) {
		bins := map[string]interface{}{
			fields.CreatedAt.String(): int(1234567890),
		}
		createdAt, err := it.extractCreatedAt(bins)
		assert.NoError(t, err)
		assert.Equal(t, 1234567890, createdAt)
	})

	t.Run("MissingCreatedAt", func(t *testing.T) {
		bins := map[string]interface{}{}
		_, err := it.extractCreatedAt(bins)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "createdAt not found")
	})

	t.Run("NilCreatedAt", func(t *testing.T) {
		bins := map[string]interface{}{
			fields.CreatedAt.String(): nil,
		}
		_, err := it.extractCreatedAt(bins)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "createdAt not found")
	})

	t.Run("InvalidCreatedAtType", func(t *testing.T) {
		bins := map[string]interface{}{
			fields.CreatedAt.String(): "not-an-int",
		}
		_, err := it.extractCreatedAt(bins)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "createdAt not int64")
	})
}

// Test_extractTransactionData tests the extractTransactionData method
func Test_extractTransactionData(t *testing.T) {
	it := &unminedTxIterator{}

	// Create a valid hash
	validHash := chainhash.DoubleHashH([]byte("test transaction"))

	t.Run("ValidTransactionData", func(t *testing.T) {
		bins := map[string]interface{}{
			fields.TxID.String():        validHash[:],
			fields.Fee.String():         uint64(1000),
			fields.SizeInBytes.String(): uint64(250),
		}
		txData, err := it.extractTransactionData(bins)
		assert.NoError(t, err)
		assert.NotNil(t, txData)
		assert.Equal(t, validHash, *txData.hash)
		assert.Equal(t, uint64(1000), txData.fee)
		assert.Equal(t, uint64(250), txData.size)
	})

	t.Run("MissingTxID", func(t *testing.T) {
		bins := map[string]interface{}{
			fields.Fee.String():         uint64(1000),
			fields.SizeInBytes.String(): uint64(250),
		}
		_, err := it.extractTransactionData(bins)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "txid not found")
	})

	t.Run("InvalidTxIDType", func(t *testing.T) {
		bins := map[string]interface{}{
			fields.TxID.String():        "not-bytes",
			fields.Fee.String():         uint64(1000),
			fields.SizeInBytes.String(): uint64(250),
		}
		_, err := it.extractTransactionData(bins)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "txid not []byte")
	})

	t.Run("MissingFee", func(t *testing.T) {
		bins := map[string]interface{}{
			fields.TxID.String():        validHash[:],
			fields.SizeInBytes.String(): uint64(250),
		}
		_, err := it.extractTransactionData(bins)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "fee not found")
	})

	t.Run("MissingSize", func(t *testing.T) {
		bins := map[string]interface{}{
			fields.TxID.String(): validHash[:],
			fields.Fee.String():  uint64(1000),
		}
		_, err := it.extractTransactionData(bins)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "size not found")
	})
}

// Test_unminedTxIterator_Err tests the Err method
func Test_unminedTxIterator_Err(t *testing.T) {
	it := &unminedTxIterator{}

	t.Run("NoError", func(t *testing.T) {
		assert.NoError(t, it.Err())
	})

	t.Run("WithError", func(t *testing.T) {
		testErr := errors.NewError("test error")
		it.err = testErr
		assert.Equal(t, testErr, it.Err())
	})
}

// Test_unminedTxIterator_Close tests the Close method with nil recordset
func Test_unminedTxIterator_Close(t *testing.T) {
	t.Run("CloseWithNilRecordset", func(t *testing.T) {
		it := &unminedTxIterator{
			recordset: nil,
			done:      false,
		}

		// Should not panic - nil recordset is handled gracefully
		err := it.Close()
		assert.NoError(t, err)
		assert.True(t, it.done)
	})

	t.Run("CloseSetsDoneFlag", func(t *testing.T) {
		it := &unminedTxIterator{
			done: false,
		}

		// Test that done flag gets set
		err := it.Close()
		assert.NoError(t, err)
		assert.True(t, it.done)
	})

	t.Run("CloseMultipleTimes", func(t *testing.T) {
		it := &unminedTxIterator{
			done: false,
		}

		// First close
		err := it.Close()
		assert.NoError(t, err)
		assert.True(t, it.done)

		// Second close should be safe
		err = it.Close()
		assert.NoError(t, err)
		assert.True(t, it.done)
	})
}

// Test_processTransactionInpoints tests the processTransactionInpoints method
func Test_processTransactionInpoints(t *testing.T) {
	it := &unminedTxIterator{}
	ctx := context.Background()
	txData := &transactionData{
		hash: &chainhash.Hash{},
	}

	t.Run("InternalTransaction", func(t *testing.T) {
		bins := map[string]interface{}{
			fields.External.String(): false,
		}

		_, err := it.processTransactionInpoints(ctx, txData, bins)
		assert.Error(t, err) // Should error because processInputsToTxInpoints will fail
	})

	t.Run("MissingExternalField", func(t *testing.T) {
		bins := map[string]interface{}{}

		_, err := it.processTransactionInpoints(ctx, txData, bins)
		assert.Error(t, err) // Should error because processInputsToTxInpoints will fail
	})
}

// Test_GetUnminedTxIterator tests the GetUnminedTxIterator method
func Test_GetUnminedTxIterator(t *testing.T) {
	t.Run("NilClient", func(t *testing.T) {
		store := &Store{
			client: nil,
		}

		it, err := store.GetUnminedTxIterator(false)
		assert.Error(t, err)
		assert.Nil(t, it)
		assert.Contains(t, err.Error(), "aerospike client not initialized")
	})
}

// Test_Next_EdgeCases tests edge cases for the Next method
func Test_Next_EdgeCases(t *testing.T) {
	ctx := context.Background()

	t.Run("AlreadyDone", func(t *testing.T) {
		it := &unminedTxIterator{
			done: true,
		}

		tx, err := it.Next(ctx)
		assert.NoError(t, err)
		assert.Nil(t, tx)
	})

	t.Run("HasError", func(t *testing.T) {
		testErr := errors.NewError("test error")
		it := &unminedTxIterator{
			err: testErr,
		}

		tx, err := it.Next(ctx)
		assert.Equal(t, testErr, err)
		assert.Nil(t, tx)
	})

	t.Run("NilRecordset", func(t *testing.T) {
		it := &unminedTxIterator{
			recordset: nil,
		}

		tx, err := it.Next(ctx)
		assert.NoError(t, err)
		assert.Nil(t, tx)
	})

	t.Run("ChannelClosed", func(t *testing.T) {
		// Create a closed channel to simulate end of iteration
		resultChan := make(chan []*utxo.UnminedTransaction)
		close(resultChan)

		errorChan := make(chan error, 1)
		close(errorChan)

		it := &unminedTxIterator{
			resultChan: resultChan,
			errorChan:  errorChan,
		}

		// With nil recordset, should return early with nil/nil
		tx, err := it.Next(ctx)
		assert.NoError(t, err)
		assert.Nil(t, tx)
	})
}

// Test_newUnminedTxIterator tests the newUnminedTxIterator constructor function
func Test_newUnminedTxIterator(t *testing.T) {
	t.Run("NilStore", func(t *testing.T) {
		// This would panic in real usage, but test the behavior
		assert.Panics(t, func() {
			_, _ = newUnminedTxIterator(nil, false)
		})
	})

	t.Run("StoreWithoutClient", func(t *testing.T) {
		store := &Store{
			client:    nil,
			namespace: "test",
			setName:   "utxos",
		}

		// This will panic because client.Query is called on nil
		assert.Panics(t, func() {
			_, _ = newUnminedTxIterator(store, false)
		})
	})
}

// Test_closeWithLogging tests the closeWithLogging method
func Test_closeWithLogging(t *testing.T) {
	t.Run("CloseWithNilRecordset", func(t *testing.T) {
		it := &unminedTxIterator{
			recordset: nil,
			store:     &Store{},
		}

		// Should not panic - nil recordset is handled gracefully
		assert.NotPanics(t, func() {
			it.closeWithLogging()
		})
	})
}

func Test_extractTxIDAndUnminedSince(t *testing.T) {
	t.Run("valid", func(t *testing.T) {
		validHash := chainhash.HashH([]byte("test"))
		bins := map[string]interface{}{
			fields.TxID.String():         validHash[:],
			fields.UnminedSince.String(): 42,
		}
		hash, unminedSince, err := extractTxIDAndUnminedSince(bins)
		require.NoError(t, err)
		assert.Equal(t, validHash, *hash)
		assert.Equal(t, 42, unminedSince)
	})

	t.Run("missing txid", func(t *testing.T) {
		_, _, err := extractTxIDAndUnminedSince(map[string]interface{}{})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "txid not found")
	})

	t.Run("wrong txid type", func(t *testing.T) {
		bins := map[string]interface{}{
			fields.TxID.String(): 12345,
		}
		_, _, err := extractTxIDAndUnminedSince(bins)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "txid not []byte")
	})

	t.Run("missing unmined_since defaults to zero", func(t *testing.T) {
		validHash := chainhash.HashH([]byte("test"))
		bins := map[string]interface{}{
			fields.TxID.String(): validHash[:],
		}
		_, unminedSince, err := extractTxIDAndUnminedSince(bins)
		require.NoError(t, err)
		assert.Equal(t, 0, unminedSince)
	})
}

func Test_processPrunerRecord(t *testing.T) {
	ctx := context.Background()

	t.Run("valid record", func(t *testing.T) {
		validHash := chainhash.HashH([]byte("test"))
		it := &unminedTxIterator{
			store: &Store{},
		}

		bins := map[string]interface{}{
			fields.TxID.String():         validHash[:],
			fields.UnminedSince.String(): 5,
			// No external flag and no inputs = empty inpoints (skipped gracefully)
		}

		result, err := it.processPrunerRecord(ctx, bins)
		require.NoError(t, err)
		// No inpoints available (no external, no inputs bin) so processTransactionInpoints
		// returns error and processPrunerRecord returns nil
		assert.Nil(t, result)
	})

	t.Run("missing txid", func(t *testing.T) {
		it := &unminedTxIterator{
			store: &Store{},
		}

		bins := map[string]interface{}{
			fields.UnminedSince.String(): 5,
		}

		_, err := it.processPrunerRecord(ctx, bins)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "txid not found")
	})

	t.Run("invalid txid type", func(t *testing.T) {
		it := &unminedTxIterator{
			store: &Store{},
		}

		bins := map[string]interface{}{
			fields.TxID.String():         "not-bytes",
			fields.UnminedSince.String(): 5,
		}

		_, err := it.processPrunerRecord(ctx, bins)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "txid not []byte")
	})

	t.Run("invalid txid length", func(t *testing.T) {
		it := &unminedTxIterator{
			store: &Store{},
		}

		bins := map[string]interface{}{
			fields.TxID.String():         []byte{0x01, 0x02}, // too short
			fields.UnminedSince.String(): 5,
		}

		_, err := it.processPrunerRecord(ctx, bins)
		require.Error(t, err)
	})

	t.Run("missing unmined since defaults to zero", func(t *testing.T) {
		validHash := chainhash.HashH([]byte("test"))
		it := &unminedTxIterator{
			store: &Store{},
		}

		bins := map[string]interface{}{
			fields.TxID.String(): validHash[:],
			// No UnminedSince - should default to 0
		}

		result, err := it.processPrunerRecord(ctx, bins)
		require.NoError(t, err)
		// inpoint error → returns nil
		assert.Nil(t, result)
	})
}
