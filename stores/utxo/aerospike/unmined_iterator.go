package aerospike

import (
	"context"
	"math"

	as "github.com/aerospike/aerospike-client-go/v8"
	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/util"
)

// unminedTxIterator implements utxo.UnminedTxIterator for Aerospike
// It scans all records in the set and yields those that are not mined (i.e., unmined/mempool)
type unminedTxIterator struct {
	store     *Store
	fullScan  bool
	err       error
	done      bool
	recordset *as.Recordset
	result    <-chan *as.Result
}

// newUnminedTxIterator creates a new iterator for scanning unmined transactions in Aerospike.
// The iterator uses a scan operation to traverse all records in the set and filters
// for transactions that don't have block IDs (indicating they are unmined/mempool transactions).
//
// Parameters:
//   - store: The Aerospike store instance to iterate over
//   - fullScan: If true, performs a full scan of all records; if false, applies a filter to limit results
//
// Returns:
//   - *unminedTxIterator: A new iterator instance ready for use
//   - error: Any error encountered during iterator initialization
func newUnminedTxIterator(store *Store, fullScan bool) (*unminedTxIterator, error) {
	it := &unminedTxIterator{
		store:    store,
		fullScan: fullScan,
	}

	stmt := as.NewStatement(store.namespace, store.setName)

	if !fullScan {
		if err := stmt.SetFilter(as.NewRangeFilter(fields.UnminedSince.String(), 1, int64(math.MaxUint32))); err != nil {
			return nil, err
		}
	}

	// Set the bins to retrieve only the necessary fields for unmined transactions
	stmt.BinNames = []string{
		fields.TxID.String(),
		fields.Fee.String(),
		fields.SizeInBytes.String(),
		fields.External.String(),
		fields.Inputs.String(),
		fields.CreatedAt.String(),
		fields.Conflicting.String(),
		fields.Locked.String(),
		fields.BlockIDs.String(),
		fields.UnminedSince.String(),
		fields.IsCoinbase.String(),
	}

	policy := util.GetAerospikeQueryPolicy(store.settings)
	policy.IncludeBinData = true

	store.logger.Infof("[newUnminedTxIterator] Starting Aerospike query for unmined transactions (fullScan=%t)", fullScan)
	recordset, err := store.client.Query(policy, stmt)
	if err != nil {
		store.logger.Errorf("[newUnminedTxIterator] Aerospike query failed: %v", err)
		return nil, err
	}
	store.logger.Infof("[newUnminedTxIterator] Aerospike query completed successfully")

	it.recordset = recordset
	it.result = recordset.Results()

	return it, nil
}

// Next advances the iterator and returns the next unmined transaction.
// It filters records to only return transactions that don't have block IDs,
// indicating they are unmined (in mempool). The method handles external storage
// retrieval for large transactions when necessary.
//
// Parameters:
//   - ctx: Context for cancellation
//
// Returns:
//   - *utxo.UnminedTransaction: The next unmined transaction, or nil if iteration is complete
//   - error: Any error encountered during iteration
func (it *unminedTxIterator) Next(ctx context.Context) (*utxo.UnminedTransaction, error) {
	if it.done || it.err != nil || it.recordset == nil {
		return nil, it.err
	}

	// Loop to handle skipping conflicting transactions without recursion
	var rec *as.Result
	var ok bool
	for {
		rec, ok = <-it.result
		if !ok || rec == nil {
			it.closeWithLogging()
			return nil, nil
		}

		if rec.Err != nil {
			it.closeWithLogging()
			it.err = rec.Err
			return nil, it.err
		}

		// Check if the transaction is conflicting and skip it if so
		// Note: This implements a dual filtering approach where conflicting transactions
		// can be filtered at both the SQL query level (for SQL-backed stores) and here
		// at the iterator level (for Aerospike stores). This ensures consistent behavior
		// across different storage backends while allowing each to optimize filtering
		// based on their capabilities.
		conflictingVal := rec.Record.Bins[fields.Conflicting.String()]
		if conflictingVal != nil {
			conflicting, ok := conflictingVal.(bool)
			if ok && conflicting {
				// Skip conflicting transaction and continue to next iteration
				continue
			}
		}

		// If we reach here, we have a non-conflicting transaction
		break
	}

	coinbaseBool, ok := rec.Record.Bins[fields.IsCoinbase.String()].(bool)
	if ok && coinbaseBool {
		// Skip coinbase transactions as they are not unmined
		return &utxo.UnminedTransaction{
			Skip: true,
		}, nil
	}

	// Extract transaction data from the record
	txData, err := it.extractTransactionData(rec.Record.Bins)
	if err != nil {
		it.closeWithLogging()
		it.err = err
		return nil, errors.NewProcessingError("invalid transaction data", err)
	}

	blockIDs, err := processBlockIDs(rec.Record.Bins)
	if err != nil {
		it.closeWithLogging()
		return nil, errors.NewProcessingError("invalid block IDs for %s", txData.hash.String(), err)
	}

	// Process external transaction if needed
	txInpoints, err := it.processTransactionInpoints(ctx, txData, rec.Record.Bins)
	if err != nil {
		if it.fullScan {
			// In full scan mode, if we encounter an error processing inpoints and the transaction
			// has block IDs, it has already been mined. We can skip it.
			return &utxo.UnminedTransaction{
				Hash: txData.hash,
				Fee:  txData.fee,
				Size: txData.size,
				Skip: true,
			}, nil
		}

		it.closeWithLogging()
		it.err = err
		return nil, errors.NewProcessingError("failed to process transaction inpoints for %s", txData.hash.String(), err)
	}

	// Extract created_at timestamp
	createdAt, err := it.extractCreatedAt(rec.Record.Bins)
	if err != nil {
		it.closeWithLogging()
		return nil, errors.NewProcessingError("invalid created_at for %s", txData.hash.String(), err)
	}

	locked, err := it.extractLocked(rec.Record.Bins)
	if err != nil {
		it.closeWithLogging()
		return nil, errors.NewProcessingError("invalid locked status for %s", txData.hash.String(), err)
	}

	return &utxo.UnminedTransaction{
		Hash:         txData.hash,
		Fee:          txData.fee,
		Size:         txData.size,
		UnminedSince: txData.unminedSince,
		TxInpoints:   txInpoints,
		CreatedAt:    createdAt,
		Locked:       locked,
		BlockIDs:     blockIDs,
	}, nil
}

// transactionData holds the basic transaction data extracted from a record
type transactionData struct {
	hash         *chainhash.Hash
	fee          uint64
	size         uint64
	unminedSince int
}

// closeWithLogging closes the iterator with error logging
func (it *unminedTxIterator) closeWithLogging() {
	if err := it.Close(); err != nil {
		it.store.logger.Warnf("failed to close iterator: %v", err)
	}
}

// extractTransactionData extracts basic transaction data from Aerospike record bins
func (it *unminedTxIterator) extractTransactionData(bins map[string]interface{}) (*transactionData, error) {
	// Extract and validate txid
	txidVal := bins[fields.TxID.String()]
	if txidVal == nil {
		return nil, errors.NewProcessingError("txid not found")
	}

	txidValBytes, ok := txidVal.([]byte)
	if !ok {
		return nil, errors.NewProcessingError("txid not []byte")
	}

	hash, err := chainhash.NewHash(txidValBytes)
	if err != nil {
		return nil, err
	}

	// Extract and validate fee
	feeVal := bins[fields.Fee.String()]
	if feeVal == nil {
		return nil, errors.NewProcessingError("fee not found")
	}

	fee, err := toUint64(feeVal)
	if err != nil {
		return nil, errors.NewProcessingError("Failed to convert fee")
	}

	// Extract and validate size
	sizeVal := bins[fields.SizeInBytes.String()]
	if sizeVal == nil {
		return nil, errors.NewProcessingError("size not found")
	}

	size, _ := toUint64(sizeVal)

	unminedSince, _ := bins[fields.UnminedSince.String()].(int)

	return &transactionData{
		hash:         hash,
		fee:          fee,
		size:         size,
		unminedSince: unminedSince,
	}, nil
}

// processTransactionInpoints processes transaction inputs based on whether it's external or internal
func (it *unminedTxIterator) processTransactionInpoints(ctx context.Context, txData *transactionData, bins map[string]interface{}) (subtree.TxInpoints, error) {
	external, ok := bins[fields.External.String()].(bool)
	if !ok || !external {
		return it.processInternalTransactionInpoints(bins)
	}

	return it.processExternalTransactionInpoints(ctx, txData.hash)
}

// processExternalTransactionInpoints processes inputs for external transactions
func (it *unminedTxIterator) processExternalTransactionInpoints(ctx context.Context, hash *chainhash.Hash) (subtree.TxInpoints, error) {
	var externalTx *bt.Tx
	var err error

	externalTx, err = it.store.GetTxFromExternalStore(ctx, *hash)
	if err != nil {
		return subtree.TxInpoints{}, err
	}

	txInpoints, err := subtree.NewTxInpointsFromTx(externalTx)
	if err != nil {
		return subtree.TxInpoints{}, errors.NewTxInvalidError("could not process tx inpoints", err)
	}

	return txInpoints, nil
}

// processInternalTransactionInpoints processes inputs for internal transactions
func (it *unminedTxIterator) processInternalTransactionInpoints(bins map[string]interface{}) (subtree.TxInpoints, error) {
	txInpoints, err := processInputsToTxInpoints(bins)
	if err != nil {
		return subtree.TxInpoints{}, errors.NewTxInvalidError("could not process input interfaces", err)
	}

	return txInpoints, nil
}

// extractCreatedAt extracts the created_at timestamp from record bins
func (it *unminedTxIterator) extractCreatedAt(bins map[string]interface{}) (int, error) {
	createdAtVal, ok := bins[fields.CreatedAt.String()]
	if !ok || createdAtVal == nil {
		return 0, errors.NewProcessingError("created_at not found")
	}

	createdAt, ok := createdAtVal.(int)
	if !ok {
		return 0, errors.NewProcessingError("created_at not int64")
	}

	return createdAt, nil
}

// extractLocked extracts the locked status from record bins
func (it *unminedTxIterator) extractLocked(bins map[string]interface{}) (bool, error) {
	// Locked status is optional, so we check if it exists
	lockedVal, ok := bins[fields.Locked.String()].(bool)
	if ok {
		return lockedVal, nil
	}

	return false, nil
}

// Err returns the first error encountered during iteration, if any.
// This should be called after Next returns nil to check if iteration
// completed successfully or due to an error.
//
// Returns:
//   - error: The error that caused iteration to stop, or nil if no error occurred
func (it *unminedTxIterator) Err() error {
	return it.err
}

// Close releases resources held by the iterator and marks it as done.
// It's safe to call Close multiple times. After calling Close, subsequent
// calls to Next will return nil.
//
// Returns:
//   - error: Always returns nil (kept for interface compatibility)
func (it *unminedTxIterator) Close() error {
	it.done = true

	return it.recordset.Close()
}

// toUint64 converts various numeric interface{} types to uint64.
// This utility function handles type assertions for Aerospike record values
// which can come in different numeric types depending on the data source.
//
// Parameters:
//   - val: The interface{} value to convert (should be a numeric type)
//
// Returns:
//   - uint64: The converted value
//   - error: Error if the value cannot be converted to uint64
//
// nolint: gosec
func toUint64(val interface{}) (uint64, error) {
	switch v := val.(type) {
	case int:
		return uint64(v), nil
	case int64:
		return uint64(v), nil
	case uint64:
		return v, nil
	case float64:
		return uint64(v), nil
	case uint32:
		return uint64(v), nil
	case float32:
		return uint64(v), nil
	case nil:
		return 0, nil
	default:
		return 0, errors.NewProcessingError("unknown type for uint64 conversion")
	}
}

// GetUnminedTxIterator implements utxo.Store for Aerospike
func (s *Store) GetUnminedTxIterator(fullScan bool) (utxo.UnminedTxIterator, error) {
	if s.client == nil {
		return nil, errors.NewProcessingError("aerospike client not initialized")
	}

	return newUnminedTxIterator(s, fullScan)
}
