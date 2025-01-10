// //go:build aerospike

// Package aerospike provides an Aerospike-based implementation of the UTXO store interface.
// It offers high performance, distributed storage capabilities with support for large-scale
// UTXO sets and complex operations like freezing, reassignment, and batch processing.
//
// # Architecture
//
// The implementation uses a combination of Aerospike Key-Value store and Lua scripts
// for atomic operations. Transactions are stored with the following structure:
//   - Main Record: Contains transaction metadata and up to 20,000 UTXOs
//   - Pagination Records: Additional records for transactions with >20,000 outputs
//   - External Storage: Optional blob storage for large transactions
//
// # Features
//
//   - Efficient UTXO lifecycle management (create, spend, unspend)
//   - Support for batched operations with LUA scripting
//   - Automatic cleanup of spent UTXOs through TTL
//   - Alert system integration for freezing/unfreezing UTXOs
//   - Metrics tracking via Prometheus
//   - Support for large transactions through external blob storage
//
// # Usage
//
//	store, err := aerospike.New(ctx, logger, settings, &url.URL{
//	    Scheme: "aerospike",
//	    Host:   "localhost:3000",
//	    Path:   "/test/utxos",
//	    RawQuery: "expiration=3600&set=txmeta",
//	})
//
// # Database Structure
//
// Normal Transaction:
//   - inputs: Transaction input data
//   - outputs: Transaction output data
//   - utxos: List of UTXO hashes
//   - nrUtxos: Total number of UTXOs
//   - spentUtxos: Number of spent UTXOs
//   - blockIDs: Block references
//   - isCoinbase: Coinbase flag
//   - spendingHeight: Coinbase maturity height
//   - frozen: Frozen status
//
// Large Transaction with External Storage:
//   - Same as normal but with external=true
//   - Transaction data stored in blob storage
//   - Multiple records for >20k outputs
//
// # Thread Safety
//
// The implementation is fully thread-safe and supports concurrent access through:
//   - Atomic operations via Lua scripts
//   - Batched operations for better performance
//   - Lock-free reads with optimistic concurrency
package aerospike

import (
	"context"

	"github.com/aerospike/aerospike-client-go/v7"
	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/util"
	"github.com/libsv/go-bt/v2/chainhash"
)

// Delete removes a transaction and its associated UTXOs from the store.
// If the transaction doesn't exist, the operation is considered successful.
//
// The operation:
//   - Removes the main transaction record
//   - Does not cascade to external storage
//   - Does not affect pagination records
//
// Note: This is a partial deletion that only removes the main transaction record.
// For complete cleanup, additional steps may be needed:
//   - Remove external transaction data (.tx file)
//   - Remove external output data (.outputs file)
//   - Remove pagination records
//   - Clean up cross-references
//
// Parameters:
//   - ctx: Context for cancellation (currently unused)
//   - hash: Transaction hash to delete
//
// Returns:
//   - nil if deletion successful or record not found
//   - error if deletion fails for other reasons
//
// Examples:
//
//	// Delete a transaction
//	err := store.Delete(ctx, txHash)
//	if err != nil {
//	    if errors.Is(err, aerospike.ErrKeyNotFound) {
//	        // Handle not found case
//	    } else {
//	        // Handle other errors
//	    }
//	}
//
// Metrics:
//   - prometheusUtxoMapDelete: Incremented on successful deletion
//   - prometheusUtxoMapErrors: Incremented on deletion errors
func (s *Store) Delete(_ context.Context, hash *chainhash.Hash) error {
	policy := util.GetAerospikeWritePolicy(s.settings, 0, aerospike.TTLDontExpire)

	key, err := aerospike.NewKey(s.namespace, s.setName, hash[:])
	if err != nil {
		return errors.NewProcessingError("error in aerospike NewKey", err)
	}

	_, err = s.client.Delete(policy, key)
	if err != nil {
		// if the key is not found, we don't need to delete, it's not there anyway
		if errors.Is(err, aerospike.ErrKeyNotFound) {
			return nil
		}

		prometheusUtxoMapErrors.WithLabelValues("Delete", err.Error()).Inc()

		return errors.NewStorageError("error in aerospike delete key", err)
	}

	prometheusUtxoMapDelete.Inc()

	return nil
}
