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
	"time"

	"github.com/bitcoin-sv/teranode/stores/blob"
	"github.com/bitcoin-sv/teranode/util"
	"github.com/bitcoin-sv/teranode/util/uaerospike"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/chainhash"
)

// GetClient was implemented to facilitate testing
func (s *Store) GetClient() *uaerospike.Client {
	return s.client
}

// SetClient was implemented to facilitate testing
func (s *Store) SetClient(c *uaerospike.Client) {
	s.client = c
}

// GetNamespace was implemented to facilitate testing
func (s *Store) GetNamespace() string {
	return s.namespace
}

// SetNamespace was implemented to facilitate testing
func (s *Store) SetNamespace(v string) {
	s.namespace = v
}

// GetName was implemented to facilitate testing
func (s *Store) GetName() string {
	return s.setName
}

// SetName was implemented to facilitate testing
func (s *Store) SetName(v string) {
	s.setName = v
}

// GetUtxoBatchSize was implemented to facilitate testing
func (s *Store) GetUtxoBatchSize() int {
	return s.utxoBatchSize
}

// SetUtxoBatchSize was implemented to facilitate testing
func (s *Store) SetUtxoBatchSize(v int) {
	s.utxoBatchSize = v
}

// SetExternalStore was implemented to facilitate testing
func (s *Store) SetExternalStore(bs blob.Store) {
	s.externalStore = bs
}

// GetExternalStore was implemented to facilitate testing
func (s *Store) GetExternalStore() blob.Store {
	return s.externalStore
}

// SetExpiration was implemented to facilitate testing
func (s *Store) SetExpiration(v time.Duration) {
	s.expiration = v
}

// SetStoreBatcher was implemented to facilitate testing
func (s *Store) SetStoreBatcher(b batcherIfc[BatchStoreItem]) {
	s.storeBatcher = b
}

func (s *Store) SetExternalTxCache(c *util.ExpiringConcurrentCache[chainhash.Hash, *bt.Tx]) {
	s.externalTxCache = c
}

////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////

// NewBatchStoreItem was implemented to facilitate testing
func NewBatchStoreItem(
	txHash *chainhash.Hash,
	isCoinbase bool,
	tx *bt.Tx,
	blockHeight uint32,
	blockIDs []uint32,
	lockTime uint32,
	done chan error,
) *BatchStoreItem {
	return &BatchStoreItem{
		txHash:      txHash,
		isCoinbase:  isCoinbase,
		tx:          tx,
		blockHeight: blockHeight,
		blockIDs:    blockIDs,
		lockTime:    lockTime,
		done:        done,
	}
}

// GetTxHash was implemented to facilitate testing
func (i *BatchStoreItem) GetTxHash() *chainhash.Hash {
	return i.txHash
}

// SendDone was implemented to facilitate testing
func (i *BatchStoreItem) SendDone(e error) {
	i.done <- e
}

// RecvDone was implemented to facilitate testing
func (i *BatchStoreItem) RecvDone() error {
	return <-i.done
}
