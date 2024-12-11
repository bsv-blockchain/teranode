// //go:build aerospike

package aerospike

import (
	"github.com/bitcoin-sv/ubsv/stores/blob"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/bitcoin-sv/ubsv/util/uaerospike"
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
func (s *Store) SetExpiration(v uint32) {
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
