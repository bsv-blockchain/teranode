package subtreeprocessor

import (
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/stores/txmetacache"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/kafka"
)

// txMetaEvictBatchSize caps the DELETE entries per Kafka message (see
// txmetacache.SplitDeleteBatches).
const txMetaEvictBatchSize = txmetacache.MaxDeleteBatch

// TxMetaEvictor tells subtree validation to drop txmeta cache entries for
// transactions that block assembly has just demoted to conflicting.
//
// Subtree validation caches non-conflicting txmeta fed by the validator's ADD
// messages on the txmeta topic and never learns about a later demotion on its
// own: block assembly's UTXO store is not cache-wrapped, so the store's own
// cache eviction never reaches the subtree validation process. Without a DELETE
// the stale non-conflicting entry would let a block containing the demoted tx
// pass the cached-meta fast path. DELETE is a hint to a cache that is repopulated
// from the store on a miss, so publishing is best effort and never fails the caller.
//
// The DELETE and the validator's ADD for the same tx are on different
// partitions, so the DELETE can be consumed first. Subtree validation's txmeta
// cache keeps recent DELETEs as tombstones and evicts any later write for them
// (see txmetacache.TxMetaCache.DeleteDemoted).
//
// A nil *TxMetaEvictor is valid and does nothing, so call sites need no guard.
type TxMetaEvictor struct {
	logger   ulogger.Logger
	producer kafka.KafkaAsyncProducerI
}

// NewTxMetaEvictor returns an evictor publishing to the given txmeta producer,
// or nil when there is no producer (Kafka txmeta not configured). The caller owns
// the producer's lifecycle (Start/Stop).
func NewTxMetaEvictor(logger ulogger.Logger, producer kafka.KafkaAsyncProducerI) *TxMetaEvictor {
	// A typed-nil *KafkaAsyncProducer stored in the interface is not == nil.
	if typed, ok := producer.(*kafka.KafkaAsyncProducer); (ok && typed == nil) || producer == nil {
		return nil
	}

	initPrometheusMetrics()

	return &TxMetaEvictor{logger: logger, producer: producer}
}

// Evict publishes a DELETE for every hash. Call it after the store write that
// marked the hashes conflicting, and after a failed one with the hashes it may
// have flipped. It never blocks on a full producer buffer and never returns an
// error; a dropped message is logged at WARN and counted per tx in
// teranode_subtreeprocessor_txmeta_delete_dropped_total.
func (e *TxMetaEvictor) Evict(hashes []chainhash.Hash) {
	if e == nil || len(hashes) == 0 {
		return
	}

	for _, batch := range txmetacache.SplitDeleteBatches(hashes) {
		key := batch[0] // copied: the producer reads the key after Evict returns

		// Keyed by tx hash like the validator's txmeta messages.
		if !e.producer.TryPublish(&kafka.Message{
			Key:   key[:],
			Value: txmetacache.EncodeDeleteBatch(batch),
		}) {
			prometheusTxMetaDeleteDropped.Add(float64(len(batch)))
			e.logger.Warnf("[TxMetaEvictor] dropped txmeta DELETE for %d demoted txs, producer buffer full or stopped; subtree validation may hold stale non-conflicting entries", len(batch))
		}
	}
}
