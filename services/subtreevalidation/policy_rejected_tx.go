package subtreevalidation

import (
	"context"
	"sync"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/util/kafka"
	kafkamessage "github.com/bsv-blockchain/teranode/util/kafka/kafka_message"
	"google.golang.org/protobuf/proto"
)

// txPolicyRejectedCache is a bounded in-memory cache of consensus-valid transactions
// that were rejected by local mining policy. Keyed by tx hash, storing raw *bt.Tx.
//
// The cache is populated from the KAFKA_TX_POLICY_REJECTED topic and consulted by
// subtree validation before making an HTTP request to another miner for a missing tx.
//
// Eviction policy: when the cache is full, one arbitrary (random) entry is removed to
// make room. This is acceptable because the cache is best-effort; a miss falls back to
// an HTTP fetch from the originating miner. LRU or FIFO would not materially improve
// hit rate in the adversarial case (a flood of all-distinct hashes defeats any eviction
// strategy equally), and the upstream gate is the validator itself — every entry in the
// topic must be a consensus-valid tx that was fully processed and policy-rejected, so
// the fill rate is bounded by the validator's own throughput rather than an unconstrained
// peer-facing API.
type txPolicyRejectedCache struct {
	mu      sync.RWMutex
	entries map[chainhash.Hash]*bt.Tx
	maxSize int
}

func newTxPolicyRejectedCache(maxBytes int) *txPolicyRejectedCache {
	estimatedEntries := maxBytes / 500 // average tx ~500 bytes
	if estimatedEntries < 1024 {
		estimatedEntries = 1024
	}

	return &txPolicyRejectedCache{
		entries: make(map[chainhash.Hash]*bt.Tx, estimatedEntries),
		maxSize: estimatedEntries,
	}
}

func (c *txPolicyRejectedCache) Get(hash chainhash.Hash) (*bt.Tx, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	tx, ok := c.entries[hash]
	return tx, ok
}

func (c *txPolicyRejectedCache) Set(hash chainhash.Hash, tx *bt.Tx) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if len(c.entries) >= c.maxSize {
		c.evictOne()
	}

	c.entries[hash] = tx
}

// evictOne removes one arbitrary entry to make room. Called under write lock.
// A random eviction is acceptable here because the cache is best-effort: misses
// just fall back to the HTTP fetch path. Go map iteration is non-deterministic,
// so this is intentionally random rather than oldest-first.
func (c *txPolicyRejectedCache) evictOne() {
	for k := range c.entries {
		delete(c.entries, k)
		return
	}
}

func (c *txPolicyRejectedCache) Len() int {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return len(c.entries)
}

// maxCachedTxBytes is the upper bound on raw transaction size stored in the
// policy-rejected cache. Transactions larger than this are skipped: they are
// unlikely to be cache hits (oversized txs are policy-rejected on size, not fee,
// and are rarely included by other miners), and parsing them with bt.NewTxFromBytes
// allocates the full *bt.Tx object graph, which spikes GC pressure at high message
// rates. The HTTP fetch fallback handles them when actually needed.
const maxCachedTxBytes = 1_000_000 // 1 MB

// policyRejectedTxMessageHandler returns a Kafka message handler that deserializes
// KafkaTxPolicyRejectedTopicMessage and stores the raw transaction in the cache.
//
// Backpressure: the consumer runs in a single goroutine (see kafka_consumer.go:Start)
// and processes records serially via franz-go's PollFetches loop. This provides a
// natural rate cap — the handler cannot consume faster than it can allocate and insert.
// Application-level rate limiting is not needed here; if the validator is emitting
// policy-rejected messages faster than this handler can keep up, Kafka's consumer-lag
// mechanism will buffer on the broker side, and cache misses will simply fall back to
// the HTTP fetch path.
func (u *Server) policyRejectedTxMessageHandler(_ context.Context) func(msg *kafka.KafkaMessage) error {
	return func(msg *kafka.KafkaMessage) error {
		if u.policyRejectedTxCache == nil {
			return nil
		}

		var m kafkamessage.KafkaTxPolicyRejectedTopicMessage
		if err := proto.Unmarshal(msg.Value, &m); err != nil {
			u.logger.Errorf("[policyRejectedTxHandler] proto unmarshal error: %v", err)
			return nil
		}

		if len(m.TxHash) != chainhash.HashSize || len(m.RawTx) == 0 {
			u.logger.Errorf("[policyRejectedTxHandler] invalid message: TxHash len=%d (want %d), RawTx len=%d", len(m.TxHash), chainhash.HashSize, len(m.RawTx))
			return nil
		}

		// Skip oversized txs to avoid a large bt.NewTxFromBytes allocation and the
		// GC churn that follows when the entry is later evicted from the cache.
		if len(m.RawTx) > maxCachedTxBytes {
			return nil
		}

		tx, err := bt.NewTxFromBytes(m.RawTx)
		if err != nil {
			u.logger.Errorf("[policyRejectedTxHandler] failed to parse tx from bytes: %v", err)
			return nil
		}

		// Reject if the claimed hash doesn't match the actual transaction to prevent cache poisoning.
		var hash chainhash.Hash
		copy(hash[:], m.TxHash)
		if *tx.TxIDChainHash() != hash {
			u.logger.Errorf("[policyRejectedTxHandler] tx hash mismatch: claimed %s, actual %s", hash, tx.TxIDChainHash())
			return nil
		}

		u.policyRejectedTxCache.Set(hash, tx)

		return nil
	}
}

// lookupPolicyRejectedTxs checks the policy-rejected cache for missing transactions
// and returns any that were found, along with the hashes that are still missing.
func (u *Server) lookupPolicyRejectedTxs(missingTxHashes []missingTxHash) (found []missingTx, stillMissing []missingTxHash) {
	if u.policyRejectedTxCache == nil {
		return nil, missingTxHashes
	}

	stillMissing = make([]missingTxHash, 0, len(missingTxHashes))

	for _, mth := range missingTxHashes {
		tx, ok := u.policyRejectedTxCache.Get(mth.hash)
		if ok {
			found = append(found, missingTx{tx: tx, idx: mth.idx})
		} else {
			stillMissing = append(stillMissing, mth)
		}
	}

	return found, stillMissing
}

// missingTxHash pairs a tx hash with its index in the txMetaSlice for cache lookups.
type missingTxHash struct {
	hash chainhash.Hash
	idx  int
}
