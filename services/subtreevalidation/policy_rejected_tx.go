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

// txPolicyRejectedCache is a memory-bounded in-memory cache of consensus-valid
// transactions that were rejected by local mining policy. Keyed by tx hash, storing
// raw *bt.Tx.
//
// The cache is populated from the KAFKA_TX_POLICY_REJECTED topic and consulted by
// subtree validation before making an HTTP request to another miner for a missing tx.
//
// Bound: the cache is bounded by the cumulative raw size (tx.Size()) of the stored
// transactions, NOT by entry count. This matters because a single entry can be up to
// maxCachedTxBytes (1 MB); an entry-count bound derived from an assumed average size
// would let an adversarial flood of large policy-rejected txs blow past the configured
// memory budget and OOM the pod. tx.Size() (the serialized byte length) is used as the
// accounting unit; it undercounts the *bt.Tx object-graph overhead but tracks the
// configured "memory in MB" budget far more faithfully than an entry count.
//
// Eviction policy: when adding an entry would exceed the byte budget, arbitrary (random)
// entries are evicted until it fits. This is acceptable because the cache is best-effort;
// a miss falls back to an HTTP fetch from the originating miner. LRU or FIFO would not
// materially improve hit rate in the adversarial case (a flood of all-distinct hashes
// defeats any eviction strategy equally), and the upstream gate is the validator itself —
// every entry in the topic must be a consensus-valid tx that was fully processed and
// policy-rejected, so the fill rate is bounded by the validator's own throughput rather
// than an unconstrained peer-facing API.
type cacheEntry struct {
	tx   *bt.Tx
	size int
}

type txPolicyRejectedCache struct {
	mu       sync.RWMutex
	entries  map[chainhash.Hash]cacheEntry
	curBytes int
	maxBytes int
}

func newTxPolicyRejectedCache(maxBytes int) *txPolicyRejectedCache {
	// Pre-size the map to reduce rehashing. This is only a capacity hint based on an
	// assumed average tx size; the real bound is maxBytes, enforced in Set.
	sizeHint := maxBytes / 500
	if sizeHint < 1024 {
		sizeHint = 1024
	}

	return &txPolicyRejectedCache{
		entries:  make(map[chainhash.Hash]cacheEntry, sizeHint),
		maxBytes: maxBytes,
	}
}

func (c *txPolicyRejectedCache) Get(hash chainhash.Hash) (*bt.Tx, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	e, ok := c.entries[hash]
	return e.tx, ok
}

func (c *txPolicyRejectedCache) Set(hash chainhash.Hash, tx *bt.Tx) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, exists := c.entries[hash]; exists {
		// Already cached; the stored bytes are unchanged so leave the budget alone.
		return
	}

	size := tx.Size()

	// Evict arbitrary entries until the new one fits within the byte budget. The
	// len > 0 guard guarantees forward progress (and lets a single oversized tx still
	// be cached when the budget is smaller than one entry — at most one such entry is
	// held at a time).
	for c.curBytes+size > c.maxBytes && len(c.entries) > 0 {
		c.evictOne()
	}

	c.entries[hash] = cacheEntry{tx: tx, size: size}
	c.curBytes += size
}

// evictOne removes one arbitrary entry and reclaims its bytes. Called under write lock.
// A random eviction is acceptable here because the cache is best-effort: misses just
// fall back to the HTTP fetch path. Go map iteration is non-deterministic, so this is
// intentionally random rather than oldest-first.
func (c *txPolicyRejectedCache) evictOne() {
	for k, e := range c.entries {
		c.curBytes -= e.size
		delete(c.entries, k)
		return
	}
}

func (c *txPolicyRejectedCache) Len() int {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return len(c.entries)
}

// Bytes returns the cumulative raw size of all cached transactions.
func (c *txPolicyRejectedCache) Bytes() int {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return c.curBytes
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
// Backpressure: the consumer fans out a goroutine per partition per fetch (see
// kafka_consumer.go:Start), so handler invocations for different partitions run
// concurrently, though processing within a single partition is serialized by a
// per-partition lock. There is no application-level rate limit; if the validator
// emits policy-rejected messages faster than this handler keeps up, consumer lag
// builds on the broker side and cache misses simply fall back to the HTTP fetch
// path. The cache itself is memory-bounded (see txPolicyRejectedCache), so a burst
// cannot grow it without bound regardless of fan-out.
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
