package subtreevalidation

import (
	"context"
	"math"
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
// Bound: the cache is bounded by the cumulative extended size (tx.ExtendedSize()) of the
// stored transactions, NOT by entry count. This matters because a single entry can be up
// to maxCachedTxBytes (1 MB); an entry-count bound derived from an assumed average size
// would let an adversarial flood of large policy-rejected txs blow past the configured
// memory budget and OOM the pod. The extended size is used because a tx parsed from
// extended format also retains every input's previous output, which tx.Size() leaves out;
// it undercounts the *bt.Tx object-graph overhead but tracks the configured "memory in MB"
// budget far more faithfully than an entry count.
//
// Eviction policy: when adding an entry would exceed the byte budget, arbitrary (random)
// entries are evicted until it fits. This is acceptable because the cache is best-effort;
// a miss falls back to an HTTP fetch from the originating miner. LRU or FIFO would not
// materially improve hit rate in the adversarial case (a flood of all-distinct hashes
// defeats any eviction strategy equally). In the policy-rejected cache the upstream gate is
// the validator itself: every entry is a tx it fully processed and rejected on policy or as
// conflicting, so the fill rate is bounded by the validator's own throughput. Descendants
// kept aside by subtree validation come from peer subtrees and are only context-free
// checked, so they live in a separate instance (keptAsideTxCache) with its own budget and
// cannot evict the validators' entries.
type cacheEntry struct {
	tx   *bt.Tx
	size int
	// conflicting is set when the validator rejected the tx because an input is already
	// spent by a different tx, rather than on policy.
	conflicting bool
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

// Get returns a copy of the cached tx: validation extends a tx's inputs in place, and two
// subtrees can validate the same cached tx at the same time.
func (c *txPolicyRejectedCache) Get(hash chainhash.Hash) (*bt.Tx, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	e, ok := c.entries[hash]
	if !ok {
		return nil, false
	}

	return e.tx.Clone(), true
}

// Conflicting reports whether hash is cached as rejected for conflicting with a tx the
// validator already accepted. A nil cache holds nothing.
func (c *txPolicyRejectedCache) Conflicting(hash chainhash.Hash) bool {
	if c == nil {
		return false
	}

	c.mu.RLock()
	defer c.mu.RUnlock()

	e, ok := c.entries[hash]

	return ok && e.conflicting
}

// conflictingScanChunk bounds how many hashes a scan looks up under one read lock. A
// scan covers a whole subtree, up to millions of hashes; holding the lock throughout
// would hold off the Kafka consumer's writes, and behind them every other reader.
const conflictingScanChunk = 4096

// scanConflicting calls found for each of hashes cached as rejected for conflicting,
// under the read lock. found must not use the cache.
func (c *txPolicyRejectedCache) scanConflicting(hashes []chainhash.Hash, found func(chainhash.Hash)) {
	for start := 0; start < len(hashes); start += conflictingScanChunk {
		end := min(start+conflictingScanChunk, len(hashes))

		c.mu.RLock()

		if len(c.entries) == 0 {
			c.mu.RUnlock()
			return
		}

		for i := start; i < end; i++ {
			if e, ok := c.entries[hashes[i]]; ok && e.conflicting {
				found(hashes[i])
			}
		}

		c.mu.RUnlock()
	}
}

// ConflictingCount returns how many of hashes are cached as rejected for conflicting.
func (c *txPolicyRejectedCache) ConflictingCount(hashes []chainhash.Hash) int {
	n := 0

	c.scanConflicting(hashes, func(chainhash.Hash) { n++ })

	return n
}

// conflictingIn returns the hashes of hashes that are cached as rejected for conflicting.
func (c *txPolicyRejectedCache) conflictingIn(hashes []chainhash.Hash) []chainhash.Hash {
	var found []chainhash.Hash

	c.scanConflicting(hashes, func(hash chainhash.Hash) { found = append(found, hash) })

	return found
}

func (c *txPolicyRejectedCache) Set(hash chainhash.Hash, tx *bt.Tx) {
	c.set(hash, tx, false)
}

// SetConflicting caches a tx the validator rejected as conflicting.
func (c *txPolicyRejectedCache) SetConflicting(hash chainhash.Hash, tx *bt.Tx) {
	c.set(hash, tx, true)
}

func (c *txPolicyRejectedCache) set(hash chainhash.Hash, tx *bt.Tx, conflicting bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if e, exists := c.entries[hash]; exists {
		// Already cached; the stored bytes are unchanged so leave the budget alone. A
		// conflicting rejection upgrades an earlier policy rejection, never the reverse.
		if conflicting && !e.conflicting {
			e.conflicting = true
			c.entries[hash] = e
		}

		return
	}

	size := cachedSize(tx)

	// Evict arbitrary entries until the new one fits within the byte budget. The
	// len > 0 guard guarantees forward progress (and lets a single oversized tx still
	// be cached when the budget is smaller than one entry — at most one such entry is
	// held at a time).
	for c.curBytes+size > c.maxBytes && len(c.entries) > 0 {
		c.evictOne()
	}

	c.entries[hash] = cacheEntry{tx: tx, size: size, conflicting: conflicting}
	c.curBytes += size
}

// cachedSize is what a cached tx can retain: the extended size, which also counts each
// input's previous output.
func cachedSize(tx *bt.Tx) int {
	return int(min(tx.ExtendedSize(), uint64(math.MaxInt))) // nolint: gosec // G115: bounded by MaxInt
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

		if m.KeptAside {
			// a loser the validator already reported needs no kept-aside budget
			if u.keptAsideTxCache != nil && !u.policyRejectedTxCache.Conflicting(hash) {
				u.keptAsideTxCache.SetConflicting(hash, tx)
			}
		} else if m.Conflicting {
			u.policyRejectedTxCache.SetConflicting(hash, tx)
		} else {
			u.policyRejectedTxCache.Set(hash, tx)
		}

		return nil
	}
}

// lookupPolicyRejectedTxs checks the policy-rejected cache for missing transactions
// and returns any that were found, along with the hashes that are still missing.
func (u *Server) lookupPolicyRejectedTxs(missingTxHashes []missingTxHash) (found []missingTx, stillMissing []missingTxHash) {
	if u.policyRejectedTxCache == nil && u.keptAsideTxCache == nil {
		return nil, missingTxHashes
	}

	stillMissing = make([]missingTxHash, 0, len(missingTxHashes))

	for _, mth := range missingTxHashes {
		tx, ok := u.cachedRejectedTx(mth.hash)
		if ok {
			found = append(found, missingTx{tx: tx, idx: mth.idx, cached: true})
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

// cachedRejectedTx returns a copy of a tx from the policy-rejected or the kept-aside cache.
func (u *Server) cachedRejectedTx(hash chainhash.Hash) (*bt.Tx, bool) {
	if u.policyRejectedTxCache != nil {
		if tx, ok := u.policyRejectedTxCache.Get(hash); ok {
			return tx, true
		}
	}

	if u.keptAsideTxCache != nil {
		return u.keptAsideTxCache.Get(hash)
	}

	return nil, false
}

// knownConflicting reports whether hash is known as conflicting: rejected as conflicting
// by a validator, or kept aside by subtree validation as a descendant of one.
func (u *Server) knownConflicting(hash chainhash.Hash) bool {
	return u.policyRejectedTxCache.Conflicting(hash) || u.keptAsideTxCache.Conflicting(hash)
}

// conflictingCount returns how many of hashes are known as conflicting. A tx can be in
// both caches, since a kept-aside message can arrive before the validator's for the same
// loser; it counts once. The caches are read chunk by chunk while the Kafka consumer
// writes, so the count is a snapshot for the fail-fast allowance, not an exact figure.
func (u *Server) conflictingCount(hashes []chainhash.Hash) int {
	n := 0

	if u.policyRejectedTxCache != nil {
		n += u.policyRejectedTxCache.ConflictingCount(hashes)
	}

	if u.keptAsideTxCache != nil {
		for _, hash := range u.keptAsideTxCache.conflictingIn(hashes) {
			if !u.policyRejectedTxCache.Conflicting(hash) {
				n++
			}
		}
	}

	return n
}
