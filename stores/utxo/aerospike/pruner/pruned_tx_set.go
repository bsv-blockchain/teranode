package pruner

import (
	"sync/atomic"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/util/cuckoo"
)

// defaultPrunedTxSetCapacity is used when NewPrunedTxSet is called with maxEntries=0.
// At ~1 byte per entry split across two generations, 2B total entries ≈ 2 GiB.
const defaultPrunedTxSetCapacity = 2_000_000_000

// PrunedTxSet is a sharded, two-generation cuckoo-filter-backed set tracking
// TXIDs of records pruned across sessions. It is used to skip wasteful parent
// updates for parents that have already been pruned.
//
// Why two generations:
//
//	A single cuckoo filter saturates eventually (entries only leave via
//	CheckAndRemove, and most TXIDs added are never asked about by a future
//	child). Once saturated, the filter freezes — new entries cannot be
//	added, so children of currently-being-pruned txs can never find their
//	parents. On dev-scale-1 at 1.7M TPS the single-filter design saturated
//	in ~50 min and the steady-state catch rate of would-be-wasted parent
//	updates collapsed from ~98% to ~14%.
//
//	The two-generation design rotates: each shard keeps a `current` filter
//	(receives new Adds) and a `previous` filter (holds the prior epoch's
//	entries, read-only). When `current` saturates, it slides into the
//	`previous` slot (dropping whatever was there) and a fresh `current` is
//	allocated. Lookup/CheckAndRemove check `current` first, then fall back
//	to `previous` on miss. This guarantees that recently-Added entries
//	are always reachable, and the set never freezes.
//
// Throughput-preserving properties:
//
//   - Before the first rotation, `previous` is nil and the hot path is
//     byte-identical to a single-filter implementation. Fresh pods see
//     zero overhead vs the previous design.
//   - After rotation, `Add` still costs one atomic.Pointer.Load + one
//     cuckoo CAS (~1 ns extra). Lookup/CheckAndRemove that HIT in current
//     return immediately — also no extra cost. Only the miss path pays
//     the ~5-10 ns extra to check `previous`.
//   - In tight-chain workloads (parent of tx_N is tx_{N-1}, produced
//     seconds apart by the same blaster worker), parent is almost always
//     in `current`, so the miss path is rare.
//
// Memory budget: maxEntries is interpreted as a TOTAL capacity across
// both generations of all shards (not a per-generation budget). Each
// per-shard generation is sized at maxEntries / (2 × shardCount), so
// the SUM of capacity across all shards × 2 generations equals the
// configured maxEntries, and total memory ≈ maxEntries × ~1 byte
// (e.g. maxEntries=10M ⇒ ~10 MiB; maxEntries=2B ⇒ ~2 GiB).
//
// Sharding picks a bucket from h[9] & mask (NOT h[0] — h[0] is consumed by
// the cuckoo fingerprint, and we want shard distribution to be independent
// of fingerprint distribution). SHA-256-derived TXIDs have uniform byte
// values, so the distribution across shards is even.
type PrunedTxSet struct {
	shards         []prunedTxShard
	mask           uint8
	perShardCap    uint // capacity of each generation in each shard
	insertFailures atomic.Int64
	rotations      atomic.Int64 // number of generation rotations across all shards
}

// minPerShardCap is the floor applied to each generation's capacity. Without
// it, a maxEntries smaller than 2×shardCount would drive perShard to 0, which
// cuckoo.NewH32 rounds up to a single 4-slot bucket — saturating (and so
// rotating) every few inserts. The floor keeps a degenerate config functional
// at negligible cost (~1 KiB per generation).
const minPerShardCap uint = 1024

type prunedTxShard struct {
	current  atomic.Pointer[cuckoo.H32]
	previous atomic.Pointer[cuckoo.H32]
}

// NewPrunedTxSet creates a sharded two-generation cuckoo-filter-backed set
// sized to the given total maxEntries (counting BOTH generations across
// all shards). shardCount is rounded up to the next power of 2 (capped at
// 256). Pass maxEntries=0 to use the default capacity.
func NewPrunedTxSet(shardCount int, maxEntries int) *PrunedTxSet {
	n := 1
	for n < shardCount {
		n <<= 1
	}
	if n > 256 {
		n = 256
	}

	if maxEntries <= 0 {
		maxEntries = defaultPrunedTxSetCapacity
	}

	// Each shard has 2 generations; total live capacity = 2 × shardCount ×
	// perShardCap. Divide accordingly so the memory budget approximately
	// matches the configured maxEntries. Caveats that make this an
	// upper-bound rather than an exact match:
	//   - cuckoo.NewH32 rounds bucket count up to the next power of two,
	//     so actual allocated slots per generation can exceed perShard
	//     by up to ~2x (e.g. 10M budget with 256 shards rounds to ~16.8M
	//     slots).
	//   - Cuckoo filters run well below 100% load before saturating, so
	//     effective stored entries are typically lower than allocated
	//     slots.
	// Operators should treat maxEntries as the target rather than a
	// hard ceiling on RSS; the worst-case allocation overhead is bounded
	// by the power-of-two rounding.
	perShard := uint(maxEntries / n / 2)
	if perShard < minPerShardCap {
		perShard = minPerShardCap
	}

	s := &PrunedTxSet{
		shards:      make([]prunedTxShard, n),
		mask:        uint8(n - 1),
		perShardCap: perShard,
	}
	for i := range s.shards {
		s.shards[i].current.Store(cuckoo.NewH32(perShard))
		// previous stays nil until first rotation in this shard
	}
	return s
}

// shard picks the per-shard pair using byte 9 of the hash, leaving bytes
// 0–8 available to the cuckoo fingerprint+index derivation.
func (s *PrunedTxSet) shard(h *chainhash.Hash) *prunedTxShard {
	return &s.shards[h[9]&s.mask]
}

// Add registers a TXID. If the current generation refuses because it is
// actually saturated, the shard rotates — current slides into previous
// (replacing it), a fresh current is allocated, and the Add is retried.
//
// A cuckoo Insert can return false for two reasons: (a) provable
// saturation (eviction loop exhausted maxKicks — Saturated() latches
// true), or (b) transient CAS contention exhausting the inner retry
// budget without latching saturated. We must only rotate on (a) —
// rotating on transient contention burns a multi-MiB allocation and
// drops the previous generation prematurely. On (b) we just retry the
// Insert into the same current; the contending goroutine has by now
// finished its CAS and the retry is overwhelmingly likely to succeed.
func (s *PrunedTxSet) Add(h chainhash.Hash) {
	sh := s.shard(&h)
	cur := sh.current.Load()
	if cur.Insert((*[32]byte)(&h)) {
		return
	}
	if !cur.Saturated() {
		// Insert failed but the filter is NOT actually saturated —
		// transient CAS contention. Retry once into the same current.
		if cur.Insert((*[32]byte)(&h)) {
			return
		}
		// Second attempt also failed and still not saturated: extreme
		// contention. Treat as a backstop insert-failure rather than
		// triggering a spurious rotation.
		if !cur.Saturated() {
			s.insertFailures.Add(1)
			return
		}
		// Fell through to saturation between attempts — drop into the
		// rotation path below.
	}
	// Provably saturated — rotate this shard. Only one goroutine wins
	// the CAS; the others observe the new current on their retry below.
	s.rotateShard(sh, cur)
	if sh.current.Load().Insert((*[32]byte)(&h)) {
		return
	}
	// Even the fresh current refused — should be impossible (filter just
	// allocated) but bookkeep for visibility.
	s.insertFailures.Add(1)
}

// rotateShard atomically swaps the shard's `current` for a fresh filter,
// preserving the old current as `previous` (replacing whatever was there).
// If another goroutine already rotated, this is a no-op.
//
// The early-exit Load check below avoids a wasteful allocation when many
// goroutines hit saturation simultaneously: only the goroutine that still
// sees the stale current pointer will allocate a replacement; the others
// observe that current has already been swapped and return immediately
// without producing GC pressure (per-shard filters can be multi-MiB).
func (s *PrunedTxSet) rotateShard(sh *prunedTxShard, oldCur *cuckoo.H32) {
	if sh.current.Load() != oldCur {
		// Another goroutine already rotated. No work, no allocation.
		return
	}
	newCur := cuckoo.NewH32(s.perShardCap)
	if sh.current.CompareAndSwap(oldCur, newCur) {
		sh.previous.Store(oldCur)
		s.rotations.Add(1)
	}
	// If the CAS failed despite the Load check, another goroutine raced
	// us between the Load and the CAS. newCur is discarded — rare and the
	// Load above caps how often this happens.
}

// Contains returns true if the TXID is in either generation. Checks
// current first (cheap hit on recent entries) and falls back to previous
// only on miss.
func (s *PrunedTxSet) Contains(h chainhash.Hash) bool {
	sh := s.shard(&h)
	if sh.current.Load().Lookup((*[32]byte)(&h)) {
		return true
	}
	if prev := sh.previous.Load(); prev != nil {
		return prev.Lookup((*[32]byte)(&h))
	}
	return false
}

// CheckAndRemove returns true and deletes the TXID's fingerprint from
// whichever generation holds it. Tries current first.
func (s *PrunedTxSet) CheckAndRemove(h chainhash.Hash) bool {
	sh := s.shard(&h)
	if sh.current.Load().Delete((*[32]byte)(&h)) {
		return true
	}
	if prev := sh.previous.Load(); prev != nil {
		return prev.Delete((*[32]byte)(&h))
	}
	return false
}

// Len returns the approximate number of fingerprints currently stored
// across both generations and all shards. Eventually consistent under
// concurrent ops.
func (s *PrunedTxSet) Len() int {
	total := 0
	for i := range s.shards {
		total += s.shards[i].current.Load().Count()
		if prev := s.shards[i].previous.Load(); prev != nil {
			total += prev.Count()
		}
	}
	return total
}

// Saturated reports whether the set has experienced any insert failures
// since construction — i.e. insertFailures > 0. There are two paths that
// can increment insertFailures, both of which are "something went wrong"
// signals rather than the routine at-capacity indicator:
//
//   - Add observed Insert failure without cur.Saturated() after a retry
//     (extreme CAS contention — typically transient but unexpected
//     under normal load).
//   - Insert failed on a freshly-rotated generation (should be
//     impossible — the filter was just allocated).
//
// Either way, Saturated()==true means inserts are silently dropping at
// the moment of observation, and operators should investigate. Use
// Rotations() for the routine "current generation filled up and was
// recycled" signal.
func (s *PrunedTxSet) Saturated() bool {
	return s.insertFailures.Load() > 0
}

// InsertFailures returns the cumulative count of Insert calls that
// failed even on a freshly-rotated generation. Should be ~0 in normal
// operation.
func (s *PrunedTxSet) InsertFailures() int64 {
	return s.insertFailures.Load()
}

// Rotations returns the cumulative number of times any shard has rotated
// its current generation into previous. Useful for sizing: rotations
// imply per-shard saturation, and a high rate suggests perShardCap is
// too small for the workload.
func (s *PrunedTxSet) Rotations() int64 {
	return s.rotations.Load()
}
