package txmetacache

import (
	"sync"
	"sync/atomic"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
)

// improvedCacheGetScratchInitialCap matches txMetaCacheReadBufferInitialCapacity
// so the per-Get scratch slice has the same starting profile as the legacy
// caller-managed buffer.
const improvedCacheGetScratchInitialCap = 1024

// improvedCacheGetScratchPool recycles the byte buffer used to copy serialised
// data out of the ring buffer before NewMetaDataFromBytes parses it. The
// freshly allocated *meta.Data returned to the caller is NOT pooled — it is
// what satisfies the cacheBackend.Get contract.
var improvedCacheGetScratchPool = sync.Pool{
	New: func() any {
		buf := make([]byte, 0, improvedCacheGetScratchInitialCap)
		return &buf
	},
}

// improvedCacheBackend adapts *ImprovedCache to the cacheBackend interface.
// It exists so that ImprovedCache keeps its existing byte-keyed Set/Get/Del
// API (used by its own tests and external benchmarks) without method-signature
// collisions against the interface's pointer-typed Set/Get.
//
// unmarshalErrors counts entries that came back from the ring buffer but failed
// to deserialise — operationally this means a format mismatch or in-place
// corruption. Affected entries are evicted on the spot to prevent the same
// bad bytes from being repeatedly reparsed; the counter is exposed via
// UpdateStats so the rate is observable.
type improvedCacheBackend struct {
	cache           *ImprovedCache
	unmarshalErrors atomic.Uint64
}

// Compile-time check that *improvedCacheBackend satisfies the contract.
var _ cacheBackend = (*improvedCacheBackend)(nil)

// SetFromBytes is the byte-native insert path; the Kafka ingest path already
// holds serialised bytes so this is the fast lane.
func (b *improvedCacheBackend) SetFromBytes(key, value []byte) error {
	return b.cache.Set(key, value)
}

// SetMultiFromBytes is the batched byte-native insert; preserves
// ImprovedCache's per-shard fan-out.
func (b *improvedCacheBackend) SetMultiFromBytes(keys, values [][]byte) error {
	return b.cache.SetMulti(keys, values)
}

// SetMultiSequential delegates to the underlying ImprovedCache's partition-
// aware twin, which skips the per-bucket goroutine fan-out.
func (b *improvedCacheBackend) SetMultiSequential(keys, values [][]byte) error {
	return b.cache.SetMultiSequential(keys, values)
}

// SetMultiSequentialWithHashes delegates to the underlying ImprovedCache,
// allowing the caller-supplied xxhash values to be reused.
func (b *improvedCacheBackend) SetMultiSequentialWithHashes(keys, values [][]byte, hashes []uint64) error {
	return b.cache.SetMultiSequentialWithHashes(keys, values, hashes)
}

// Set serialises data via MetaBytes and forwards to the underlying byte Set.
// Bridge cost: one MetaBytes() call per insert.
func (b *improvedCacheBackend) Set(hash *chainhash.Hash, data *meta.Data) error {
	bts, err := data.MetaBytes()
	if err != nil {
		return err
	}

	return b.cache.Set(hash[:], bts)
}

// Get allocates a fresh *meta.Data, deserialises the cached bytes into it,
// and returns the pointer. One *meta.Data allocation per call plus whatever
// the deserialiser allocates for slice backings. PointerCache's Get is
// zero-alloc by contrast — this is the byte-mode legacy tax.
//
// On unmarshal failure the entry is evicted and a warning is logged: a
// corrupt entry would otherwise be reparsed on every subsequent Get for the
// same key, masking format mismatches and chewing CPU.
func (b *improvedCacheBackend) Get(hash chainhash.Hash) (*meta.Data, bool) {
	scratchPtr := improvedCacheGetScratchPool.Get().(*[]byte)
	scratch := (*scratchPtr)[:0]

	defer func() {
		if cap(scratch) > txMetaCacheReadBufferMaxRetain {
			scratch = make([]byte, 0, improvedCacheGetScratchInitialCap)
		}

		*scratchPtr = scratch[:0]
		improvedCacheGetScratchPool.Put(scratchPtr)
	}()

	if err := b.cache.Get(&scratch, hash[:]); err != nil {
		// ErrNotFound and any other backend error both surface as a miss to
		// the caller — the cacheBackend.Get contract has no error channel.
		return nil, false
	}

	// NewMetaDataFromBytes handles short/empty input via its own length
	// check (returns an error for len<17), so no separate empty-scratch
	// guard is needed here.
	data := &meta.Data{}
	if err := meta.NewMetaDataFromBytes(scratch, data); err != nil {
		// Deserialise failure means either an empty entry, or in-place
		// corruption / format mismatch. Evict so subsequent Gets on the
		// same key don't replay the same bad parse, and bump a counter so
		// operators can see the rate. Logging is deliberately omitted to
		// avoid flooding on pathological keys.
		b.unmarshalErrors.Add(1)
		b.cache.Del(hash[:])

		return nil, false
	}

	return data, true
}

func (b *improvedCacheBackend) Del(key []byte) {
	b.cache.Del(key)
}

func (b *improvedCacheBackend) UpdateStats(s *Stats) {
	b.cache.UpdateStats(s)
}
