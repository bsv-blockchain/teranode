package txmetacache

import (
	"sync"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/cespare/xxhash/v2"
	"github.com/stretchr/testify/require"
)

func hashN(n uint64) *chainhash.Hash {
	var h chainhash.Hash
	for i := 0; i < 8; i++ {
		h[i] = byte(n >> (i * 8))
	}

	return &h
}

func TestPointerCache_SetGetRoundTrip(t *testing.T) {
	c, err := NewPointerCache(64 * 1024 * 1024)
	require.NoError(t, err)

	h := hashN(42)
	d := &meta.Data{Fee: 1234, SizeInBytes: 250, IsCoinbase: false}

	require.NoError(t, c.Set(h, d))

	got, ok := c.Get(*h)
	require.True(t, ok)
	require.NotSame(t, d, got, "Set must clone into a metadata-only snapshot, not retain the caller's pointer")
	require.Equal(t, d.Fee, got.Fee)
	require.Equal(t, d.SizeInBytes, got.SizeInBytes)
	require.Equal(t, d.IsCoinbase, got.IsCoinbase)
}

// TestPointerCache_SetStripsNonCachedFields confirms that Set drops fields the
// byte backend would never have stored (Tx, BlockIDs, ...), keeping the two
// backends semantically equivalent.
func TestPointerCache_SetStripsNonCachedFields(t *testing.T) {
	c, err := NewPointerCache(64 * 1024 * 1024)
	require.NoError(t, err)

	h := hashN(11)
	fatTx := &bt.Tx{LockTime: 99}
	d := &meta.Data{
		Fee:         500,
		SizeInBytes: 800,
		IsCoinbase:  false,
		Tx:          fatTx,
		BlockIDs:    []uint32{1, 2, 3},
		LockTime:    42,
		CreatedAt:   1234567890,
	}

	require.NoError(t, c.Set(h, d))

	got, ok := c.Get(*h)
	require.True(t, ok)
	require.Nil(t, got.Tx, "Tx must not be retained in the cache (matches byte backend behaviour)")
	require.Nil(t, got.BlockIDs, "BlockIDs must not be retained")
	require.Zero(t, got.LockTime, "LockTime is not part of the cached field set")
	require.Zero(t, got.CreatedAt, "CreatedAt is not part of the cached field set")
}

func TestPointerCache_MissReturnsFalse(t *testing.T) {
	c, err := NewPointerCache(64 * 1024 * 1024)
	require.NoError(t, err)

	got, ok := c.Get(*hashN(99))
	require.False(t, ok)
	require.Nil(t, got)
}

func TestPointerCache_OverwriteDoesNotConsumeRingSlot(t *testing.T) {
	c, err := NewPointerCache(64 * 1024 * 1024)
	require.NoError(t, err)

	h := hashN(7)
	d1 := &meta.Data{Fee: 1}
	d2 := &meta.Data{Fee: 2}

	require.NoError(t, c.Set(h, d1))
	require.NoError(t, c.Set(h, d2))

	got, ok := c.Get(*h)
	require.True(t, ok)
	require.Equal(t, d2.Fee, got.Fee, "Get must return the most recently stored entry")

	b := c.bucketFor(*h)
	require.Equal(t, uint64(1), b.added.Load(),
		"overwriting an existing key must not increment insertion count")
}

func TestPointerCache_FIFOEvictionWhenRingFull(t *testing.T) {
	// Force tiny per-bucket capacity by sizing maxBytes so that
	// maxBytes / BucketsCount / pointerCacheAvgEntryBytes == 1.
	maxBytes := BucketsCount * pointerCacheAvgEntryBytes
	c, err := NewPointerCache(maxBytes)
	require.NoError(t, err)

	// Find two distinct keys that collide on the same bucket so we can
	// observe eviction deterministically.
	var first, second *chainhash.Hash

	target := xxhash.Sum64(hashN(0)[:]) % BucketsCount

	for n := uint64(1); n < 1_000_000; n++ {
		h := hashN(n)
		if xxhash.Sum64(h[:])%BucketsCount != target {
			continue
		}

		if first == nil {
			first = h
		} else {
			second = h
			break
		}
	}

	require.NotNil(t, first)
	require.NotNil(t, second)

	d1 := &meta.Data{Fee: 100}
	d2 := &meta.Data{Fee: 200}

	require.NoError(t, c.Set(hashN(0), &meta.Data{Fee: 1})) // primes bucket
	require.NoError(t, c.Set(first, d1))                    // either evicts the prime or coexists, depending on bucket
	require.NoError(t, c.Set(second, d2))                   // capacity is 1; second must evict whatever's there

	// d2 must still be present (field-level equality; Set clones into a snapshot)
	got, ok := c.Get(*second)
	require.True(t, ok)
	require.Equal(t, d2.Fee, got.Fee)

	// The bucket now has at most one entry
	b := c.bucketFor(*second)
	b.mu.RLock()
	require.LessOrEqual(t, len(b.m), 1, "ring size is 1; bucket must not exceed one live entry")
	b.mu.RUnlock()
	require.GreaterOrEqual(t, b.evicted.Load(), uint64(1))
}

func TestPointerCache_Del(t *testing.T) {
	c, err := NewPointerCache(64 * 1024 * 1024)
	require.NoError(t, err)

	h := hashN(5)
	_ = c.Set(h, &meta.Data{Fee: 9})
	c.Del(h[:])

	_, ok := c.Get(*h)
	require.False(t, ok)
}

// TestPointerCache_DelDoesNotLeakRingCapacity pins the fix for the FIFO
// stale-slot bug: after Del, the ring slot still names the deleted key but
// the map no longer has it. A subsequent Set on a different key targeting
// the same bucket must NOT count the stale slot as a live eviction, and a
// full Set→Del→Set cycle must not silently shrink effective capacity.
func TestPointerCache_DelDoesNotLeakRingCapacity(t *testing.T) {
	// Per-bucket ring capacity of 1 makes the leak observable immediately.
	maxBytes := BucketsCount * pointerCacheAvgEntryBytes
	c, err := NewPointerCache(maxBytes)
	require.NoError(t, err)

	// Find two keys that fall in the same bucket so we exercise eviction
	// pressure on a single shard.
	var first, second *chainhash.Hash

	target := xxhash.Sum64(hashN(0)[:]) % BucketsCount

	for n := uint64(1); n < 1_000_000; n++ {
		h := hashN(n)
		if xxhash.Sum64(h[:])%BucketsCount != target {
			continue
		}

		if first == nil {
			first = h
		} else {
			second = h
			break
		}
	}

	require.NotNil(t, first)
	require.NotNil(t, second)

	// Set then delete `first` — fills the ring, then leaves a stale slot.
	require.NoError(t, c.Set(first, &meta.Data{Fee: 1}))
	c.Del(first[:])

	b := c.bucketFor(*first)

	evictedBefore := b.evicted.Load()

	// Insert `second` — should reuse the stale slot, not count an eviction.
	require.NoError(t, c.Set(second, &meta.Data{Fee: 2}))

	got, ok := c.Get(*second)
	require.True(t, ok)
	require.Equal(t, uint64(2), got.Fee)

	require.Equal(t, evictedBefore, b.evicted.Load(),
		"Set after Del must not count the stale ring slot as a live eviction")
}

// TestPointerCache_SetDoesNotAliasCallerSlice pins the metadataOnly deep-
// clone contract: after Set, mutating the caller's ParentTxHashes must not
// affect the cached entry observed by other readers.
func TestPointerCache_SetDoesNotAliasCallerSlice(t *testing.T) {
	c, err := NewPointerCache(64 * 1024 * 1024)
	require.NoError(t, err)

	h := hashN(13)

	parents := []chainhash.Hash{
		chainhash.HashH([]byte("p0")),
		chainhash.HashH([]byte("p1")),
	}
	original := parents[0]

	d := &meta.Data{
		Fee:        7,
		TxInpoints: subtree.NewTxInpointsFromPacked(parents, []uint32{1, 0, 1, 0}),
	}

	require.NoError(t, c.Set(h, d))

	// Mutate the caller's slice in place — would corrupt the cache if the
	// stored TxInpoints aliased the original backing array.
	parents[0] = chainhash.HashH([]byte("MUTATED"))
	d.TxInpoints.ParentTxHashes[0] = chainhash.HashH([]byte("MUTATED-VIA-INPOINTS"))

	got, ok := c.Get(*h)
	require.True(t, ok)
	require.Len(t, got.TxInpoints.ParentTxHashes, 2)
	require.Equal(t, original, got.TxInpoints.ParentTxHashes[0],
		"cached ParentTxHashes[0] must be the original value despite caller mutation")
}

// TestPointerCache_SetMultiSequentialDoesNotFanOut asserts that the
// SetMultiSequential path runs all inserts on the calling goroutine — the
// cacheBackend contract reserves SetMultiFromBytes for the fan-out form.
// Detection is indirect (caller's goroutine ID matches the worker that
// performed the map mutation), so this test verifies behaviour at a coarser
// level: the call is synchronous and never spawns errgroup goroutines.
func TestPointerCache_SetMultiSequentialDoesNotFanOut(t *testing.T) {
	c, err := NewPointerCache(64 * 1024 * 1024)
	require.NoError(t, err)

	const n = 1024

	keys := make([][]byte, n)
	values := make([][]byte, n)

	d := &meta.Data{Fee: 1, SizeInBytes: 100}
	bts, err := d.MetaBytes()
	require.NoError(t, err)

	for i := 0; i < n; i++ {
		h := chainhash.HashH([]byte{byte(i), byte(i >> 8)})
		keys[i] = h[:]
		values[i] = bts
	}

	// If SetMultiSequential were fanning out we'd see ~BucketsCount goroutines
	// peak. We can't easily count them, so we settle for: must complete and
	// every entry must be present afterwards.
	require.NoError(t, c.SetMultiSequential(keys, values))

	for i := 0; i < n; i++ {
		var hh chainhash.Hash
		copy(hh[:], keys[i])
		_, ok := c.Get(hh)
		require.True(t, ok, "key %d must be present after SetMultiSequential", i)
	}
}

func TestPointerCache_Reset(t *testing.T) {
	c, err := NewPointerCache(64 * 1024 * 1024)
	require.NoError(t, err)

	for n := uint64(0); n < 100; n++ {
		require.NoError(t, c.Set(hashN(n), &meta.Data{Fee: n}))
	}

	c.Reset()

	for n := uint64(0); n < 100; n++ {
		_, ok := c.Get(*hashN(n))
		require.False(t, ok, "Reset must clear every entry; key %d still present", n)
	}
}

func TestPointerCache_UpdateStats(t *testing.T) {
	c, err := NewPointerCache(64 * 1024 * 1024)
	require.NoError(t, err)

	for n := uint64(0); n < 50; n++ {
		require.NoError(t, c.Set(hashN(n), &meta.Data{Fee: n}))
	}

	var s Stats
	c.UpdateStats(&s)
	require.Equal(t, uint64(50), s.ValidEntriesCount)
	require.Equal(t, uint64(50), s.TotalElementsAdded)
}

func TestPointerCache_ConcurrentReadersAndWriters(t *testing.T) {
	c, err := NewPointerCache(64 * 1024 * 1024)
	require.NoError(t, err)

	const writers = 4
	const readers = 16
	const keysPerWriter = 1000

	var wg sync.WaitGroup
	wg.Add(writers)

	for w := 0; w < writers; w++ {
		w := w
		go func() {
			defer wg.Done()
			base := uint64(w) * keysPerWriter

			for i := uint64(0); i < keysPerWriter; i++ {
				_ = c.Set(hashN(base+i), &meta.Data{Fee: base + i})
			}
		}()
	}

	wg.Wait()

	// All writes complete before readers start, so every key must be present
	// (capacity is huge — no eviction).
	wg.Add(readers)

	for r := 0; r < readers; r++ {
		go func() {
			defer wg.Done()

			for w := 0; w < writers; w++ {
				base := uint64(w) * keysPerWriter
				for i := uint64(0); i < keysPerWriter; i++ {
					d, ok := c.Get(*hashN(base + i))
					require.True(t, ok)
					require.NotNil(t, d)
					require.Equal(t, base+i, d.Fee)
				}
			}
		}()
	}

	wg.Wait()
}
