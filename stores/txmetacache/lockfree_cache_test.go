package txmetacache

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/stretchr/testify/require"
)

// lfValue is the value the tests store under sequence number i: its length
// varies with i (so an overwritten record's length field changes under a
// reader) and every byte is derived from i, so any byte from another record
// is detected.
func lfValue(i uint64) []byte {
	n := 17 + int(i%200)
	v := make([]byte, n)

	binary.LittleEndian.PutUint64(v, i)

	for j := 8; j < n; j++ {
		v[j] = byte(i*31 + uint64(j))
	}

	return v
}

func newTestLockFreeCache(t *testing.T, maxBytes int) *LockFreeCache {
	t.Helper()

	c, err := NewLockFreeCache(maxBytes)
	require.NoError(t, err)

	return c
}

// forEachLFImpl runs fn against every lock-free cache implementation.
func forEachLFImpl(t *testing.T, maxBytes int, fn func(t *testing.T, c rawLockFreeCache)) {
	t.Run("sharded", func(t *testing.T) { fn(t, newTestLockFreeCache(t, maxBytes)) })
	t.Run("lanes", func(t *testing.T) {
		c, err := NewLaneCache(maxBytes)
		require.NoError(t, err)
		fn(t, c)
	})
}

func TestLockFreeCache_RoundTrip(t *testing.T) {
	forEachLFImpl(t, 64*1024*1024, func(t *testing.T, c rawLockFreeCache) {

		for i := uint64(0); i < 10_000; i++ {
			k := mixedKey(i)
			require.NoError(t, c.Set(k[:], lfValue(i)))
		}

		var (
			dst    []byte
			misses int
		)

		for i := uint64(0); i < 10_000; i++ {
			k := mixedKey(i)
			dst = dst[:0]

			if !c.Get(&dst, k[:]) {
				misses++
				continue
			}

			require.Equal(t, lfValue(i), dst, "key %d returned another key's bytes", i)
		}

		// publish treats a fingerprint match as the same key (see
		// LockFreeCache.publish), so a key may very rarely lose its slot to a
		// colliding one. A miss is allowed; wrong bytes never are.
		require.LessOrEqual(t, misses, 5)

		missing := mixedKey(1 << 40)
		require.False(t, c.Get(&dst, missing[:]))
	})
}

func TestLockFreeCache_GetAppendsToDst(t *testing.T) {
	forEachLFImpl(t, 1024*1024, func(t *testing.T, c rawLockFreeCache) {
		k := mixedKey(1)
		require.NoError(t, c.Set(k[:], []byte("value")))

		dst := []byte("prefix-")
		require.True(t, c.Get(&dst, k[:]))
		require.Equal(t, "prefix-value", string(dst))
	})
}

func TestLockFreeCache_OverwriteLatestWins(t *testing.T) {
	forEachLFImpl(t, 1024*1024, func(t *testing.T, c rawLockFreeCache) {
		k := mixedKey(7)

		require.NoError(t, c.Set(k[:], []byte("first")))
		require.NoError(t, c.Set(k[:], []byte("second value")))

		var dst []byte
		require.True(t, c.Get(&dst, k[:]))
		require.Equal(t, "second value", string(dst))
	})
}

func TestLockFreeCache_Del(t *testing.T) {
	forEachLFImpl(t, 1024*1024, func(t *testing.T, c rawLockFreeCache) {
		k := mixedKey(3)
		other := mixedKey(4)

		require.NoError(t, c.Set(k[:], []byte("a")))
		require.NoError(t, c.Set(other[:], []byte("b")))

		c.Del(k[:])

		var dst []byte
		require.False(t, c.Get(&dst, k[:]))
		require.True(t, c.Get(&dst, other[:]))

		// Deleting a missing key is a no-op.
		c.Del(k[:])

		// A key can be set again after deletion.
		require.NoError(t, c.Set(k[:], []byte("again")))
		dst = dst[:0]
		require.True(t, c.Get(&dst, k[:]))
		require.Equal(t, "again", string(dst))
	})
}

func TestLockFreeCache_WrapEvictsOldestNeverReturnsWrongBytes(t *testing.T) {
	forEachLFImpl(t, 1024*1024, func(t *testing.T, c rawLockFreeCache) {

		// Far more than fits: every shard's ring laps many times.
		const n = 200_000

		for i := uint64(0); i < n; i++ {
			k := mixedKey(i)
			require.NoError(t, c.Set(k[:], lfValue(i)))
		}

		var (
			dst          []byte
			oldHits      int
			recentMisses int
		)

		for i := uint64(0); i < n; i++ {
			k := mixedKey(i)
			dst = dst[:0]

			if !c.Get(&dst, k[:]) {
				if i >= n-500 {
					recentMisses++
				}

				continue
			}

			require.Equal(t, lfValue(i), dst, "key %d returned another record's bytes", i)

			if i < n/2 {
				oldHits++
			}
		}

		require.Zero(t, oldHits, "entries from the first half must have been lapped")
		require.Less(t, recentMisses, 25, "the most recent writes should almost all still be cached")
	})
}

func TestLockFreeCache_RejectsOversizedValueAndBadKey(t *testing.T) {
	forEachLFImpl(t, 1024*1024, func(t *testing.T, c rawLockFreeCache) {
		k := mixedKey(1)

		require.Error(t, c.Set(k[:], make([]byte, 1<<maxValueSizeLog)))
		require.NoError(t, c.Set(k[:], make([]byte, (1<<maxValueSizeLog)-1)))

		require.Error(t, c.Set(k[:31], []byte("x")))
		require.Error(t, c.Set(append(k[:], 0), []byte("x")))

		var s Stats
		c.UpdateStats(&s)
		require.Equal(t, uint64(1), c.Oversize())
	})
}

func TestLockFreeCache_SameSlotDifferentKeyNeverReturnsWrongBytes(t *testing.T) {
	forEachLFImpl(t, 1024*1024, func(t *testing.T, c rawLockFreeCache) {

		// Shard, set and fingerprint come from bytes 0..15 only: b collides with a
		// in all three and differs in the remaining bytes, so only the full key
		// comparison can tell them apart.
		a := mixedKey(11)
		b := a
		b[31] ^= 0xff

		require.NoError(t, c.Set(a[:], []byte("a")))

		var dst []byte
		require.False(t, c.Get(&dst, b[:]), "b was never stored")

		// Inserting b takes a's slot (a fingerprint match is treated as the same
		// key, see publish): a is evicted, which a cache may do, but a lookup of
		// either key must never return the other's bytes.
		require.NoError(t, c.Set(b[:], []byte("b")))

		dst = dst[:0]
		require.True(t, c.Get(&dst, b[:]))
		require.Equal(t, "b", string(dst))

		dst = dst[:0]
		if c.Get(&dst, a[:]) {
			require.Equal(t, "a", string(dst))
		}

		// Deleting a must not delete b: Del compares the full key.
		c.Del(a[:])
		dst = dst[:0]
		require.True(t, c.Get(&dst, b[:]))
		require.Equal(t, "b", string(dst))
	})
}

func TestLockFreeCache_Reset(t *testing.T) {
	forEachLFImpl(t, 1024*1024, func(t *testing.T, c rawLockFreeCache) {

		for i := uint64(0); i < 1000; i++ {
			k := mixedKey(i)
			require.NoError(t, c.Set(k[:], lfValue(i)))
		}

		c.Reset()

		var dst []byte

		for i := uint64(0); i < 1000; i++ {
			k := mixedKey(i)
			require.False(t, c.Get(&dst, k[:]), "key %d survived Reset", i)
		}

		k := mixedKey(5)
		require.NoError(t, c.Set(k[:], []byte("after")))
		require.True(t, c.Get(&dst, k[:]))
		require.Equal(t, "after", string(dst))
	})
}

func TestLockFreeCache_GetMulti(t *testing.T) {
	forEachLFImpl(t, 1024*1024, func(t *testing.T, c rawLockFreeCache) {

		keys := make([]chainhash.Hash, 64)

		for i := range keys {
			keys[i] = mixedKey(uint64(i))

			if i%3 != 0 {
				require.NoError(t, c.Set(keys[i][:], lfValue(uint64(i))))
			}
		}

		dst := []byte("junk")
		spans := make([][2]int, len(keys))
		c.GetMulti(&dst, keys, spans)

		for i := range keys {
			if i%3 == 0 {
				require.Equal(t, [2]int{-1, -1}, spans[i], "key %d", i)
				continue
			}

			require.Equal(t, lfValue(uint64(i)), dst[spans[i][0]:spans[i][1]], "key %d", i)
		}

		require.Equal(t, "junk", string(dst[:4]), "GetMulti must append, not overwrite")
	})
}

// TestLockFreeCache_ConcurrentLapNeverTornRead is the torn-read gate. Writers
// lap a tiny cache as fast as they can, with value lengths that change from
// record to record, while readers check that every hit carries exactly the
// bytes of the key they asked for. A reader that copies a record while it is
// being overwritten must notice and report a miss.
//
// The race detector does not see accesses to the off-heap arena, so this test
// is the real check of the read-validation protocol, not -race.
func TestLockFreeCache_ConcurrentLapNeverTornRead(t *testing.T) {
	forEachLFImpl(t, 256*1024, func(t *testing.T, c rawLockFreeCache) {

		const (
			writers = 8
			readers = 8
		)

		duration := 2 * time.Second
		if testing.Short() {
			duration = 300 * time.Millisecond
		}

		var (
			next   atomic.Uint64
			stop   atomic.Bool
			hits   atomic.Int64
			misses atomic.Int64
			wg     sync.WaitGroup
			failMu sync.Mutex
			failed string
		)

		for w := 0; w < writers; w++ {
			wg.Add(1)

			// Odd writers use the batched SetMulti path, even ones Set.
			batched := w%2 == 1

			go func() {
				defer wg.Done()

				for !stop.Load() {
					if batched {
						const n = 8

						base := next.Add(n) - n + 1
						keys := make([][]byte, n)
						values := make([][]byte, n)

						for j := uint64(0); j < n; j++ {
							k := mixedKey(base + j)
							keys[j] = k[:]
							values[j] = lfValue(base + j)
						}

						if err := c.SetMulti(keys, values); err != nil {
							failMu.Lock()
							failed = err.Error()
							failMu.Unlock()

							return
						}

						continue
					}

					i := next.Add(1)
					k := mixedKey(i)

					if err := c.Set(k[:], lfValue(i)); err != nil {
						failMu.Lock()
						failed = err.Error()
						failMu.Unlock()

						return
					}

					// Some deletes, to race slot clears against inserts and reads.
					if i%17 == 0 {
						old := mixedKey(i - 5)
						c.Del(old[:])
					}
				}
			}()
		}

		for r := 0; r < readers; r++ {
			wg.Add(1)

			go func(r int) {
				defer wg.Done()

				var dst []byte

				rng := uint64(r+1) * 0x9e3779b97f4a7c15

				for !stop.Load() {
					hi := next.Load()
					if hi < 2 {
						continue
					}

					rng ^= rng << 13
					rng ^= rng >> 7
					rng ^= rng << 17

					// Mostly the newest few thousand keys: those are the ones being
					// lapped right now.
					span := min(hi-1, 4096)
					i := hi - rng%span

					k := mixedKey(i)
					dst = dst[:0]

					if !c.Get(&dst, k[:]) {
						misses.Add(1)
						continue
					}

					hits.Add(1)

					if want := lfValue(i); string(dst) != string(want) {
						failMu.Lock()
						failed = fmt.Sprintf("torn read key=%d got len=%d first=%x want len=%d", i, len(dst), dst[:min(8, len(dst))], len(want))
						failMu.Unlock()
						stop.Store(true)

						return
					}
				}
			}(r)
		}

		time.Sleep(duration)
		stop.Store(true)
		wg.Wait()

		require.Empty(t, failed)
		require.Positive(t, hits.Load(), "the readers must have hit something, or the test proves nothing")
		t.Logf("hits=%d misses=%d writes=%d", hits.Load(), misses.Load(), next.Load())
	})
}

// TestLockFreeCache_OlderPublishNeverShadowsNewer covers two writers racing on
// one key: the record reserved later (newer) is published first, then the
// earlier (older) record's publish runs. The older one must not be indexed
// alongside the newer one, or a lookup could return the stale value.
func TestLockFreeCache_OlderPublishNeverShadowsNewer(t *testing.T) {
	c := newTestLockFreeCache(t, 1024*1024)
	k := mixedKey(42)

	s, set, fp := c.locate(k[:])

	older := c.reserve(s, recordBytes(len("older")))
	newer := c.reserve(s, recordBytes(len("newer")))

	c.writeRecord(s, older%c.ringSize, k[:], []byte("older"))
	c.writeRecord(s, newer%c.ringSize, k[:], []byte("newer"))

	c.publish(s, set, fp, newer)
	c.publish(s, set, fp, older)

	var dst []byte
	require.True(t, c.Get(&dst, k[:]))
	require.Equal(t, "newer", string(dst))

	indexed := 0

	for i := 0; i < lfSlotsPerSet; i++ {
		if v := *slotAt(set, i); v != 0 && v>>lfOffsetBits == fp {
			indexed++
		}
	}

	require.Equal(t, 1, indexed, "the key must be indexed once")
}

// lfRegion is the off-heap memory behind either implementation.
func lfRegion(t *testing.T, c rawLockFreeCache) []byte {
	t.Helper()

	switch v := c.(type) {
	case *LockFreeCache:
		return v.region
	case *LaneCache:
		return v.region
	default:
		t.Fatalf("unknown implementation %T", c)
		return nil
	}
}

// TestLockFreeCache_CorruptedRecordIsAMiss is the deterministic check of the
// checksum: a record whose value bytes change after it was written (a late or
// overlapping writer) must read as a miss, never as data. The concurrent
// stress test can only hit that window by chance.
func TestLockFreeCache_CorruptedRecordIsAMiss(t *testing.T) {
	forEachLFImpl(t, 1024*1024, func(t *testing.T, c rawLockFreeCache) {
		for i := uint64(0); i < 32; i++ {
			k := mixedKey(1050 + i)
			v := lfValue(1050 + i) // 67+ bytes long
			require.NoError(t, c.Set(k[:], v))

			var dst []byte
			require.True(t, c.Get(&dst, k[:]))

			region := lfRegion(t, c)

			// Find the record by its key, which is unique; in both layouts
			// the value follows the key directly.
			at := bytes.Index(region, k[:])
			require.GreaterOrEqual(t, at, 0, "record not found")

			flip := at + lfKeyBytes + int(i)%len(v)
			region[flip] ^= 0x01

			dst = dst[:0]
			require.False(t, c.Get(&dst, k[:]), "a corrupted record must not be served (byte %d)", flip)

			region[flip] ^= 0x01

			dst = dst[:0]
			require.True(t, c.Get(&dst, k[:]), "restoring the byte restores the record")
			require.Equal(t, v, dst)
		}
	})
}
