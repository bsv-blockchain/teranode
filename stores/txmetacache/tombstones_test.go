package txmetacache

import (
	"context"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/cespare/xxhash/v2"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

type fakeClock struct{ t time.Time }

func (c *fakeClock) now() time.Time { return c.t }

func TestTombstones_RemembersDeletesForAtLeastTheWindow(t *testing.T) {
	clock := &fakeClock{t: time.Unix(1_000_000, 0)}
	ts := newTombstones(time.Minute, clock.now)

	deleted := chainhash.HashH([]byte("deleted"))
	other := chainhash.HashH([]byte("other"))

	require.False(t, ts.contains(deleted[:]), "empty set holds nothing")

	ts.add([]chainhash.Hash{deleted})

	require.True(t, ts.contains(deleted[:]))
	require.False(t, ts.contains(other[:]))
	require.False(t, ts.contains(deleted[:31]), "a key that is not a tx hash is never tombstoned")

	// Still remembered just before one full window has passed, and across the
	// first rotation.
	clock.t = clock.t.Add(59 * time.Second)
	require.True(t, ts.contains(deleted[:]))

	clock.t = clock.t.Add(30 * time.Second)
	require.True(t, ts.contains(deleted[:]), "an entry survives the first rotation")

	// Gone after two windows.
	clock.t = clock.t.Add(2 * time.Minute)
	require.False(t, ts.contains(deleted[:]), "an entry is forgotten after two windows")
	require.Zero(t, ts.size.Load(), "an expired set is empty again, so the write paths are back on their fast path")
}

func TestTombstones_NilIsEmpty(t *testing.T) {
	var ts *tombstones

	h := chainhash.HashH([]byte("x"))

	ts.add([]chainhash.Hash{h})
	require.True(t, ts.empty())
	require.False(t, ts.contains(h[:]))
}

func TestNewTxMetaCache_TombstoneWindowComesFromSettings(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)
	require.Equal(t, 30*time.Minute, tSettings.SubtreeValidation.TxMetaTombstoneWindow, "default")

	tSettings.SubtreeValidation.TxMetaTombstoneWindow = 7 * time.Minute

	c, err := NewTxMetaCache(t.Context(), tSettings, ulogger.TestLogger{}, &utxo.MockUtxostore{}, Unallocated, 32)
	require.NoError(t, err)
	require.Equal(t, 7*time.Minute, c.(*TxMetaCache).tombstones.window)

	// A non-positive window forgets every DELETE at once.
	tSettings.SubtreeValidation.TxMetaTombstoneWindow = 0

	_, err = NewTxMetaCache(t.Context(), tSettings, ulogger.TestLogger{}, &utxo.MockUtxostore{}, Unallocated, 32)
	require.ErrorIs(t, err, errors.ErrConfiguration)
}

// tombstoneTestEntry is a cacheable, non-conflicting meta and its serialised form.
func tombstoneTestEntry(t *testing.T, seed string) (chainhash.Hash, *meta.Data, []byte) {
	t.Helper()

	data := &meta.Data{
		Fee:         1,
		SizeInBytes: 100,
		TxInpoints:  subtree.NewTxInpointsFromPacked([]chainhash.Hash{chainhash.HashH([]byte(seed + "-parent"))}, []uint32{1, 0}),
	}

	b, err := data.MetaBytes()
	require.NoError(t, err)

	return chainhash.HashH([]byte(seed)), data, b
}

func newTombstoneTestCache(t *testing.T, bucketType BucketType, store utxo.Store) *TxMetaCache {
	t.Helper()

	c, err := NewTxMetaCache(t.Context(), test.CreateBaseTestSettings(t), ulogger.TestLogger{}, store, bucketType, 64)
	require.NoError(t, err)

	return c.(*TxMetaCache)
}

func isCached(c *TxMetaCache, h chainhash.Hash) bool {
	_, ok := c.GetMetaCached(context.Background(), h)

	return ok
}

var tombstoneTestBucketTypes = []struct {
	name       string
	bucketType BucketType
}{
	{"improved", Unallocated},
	{"pointer", Pointer},
	{"lockfree", Lockfree},
}

// writePaths are every TxMetaCache method that caches a non-conflicting meta.
var writePaths = []struct {
	name  string
	write func(c *TxMetaCache, h chainhash.Hash, data *meta.Data, b []byte) error
}{
	{"SetCache", func(c *TxMetaCache, h chainhash.Hash, data *meta.Data, _ []byte) error { return c.SetCache(&h, data) }},
	{"SetCacheFromBytes", func(c *TxMetaCache, h chainhash.Hash, _ *meta.Data, b []byte) error {
		return c.SetCacheFromBytes(h[:], b)
	}},
	{"SetCacheMulti", func(c *TxMetaCache, h chainhash.Hash, _ *meta.Data, b []byte) error {
		return c.SetCacheMulti([][]byte{h[:]}, [][]byte{b})
	}},
	{"SetCacheMultiSequential", func(c *TxMetaCache, h chainhash.Hash, _ *meta.Data, b []byte) error {
		return c.SetCacheMultiSequential([][]byte{h[:]}, [][]byte{b})
	}},
	{"SetCacheMultiSequentialWithHashes", func(c *TxMetaCache, h chainhash.Hash, _ *meta.Data, b []byte) error {
		return c.SetCacheMultiSequentialWithHashes([][]byte{h[:]}, [][]byte{b}, []uint64{xxhash.Sum64(h[:])})
	}},
	{"SetCacheMultiValuesRaw", func(c *TxMetaCache, h chainhash.Hash, _ *meta.Data, b []byte) error {
		return c.SetCacheMultiValuesRaw([][]byte{h[:]}, [][]byte{b})
	}},
}

// TestTxMetaCache_DeleteDemotedBlocksLaterWrites pins that once DeleteDemoted
// evicted a tx, no write path caches it again, on any backend, while other txs
// are still cached and a plain Delete (the setMined eviction) blocks nothing.
func TestTxMetaCache_DeleteDemotedBlocksLaterWrites(t *testing.T) {
	for _, bt := range tombstoneTestBucketTypes {
		for _, wp := range writePaths {
			t.Run(bt.name+"/"+wp.name, func(t *testing.T) {
				c := newTombstoneTestCache(t, bt.bucketType, &utxo.MockUtxostore{})

				demoted, demotedData, demotedBytes := tombstoneTestEntry(t, "demoted")
				mined, minedData, minedBytes := tombstoneTestEntry(t, "mined")
				other, otherData, otherBytes := tombstoneTestEntry(t, "other")

				require.NoError(t, wp.write(c, demoted, demotedData, demotedBytes))
				require.True(t, isCached(c, demoted))

				before := testutil.ToFloat64(prometheusTxMetaCacheTombstonedWritesEvicted)

				c.DeleteDemoted([]chainhash.Hash{demoted})
				require.False(t, isCached(c, demoted), "DeleteDemoted evicts")

				require.NoError(t, wp.write(c, demoted, demotedData, demotedBytes))
				require.False(t, isCached(c, demoted), "a write after DeleteDemoted must not cache the demoted tx again")
				require.Equal(t, 1.0, testutil.ToFloat64(prometheusTxMetaCacheTombstonedWritesEvicted)-before)

				require.NoError(t, c.Delete(t.Context(), &mined))
				require.NoError(t, wp.write(c, mined, minedData, minedBytes))
				require.True(t, isCached(c, mined), "a plain Delete leaves no tombstone")

				require.NoError(t, wp.write(c, other, otherData, otherBytes))
				require.True(t, isCached(c, other))
			})
		}
	}
}

// deleteDuringWriteBackend runs DeleteDemoted for a key while that key's write
// is in progress, before the write lands: the DELETE has to win anyway.
type deleteDuringWriteBackend struct {
	cacheBackend
	during func()
}

func (b *deleteDuringWriteBackend) fire() {
	if f := b.during; f != nil {
		b.during = nil
		f()
	}
}

func (b *deleteDuringWriteBackend) SetMultiSequential(keys, values [][]byte) error {
	b.fire()

	return b.cacheBackend.SetMultiSequential(keys, values)
}

func (b *deleteDuringWriteBackend) SetMultiSequentialWithHashes(keys, values [][]byte, hashes []uint64) error {
	b.fire()

	return b.cacheBackend.SetMultiSequentialWithHashes(keys, values, hashes)
}

func TestTxMetaCache_DeleteDemotedDuringWriteIsNotCached(t *testing.T) {
	for _, bt := range tombstoneTestBucketTypes {
		t.Run(bt.name, func(t *testing.T) {
			c := newTombstoneTestCache(t, bt.bucketType, &utxo.MockUtxostore{})

			demoted, _, demotedBytes := tombstoneTestEntry(t, "demoted")
			other, _, otherBytes := tombstoneTestEntry(t, "other")

			backend := &deleteDuringWriteBackend{cacheBackend: c.cache}
			backend.during = func() { c.DeleteDemoted([]chainhash.Hash{demoted}) }
			c.cache = backend

			require.NoError(t, c.SetCacheMultiSequential([][]byte{demoted[:], other[:]}, [][]byte{demotedBytes, otherBytes}))
			require.Nil(t, backend.during, "the DELETE must have run inside the write")

			require.False(t, isCached(c, demoted), "a DELETE that lands during the write must still win")
			require.True(t, isCached(c, other))
		})
	}
}

// keyRecordingBackend records the keys handed to its batch writes.
type keyRecordingBackend struct {
	cacheBackend
	written [][]byte
}

func (b *keyRecordingBackend) SetMultiSequential(keys, values [][]byte) error {
	b.written = append(b.written, keys...)

	return b.cacheBackend.SetMultiSequential(keys, values)
}

func (b *keyRecordingBackend) SetMultiSequentialWithHashes(keys, values [][]byte, hashes []uint64) error {
	b.written = append(b.written, keys...)

	return b.cacheBackend.SetMultiSequentialWithHashes(keys, values, hashes)
}

// TestTxMetaCache_TombstonedKeysAreNeverWritten pins that a write of an already
// tombstoned tx is dropped before it reaches the backend, so a concurrent
// reader never sees the demoted tx cached, not even for the length of the
// write. The other entries and their hashes stay aligned.
func TestTxMetaCache_TombstonedKeysAreNeverWritten(t *testing.T) {
	c := newTombstoneTestCache(t, Unallocated, &utxo.MockUtxostore{})

	first, _, firstBytes := tombstoneTestEntry(t, "first")
	demoted, _, demotedBytes := tombstoneTestEntry(t, "demoted")
	last, _, lastBytes := tombstoneTestEntry(t, "last")

	c.DeleteDemoted([]chainhash.Hash{demoted})

	backend := &keyRecordingBackend{cacheBackend: c.cache}
	c.cache = backend

	keys := [][]byte{first[:], demoted[:], last[:]}
	values := [][]byte{firstBytes, demotedBytes, lastBytes}

	require.NoError(t, c.SetCacheMultiSequential(keys, values))
	require.NoError(t, c.SetCacheMultiSequentialWithHashes(keys, values,
		[]uint64{xxhash.Sum64(first[:]), xxhash.Sum64(demoted[:]), xxhash.Sum64(last[:])}))

	require.Equal(t, [][]byte{first[:], last[:], first[:], last[:]}, backend.written,
		"the tombstoned key must never reach the backend")
	require.True(t, isCached(c, first))
	require.True(t, isCached(c, last), "entries after a dropped one keep their own value and hash")
	require.False(t, isCached(c, demoted))
}

// TestTxMetaCache_ReadThroughRacingDemotionIsNotCached pins the read-through
// race: the store read returns the tx as non-conflicting, the demotion's DELETE
// is consumed before the read's cache write, and that write must not bring the
// tx back.
func TestTxMetaCache_ReadThroughRacingDemotionIsNotCached(t *testing.T) {
	for _, bt := range tombstoneTestBucketTypes {
		t.Run(bt.name+"/GetMeta", func(t *testing.T) {
			store := &utxo.MockUtxostore{}
			c := newTombstoneTestCache(t, bt.bucketType, store)

			demoted, demotedData, _ := tombstoneTestEntry(t, "demoted")

			store.On("GetMeta", mock.Anything, &demoted, mock.Anything).
				Run(func(mock.Arguments) { c.DeleteDemoted([]chainhash.Hash{demoted}) }).
				Return(demotedData)

			got := &meta.Data{}
			require.NoError(t, c.GetMeta(t.Context(), &demoted, got))
			require.Equal(t, demotedData.Fee, got.Fee, "the read itself still answers from the store")

			require.False(t, isCached(c, demoted), "a read racing the demotion must not cache the tx")
		})

		t.Run(bt.name+"/BatchDecorate", func(t *testing.T) {
			store := &utxo.MockUtxostore{}
			c := newTombstoneTestCache(t, bt.bucketType, store)

			demoted, demotedData, _ := tombstoneTestEntry(t, "demoted")
			other, otherData, _ := tombstoneTestEntry(t, "other")

			store.On("BatchDecorate", mock.Anything, mock.Anything, mock.Anything).
				Run(func(args mock.Arguments) {
					for _, u := range args.Get(1).([]*utxo.UnresolvedMetaData) {
						if u.Hash == demoted {
							u.Data = demotedData
						} else {
							u.Data = otherData
						}
					}

					c.DeleteDemoted([]chainhash.Hash{demoted})
				}).
				Return(nil)

			require.NoError(t, c.BatchDecorate(t.Context(), []*utxo.UnresolvedMetaData{{Hash: demoted}, {Hash: other}}))

			require.False(t, isCached(c, demoted), "a write-back racing the demotion must not cache the tx")
			require.True(t, isCached(c, other))
		})
	}
}

// staleWriteBackend reproduces a writer that passed the tombstone check before
// DeleteDemoted ran: DeleteDemoted completes, then the old write lands, then
// readInGap runs before the writer's post-write eviction.
type staleWriteBackend struct {
	cacheBackend
	before    func()
	readInGap func()
}

func (b *staleWriteBackend) SetMultiSequential(keys, values [][]byte) error {
	if b.before != nil {
		b.before()
	}

	err := b.cacheBackend.SetMultiSequential(keys, values)

	if b.readInGap != nil {
		b.readInGap()
	}

	return err
}

// TestTxMetaCache_NoStaleHitAfterDeleteDemotedReturns pins that once
// DeleteDemoted has returned, no read path returns the demoted tx, even while
// an older write of it has landed and not yet been evicted.
func TestTxMetaCache_NoStaleHitAfterDeleteDemotedReturns(t *testing.T) {
	for _, bt := range tombstoneTestBucketTypes {
		t.Run(bt.name, func(t *testing.T) {
			store := &utxo.MockUtxostore{}
			c := newTombstoneTestCache(t, bt.bucketType, store)

			demoted, demotedData, demotedBytes := tombstoneTestEntry(t, "demoted")

			// GetMeta falls through to the store on a miss: the store has it demoted.
			store.On("GetMeta", mock.Anything, &demoted, mock.Anything).
				Return(&meta.Data{Fee: demotedData.Fee, SizeInBytes: demotedData.SizeInBytes, Conflicting: true})

			var (
				single, multi, into, getMeta bool
				ran                          bool
			)

			backend := &staleWriteBackend{cacheBackend: c.cache}
			backend.before = func() { c.DeleteDemoted([]chainhash.Hash{demoted}) }
			backend.readInGap = func() {
				ran = true

				_, raw := c.cache.Get(demoted)
				require.True(t, raw, "the stale write must have landed, otherwise this test proves nothing")

				_, single = c.GetMetaCached(context.Background(), demoted)

				out := make([]*meta.Data, 1)
				c.GetMetaCachedMulti(context.Background(), []chainhash.Hash{demoted}, nil, out)
				multi = out[0] != nil

				cm := make([]CachedMeta, 1)
				found := make([]bool, 1)
				_, err := c.GetMetaCachedInto(context.Background(), []chainhash.Hash{demoted}, nil, cm, found)
				require.NoError(t, err)
				into = found[0]

				got := &meta.Data{}
				require.NoError(t, c.GetMeta(context.Background(), &demoted, got))
				getMeta = !got.Conflicting
			}

			// Bypass the pre-write check the way a writer that checked before
			// DeleteDemoted would: hand the write straight to the backend.
			c.cache = backend
			require.NoError(t, backend.SetMultiSequential([][]byte{demoted[:]}, [][]byte{demotedBytes}))
			require.True(t, ran)

			require.False(t, single, "GetMetaCached must not return a tx DeleteDemoted already evicted")
			require.False(t, multi, "GetMetaCachedMulti must not return a tx DeleteDemoted already evicted")
			require.False(t, into, "GetMetaCachedInto must not return a tx DeleteDemoted already evicted")
			require.False(t, getMeta, "GetMeta must answer from the store, not the stale entry")
		})
	}
}

func TestSplitDeleteBatches(t *testing.T) {
	hashes := func(n int) []chainhash.Hash {
		out := make([]chainhash.Hash, n)
		for i := range out {
			out[i] = chainhash.HashH([]byte{byte(i), byte(i >> 8), byte(i >> 16)})
		}

		return out
	}

	for _, n := range []int{0, 1, V2LookalikeCount, MaxDeleteBatch, MaxDeleteBatch + V2LookalikeCount, 3*MaxDeleteBatch + 5} {
		all := hashes(n)

		var joined []chainhash.Hash

		for _, b := range SplitDeleteBatches(all) {
			require.NotEmpty(t, b)
			require.LessOrEqual(t, len(b), MaxDeleteBatch)
			require.NotEqual(t, V2LookalikeCount, len(b), "no batch may be 767 long")

			joined = append(joined, b...)
		}

		require.Equal(t, len(all), len(joined), "n=%d", n)
		if n > 0 {
			require.Equal(t, all, joined, "batches keep the order, n=%d", n)
		}
	}
}
