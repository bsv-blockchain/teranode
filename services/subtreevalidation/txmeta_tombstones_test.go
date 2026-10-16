package subtreevalidation

import (
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/stores/txmetacache"
	"github.com/bsv-blockchain/teranode/util/kafka"
	"github.com/cespare/xxhash/v2"
	"github.com/stretchr/testify/require"
)

type fakeClock struct{ t time.Time }

func (c *fakeClock) now() time.Time { return c.t }

func TestTxMetaTombstones_RemembersDeletesForAtLeastTheWindow(t *testing.T) {
	clock := &fakeClock{t: time.Unix(1_000_000, 0)}
	ts := newTxMetaTombstones(time.Minute, clock.now)

	deleted := chainhash.HashH([]byte("deleted"))
	other := chainhash.HashH([]byte("other"))

	require.False(t, ts.contains(deleted[:]), "empty set holds nothing")

	ts.add([]chainhash.Hash{deleted})

	require.True(t, ts.contains(deleted[:]))
	require.False(t, ts.contains(other[:]))

	// Still remembered just before one full window has passed, and across the
	// first rotation.
	clock.t = clock.t.Add(59 * time.Second)
	require.True(t, ts.contains(deleted[:]))

	clock.t = clock.t.Add(30 * time.Second)
	require.True(t, ts.contains(deleted[:]), "an entry survives the first rotation")

	// Gone after two windows.
	clock.t = clock.t.Add(2 * time.Minute)
	require.False(t, ts.contains(deleted[:]), "an entry is forgotten after two windows")
	require.Zero(t, ts.size.Load(), "an expired set is empty again, so the ADD path is back on its fast path")
}

func TestTxMetaTombstones_NilIsEmpty(t *testing.T) {
	var ts *txMetaTombstones

	h := chainhash.HashH([]byte("x"))

	ts.add([]chainhash.Hash{h})
	require.False(t, ts.contains(h[:]))
}

// TestTxmetaHandler_LateAddAfterDeleteIsNotCached pins the reordering case: a
// DELETE from block assembly overtakes the validator's ADD for the same tx (they
// travel on different partitions). The late ADD must not re-cache the demoted tx
// as non-conflicting; other ADDs in the same message are still applied.
func TestTxmetaHandler_LateAddAfterDeleteIsNotCached(t *testing.T) {
	h := newTxmetaE2EHarness(t, "v1", 32)
	defer h.close()

	h.server.txMetaTombstones = newTxMetaTombstones(txMetaTombstoneWindow, time.Now)

	hashes, payloads := makeUniqueHashes(t, 2, 64)
	demoted, unrelated := hashes[0], hashes[1]

	require.NoError(t, h.server.txmetaHandler(h.ctx, &kafka.KafkaMessage{Value: txmetacache.EncodeDeleteBatch([]chainhash.Hash{demoted})}))

	h.publishV1(t, hashes, payloads)
	h.waitProcessed(t, 2, 5*time.Second)

	_, cached := h.cache.GetRaw(demoted[:])
	require.False(t, cached, "an ADD arriving after the DELETE for the same tx must not re-cache it")

	_, cached = h.cache.GetRaw(unrelated[:])
	require.True(t, cached, "other ADDs in the message are still applied")
}

// deleteDuringSetCache runs beforeSet once, inside the next batch write and just
// before it, to place a DELETE between an ADD's tombstone check and its Set.
type deleteDuringSetCache struct {
	*e2eCache
	beforeSet func()
}

func (c *deleteDuringSetCache) runBeforeSet() {
	if f := c.beforeSet; f != nil {
		c.beforeSet = nil
		f()
	}
}

func (c *deleteDuringSetCache) SetCacheMultiSequential(keys, values [][]byte) error {
	c.runBeforeSet()

	return c.e2eCache.SetCacheMultiSequential(keys, values)
}

func (c *deleteDuringSetCache) SetCacheMultiSequentialWithHashes(keys, values [][]byte, hashes []uint64) error {
	c.runBeforeSet()

	return c.e2eCache.SetCacheMultiSequentialWithHashes(keys, values, hashes)
}

// serializeV2ForE2E encodes all ADDs as one v2 message, in the layout publishV2 uses.
func serializeV2ForE2E(hashes []chainhash.Hash, payloads [][]byte) []byte {
	size := txmetacache.WireV2HeaderLen
	for i := range hashes {
		size += txmetacache.WireV2MinEntrySize + len(payloads[i])
	}

	buf := make([]byte, size)
	buf[0] = txmetacache.WireV2Magic
	buf[1] = txmetacache.WireV2Version
	putUint32LE(buf[4:], uint32(len(hashes)))
	off := txmetacache.WireV2HeaderLen

	for i := range hashes {
		binary8LE(buf[off:], xxhash.Sum64(hashes[i][:]))
		off += 8
		copy(buf[off:], hashes[i][:])
		off += 32
		buf[off] = txmetacache.WireActionADD
		off++
		putUint32LE(buf[off:], uint32(len(payloads[i])))
		off += 4
		copy(buf[off:], payloads[i])
		off += len(payloads[i])
	}

	return buf
}

// TestTxmetaHandler_DeleteBetweenCheckAndSetIsNotCached pins the interleaving the
// parse-time tombstone check alone misses: the ADD passes the check, then a
// DELETE for the same tx is consumed from another partition before the ADD's
// batch Set. The tx must still end up evicted; other ADDs stay cached. Both wire
// formats are covered, since they reach the cache through different Set calls.
func TestTxmetaHandler_DeleteBetweenCheckAndSetIsNotCached(t *testing.T) {
	for _, tc := range []struct {
		format    string
		serialize func([]chainhash.Hash, [][]byte) []byte
	}{
		{"v1", serializeV1ForE2E},
		{"v2", serializeV2ForE2E},
	} {
		t.Run(tc.format, func(t *testing.T) {
			h := newTxmetaE2EHarness(t, tc.format, 32)
			defer h.close()

			h.server.txMetaTombstones = newTxMetaTombstones(txMetaTombstoneWindow, time.Now)

			hashes, payloads := makeUniqueHashes(t, 2, 64)
			demoted, unrelated := hashes[0], hashes[1]

			cache := &deleteDuringSetCache{e2eCache: h.cache}
			cache.beforeSet = func() {
				require.NoError(t, h.server.txmetaHandler(h.ctx, &kafka.KafkaMessage{Value: txmetacache.EncodeDeleteBatch([]chainhash.Hash{demoted})}))
			}
			h.server.utxoStore = cache

			require.NoError(t, h.server.txmetaHandler(h.ctx, &kafka.KafkaMessage{Value: tc.serialize(hashes, payloads)}))
			require.Nil(t, cache.beforeSet, "the DELETE must have run inside the ADD's Set")

			_, cached := h.cache.GetRaw(demoted[:])
			require.False(t, cached, "a DELETE landing between the tombstone check and the Set must still evict the tx")

			_, cached = h.cache.GetRaw(unrelated[:])
			require.True(t, cached, "other ADDs in the message are still applied")
		})
	}
}
