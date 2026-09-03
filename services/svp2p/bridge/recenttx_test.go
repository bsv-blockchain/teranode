package bridge

import (
	"context"
	"crypto/rand"
	"io"
	"net/url"
	"os"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/services/svp2p/protocol"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/txmetacache"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	utxosql "github.com/bsv-blockchain/teranode/stores/utxo/sql"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/kafka"
	"github.com/stretchr/testify/require"
)

// testHash builds a deterministic, distinct hash from n. The hashes only
// have to be distinct: nothing in the index interprets their content.
func testHash(n byte) chainhash.Hash {
	var h chainhash.Hash
	h[0] = n
	h[1] = 0xa5

	return h
}

// noFetch is the fetch seam for tests that never call Open.
func noFetch(context.Context, chainhash.Hash) (io.ReadCloser, uint64, error) {
	return nil, 0, errors.NewTxNotFoundError("no fetch seam in this test")
}

// TestRecentTxIndex_SatisfiesTxIndex fixes the seam Task 4 declared: the
// index is what PeerManager.SetTxIndex is handed.
func TestRecentTxIndex_SatisfiesTxIndex(t *testing.T) {
	var idx protocol.TxIndex = NewRecentTxIndex(4, noFetch)

	require.NotNil(t, idx)
}

// TestRecentTxIndex_AddDedupsAndCounts covers the dedup map: the same hash
// added twice occupies one ring slot, so it can never evict a distinct
// hash that is still wanted.
func TestRecentTxIndex_AddDedupsAndCounts(t *testing.T) {
	idx := NewRecentTxIndex(4, noFetch)

	idx.Add(testHash(1))
	idx.Add(testHash(2))
	idx.Add(testHash(1))
	idx.Add(testHash(1))

	require.Equal(t, 2, idx.Len(), "a repeated hash must not take a second ring slot")

	k0, k1 := uint64(0x0706050403020100), uint64(0x0f0e0d0c0b0a0908)

	matched, collision := idx.Match(k0, k1, []uint64{
		protocol.ShortID(k0, k1, testHash(1)),
		protocol.ShortID(k0, k1, testHash(2)),
	})
	require.False(t, collision, "one hash held once cannot collide with itself")
	require.Equal(t, testHash(1), *matched[0])
	require.Equal(t, testHash(2), *matched[1])
}

// TestRecentTxIndex_EvictsOldestAtCapacity walks the ring past its wrap
// point twice, so the eviction order is proved across the index reset, not
// only on the first pass.
func TestRecentTxIndex_EvictsOldestAtCapacity(t *testing.T) {
	const capacity = 3

	idx := NewRecentTxIndex(capacity, noFetch)

	for n := byte(1); n <= 8; n++ {
		idx.Add(testHash(n))

		require.LessOrEqual(t, idx.Len(), capacity, "the ring must never exceed its capacity")
	}

	require.Equal(t, capacity, idx.Len())

	k0, k1 := uint64(1), uint64(2)

	ids := make([]uint64, 0, 8)
	for n := byte(1); n <= 8; n++ {
		ids = append(ids, protocol.ShortID(k0, k1, testHash(n)))
	}

	matched, collision := idx.Match(k0, k1, ids)
	require.False(t, collision)

	for n := 0; n < 5; n++ {
		require.Nil(t, matched[n], "hash %d is the oldest and must have been evicted", n+1)
	}

	for n := 5; n < 8; n++ {
		require.NotNil(t, matched[n], "hash %d is one of the last three added and must be held", n+1)
		require.Equal(t, testHash(byte(n+1)), *matched[n])
	}
}

// TestRecentTxIndex_ReAddingAnEvictedHashHoldsItAgain proves the dedup map
// loses the evicted key: an evicted hash is a new hash again, not one the
// dedup check silently drops.
func TestRecentTxIndex_ReAddingAnEvictedHashHoldsItAgain(t *testing.T) {
	idx := NewRecentTxIndex(2, noFetch)

	idx.Add(testHash(1))
	idx.Add(testHash(2))
	idx.Add(testHash(3)) // evicts 1
	idx.Add(testHash(1)) // evicts 2, holds 1 again

	require.Equal(t, 2, idx.Len())

	k0, k1 := uint64(9), uint64(9)

	matched, collision := idx.Match(k0, k1, []uint64{
		protocol.ShortID(k0, k1, testHash(1)),
		protocol.ShortID(k0, k1, testHash(2)),
		protocol.ShortID(k0, k1, testHash(3)),
	})
	require.False(t, collision)
	require.NotNil(t, matched[0], "the re-added hash must be held again")
	require.Nil(t, matched[1], "the hash the re-add evicted must be gone")
	require.NotNil(t, matched[2])
}

// TestRecentTxIndex_MatchMissesUnknownShortIDs is the gap case the manager
// turns into getblocktxn: a short ID no held hash produces returns nil in
// its own position, and the held ones keep theirs.
func TestRecentTxIndex_MatchMissesUnknownShortIDs(t *testing.T) {
	idx := NewRecentTxIndex(8, noFetch)
	idx.Add(testHash(1))
	idx.Add(testHash(2))

	k0, k1 := uint64(0xdeadbeef), uint64(0xfeedface)

	matched, collision := idx.Match(k0, k1, []uint64{
		protocol.ShortID(k0, k1, testHash(9)),
		protocol.ShortID(k0, k1, testHash(2)),
		protocol.ShortID(k0, k1, testHash(8)),
		protocol.ShortID(k0, k1, testHash(1)),
	})
	require.False(t, collision)
	require.Len(t, matched, 4)
	require.Nil(t, matched[0])
	require.Equal(t, testHash(2), *matched[1])
	require.Nil(t, matched[2])
	require.Equal(t, testHash(1), *matched[3])
}

// TestRecentTxIndex_MatchUnderDifferentKeysMissesEverything proves Match
// keys on (k0,k1) rather than on the hash: short IDs computed under one
// nonce identify nothing under another.
func TestRecentTxIndex_MatchUnderDifferentKeysMissesEverything(t *testing.T) {
	idx := NewRecentTxIndex(8, noFetch)
	idx.Add(testHash(1))

	matched, collision := idx.Match(2, 2, []uint64{protocol.ShortID(1, 1, testHash(1))})
	require.False(t, collision)
	require.Nil(t, matched[0], "a short ID from other keys must not match")
}

// TestRecentTxIndex_MatchEmptyCases guards the two degenerate inputs the
// manager can reach: an empty index, and a compact block whose every
// transaction is prefilled.
func TestRecentTxIndex_MatchEmptyCases(t *testing.T) {
	idx := NewRecentTxIndex(8, noFetch)

	matched, collision := idx.Match(1, 2, []uint64{protocol.ShortID(1, 2, testHash(1))})
	require.False(t, collision)
	require.Len(t, matched, 1)
	require.Nil(t, matched[0])

	idx.Add(testHash(1))

	matched, collision = idx.Match(1, 2, nil)
	require.False(t, collision)
	require.Empty(t, matched)
}

// TestRecentTxIndex_MatchFlagsShortIDCollision is the READ_STATUS_FAILED
// input: two held hashes produce one short ID. The collision is built
// through the index's own short-ID seam, so the test states the condition
// directly instead of brute-forcing 48 bits for it.
func TestRecentTxIndex_MatchFlagsShortIDCollision(t *testing.T) {
	idx := NewRecentTxIndex(8, noFetch)
	idx.shortID = collidingShortID(map[chainhash.Hash]uint64{
		testHash(1): 0x11,
		testHash(2): 0x11,
		testHash(3): 0x33,
	})

	idx.Add(testHash(1))
	idx.Add(testHash(2))
	idx.Add(testHash(3))

	matched, collision := idx.Match(0, 0, []uint64{0x11, 0x33})
	require.True(t, collision, "two held hashes on one short ID must be reported")
	require.Nil(t, matched[0], "a collided short ID identifies no transaction")
	require.Equal(t, testHash(3), *matched[1], "the uncollided short ID still resolves")
}

// TestRecentTxIndex_MatchIgnoresCollisionsOutsideTheBlock keeps the cost of
// a collision proportional to its effect: two held hashes may share a short
// ID for ever, and it means nothing until a compact block asks for that ID.
func TestRecentTxIndex_MatchIgnoresCollisionsOutsideTheBlock(t *testing.T) {
	idx := NewRecentTxIndex(8, noFetch)
	idx.shortID = collidingShortID(map[chainhash.Hash]uint64{
		testHash(1): 0x11,
		testHash(2): 0x11,
		testHash(3): 0x33,
	})

	idx.Add(testHash(1))
	idx.Add(testHash(2))
	idx.Add(testHash(3))

	matched, collision := idx.Match(0, 0, []uint64{0x33})
	require.False(t, collision, "a collision on a short ID the block never asks for is not a collision")
	require.Equal(t, testHash(3), *matched[0])
}

// TestRecentTxIndex_MatchRepeatsAHashForARepeatedShortID keeps Match total
// over its input: the caller rejects a compact block that repeats a short
// ID, but Match must still return one entry per requested position.
func TestRecentTxIndex_MatchRepeatsAHashForARepeatedShortID(t *testing.T) {
	idx := NewRecentTxIndex(8, noFetch)
	idx.Add(testHash(1))

	id := protocol.ShortID(3, 4, testHash(1))

	matched, collision := idx.Match(3, 4, []uint64{id, id})
	require.False(t, collision)
	require.Len(t, matched, 2)
	require.Equal(t, testHash(1), *matched[0])
	require.Equal(t, testHash(1), *matched[1])
}

// collidingShortID is a short-ID function that maps hashes by table, the
// seam the collision tests need. Any hash outside the table takes a value
// no test asks for.
func collidingShortID(table map[chainhash.Hash]uint64) func(k0, k1 uint64, hash chainhash.Hash) uint64 {
	return func(_, _ uint64, hash chainhash.Hash) uint64 {
		if id, ok := table[hash]; ok {
			return id
		}

		return 0xffffffffffff
	}
}

// TestRecentTxIndex_OpenReturnsTheStoredBytes proves Open reads the fetch
// seam and reports the length the caller streams against.
func TestRecentTxIndex_OpenReturnsTheStoredBytes(t *testing.T) {
	const body = "raw transaction bytes"

	var gotHash chainhash.Hash

	idx := NewRecentTxIndex(8, func(_ context.Context, hash chainhash.Hash) (io.ReadCloser, uint64, error) {
		gotHash = hash

		return io.NopCloser(strings.NewReader(body)), uint64(len(body)), nil
	})

	rc, size, err := idx.Open(context.Background(), testHash(7))
	require.NoError(t, err)
	require.Equal(t, testHash(7), gotHash)
	require.Equal(t, uint64(len(body)), size)

	raw, err := io.ReadAll(rc)
	require.NoError(t, err)
	require.NoError(t, rc.Close())
	require.Equal(t, body, string(raw))
}

// TestRecentTxIndex_OpenUnknownIsErrTxUnknown covers both shapes of "the
// store does not hold it" the fetch seam produces (fetch.go FetchTx).
func TestRecentTxIndex_OpenUnknownIsErrTxUnknown(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
	}{
		{"tx not found", errors.NewTxNotFoundError("not retained in full")},
		{"not found", errors.ErrNotFound},
	} {
		t.Run(tc.name, func(t *testing.T) {
			idx := NewRecentTxIndex(8, func(context.Context, chainhash.Hash) (io.ReadCloser, uint64, error) {
				return nil, 0, tc.err
			})

			rc, size, err := idx.Open(context.Background(), testHash(7))
			require.Nil(t, rc)
			require.Zero(t, size)
			require.True(t, errors.Is(err, protocol.ErrTxUnknown), "expected ErrTxUnknown, got %v", err)
		})
	}
}

// TestRecentTxIndex_OpenPassesOtherErrorsThrough keeps a sick store
// distinguishable from a missing transaction: only not-found becomes
// ErrTxUnknown.
func TestRecentTxIndex_OpenPassesOtherErrorsThrough(t *testing.T) {
	storeErr := errors.NewStorageError("aerospike is unreachable")

	idx := NewRecentTxIndex(8, func(context.Context, chainhash.Hash) (io.ReadCloser, uint64, error) {
		return nil, 0, storeErr
	})

	_, _, err := idx.Open(context.Background(), testHash(7))
	require.False(t, errors.Is(err, protocol.ErrTxUnknown), "a store fault must not read as a missing transaction")
	require.True(t, errors.Is(err, storeErr))
}

// TestRecentTxIndex_OpenWithoutAFetchSeamIsUnknown covers the depless
// bridge, which holds an index but no store to read bytes from.
func TestRecentTxIndex_OpenWithoutAFetchSeamIsUnknown(t *testing.T) {
	idx := NewRecentTxIndex(8, nil)

	_, _, err := idx.Open(context.Background(), testHash(7))
	require.True(t, errors.Is(err, protocol.ErrTxUnknown))
}

// TestRecentTxIndex_ZeroCapacityIsInert is the compact-blocks-off state:
// the index exists, costs nothing, and matches nothing.
func TestRecentTxIndex_ZeroCapacityIsInert(t *testing.T) {
	idx := NewRecentTxIndex(0, noFetch)

	idx.Add(testHash(1))

	require.Zero(t, idx.Len())

	matched, collision := idx.Match(1, 2, []uint64{protocol.ShortID(1, 2, testHash(1))})
	require.False(t, collision)
	require.Nil(t, matched[0])
}

// TestRecentTxIndex_NilIndexIsSafe covers the feed sites: a bridge that was
// never built through New (a depless caller, or a test harness) hands the
// Kafka consumer a nil index, and an ADD entry must not panic on it.
func TestRecentTxIndex_NilIndexIsSafe(t *testing.T) {
	var idx *RecentTxIndex

	require.NotPanics(t, func() {
		idx.Add(testHash(1))
		require.Zero(t, idx.Len())

		matched, collision := idx.Match(1, 2, []uint64{1})
		require.False(t, collision)
		require.Nil(t, matched[0])
	})
}

// TestRecentTxIndex_ConcurrentAddAndMatch is the lock-discipline test: the
// txmeta consumer and the orphan pool write while a compact block reads.
// It runs under -race, which is what makes it worth anything. This is the
// one test in the file that is about concurrency, so it is the one that
// spawns goroutines.
func TestRecentTxIndex_ConcurrentAddAndMatch(t *testing.T) {
	idx := NewRecentTxIndex(64, noFetch)

	const rounds = 200

	var (
		wg      sync.WaitGroup
		badLens atomic.Int64
	)

	wg.Add(3)

	go func() {
		defer wg.Done()

		for n := 0; n < rounds; n++ {
			idx.Add(testHash(byte(n)))
		}
	}()

	go func() {
		defer wg.Done()

		for n := 0; n < rounds; n++ {
			idx.Add(testHash(byte(n % 7)))
		}
	}()

	go func() {
		defer wg.Done()

		for n := 0; n < rounds; n++ {
			k0 := uint64(n)

			ids := []uint64{
				protocol.ShortID(k0, 1, testHash(byte(n))),
				protocol.ShortID(k0, 1, testHash(byte(n+1))),
			}

			// Asserted through a counter rather than require: this
			// goroutine is not the test goroutine, so it must not call
			// FailNow.
			if matched, _ := idx.Match(k0, 1, ids); len(matched) != len(ids) {
				badLens.Add(1)
			}

			_ = idx.Len()
		}
	}()

	wg.Wait()

	require.Zero(t, badLens.Load(), "Match must return one entry per requested short ID under concurrent writes")
	require.Equal(t, 64, idx.Len())
}

// heldByIndex reports whether the index matches hash under freshly chosen
// keys — the only observation the feed-site tests need, and the one the
// compact-block path actually makes.
func heldByIndex(idx *RecentTxIndex, hash chainhash.Hash) bool {
	const k0, k1 = 0x1234, 0x5678

	matched, _ := idx.Match(k0, k1, []uint64{protocol.ShortID(k0, k1, hash)})

	return matched[0] != nil && *matched[0] == hash
}

// TestHandleTxMetaMessage_FeedsTheRecentTxIndex is the txmeta feed site: the
// entries that reach onTx are exactly the entries the index keeps, so the
// index holds what a mempool would hold — no coinbase, nothing that arrived
// in a block, nothing deleted.
func TestHandleTxMetaMessage_FeedsTheRecentTxIndex(t *testing.T) {
	plainHash := mustHash(t, "1111111111111111111111111111111111111111111111111111111111111111")
	coinbaseHash := mustHash(t, "2222222222222222222222222222222222222222222222222222222222222222")
	inBlockHash := mustHash(t, "3333333333333333333333333333333333333333333333333333333333333333")
	deletedHash := mustHash(t, "4444444444444444444444444444444444444444444444444444444444444444")

	data := buildTXmetaBatchMessage(t, []txmetaTestEntry{
		{hash: plainHash, action: txmetacache.WireActionADD, meta: meta.Data{Fee: 500, SizeInBytes: 250}},
		{hash: coinbaseHash, action: txmetacache.WireActionADD, meta: meta.Data{Fee: 0, SizeInBytes: 100, IsCoinbase: true}},
		{hash: inBlockHash, action: txmetacache.WireActionADD, meta: meta.Data{Fee: 10, SizeInBytes: 300, InBlock: true}},
		{hash: deletedHash, action: txmetacache.WireActionDELETE},
	})

	idx := NewRecentTxIndex(16, noFetch)

	var relayed []chainhash.Hash

	err := handleTxMetaMessage(ulogger.TestLogger{}, &kafka.KafkaMessage{Value: data}, idx, func(hash chainhash.Hash, _, _ uint64) {
		relayed = append(relayed, hash)
	})
	require.NoError(t, err)

	require.Equal(t, []chainhash.Hash{plainHash}, relayed)
	require.Equal(t, 1, idx.Len(), "only the relayable ADD entry belongs in the index")
	require.True(t, heldByIndex(idx, plainHash))
	require.False(t, heldByIndex(idx, coinbaseHash))
	require.False(t, heldByIndex(idx, inBlockHash))
	require.False(t, heldByIndex(idx, deletedHash))
}

// TestHandleTxMetaMessage_NilIndexStillRelays proves the feed is additive:
// a consumer started with no index (compact blocks off) relays exactly as
// it did before.
func TestHandleTxMetaMessage_NilIndexStillRelays(t *testing.T) {
	plainHash := mustHash(t, "5555555555555555555555555555555555555555555555555555555555555555")

	data := buildTXmetaBatchMessage(t, []txmetaTestEntry{
		{hash: plainHash, action: txmetacache.WireActionADD, meta: meta.Data{Fee: 1, SizeInBytes: 2}},
	})

	var relayed []chainhash.Hash

	err := handleTxMetaMessage(ulogger.TestLogger{}, &kafka.KafkaMessage{Value: data}, nil, func(hash chainhash.Hash, _, _ uint64) {
		relayed = append(relayed, hash)
	})
	require.NoError(t, err)
	require.Equal(t, []chainhash.Hash{plainHash}, relayed)
}

// TestOrphanPool_AddDoesNotFeedTheRecentTxIndex replaces the Task 5 test that
// asserted the opposite. That test fixed a feed the final review found harmful
// (F2): the orphan pool named hashes whose bytes RecentTxIndex.Open cannot
// serve, because an orphan never reaches the UTXO store. See
// TestIngestTx_OrphanIsNotAddedToTheRecentTxIndex (orphans_test.go) for the
// behavioural half of this, driven through the real ingest path.
//
// What survives from the old test is the part that was always right: the pool
// takes an orphan once, and a duplicate changes nothing.
func TestOrphanPool_AddDoesNotFeedTheRecentTxIndex(t *testing.T) {
	tSettings := newOrphanTestSettings(time.Hour, 100)

	validate, _ := newCountingValidateFunc(func(chainhash.Hash, int) (*meta.Data, error) {
		return nil, errors.ErrTxMissingParent
	})

	pool := newOrphanPool(tSettings, ulogger.TestLogger{}, validate)
	t.Cleanup(pool.stop)

	orphan := makeIngestTestTx(t, "recent-index-orphan")

	pool.add(orphan)
	pool.add(orphan) // duplicate: the pool does not take it twice

	require.Equal(t, 1, pool.m.Len())

	// The pool holds no index reference at all now, which is what makes the
	// feed impossible rather than merely absent.
	idx := NewRecentTxIndex(16, noFetch)
	require.Equal(t, 0, idx.Len())
	require.False(t, heldByIndex(idx, *orphan.TxIDChainHash()))
}

// TestNew_BuildsTheIndexFromTheCompactBlockSettings fixes the wiring the
// service depends on: the flag decides whether the bridge holds a live
// index at all, and the capacity setting sizes it.
func TestNew_BuildsTheIndexFromTheCompactBlockSettings(t *testing.T) {
	for _, tc := range []struct {
		name     string
		enabled  bool
		capacity int
		want     int
	}{
		{"compact blocks on", true, 4, 4},
		{"compact blocks off", false, 4, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tSettings := &settings.Settings{}
			tSettings.Legacy.OrphanEvictionDuration = time.Hour
			tSettings.Legacy.MaxOrphanTxs = 10
			tSettings.Legacy.CompactBlocks = tc.enabled
			tSettings.Legacy.CompactBlocksRecentTxs = tc.capacity

			sm := New(ulogger.TestLogger{}, tSettings, nil, nil, nil, nil, nil, nil, nil, nil)
			t.Cleanup(sm.Stop)

			idx := sm.RecentTxIndex()
			require.NotNil(t, idx)
			require.Same(t, idx, sm.TxIndex(), "both accessors must hand out the one index")

			for n := byte(1); n <= 6; n++ {
				idx.Add(testHash(n))
			}

			require.Equal(t, tc.want, idx.Len())
		})
	}
}

// TestNew_IndexOpensThroughFetchTx proves the fetch seam Open reads is the
// bridge's own FetchTx, not a second path into the store: an unheld
// transaction comes back as ErrTxUnknown through the real wiring.
func TestNew_IndexOpensThroughFetchTx(t *testing.T) {
	tSettings := settings.NewSettings()
	tSettings.Legacy.OrphanEvictionDuration = time.Hour
	tSettings.Legacy.MaxOrphanTxs = 10
	tSettings.Legacy.CompactBlocks = true
	tSettings.Legacy.CompactBlocksRecentTxs = 8

	storeURL, err := url.Parse("sqlitememory:///" + t.Name())
	require.NoError(t, err)
	tSettings.UtxoStore.UtxoStore = storeURL

	utxoStore, err := utxosql.New(context.Background(), ulogger.TestLogger{}, tSettings, storeURL)
	require.NoError(t, err)
	t.Cleanup(func() { _ = utxoStore.Close(context.Background()) })

	sm := New(ulogger.TestLogger{}, tSettings, nil, nil, nil, nil, utxoStore, nil, nil, nil)
	t.Cleanup(sm.Stop)

	_, _, err = sm.RecentTxIndex().Open(context.Background(), testHash(1))
	require.True(t, errors.Is(err, protocol.ErrTxUnknown), "expected ErrTxUnknown, got %v", err)
}

// TestRecentTxIndex_FootprintAtDefaultCapacity is the measurement behind the
// footprint figure in RecentTxIndex's own doc comment and in
// legacy_compactBlocksRecentTxs's settings documentation: fill the ring at
// the shipped default and report what it actually costs.
//
// It is opt-in, and stays out of CI and `make test`, because it holds half a
// gigabyte for a few seconds and its numbers are machine-dependent. Run it
// with:
//
//	SVP2P_MEASURE_INDEX=1 go test ./services/svp2p/bridge/ \
//	    -run TestRecentTxIndex_FootprintAtDefaultCapacity -v -count=1
//
// The assertions are deliberately loose bounds, not the measured numbers: a
// tightened one would fail on an unrelated allocator change, while these
// still catch a per-entry cost that has moved by a factor.
func TestRecentTxIndex_FootprintAtDefaultCapacity(t *testing.T) {
	if testing.Short() {
		t.Skip("footprint measurement holds ~500 MiB; skipped under -short")
	}

	if os.Getenv("SVP2P_MEASURE_INDEX") == "" {
		t.Skip("set SVP2P_MEASURE_INDEX=1 to run the recent-tx index footprint measurement")
	}

	// The shipped legacy_compactBlocksRecentTxs default. Written out rather
	// than read from settings.NewSettings(), so a settings_local.conf
	// override cannot silently change what this measures.
	const defaultCapacity = 5_000_000

	idx := NewRecentTxIndex(defaultCapacity, noFetch)

	runtime.GC()

	var before, after runtime.MemStats

	runtime.ReadMemStats(&before)

	// Random hashes, because the dedup map's cost depends on its keys being
	// distinct and well spread — which is what real transaction IDs are.
	const batch = 10_000

	buf := make([]byte, chainhash.HashSize*batch)

	for n := 0; n < defaultCapacity/batch; n++ {
		_, err := rand.Read(buf)
		require.NoError(t, err)

		for k := 0; k < batch; k++ {
			var hash chainhash.Hash

			copy(hash[:], buf[k*chainhash.HashSize:(k+1)*chainhash.HashSize])
			idx.Add(hash)
		}
	}

	runtime.GC()
	runtime.ReadMemStats(&after)

	require.Equal(t, defaultCapacity, idx.Len())

	heapDelta := int64(after.HeapInuse) - int64(before.HeapInuse)

	t.Logf("recent-tx index at %d entries: HeapInuse delta %.1f MiB (%.1f bytes per hash)",
		idx.Len(), float64(heapDelta)/(1<<20), float64(heapDelta)/float64(defaultCapacity))

	// One compact block's worth of short IDs, none of which the index holds:
	// the whole ring is walked, which is the worst case and the one the
	// 5,000,000-SipHashes cost estimate is about.
	ids := make([]uint64, 3000)
	for n := range ids {
		ids[n] = uint64(n)
	}

	start := time.Now()
	matched, collision := idx.Match(1, 2, ids)
	elapsed := time.Since(start)

	t.Logf("Match over %d entries took %s", idx.Len(), elapsed)

	require.Len(t, matched, len(ids))
	require.False(t, collision)

	require.Greater(t, heapDelta, int64(300)<<20, "the index costs far less than expected — check the ring and the dedup map are both being filled")
	require.Less(t, heapDelta, int64(1)<<30, "the index costs more than 1 GiB at the default capacity, which the settings documentation does not warn about")
	require.Less(t, elapsed, 2*time.Second, "a full-ring Match must stay far below the per-block download timeout")
}
