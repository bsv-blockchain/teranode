package bridge

import (
	"context"
	"encoding/binary"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	txmap "github.com/bsv-blockchain/go-tx-map"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/services/validator"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

// makeChildOrphanTx builds a real bt.Tx whose sole input spends output 0 of
// parent — the parent-hash relationship the release walk (G3) actually
// walks, unlike makeIngestTestTx's arbitrary seed-derived previous txid
// (ingest_tx_test.go), which is enough for the pool-mechanics tests below
// but not for a real dependency chain.
func makeChildOrphanTx(t *testing.T, parent chainhash.Hash, seed string) *bt.Tx {
	t.Helper()

	tx := bt.NewTx()
	tx.Version = 1

	require.NoError(t, tx.From(parent.String(), 0, "76a914", uint64(1_000_000)))
	require.NoError(t, tx.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", uint64(900_000)))
	// Nudge the hash so same-parent siblings in the same test don't collide.
	tx.LockTime = uint32(len(seed)) //nolint:gosec // test-only uniqueness nudge

	return tx
}

// makeOrphanTxWithParents builds a real bt.Tx with one input per entry in
// parents (so it can have more than one resident parent, unlike
// makeChildOrphanTx's single input), nudged uniquely via a hash of seed
// rather than seed's length — makeChildOrphanTx's length-based nudge
// collides when two same-parent siblings in the same test happen to share a
// seed length, which the multi-parent test below cannot risk.
func makeOrphanTxWithParents(t *testing.T, seed string, parents ...chainhash.Hash) *bt.Tx {
	t.Helper()

	tx := bt.NewTx()
	tx.Version = 1

	for _, parent := range parents {
		require.NoError(t, tx.From(parent.String(), 0, "76a914", uint64(1_000_000)))
	}

	require.NoError(t, tx.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", uint64(900_000)))

	nudge := chainhash.HashH([]byte(seed))
	tx.LockTime = binary.BigEndian.Uint32(nudge[:4]) //nolint:gosec // test-only uniqueness nudge, collision-safe via seed hash

	return tx
}

// newOrphanTestSettings builds the minimal *settings.Settings the pool
// reads from: the two legacy_* keys G1 says are already wired
// (settings/settings.go:662-663).
func newOrphanTestSettings(evictionDuration time.Duration, maxOrphanTxs int) *settings.Settings {
	tSettings := &settings.Settings{}
	tSettings.Legacy.OrphanEvictionDuration = evictionDuration
	tSettings.Legacy.MaxOrphanTxs = maxOrphanTxs

	return tSettings
}

// newCountingValidateFunc returns an orphanValidateFunc plus a thread-safe
// per-hash call counter, for tests that need to prove exactly how many
// times (and in what outcome) a given tx was (re)validated.
func newCountingValidateFunc(respond func(hash chainhash.Hash, callNumber int) (*meta.Data, error)) (orphanValidateFunc, func(chainhash.Hash) int) {
	var mu sync.Mutex

	calls := map[chainhash.Hash]int{}

	fn := func(_ context.Context, tx *bt.Tx) (*meta.Data, error) {
		hash := *tx.TxIDChainHash()

		mu.Lock()
		n := calls[hash]
		calls[hash] = n + 1
		mu.Unlock()

		return respond(hash, n)
	}

	count := func(hash chainhash.Hash) int {
		mu.Lock()
		defer mu.Unlock()

		return calls[hash]
	}

	return fn, count
}

// newOrphanTestBridge builds the minimal svp2pBridge IngestTx's orphan
// wiring actually touches, mirroring newIngestTxTestBridge
// (ingest_tx_test.go) but with an orphanPool constructed the same way New()
// (bridge.go) constructs one in production. Registers t.Cleanup(sm.Stop) so
// the pool's two background goroutines (TTL ticker, eviction worker) never
// outlive the test — fix round 1, Issue I4.
func newOrphanTestBridge(t *testing.T, v validator.Interface, tSettings *settings.Settings) *svp2pBridge {
	t.Helper()

	sm := &svp2pBridge{
		logger:           ulogger.TestLogger{},
		validationClient: v,
		rejectedTxns:     txmap.NewSyncedMap[chainhash.Hash, struct{}](maxRejectedTxns),
	}

	// The recent-transaction index is wired exactly as New() wires it
	// (bridge.go), so a test can observe what the ingest path does and does
	// not put in it.
	sm.recentTx = NewRecentTxIndex(16, sm.openTx)

	sm.orphanPool = newOrphanPool(tSettings, sm.logger, func(ctx context.Context, tx *bt.Tx) (*meta.Data, error) {
		return sm.validationClient.Validate(ctx, tx, 0)
	})

	t.Cleanup(sm.Stop)

	return sm
}

// TestOrphanPool_DuplicateOrphanNotReAdded is G6's shape for the
// duplicate-orphan case: re-adding the same hash must leave the existing
// entry untouched (proved by pointer identity, not merely "still present"),
// paired with a distinct orphan added alongside so the pool's positive
// state is checked, not just an absence.
func TestOrphanPool_DuplicateOrphanNotReAdded(t *testing.T) {
	tSettings := newOrphanTestSettings(time.Hour, 100)

	validate, _ := newCountingValidateFunc(func(chainhash.Hash, int) (*meta.Data, error) {
		return nil, errors.ErrTxMissingParent
	})

	pool := newOrphanPool(tSettings, ulogger.TestLogger{}, validate)
	t.Cleanup(pool.stop)

	original := makeIngestTestTx(t, "dup-orphan-original")
	distinct := makeIngestTestTx(t, "dup-orphan-distinct")

	pool.add(original)

	entryBefore, ok := pool.m.Get(*original.TxIDChainHash())
	require.True(t, ok)

	pool.add(original) // duplicate: must not replace the existing entry

	entryAfter, ok := pool.m.Get(*original.TxIDChainHash())
	require.True(t, ok)
	require.Same(t, entryBefore, entryAfter, "a duplicate add must not replace the existing entry")

	pool.add(distinct)

	require.Equal(t, 2, pool.m.Len(), "the pool must hold exactly the original plus the distinct orphan")

	_, originalPresent := pool.m.Get(*original.TxIDChainHash())
	require.True(t, originalPresent)

	_, distinctPresent := pool.m.Get(*distinct.TxIDChainHash())
	require.True(t, distinctPresent)
}

// TestOrphanPool_CapEvictionGivesFinalAttemptAndKeepsSurvivors covers both
// "eviction at cap" (the plan's own Step 1) and G2's cap-eviction final
// validation attempt in one test, and asserts the survivor set rather than
// only the evicted entry's absence (G6).
func TestOrphanPool_CapEvictionGivesFinalAttemptAndKeepsSurvivors(t *testing.T) {
	tSettings := newOrphanTestSettings(time.Hour, 2)

	validate, callCount := newCountingValidateFunc(func(chainhash.Hash, int) (*meta.Data, error) {
		return nil, errors.ErrTxMissingParent
	})

	pool := newOrphanPool(tSettings, ulogger.TestLogger{}, validate)
	t.Cleanup(pool.stop)

	a := makeIngestTestTx(t, "cap-a")
	b := makeIngestTestTx(t, "cap-b")
	c := makeIngestTestTx(t, "cap-c")

	// addedAt ordering (what evictOldestLocked picks the oldest by) is
	// established inside Set, called from add — so the sleeps belong
	// between these calls, not between the tx-creation calls above (fix
	// round 1, Minor M6: the previous placement didn't do what its comment
	// implied, even though nanosecond timestamp resolution happened to make
	// the test pass anyway).
	pool.add(a)
	time.Sleep(2 * time.Millisecond)
	pool.add(b)
	time.Sleep(2 * time.Millisecond)
	pool.add(c) // over cap: evicts a, the oldest by insertion time

	require.Equal(t, 2, pool.m.Len())

	_, aPresent := pool.m.Get(*a.TxIDChainHash())
	require.False(t, aPresent)

	_, bPresent := pool.m.Get(*b.TxIDChainHash())
	require.True(t, bPresent, "the survivor set must still hold b")

	_, cPresent := pool.m.Get(*c.TxIDChainHash())
	require.True(t, cPresent, "the survivor set must still hold c, the entry whose insert triggered the eviction")

	// Fix round 1, Issue I1: the final validation attempt now runs
	// asynchronously on the pool's own eviction worker, off expiringmap's
	// lock, so it is no longer guaranteed to have happened by the time add
	// returns. Wait on the attempt itself rather than asserting it
	// synchronously.
	require.Eventually(t, func() bool {
		return callCount(*a.TxIDChainHash()) >= 1
	}, 2*time.Second, 5*time.Millisecond, "the evicted orphan must get its final validation attempt")

	require.Equal(t, 1, callCount(*a.TxIDChainHash()), "the evicted orphan must get exactly one final validation attempt")
	require.Equal(t, 0, callCount(*b.TxIDChainHash()))
	require.Equal(t, 0, callCount(*c.TxIDChainHash()))
}

// TestOrphanPool_TTLEvictionGivesFinalAttemptAndStaysUsable is the TTL
// counterpart: it proves the sweep gives the stale orphan its final
// attempt (mirroring the cap path, G2), and then proves the pool is still
// usable afterwards by adding a fresh orphan post-sweep (G6 — "everything
// expired" must not pass this test too).
func TestOrphanPool_TTLEvictionGivesFinalAttemptAndStaysUsable(t *testing.T) {
	tSettings := newOrphanTestSettings(20*time.Millisecond, 100)

	validate, callCount := newCountingValidateFunc(func(chainhash.Hash, int) (*meta.Data, error) {
		return nil, errors.ErrTxMissingParent
	})

	pool := newOrphanPool(tSettings, ulogger.TestLogger{}, validate)
	t.Cleanup(pool.stop)

	stale := makeIngestTestTx(t, "ttl-stale")
	staleHash := *stale.TxIDChainHash()
	pool.add(stale)

	// Wait for the eviction attempt itself, not for Get to report the entry
	// gone: ExpiringMap.Get treats an entry past its expiry as absent from
	// the moment wall-clock time passes it (a lazy check with no side
	// effect), which can go true well before the background ticker's sweep
	// actually runs onEvict. Waiting on the attempt counter pins the sweep
	// having actually fired.
	require.Eventually(t, func() bool {
		return callCount(staleHash) >= 1
	}, 2*time.Second, 5*time.Millisecond, "the TTL sweep must evict the stale orphan and give it a final attempt")

	require.Equal(t, 1, callCount(staleHash), "TTL eviction must give the orphan exactly one final validation attempt")

	_, stalePresent := pool.m.Get(staleHash)
	require.False(t, stalePresent, "the swept entry must actually be gone from the pool")

	fresh := makeIngestTestTx(t, "ttl-fresh")
	pool.add(fresh)

	_, freshPresent := pool.m.Get(*fresh.TxIDChainHash())
	require.True(t, freshPresent, "the pool must still accept new orphans after a TTL sweep")
}

// TestOrphanPool_ZeroMaxOrphanTxsDisablesCap is G2's second omitted
// behaviour: legacy_maxOrphanTxs = 0 restores unbounded behaviour, it is
// not a synonym for "no orphans" or an error.
func TestOrphanPool_ZeroMaxOrphanTxsDisablesCap(t *testing.T) {
	tSettings := newOrphanTestSettings(time.Hour, 0)

	validate, _ := newCountingValidateFunc(func(chainhash.Hash, int) (*meta.Data, error) {
		return nil, errors.ErrTxMissingParent
	})

	pool := newOrphanPool(tSettings, ulogger.TestLogger{}, validate)
	t.Cleanup(pool.stop)

	const n = 500
	for i := 0; i < n; i++ {
		pool.add(makeIngestTestTx(t, fmt.Sprintf("unbounded-%d", i)))
	}

	require.Equal(t, n, pool.m.Len(), "legacy_maxOrphanTxs=0 must restore unbounded behaviour, not evict at an implicit cap")
}

// TestIngestTx_AcceptedTxReleasesRecursiveChainOfThreeOrphans is the plan's
// own Step 1 case and G3's central scenario: three orphans arrive forming
// a dependency chain (root -> tx1 -> tx2 -> tx3), each classified orphan on
// first arrival, then the root arrives, is accepted, and the release walk
// must promote all three in one IngestTx call and report them for the
// announce seam.
func TestIngestTx_AcceptedTxReleasesRecursiveChainOfThreeOrphans(t *testing.T) {
	tSettings := newOrphanTestSettings(time.Hour, 100)

	root := makeIngestTestTx(t, "chain-root")
	rootHash := *root.TxIDChainHash()

	tx1 := makeChildOrphanTx(t, rootHash, "chain-1")
	tx1Hash := *tx1.TxIDChainHash()

	tx2 := makeChildOrphanTx(t, tx1Hash, "chain-2")
	tx2Hash := *tx2.TxIDChainHash()

	tx3 := makeChildOrphanTx(t, tx2Hash, "chain-3")
	tx3Hash := *tx3.TxIDChainHash()

	orphanHashes := map[chainhash.Hash]bool{tx1Hash: true, tx2Hash: true, tx3Hash: true}

	rv := newRecordingValidator(func(hash chainhash.Hash) (*meta.Data, error) {
		return nil, errors.ErrTxMissingParent // placeholder, overridden below
	})

	var mu sync.Mutex

	seen := map[chainhash.Hash]int{}
	rv.MockValidator.ValidateFunc = func(_ context.Context, tx *bt.Tx) (*meta.Data, error) {
		hash := *tx.TxIDChainHash()

		mu.Lock()
		rv.calls = append(rv.calls, hash)
		n := seen[hash]
		seen[hash] = n + 1
		mu.Unlock()

		if orphanHashes[hash] && n == 0 {
			return nil, errors.ErrTxMissingParent
		}

		return &meta.Data{Fee: 100, SizeInBytes: 200}, nil
	}

	sm := newOrphanTestBridge(t, rv.MockValidator, tSettings)

	for _, tx := range []*bt.Tx{tx1, tx2, tx3} {
		result, err := sm.IngestTx(t.Context(), tx.Bytes(), "peer1:8333")
		require.NoError(t, err)
		require.True(t, result.Orphan)
	}

	require.Equal(t, 3, sm.orphanPool.m.Len(), "all three orphans must be sitting in the pool before the root arrives")

	result, err := sm.IngestTx(t.Context(), root.Bytes(), "peer1:8333")
	require.NoError(t, err)
	require.True(t, result.Accepted)

	require.Len(t, result.ReleasedOrphans, 3, "the recursive chain of 3 must be released in full")

	released := map[chainhash.Hash]ReleasedOrphan{}
	for _, ro := range result.ReleasedOrphans {
		released[ro.TxHash] = ro
	}

	for _, hash := range []chainhash.Hash{tx1Hash, tx2Hash, tx3Hash} {
		ro, ok := released[hash]
		require.True(t, ok, "every orphan in the chain must be released")
		require.Equal(t, uint64(100), ro.Fee)
		require.Equal(t, uint64(200), ro.Size)
	}

	require.Equal(t, 0, sm.orphanPool.m.Len(), "every released orphan must be removed from the pool")
}

// TestOrphanPool_ReleaseValidatesMultiParentOrphanExactlyOnce is fix round
// 1, Issue I2: an orphan with two resident parents, both promoted in the
// same walk, must be validated and released exactly once — legacy's own
// behaviour, because legacy deletes a released orphan immediately (at the
// head of the very next recursive call into it), so by the time the second
// parent's scan runs the orphan is already gone. Before the fix (deferred
// deletion until the entry's own queue pop), X stayed visible to Items()
// for whichever parent was processed second, and was validated, appended
// and queued once per resident parent.
func TestOrphanPool_ReleaseValidatesMultiParentOrphanExactlyOnce(t *testing.T) {
	tSettings := newOrphanTestSettings(time.Hour, 100)

	root := makeIngestTestTx(t, "multi-parent-root")
	rootHash := *root.TxIDChainHash()

	a := makeOrphanTxWithParents(t, "multi-parent-a", rootHash)
	aHash := *a.TxIDChainHash()

	b := makeOrphanTxWithParents(t, "multi-parent-b", rootHash)
	bHash := *b.TxIDChainHash()

	// x depends on BOTH a and b — the shape that triggers the divergence:
	// root's release promotes a and b in the same walk, and x is a's and
	// b's common child.
	x := makeOrphanTxWithParents(t, "multi-parent-x", aHash, bHash)
	xHash := *x.TxIDChainHash()

	validate, callCount := newCountingValidateFunc(func(chainhash.Hash, int) (*meta.Data, error) {
		return &meta.Data{Fee: 1, SizeInBytes: 1}, nil
	})

	pool := newOrphanPool(tSettings, ulogger.TestLogger{}, validate)
	t.Cleanup(pool.stop)

	pool.add(a)
	pool.add(b)
	pool.add(x)

	released := pool.release(t.Context(), rootHash)

	require.Equal(t, 1, callCount(xHash), "x must be validated exactly once even though both its parents were promoted in the same walk")

	xOccurrences := 0
	aReleased, bReleased := false, false

	for _, ro := range released {
		switch ro.TxHash {
		case xHash:
			xOccurrences++
		case aHash:
			aReleased = true
		case bHash:
			bReleased = true
		}
	}

	require.Equal(t, 1, xOccurrences, "x must appear exactly once in released, not once per resident parent")
	// G6 shape: the fix must not just suppress x's duplicate — a and b
	// (and x itself, once) must still have actually released.
	require.True(t, aReleased, "a must still be released")
	require.True(t, bReleased, "b must still be released")
	require.Len(t, released, 3, "exactly a, b and x — no more, no fewer")

	require.Equal(t, 0, pool.m.Len(), "every released orphan, including the shared child, must be removed from the pool")
}

// TestOrphanPool_ReleaseErrorLadder is fix round 1, Issue I3: the three
// release-time error branches (missing-parent/locked, ErrTxConflicting,
// any other rejection) had zero coverage — every existing chain test's
// release-time validate always succeeds. Each subtest below pins one
// branch in G6 shape: the branch's own effect on the triggering orphan,
// paired with a sibling that still released, so a test that merely never
// reached the branch cannot pass it.
func TestOrphanPool_ReleaseErrorLadder(t *testing.T) {
	t.Run("missing parent or locked leaves the orphan in the pool and the walk continues", func(t *testing.T) {
		tSettings := newOrphanTestSettings(time.Hour, 100)

		root := makeIngestTestTx(t, "ladder-waiting-root")
		rootHash := *root.TxIDChainHash()

		stillWaiting := makeChildOrphanTx(t, rootHash, "ladder-still-waiting")
		stillWaitingHash := *stillWaiting.TxIDChainHash()

		sibling := makeChildOrphanTx(t, rootHash, "ladder-waiting-sibling")
		siblingHash := *sibling.TxIDChainHash()

		validate := func(_ context.Context, tx *bt.Tx) (*meta.Data, error) {
			if *tx.TxIDChainHash() == stillWaitingHash {
				return nil, errors.ErrTxMissingParent
			}

			return &meta.Data{Fee: 1, SizeInBytes: 1}, nil
		}

		pool := newOrphanPool(tSettings, ulogger.TestLogger{}, validate)
		t.Cleanup(pool.stop)

		pool.add(stillWaiting)
		pool.add(sibling)

		released := pool.release(t.Context(), rootHash)

		// Positive: the still-waiting orphan is actually still in the pool
		// (not merely absent from released), and the sibling did release —
		// proving the walk continued past the still-waiting branch rather
		// than stopping or dropping the entry.
		_, stillPresent := pool.m.Get(stillWaitingHash)
		require.True(t, stillPresent, "an orphan still missing a parent must remain in the pool")

		require.Len(t, released, 1)
		require.Equal(t, siblingHash, released[0].TxHash, "the sibling must still release despite the still-waiting orphan")
	})

	t.Run("conflicting removes the orphan and the walk continues", func(t *testing.T) {
		tSettings := newOrphanTestSettings(time.Hour, 100)

		root := makeIngestTestTx(t, "ladder-conflict-root")
		rootHash := *root.TxIDChainHash()

		conflicting := makeChildOrphanTx(t, rootHash, "ladder-conflicting")
		conflictingHash := *conflicting.TxIDChainHash()

		sibling := makeChildOrphanTx(t, rootHash, "ladder-conflict-sibling")
		siblingHash := *sibling.TxIDChainHash()

		validate := func(_ context.Context, tx *bt.Tx) (*meta.Data, error) {
			if *tx.TxIDChainHash() == conflictingHash {
				return nil, errors.ErrTxConflicting
			}

			return &meta.Data{Fee: 1, SizeInBytes: 1}, nil
		}

		pool := newOrphanPool(tSettings, ulogger.TestLogger{}, validate)
		t.Cleanup(pool.stop)

		pool.add(conflicting)
		pool.add(sibling)

		released := pool.release(t.Context(), rootHash)

		// This is the named fidelity fix (orphans.go's own ErrTxConflicting
		// comment): the conflicting orphan must actually be gone, not left
		// for its own TTL/cap eviction the way legacy's equivalent branch
		// (deleting the wrong hash) leaves it.
		_, conflictingPresent := pool.m.Get(conflictingHash)
		require.False(t, conflictingPresent, "a conflicting (double-spend) orphan must be removed from the pool")

		require.Len(t, released, 1)
		require.Equal(t, siblingHash, released[0].TxHash, "the sibling must still release despite the conflicting orphan")
	})

	t.Run("any other rejection leaves the orphan in the pool and the walk continues", func(t *testing.T) {
		tSettings := newOrphanTestSettings(time.Hour, 100)

		root := makeIngestTestTx(t, "ladder-invalid-root")
		rootHash := *root.TxIDChainHash()

		invalid := makeChildOrphanTx(t, rootHash, "ladder-invalid")
		invalidHash := *invalid.TxIDChainHash()

		sibling := makeChildOrphanTx(t, rootHash, "ladder-invalid-sibling")
		siblingHash := *sibling.TxIDChainHash()

		validate := func(_ context.Context, tx *bt.Tx) (*meta.Data, error) {
			if *tx.TxIDChainHash() == invalidHash {
				return nil, errors.ErrTxInvalid
			}

			return &meta.Data{Fee: 1, SizeInBytes: 1}, nil
		}

		pool := newOrphanPool(tSettings, ulogger.TestLogger{}, validate)
		t.Cleanup(pool.stop)

		pool.add(invalid)
		pool.add(sibling)

		released := pool.release(t.Context(), rootHash)

		// Matches legacy: a rejection that is neither missing-parent/locked
		// nor conflicting leaves the orphan in the pool (until its own
		// TTL/cap eviction), it does not drop it outright.
		_, invalidPresent := pool.m.Get(invalidHash)
		require.True(t, invalidPresent, "any other rejection must leave the orphan in the pool rather than dropping it")

		require.Len(t, released, 1)
		require.Equal(t, siblingHash, released[0].TxHash, "the sibling must still release despite the other orphan's rejection")
	})
}

// TestOrphanPool_ReleaseHandlesLongChainWithUnboundedCap is G3's scale
// argument made concrete: an iterative worklist walks a long dependency
// chain without recursing, so a peer that builds a very long orphan chain
// under legacy_maxOrphanTxs=0 (G2's disabled cap, the one configuration
// where the pool itself imposes no bound) cannot turn the release walk
// into unbounded call-stack growth. This does not prove Go's own recursion
// would have crashed at this length (goroutine stacks grow dynamically) —
// it proves the chosen design has no stack-depth term in it at all,
// independent of chain length.
func TestOrphanPool_ReleaseHandlesLongChainWithUnboundedCap(t *testing.T) {
	tSettings := newOrphanTestSettings(time.Hour, 0)

	const chainLength = 2000

	root := makeIngestTestTx(t, "long-chain-root")
	rootHash := *root.TxIDChainHash()

	txs := make([]*bt.Tx, 0, chainLength)
	prev := rootHash

	for i := 0; i < chainLength; i++ {
		tx := makeChildOrphanTx(t, prev, fmt.Sprintf("long-chain-%d", i))
		txs = append(txs, tx)
		prev = *tx.TxIDChainHash()
	}

	// Unlike the chain-of-3 wiring test, these orphans are inserted
	// directly via add rather than through IngestTx, so there is no prior
	// "classified orphan" validate call to account for here: every
	// release-time validate call is the tx's first, and it succeeds
	// unconditionally. This test is about release's own iteration
	// mechanics at scale, not re-proving the classification path the other
	// test already covers.
	validate := func(_ context.Context, _ *bt.Tx) (*meta.Data, error) {
		return &meta.Data{Fee: 1, SizeInBytes: 1}, nil
	}

	pool := newOrphanPool(tSettings, ulogger.TestLogger{}, validate)
	t.Cleanup(pool.stop)

	for _, tx := range txs {
		pool.add(tx)
	}

	require.Equal(t, chainLength, pool.m.Len())

	released := pool.release(t.Context(), rootHash)

	require.Len(t, released, chainLength, "the entire chain must release without a stack-depth failure")
	require.Equal(t, 0, pool.m.Len())
}

// TestIngestTx_OrphanIsNotAddedToTheRecentTxIndex is F2.
//
// An orphan is a transaction this node could NOT validate. It lives in the
// orphan pool's memory and never reaches the UTXO store, so RecentTxIndex.Open
// — which reads the store through the bridge's own fetch seam — cannot serve
// its bytes.
//
// Indexing it is therefore strictly worse than leaving it out. A hash the index
// names is matched during reconstruction and the slot is marked held, so it is
// NOT requested in the getblocktxn. The exchange completes, the ingest starts,
// and the assembly then fails at that slot with READ_STATUS_FAILED, which
// releases the block and fetches the whole thing by getdata. Left out, the same
// slot would simply have been a gap and the reconstruction would have
// succeeded — one round trip instead of a wasted one plus a full block.
//
// SVNode's vExtraTxnForCompact keeps the transaction BYTES
// (blockencodings.cpp:194-215), so its equivalent buffer can serve what it
// names. This port copied the feed but not the bytes; carrying them is a
// possible follow-up, and until then the honest thing is not to name what we
// cannot serve.
func TestIngestTx_OrphanIsNotAddedToTheRecentTxIndex(t *testing.T) {
	tSettings := newOrphanTestSettings(time.Hour, 100)

	orphan := makeIngestTestTx(t, "orphan-must-not-be-indexed")

	rv := newRecordingValidator(func(chainhash.Hash) (*meta.Data, error) {
		return nil, errors.ErrTxMissingParent
	})

	sm := newOrphanTestBridge(t, rv.MockValidator, tSettings)

	result, err := sm.IngestTx(t.Context(), orphan.Bytes(), "peer1:8333")
	require.NoError(t, err)
	require.True(t, result.Orphan, "the transaction must have been pooled as an orphan")

	require.Equal(t, 1, sm.orphanPool.m.Len(), "the orphan must be in the pool")

	require.Equal(t, 0, sm.recentTx.Len(),
		"an orphan's bytes are not in the store, so its hash must not be in the index")

	// The index is live, not merely empty: the txmeta consumer's own feed —
	// the one whose transactions ARE in the store — still reaches it.
	stored := makeIngestTestTx(t, "stored-and-indexed")
	sm.recentTx.Add(*stored.TxIDChainHash())

	require.Equal(t, 1, sm.recentTx.Len(), "the store-backed feed must still fill the index")
}
