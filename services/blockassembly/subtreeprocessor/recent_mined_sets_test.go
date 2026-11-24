package subtreeprocessor

import (
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/stretchr/testify/require"
)

func frozenHashSet(t *testing.T, hashes ...chainhash.Hash) *SortedHashSet {
	t.Helper()

	set := NewSortedHashSet(len(hashes))
	for _, h := range hashes {
		require.NoError(t, set.Put(h))
	}

	require.NoError(t, set.Freeze(1))

	return set
}

func minedSetBlock(nonce uint32) *model.Block {
	return &model.Block{Header: &model.BlockHeader{Version: 1, Nonce: nonce, HashPrevBlock: &chainhash.Hash{}, HashMerkleRoot: &chainhash.Hash{}}}
}

func newMinedSetProcessor(t *testing.T, retention time.Duration) (*SubtreeProcessor, *time.Time) {
	t.Helper()

	stp, _, _, _ := newResetWALTestProcessor(t, false)
	stp.settings.BlockAssembly.MinedTxSetRetention = retention

	now := time.Date(2030, 1, 2, 3, 4, 5, 0, time.UTC)
	stp.clock = clockFunc(func() time.Time { return now })

	return stp, &now
}

// clockFunc is a clock a test moves by changing the time its function returns.
type clockFunc func() time.Time

func (c clockFunc) Now() time.Time { return c() }

func batchOf(hashes ...chainhash.Hash) *TxBatch {
	b := &TxBatch{}
	for i, h := range hashes {
		b.nodes = append(b.nodes, subtreepkg.Node{Hash: h, Fee: 1, SizeInBytes: 1})
		b.txInpoints = append(b.txInpoints, inpointsPtr())
		b.validatedHeights = append(b.validatedHeights, uint32(100+i)) //nolint:gosec // small test index
	}

	return b
}

func batchHashes(b *TxBatch) []chainhash.Hash {
	out := make([]chainhash.Hash, len(b.nodes))
	for i := range b.nodes {
		out[i] = b.nodes[i].Hash
	}

	return out
}

// A queued transaction a kept block set holds is dropped, and the rest keep their order and
// their own inpoints and validated heights.
func TestFilterRecentlyMined_DropsMinedAndKeepsOrder(t *testing.T) {
	stp, _ := newMinedSetProcessor(t, 10*time.Second)

	a, b, c, d, e := chainhash.HashH([]byte("a")), chainhash.HashH([]byte("b")), chainhash.HashH([]byte("c")), chainhash.HashH([]byte("d")), chainhash.HashH([]byte("e"))
	stp.keepMinedSet(minedSetBlock(1), frozenHashSet(t, b))
	stp.keepMinedSet(minedSetBlock(2), frozenHashSet(t, e))

	first, second := batchOf(a, b, c), batchOf(d, e)
	cInpoints := first.txInpoints[2]

	stp.filterRecentlyMined([]*TxBatch{first, second}, nil)

	require.Equal(t, []chainhash.Hash{a, c}, batchHashes(first))
	require.Equal(t, []uint32{100, 102}, first.validatedHeights, "each kept transaction keeps its own validated height")
	require.Same(t, cInpoints, first.txInpoints[1], "each kept transaction keeps its own inpoints")
	require.Equal(t, []chainhash.Hash{d}, batchHashes(second), "a set from any kept block filters")
	require.Equal(t, []uint32{100}, second.validatedHeights)
}

// The lookups run in parallel chunks across batch boundaries; every position must still map to
// its own batch.
func TestFilterRecentlyMined_ManyBatchesAcrossChunks(t *testing.T) {
	stp, _ := newMinedSetProcessor(t, 10*time.Second)

	minedHashes := make([]chainhash.Hash, 0, 300)

	batches := make([]*TxBatch, 0, 300)
	for i := range 300 {
		h1 := chainhash.HashH([]byte{byte(i), byte(i >> 8), 1})
		h2 := chainhash.HashH([]byte{byte(i), byte(i >> 8), 2})
		h3 := chainhash.HashH([]byte{byte(i), byte(i >> 8), 3})
		batches = append(batches, batchOf(h1, h2, h3))
		minedHashes = append(minedHashes, h2)
	}

	stp.keepMinedSet(minedSetBlock(1), frozenHashSet(t, minedHashes...))
	stp.filterRecentlyMined(batches, nil)

	for i, b := range batches {
		require.Equal(t, []chainhash.Hash{
			chainhash.HashH([]byte{byte(i), byte(i >> 8), 1}),
			chainhash.HashH([]byte{byte(i), byte(i >> 8), 3}),
		}, batchHashes(b), "batch %d", i)
	}
}

// A set is dropped once its retention has passed, which frees it, and a retention of 0 keeps
// nothing.
func TestKeepMinedSet_RetentionAndPrune(t *testing.T) {
	stp, now := newMinedSetProcessor(t, 10*time.Second)
	h := chainhash.HashH([]byte("mined"))

	stp.keepMinedSet(minedSetBlock(1), frozenHashSet(t, h))
	require.Equal(t, 1, stp.keptMinedSetCount())

	*now = now.Add(10*time.Second - time.Millisecond)
	stp.pruneMinedSets()
	require.Equal(t, 1, stp.keptMinedSetCount(), "kept until the retention has passed")

	*now = now.Add(time.Millisecond)
	stp.pruneMinedSets()
	require.Zero(t, stp.keptMinedSetCount(), "dropped once the retention has passed")

	b := batchOf(h)
	stp.filterRecentlyMined([]*TxBatch{b}, nil)
	require.Equal(t, []chainhash.Hash{h}, batchHashes(b), "a dropped set filters nothing")

	stp.settings.BlockAssembly.MinedTxSetRetention = 0
	stp.keepMinedSet(minedSetBlock(2), frozenHashSet(t, h))
	require.Zero(t, stp.keptMinedSetCount(), "a retention of 0 keeps nothing")

	stp.settings.BlockAssembly.MinedTxSetRetention = time.Second
	stp.keepMinedSet(minedSetBlock(3), nil)
	stp.keepMinedSet(minedSetBlock(4), frozenHashSet(t))
	require.Zero(t, stp.keptMinedSetCount(), "a block with no transaction set keeps nothing")
}

// Keeping the same block again replaces its set rather than holding two.
func TestKeepMinedSet_SameBlockReplaces(t *testing.T) {
	stp, _ := newMinedSetProcessor(t, 10*time.Second)
	first, second := chainhash.HashH([]byte("first")), chainhash.HashH([]byte("second"))

	stp.keepMinedSet(minedSetBlock(1), frozenHashSet(t, first))
	stp.keepMinedSet(minedSetBlock(1), frozenHashSet(t, second))
	require.Equal(t, 1, stp.keptMinedSetCount())

	b := batchOf(first, second)
	stp.filterRecentlyMined([]*TxBatch{b}, nil)
	require.Equal(t, []chainhash.Hash{first}, batchHashes(b))
}

// A hand-off is checked against the kept sets when it arrives: a transaction a kept set holds never
// reaches the queue, and a batch left empty is admitted without taking queue room. This is what
// keeps such a transaction out when the set is dropped before the next drain, as in a catch-up of
// three or more slow blocks.
func TestAddBatchBounded_DropsKeptSetTransactionsOnArrival(t *testing.T) {
	stp, _ := newMinedSetProcessor(t, 10*time.Second)
	a, b := chainhash.HashH([]byte("a")), chainhash.HashH([]byte("b"))
	stp.keepMinedSet(minedSetBlock(1), frozenHashSet(t, b))

	first := batchOf(a, b)
	require.Equal(t, Admitted, stp.AddBatchBounded(first.nodes, first.txInpoints, false, first.validatedHeights))
	require.Equal(t, int64(1), stp.queue.length(), "only the transaction no kept set holds is queued")

	onlyMined := batchOf(b)
	require.Equal(t, Admitted, stp.AddBatchBounded(onlyMined.nodes, onlyMined.txInpoints, false, onlyMined.validatedHeights))
	require.Equal(t, int64(1), stp.queue.length(), "a batch left empty is admitted and queues nothing")

	stp.AddBatch([]subtreepkg.Node{{Hash: b, Fee: 1, SizeInBytes: 1}}, []*subtreepkg.TxInpoints{inpointsPtr()})
	require.Equal(t, int64(1), stp.queue.length(), "AddBatch checks on arrival too")
}

// A drain skips its own block's set, which it checks itself, and still checks the other kept sets.
func TestFilterRecentlyMined_SkipsTheDrainingBlocksSet(t *testing.T) {
	stp, _ := newMinedSetProcessor(t, 10*time.Second)
	own, other := chainhash.HashH([]byte("own")), chainhash.HashH([]byte("other"))
	ownSet := frozenHashSet(t, own)
	stp.keepMinedSet(minedSetBlock(1), ownSet)
	stp.keepMinedSet(minedSetBlock(2), frozenHashSet(t, other))

	b := batchOf(own, other)
	stp.filterRecentlyMined([]*TxBatch{b}, ownSet)
	require.Equal(t, []chainhash.Hash{own}, batchHashes(b))
}

// With no kept set holding any of its transactions, the filter leaves a batch's slices as they were.
func TestFilterRecentlyMined_NothingMinedLeavesBatchUnchanged(t *testing.T) {
	stp, _ := newMinedSetProcessor(t, 10*time.Second)
	stp.keepMinedSet(minedSetBlock(1), frozenHashSet(t, chainhash.HashH([]byte("elsewhere"))))

	b := batchOf(chainhash.HashH([]byte("a")), chainhash.HashH([]byte("b")))
	nodes := b.nodes

	stp.filterRecentlyMined([]*TxBatch{b}, nil)
	require.Len(t, b.nodes, 2)
	require.Same(t, &nodes[0], &b.nodes[0])
}

// A moved-back block's set is forgotten, and a reset forgets every set.
func TestForgetAndClearMinedSets(t *testing.T) {
	stp, _ := newMinedSetProcessor(t, 10*time.Second)
	first, second := chainhash.HashH([]byte("first")), chainhash.HashH([]byte("second"))
	firstBlock, secondBlock := minedSetBlock(1), minedSetBlock(2)

	stp.keepMinedSet(firstBlock, frozenHashSet(t, first))
	stp.keepMinedSet(secondBlock, frozenHashSet(t, second))

	stp.forgetMinedSets([]*model.Block{firstBlock})

	b := batchOf(first, second)
	stp.filterRecentlyMined([]*TxBatch{b}, nil)
	require.Equal(t, []chainhash.Hash{first}, batchHashes(b), "a moved-back block's transactions are placed again")

	stp.clearMinedSets()
	require.Zero(t, stp.keptMinedSetCount())
}

// An ordinary block move: a transaction the block mined whose hand-off reaches the queue after
// the block's drain is dropped by the steady-state dequeue loop. Its batch is stamped before the
// block, as a batch stamped before its block's drain and linked after it is.
func TestMoveForwardBlock_LateHandOffOfMinedTxStaysOut(t *testing.T) {
	cases := map[string]struct {
		retention time.Duration
		reEnters  bool
	}{
		"kept_set_drops_it":          {retention: 10 * time.Second},
		"retention_off_lets_it_back": {retention: 0, reEnters: true},
	}

	for variant, opts := range catchUpTxMapVariants(t) {
		for name, tc := range cases {
			t.Run(variant+"/"+name, func(t *testing.T) {
				stp, store, subtreeStore, chain := newResetWALTestProcessor(t, false, opts...)
				stp.settings.BlockAssembly.MinedTxSetRetention = tc.retention

				minedTx, _ := createResetWALConflict(t, store, 31)
				mined := *minedTx.TxIDChainHash()
				unmined := chainhash.HashH([]byte("late-hand-off-not-mined"))

				chain.moveForward[0].Subtrees = []*chainhash.Hash{storeTestSubtree(t, subtreeStore, false, minedTx)}
				stp.queue.clock = fixedClock{t: time.Now().Add(-time.Second)}

				stp.Start(t.Context())

				require.NoError(t, stp.MoveForwardBlock(chain.moveForward[0]))
				require.Eventually(t, func() bool { return !stp.DrainingAfterBlock() }, 5*time.Second, time.Millisecond,
					"the block's own drain must end before the hand-off arrives")

				stp.AddBatch(
					[]subtreepkg.Node{{Hash: mined, Fee: 1, SizeInBytes: 1}, {Hash: unmined, Fee: 1, SizeInBytes: 1}},
					[]*subtreepkg.TxInpoints{inpointsPtr(), inpointsPtr()},
				)

				require.Eventually(t, func() bool {
					return stp.queue.length() == 0 && stp.GetCurrentTxMap().Exists(unmined)
				}, 5*time.Second, time.Millisecond, "the dequeue loop must place the unmined transaction")

				require.Equal(t, tc.reEnters, stp.GetCurrentTxMap().Exists(mined))
			})
		}
	}
}

// A reset forgets every kept set, so a hand-off of a transaction a block it may have moved back
// mined is queued again.
func TestReset_ClearsKeptMinedSets(t *testing.T) {
	stp, _ := newMinedSetProcessor(t, 10*time.Second)
	h := chainhash.HashH([]byte("mined-before-reset"))
	stp.keepMinedSet(minedSetBlock(1), frozenHashSet(t, h))

	require.NoError(t, stp.reset(&model.BlockHeader{
		Version:        1,
		HashPrevBlock:  &chainhash.Hash{},
		HashMerkleRoot: &chainhash.Hash{},
		Nonce:          2240,
	}, nil, nil, false, func() error { return nil }))

	require.Zero(t, stp.keptMinedSetCount())

	b := batchOf(h)
	require.Equal(t, Admitted, stp.AddBatchBounded(b.nodes, b.txInpoints, false, b.validatedHeights))
	require.Equal(t, int64(1), stp.queue.length(), "the transaction is queued again after the reset")
}
