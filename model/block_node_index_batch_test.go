package model

import (
	"fmt"
	"math"
	"math/rand"
	"runtime"
	"sync"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	txmap "github.com/bsv-blockchain/go-tx-map"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/stretchr/testify/require"
)

var batchHashes = map[string]func(n int) func(chainhash.Hash) uint64{
	"seeded": func(int) func(chainhash.Hash) uint64 { return seededHash(7) },
	"weak":   func(int) func(chainhash.Hash) uint64 { return weakHash },
	"const":  func(int) func(chainhash.Hash) uint64 { return constHash(42) },
}

// requireSameIndex checks that got answers every lookup in probes like want.
func requireSameIndex(t *testing.T, want, got *nodeIndex, probes []chainhash.Hash) {
	t.Helper()

	require.Equal(t, want.Length(), got.Length())

	for i := range probes {
		wantIdx, wantOK := want.Get(probes[i])
		gotIdx, gotOK := got.Get(probes[i])
		require.Equal(t, wantOK, gotOK, "probe %d", i)
		require.Equal(t, wantIdx, gotIdx, "probe %d", i)
	}
}

// putRange stores the same entries as a Put of every index in the range.
func TestNodeIndex_PutRangeMatchesPut(t *testing.T) {
	for name, newHash := range batchHashes {
		for _, n := range []int{0, 1, 9, 5000, putRangeChunk + 1000} {
			if name == "const" && n > 500 {
				continue // every key in one bucket chain: quadratic
			}

			t.Run(fmt.Sprintf("%s/n=%d", name, n), func(t *testing.T) {
				nodes := newTestNodeHashes(n + 100)

				want := newNodeIndex(n, nodes.hashAt, newHash(n))
				for i := 0; i < n; i++ {
					require.NoError(t, want.Put(nodes[i], uint32(i))) //nolint:gosec // test
				}

				got := newNodeIndex(n, nodes.hashAt, newHash(n))
				_, err := got.putRange(0, n)
				require.NoError(t, err)

				requireSameIndex(t, want, got, nodes) // includes 100 that were never put
			})
		}
	}
}

// Ranges that start past zero and an index that grows while a range goes in.
func TestNodeIndex_PutRangeOffsetAndGrowth(t *testing.T) {
	nodes := newTestNodeHashes(50000)

	want := newNodeIndex(16, nodes.hashAt, seededHash(3))
	got := newNodeIndex(16, nodes.hashAt, seededHash(3))

	for _, r := range [][2]int{{0, 1}, {1, 10000}, {30000, 50000}, {10000, 30000}} {
		for i := r[0]; i < r[1]; i++ {
			require.NoError(t, want.Put(nodes[i], uint32(i))) //nolint:gosec // test
		}

		_, err := got.putRange(uint32(r[0]), r[1]-r[0]) //nolint:gosec // test
		require.NoError(t, err)
	}

	requireSameIndex(t, want, got, nodes)
	require.Greater(t, got.slots(), 16*nodeBucketSlots, "the index grew")
}

// A duplicate is reported at the first index whose hash is already in the index or
// earlier in the range, as a Put of every index in order would find it.
func TestNodeIndex_PutRangeReportsFirstDuplicate(t *testing.T) {
	for name, newHash := range batchHashes {
		// 400 entries are batched into one shard, and put one by one into 2048 shards
		for _, capacity := range []int{400, 1 << 22} {
			t.Run(fmt.Sprintf("%s/capacity=%d", name, capacity), func(t *testing.T) {
				const n = 400

				nodes := newTestNodeHashes(n)
				nodes[150] = nodes[20]  // within the range
				nodes[90] = nodes[300]  // the later one is the duplicate
				nodes[350] = nodes[399] // after the first duplicate

				x := newNodeIndex(capacity, nodes.hashAt, newHash(n))
				dup, err := x.putRange(0, n)
				require.ErrorIs(t, err, errors.ErrTxExists)
				require.Contains(t, err.Error(), "hash already exists in map")
				require.Equal(t, uint32(150), dup)

				// against an entry put before the range
				nodes = newTestNodeHashes(n)
				nodes[250] = nodes[5]
				x = newNodeIndex(capacity, nodes.hashAt, newHash(n))
				require.NoError(t, x.Put(nodes[5], 5))

				dup, err = x.putRange(100, 300)
				require.ErrorIs(t, err, errors.ErrTxExists)
				require.Equal(t, uint32(250), dup)

				idx, ok := x.Get(nodes[5])
				require.True(t, ok)
				require.Equal(t, uint32(5), idx, "the first index is kept")
			})
		}
	}
}

// The first duplicate of a range longer than one chunk is the one reported, whichever
// chunk it is in.
func TestNodeIndex_PutRangeFirstDuplicateAcrossChunks(t *testing.T) {
	n := 2*putRangeChunk + 10

	nodes := newTestNodeHashes(n)
	nodes[putRangeChunk+5] = nodes[3]
	nodes[2*putRangeChunk+1] = nodes[4]

	x := newNodeIndex(n, nodes.hashAt, seededHash(5))
	dup, err := x.putRange(0, n)
	require.ErrorIs(t, err, errors.ErrTxExists)
	require.Equal(t, uint32(putRangeChunk+5), dup) //nolint:gosec // test
}

func TestNodeIndex_PutRangeFrozen(t *testing.T) {
	nodes := newTestNodeHashes(10)
	x := newNodeIndex(10, nodes.hashAt, seededHash(1))
	x.Freeze()

	_, err := x.putRange(0, 10)
	require.ErrorContains(t, err, "frozen")
	require.Zero(t, x.Length())
}

// Concurrent ranges on one index, for the race detector.
func TestNodeIndex_PutRangeConcurrent(t *testing.T) {
	const (
		n       = 200000
		workers = 8
	)

	nodes := newTestNodeHashes(n)
	x := newNodeIndex(n, nodes.hashAt, seededHash(9))

	var wg sync.WaitGroup

	for w := 0; w < workers; w++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			lo := w * n / workers
			_, err := x.putRange(uint32(lo), (w+1)*n/workers-lo) //nolint:gosec // test
			require.NoError(t, err)
		}()
	}

	wg.Wait()
	x.Freeze()

	require.Equal(t, n, x.Length())

	for i := range nodes {
		idx, ok := x.Get(nodes[i])
		require.True(t, ok)
		require.Equal(t, uint32(i), idx) //nolint:gosec // test
	}
}

// lookups returns a batch of probes: hits on put entries, misses, and repeats.
func lookups(nodes testNodeHashes, put, count int, rng *rand.Rand) []nodeLookup {
	ls := make([]nodeLookup, count)

	for i := range ls {
		if rng.Intn(3) == 0 {
			ls[i].hash = &nodes[put+rng.Intn(len(nodes)-put)]
		} else {
			ls[i].hash = &nodes[rng.Intn(put)]
		}
	}

	return ls
}

// getBatch answers every lookup like Get, frozen or not.
func TestNodeIndex_GetBatchMatchesGet(t *testing.T) {
	for name, newHash := range batchHashes {
		for _, frozen := range []bool{true, false} {
			t.Run(fmt.Sprintf("%s/frozen=%v", name, frozen), func(t *testing.T) {
				const put = 400

				nodes := newTestNodeHashes(2 * put)
				x := newNodeIndex(put, nodes.hashAt, newHash(put))

				for i := 0; i < put; i++ {
					require.NoError(t, x.Put(nodes[i], uint32(i))) //nolint:gosec // test
				}

				if frozen {
					x.Freeze()
				}

				rng := rand.New(rand.NewSource(1)) //nolint:gosec // test

				for _, count := range []int{0, 1, 7, 64, 500} {
					ls := lookups(nodes, put, count, rng)

					// stale results must be overwritten
					for i := range ls {
						ls[i].idx, ls[i].found = math.MaxUint64, true
					}

					x.getBatch(ls)

					for i := range ls {
						idx, ok := x.Get(*ls[i].hash)
						require.Equal(t, ok, ls[i].found, "count %d lookup %d", count, i)

						if ok {
							require.Equal(t, uint64(idx), ls[i].idx, "count %d lookup %d", count, i)
						}
					}
				}
			})
		}
	}
}

// blockNodeIndex.putRange: 32-bit bound, and duplicates against the overflow map in
// both orders.
func TestBlockNodeIndex_PutRange(t *testing.T) {
	subs, byIdx := buildLayout(t, 4, []int{4, 4, 3})
	m := newBlockNodeIndex(0, len(subs), 4)

	for s, st := range subs {
		require.NoError(t, m.register(s, st.Nodes))
	}

	_, err := m.putRange(math.MaxUint32, 2)
	require.ErrorContains(t, err, "32-bit")
	require.Zero(t, m.Length(), "a rejected range stores nothing")

	_, err = m.putRange(1, 3)
	require.NoError(t, err)
	_, err = m.putRange(4, 4)
	require.NoError(t, err)
	_, err = m.putRange(8, 3)
	require.NoError(t, err)

	m.Freeze()

	for idx, h := range byIdx {
		got, ok := m.Get(h)
		if idx == 0 {
			require.False(t, ok, "the coinbase placeholder was not put")
			continue
		}

		require.True(t, ok, "idx %d", idx)
		require.Equal(t, idx, got)
	}
}

func TestBlockNodeIndex_PutRangeSeesOverflow(t *testing.T) {
	// subtree 0 (capacity 2) holds 5 nodes and overflows; subtree 1 is the last
	subs, _ := buildLayout(t, 2, []int{5, 3})

	m := newBlockNodeIndex(0, 2, 2)
	require.NoError(t, m.register(0, subs[0].Nodes))
	require.NoError(t, m.register(1, subs[1].Nodes))

	// the overflowing subtree holds subtree 1's second tx
	require.NoError(t, m.putOverflow(subs[1].Nodes[1].Hash, 7))

	dup, err := m.putRange(2, 3)
	require.ErrorIs(t, err, errors.ErrTxExists)
	require.Contains(t, err.Error(), "hash already exists in map")
	require.Equal(t, uint64(3), dup)

	// and the other way round: putOverflow after the range
	m = newBlockNodeIndex(0, 2, 2)
	require.NoError(t, m.register(0, subs[0].Nodes))
	require.NoError(t, m.register(1, subs[1].Nodes))

	_, err = m.putRange(2, 3)
	require.NoError(t, err)
	require.ErrorIs(t, m.putOverflow(subs[1].Nodes[2].Hash, 9), errors.ErrTxExists)
}

func TestBlockNodeIndex_GetBatch(t *testing.T) {
	subs, _ := buildLayout(t, 2, []int{5, 3})

	m := newBlockNodeIndex(0, 2, 2)
	require.NoError(t, m.register(0, subs[0].Nodes))
	require.NoError(t, m.register(1, subs[1].Nodes))

	for i := 1; i < 5; i++ {
		require.NoError(t, m.putOverflow(subs[0].Nodes[i].Hash, uint64(i))) //nolint:gosec // test
	}

	_, err := m.putRange(2, 3)
	require.NoError(t, err)
	m.Freeze()

	missing := seqHash(1 << 50)
	probes := []*chainhash.Hash{&subs[0].Nodes[3].Hash, &subs[1].Nodes[0].Hash, &missing, &subs[1].Nodes[2].Hash, &subs[0].Nodes[1].Hash}

	ls := make([]nodeLookup, len(probes))
	for i := range probes {
		ls[i].hash = probes[i]
	}

	m.getBatch(ls)

	for i := range ls {
		idx, ok := m.Get(*probes[i])
		require.Equal(t, ok, ls[i].found, "probe %d", i)
		require.Equal(t, idx, ls[i].idx, "probe %d", i)
	}

	require.True(t, ls[0].found, "an overflow entry is found")
	require.Equal(t, uint64(3), ls[0].idx)
}

// checkParents answers from the lookups when it has them, without reading the txMap.
func TestCheckParents_UsesLookups(t *testing.T) {
	block := &Block{} // no txMap: a Get would panic

	in, after, outside := seqHash(1), seqHash(2), seqHash(3)
	parents := []chainhash.Hash{in, outside}
	lookups := []nodeLookup{{hash: &parents[0], idx: 4, found: true}, {hash: &parents[1]}}

	txHash := seqHash(10)

	missing, err := block.checkParents(parents, nil, lookups, 5, subtreepkg.Node{Hash: txHash}, &txHash, 0, 5)
	require.NoError(t, err)
	require.Equal(t, []missingParentTx{{parentTxHash: outside, txHash: txHash}}, missing)

	// a parent later in the block
	parents = []chainhash.Hash{after}
	lookups = []nodeLookup{{hash: &parents[0], idx: 9, found: true}}

	_, err = block.checkParents(parents, nil, lookups, 5, subtreepkg.Node{Hash: txHash}, &txHash, 0, 5)
	require.ErrorIs(t, err, errors.ErrBlockInvalid)
	require.ErrorContains(t, err, "comes before parent transaction")
}

// parentLookups answers each transaction's parents like a Get of each, across window
// boundaries and in any order, and only for an in-memory node index.
func TestParentLookups_Windows(t *testing.T) {
	const txs = 3*parentLookupWindow + 5

	nodes := newTestNodeHashes(2 * txs)
	m := newBlockNodeIndex(txs, 1, txs)
	m.nodes[0] = make([]subtreepkg.Node, txs)

	for i := 0; i < txs; i++ {
		m.nodes[0][i].Hash = nodes[i]
	}

	_, err := m.putRange(0, txs)
	require.NoError(t, err)
	m.Freeze()

	subtree, err := subtreepkg.NewTreeByLeafCount(1024)
	require.NoError(t, err)

	meta := subtreepkg.NewSubtreeMeta(subtree)
	meta.TxInpoints = make([]subtreepkg.TxInpoints, txs)

	for i := 0; i < txs; i++ {
		// i%4 parents: in the block, outside it, or none
		var parents []chainhash.Hash
		for k := 0; k < i%4; k++ {
			parents = append(parents, nodes[(i*7+k*txs)%len(nodes)])
		}

		meta.TxInpoints[i] = subtreepkg.NewTxInpointsFromPacked(parents, make([]uint32, 2*len(parents)))
	}

	var p parentLookups

	order := make([]int, 0, 2*txs)
	for i := 0; i < txs; i++ {
		order = append(order, i)
	}

	order = append(order, txs-1, 0, parentLookupWindow, 1) // back to earlier windows

	for _, i := range order {
		got := p.of(m, meta, i)
		parents := meta.TxInpoints[i].ParentTxHashes
		require.Len(t, got, len(parents), "tx %d", i)

		for k := range parents {
			idx, ok := m.Get(parents[k])
			require.Same(t, &parents[k], got[k].hash, "tx %d parent %d", i, k)
			require.Equal(t, ok, got[k].found, "tx %d parent %d", i, k)
			require.Equal(t, idx, got[k].idx, "tx %d parent %d", i, k)
		}
	}

	require.Nil(t, p.of(m, meta, txs), "past the meta")
	require.Nil(t, p.of(txmap.NewSplitSwissMapUint64(10), meta, 0), "not a node index")
}

// A short range allocates about what it holds, not a full chunk of scratch, so small
// blocks and small machines don't pay for the batching.
func TestNodeIndex_PutRangeSmallAllocatesLittle(t *testing.T) {
	nodes := newTestNodeHashes(64)

	for _, capacity := range []int{64, 1 << 24} { // one shard, and 8192 shards
		x := newNodeIndex(capacity, nodes.hashAt, seededHash(1))

		// drop pooled scratch (sync.Pool keeps it for two GCs)
		runtime.GC()
		runtime.GC()

		var before, after runtime.MemStats

		runtime.ReadMemStats(&before)

		_, err := x.putRange(0, len(nodes))
		require.NoError(t, err)

		runtime.ReadMemStats(&after)
		require.Less(t, after.TotalAlloc-before.TotalAlloc, uint64(64<<10), "capacity %d", capacity)
	}
}
