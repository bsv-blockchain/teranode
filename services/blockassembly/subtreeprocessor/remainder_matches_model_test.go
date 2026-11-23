package subtreeprocessor

import (
	"bytes"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	txmap "github.com/bsv-blockchain/go-tx-map"
	"github.com/bsv-blockchain/teranode/stores/blob/memory"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// remainderModelShape bounds the random inputs: how many nodes a chained
// subtree may hold, the leaf counts the processor's subtrees may have, and
// the threshold between the sequential and the parallel insert, which sits
// inside the range of remainder sizes the shape generates.
type remainderModelShape struct {
	name        string
	seeds       int64
	subtreeCap  int
	maxSubtrees int
	leafCounts  []int
	threshold   int
}

var (
	// Many small cases: every mix of flags, empty subtrees, all-removed
	// blocks, both sides of the threshold.
	remainderModelSmall = remainderModelShape{name: "small", seeds: 400, subtreeCap: 64, maxSubtrees: 8, leafCounts: []int{4, 8, 16, 64}, threshold: 64}

	// A few large cases: subtrees span several 16,384-node lookup chunks, so
	// each subtree's remaining count is summed from more than one chunk.
	remainderModelLarge = remainderModelShape{name: "large", seeds: 12, subtreeCap: 1 << 16, maxSubtrees: 4, leafCounts: []int{1024, 4096}, threshold: 30_000}
)

// remainderModelCase is one randomised input to processRemainderTxHashes and
// everything a straightforward reference walk needs to predict its output.
type remainderModelCase struct {
	leafCount         int
	skipNotification  bool
	chained           []*subtreepkg.Subtree
	transactionMap    *SortedHashSet
	losingMap         txmap.TxMap
	removeMap         []chainhash.Hash
	conflicting       map[chainhash.Hash]struct{}
	sourceTxMap       *SplitTxInpointsMap
	parents           map[chainhash.Hash][]chainhash.Hash
	inTxMap, inLosing map[chainhash.Hash]bool
	inRemove          map[chainhash.Hash]bool
	allHashes         []chainhash.Hash
}

func randomHash(rng *rand.Rand) chainhash.Hash {
	var h chainhash.Hash

	_, _ = rng.Read(h[:])

	return h
}

// buildRemainderModelCase draws a reorg-shaped input: chained subtrees of
// random fill (some empty, some starting with a coinbase placeholder) whose
// nodes are a mix of block txs, losers, removeMap entries and survivors, with
// parent links to earlier nodes so a conflicting tx cascades down chains.
func buildRemainderModelCase(t *testing.T, rng *rand.Rand, shape remainderModelShape) *remainderModelCase {
	t.Helper()

	c := &remainderModelCase{
		leafCount:        shape.leafCounts[rng.Intn(len(shape.leafCounts))],
		skipNotification: rng.Intn(2) == 0,
		transactionMap:   NewSortedHashSet(1024),
		losingMap:        txmap.NewSplitSwissMap(64, 4),
		conflicting:      map[chainhash.Hash]struct{}{},
		sourceTxMap:      NewSplitTxInpointsMap(16),
		parents:          map[chainhash.Hash][]chainhash.Hash{},
		inTxMap:          map[chainhash.Hash]bool{},
		inLosing:         map[chainhash.Hash]bool{},
		inRemove:         map[chainhash.Hash]bool{},
	}

	// Mostly mixed blocks, some where every node is removed one way or another.
	allRemoved := rng.Intn(10) == 0
	surviveShare := 20 + rng.Intn(70)

	subtreeCount := rng.Intn(shape.maxSubtrees + 1)
	for s := 0; s < subtreeCount; s++ {
		st, err := subtreepkg.NewTreeByLeafCount(shape.subtreeCap)
		require.NoError(t, err)

		if (s == 0 && rng.Intn(5) != 0) || rng.Intn(12) == 0 {
			require.NoError(t, st.AddCoinbaseNode())
		}

		fill := rng.Intn(shape.subtreeCap + 1 - st.Length())
		if rng.Intn(8) == 0 {
			fill = 0
		}

		for i := 0; i < fill; i++ {
			hash := randomHash(rng)

			var parents []chainhash.Hash
			if len(c.allHashes) > 0 && rng.Intn(3) == 0 {
				parents = append(parents, c.allHashes[rng.Intn(len(c.allHashes))])
			}

			parents = append(parents, randomHash(rng))

			roll := rng.Intn(100)
			if allRemoved && roll < surviveShare {
				roll = surviveShare
			}

			switch {
			case roll < surviveShare:
				// survives the flags
			case roll < surviveShare+(100-surviveShare)/3:
				require.NoError(t, c.transactionMap.Put(hash))
				c.inTxMap[hash] = true

				if rng.Intn(4) == 0 {
					require.NoError(t, c.losingMap.Put(hash, 1))
					c.inLosing[hash] = true
				}
			case roll < surviveShare+2*(100-surviveShare)/3:
				require.NoError(t, c.losingMap.Put(hash, 1))
				c.inLosing[hash] = true
			default:
				c.removeMap = append(c.removeMap, hash)
				c.inRemove[hash] = true

				if rng.Intn(2) == 0 {
					require.NoError(t, c.transactionMap.Put(hash))
					c.inTxMap[hash] = true
				}
			}

			require.NoError(t, st.AddSubtreeNode(subtreepkg.Node{Hash: hash, Fee: uint64(rng.Intn(1000)), SizeInBytes: uint64(100 + rng.Intn(400))}))
			c.sourceTxMap.Set(hash, &subtreepkg.TxInpoints{ParentTxHashes: parents})
			c.parents[hash] = parents
			c.allHashes = append(c.allHashes, hash)
		}

		c.chained = append(c.chained, st)
	}

	// The caller always passes the old current subtree last; sometimes it is empty.
	if rng.Intn(3) == 0 {
		st, err := subtreepkg.NewTreeByLeafCount(shape.subtreeCap)
		require.NoError(t, err)
		c.chained = append(c.chained, st)
	}

	// removeMap entries that are not in any subtree must stay.
	for i := rng.Intn(4); i > 0; i-- {
		c.removeMap = append(c.removeMap, randomHash(rng))
	}

	// Seed the conflict cascade with a few earlier txs of any kind.
	if len(c.allHashes) > 0 && rng.Intn(5) != 0 {
		for i := 1 + rng.Intn(4); i > 0; i-- {
			c.conflicting[c.allHashes[rng.Intn(len(c.allHashes))]] = struct{}{}
		}

		c.conflicting[randomHash(rng)] = struct{}{}
	}

	require.NoError(t, c.transactionMap.Freeze(2))

	return c
}

// expectedSurvivors is the reference walk: every subtree in order, coinbase
// placeholders skipped, removeMap first, then the block's txs and losers, then
// the conflict cascade over what is left, parents before children.
func (c *remainderModelCase) expectedSurvivors() ([]subtreepkg.Node, map[chainhash.Hash]struct{}) {
	conflicting := make(map[chainhash.Hash]struct{}, len(c.conflicting))
	for h := range c.conflicting {
		conflicting[h] = struct{}{}
	}

	out := make([]subtreepkg.Node, 0)

	for _, st := range c.chained {
		for _, node := range st.Nodes {
			if node.Hash.Equal(*subtreepkg.CoinbasePlaceholderHash) || c.inRemove[node.Hash] || c.inTxMap[node.Hash] || c.inLosing[node.Hash] {
				continue
			}

			if len(c.conflicting) > 0 && rejectConflicting(node.Hash, &subtreepkg.TxInpoints{ParentTxHashes: c.parents[node.Hash]}, conflicting) {
				continue
			}

			out = append(out, node)
		}
	}

	return out, conflicting
}

func newRemainderModelProcessor(t *testing.T, leafCount, threshold int) (*SubtreeProcessor, chan NewSubtreeRequest) {
	t.Helper()

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.BlockAssembly.TxMapDirs = nil
	tSettings.BlockAssembly.SubtreeMmapDir = ""
	tSettings.BlockAssembly.InitialMerkleItemsPerSubtree = leafCount
	tSettings.BlockAssembly.ParallelSetIfNotExistsThreshold = threshold

	// Large enough that no send blocks; drained after the call, in order.
	newSubtreeChan := make(chan NewSubtreeRequest, 4096)

	stp, err := NewSubtreeProcessor(t.Context(), ulogger.TestLogger{}, tSettings, memory.New(), nil, nil, newSubtreeChan)
	require.NoError(t, err)

	return stp, newSubtreeChan
}

// TestProcessRemainderTxHashes_MatchesReferenceModel pins the remainder
// pass's whole output against a sequential reference walk over random
// reorg-shaped inputs: the leaves of every resulting subtree in order, what
// lands in currentTxMap, what is left in removeMap, the cascade's additions
// to the conflicting set, and the subtree notifications. The cases straddle
// ParallelSetIfNotExistsThreshold, so both insert paths are covered.
//
// With REMAINDER_MODEL_DUMP set to a directory, every case's observed output
// is also written there, so two builds of the pass can be diffed directly.
func TestProcessRemainderTxHashes_MatchesReferenceModel(t *testing.T) {
	for _, shape := range []remainderModelShape{remainderModelSmall, remainderModelLarge} {
		t.Run(shape.name, func(t *testing.T) {
			runRemainderModelSeeds(t, shape)
		})
	}
}

func runRemainderModelSeeds(t *testing.T, shape remainderModelShape) {
	dumpDir := os.Getenv("REMAINDER_MODEL_DUMP")

	var dump bytes.Buffer

	paths := map[string]int{}

	for seed := int64(1); seed <= shape.seeds; seed++ {
		rng := rand.New(rand.NewSource(seed)) //nolint:gosec // deterministic test input
		c := buildRemainderModelCase(t, rng, shape)

		stp, newSubtreeChan := newRemainderModelProcessor(t, c.leafCount, shape.threshold)

		for _, h := range c.removeMap {
			require.NoError(t, stp.removeMap.Put(h, 1))
		}

		removeBefore := stp.removeMap.Keys()
		conflicting := c.conflicting

		var conflictingArg map[chainhash.Hash]struct{}
		if len(conflicting) > 0 {
			conflictingArg = make(map[chainhash.Hash]struct{}, len(conflicting))
			for h := range conflicting {
				conflictingArg[h] = struct{}{}
			}
		}

		err := stp.processRemainderTxHashes(t.Context(), c.chained, c.transactionMap, c.losingMap, conflictingArg, c.sourceTxMap, c.skipNotification)
		require.NoError(t, err, "seed %d", seed)

		var notified []NewSubtreeRequest

	drain:
		for {
			select {
			case req := <-newSubtreeChan:
				notified = append(notified, req)
				req.ErrChan <- nil
			default:
				break drain
			}
		}

		survivors, wantConflicting := c.expectedSurvivors()

		if len(survivors) >= shape.threshold {
			paths["parallel"]++
		} else {
			paths["sequential"]++
		}

		// The leaves, in order: the fresh processor's coinbase placeholder,
		// then every survivor.
		got := make([]subtreepkg.Node, 0)
		for _, st := range stp.chainedSubtrees {
			require.Equal(t, c.leafCount, st.Length(), "seed %d: a chained subtree must be full", seed)
			got = append(got, st.Nodes...)
		}

		current := stp.currentSubtree.Load()
		require.NotNil(t, current)
		require.Less(t, current.Length(), c.leafCount, "seed %d: the open subtree must not be full", seed)
		got = append(got, current.Nodes...)

		require.NotEmpty(t, got, "seed %d", seed)
		require.True(t, got[0].Hash.Equal(*subtreepkg.CoinbasePlaceholderHash), "seed %d: the first leaf must be the coinbase placeholder", seed)
		require.Equal(t, survivors, got[1:], "seed %d: leaves must be exactly the survivors, in order", seed)

		// currentTxMap holds exactly the survivors, with their inpoints.
		require.Equal(t, len(survivors), stp.currentTxMap.Length(), "seed %d", seed)

		for _, n := range survivors {
			inpoints, ok := stp.currentTxMap.Get(n.Hash)
			require.True(t, ok, "seed %d: survivor %s missing from currentTxMap", seed, n.Hash)
			require.Equal(t, c.parents[n.Hash], inpoints.ParentTxHashes, "seed %d", seed)
		}

		// removeMap loses exactly the entries the subtrees held.
		var wantRemove []chainhash.Hash
		for _, h := range removeBefore {
			if !c.inRemove[h] {
				wantRemove = append(wantRemove, h)
			}
		}

		gotRemove := stp.removeMap.Keys()
		sortHashes(wantRemove)
		sortHashes(gotRemove)
		require.Equal(t, len(wantRemove), len(gotRemove), "seed %d: removeMap contents", seed)

		if len(wantRemove) > 0 {
			require.Equal(t, wantRemove, gotRemove, "seed %d: removeMap contents", seed)
		}

		// The cascade adds every descendant it drops to the caller's set.
		if conflictingArg != nil {
			require.Equal(t, wantConflicting, conflictingArg, "seed %d: conflicting set", seed)
		}

		// One notification per completed subtree, in chain order.
		require.Len(t, notified, len(stp.chainedSubtrees), "seed %d", seed)

		for i, req := range notified {
			require.Same(t, stp.chainedSubtrees[i], req.Subtree, "seed %d: notification %d", seed, i)
			require.Equal(t, c.skipNotification, req.SkipNotification, "seed %d", seed)
		}

		if dumpDir != "" {
			writeRemainderModelDump(&dump, seed, stp, notified, gotRemove, conflictingArg, c)
		}
	}

	require.Positive(t, paths["parallel"], "no case reached the parallel insert")
	require.Positive(t, paths["sequential"], "no case reached the sequential insert")
	t.Logf("cases by insert path: %v", paths)

	if dumpDir != "" {
		require.NoError(t, os.WriteFile(filepath.Join(dumpDir, "remainder_model_dump_"+shape.name+".txt"), dump.Bytes(), 0o600))
	}
}

func sortHashes(hashes []chainhash.Hash) {
	sort.Slice(hashes, func(i, j int) bool { return bytes.Compare(hashes[i][:], hashes[j][:]) < 0 })
}

// writeRemainderModelDump records every observable output of one case as
// text, so the dump of one build can be diffed against another's.
func writeRemainderModelDump(w *bytes.Buffer, seed int64, stp *SubtreeProcessor, notified []NewSubtreeRequest,
	removeMap []chainhash.Hash, conflicting map[chainhash.Hash]struct{}, c *remainderModelCase) {
	fmt.Fprintf(w, "seed %d leafCount %d skip %v\n", seed, c.leafCount, c.skipNotification)

	writeSubtree := func(label string, st *subtreepkg.Subtree) {
		fmt.Fprintf(w, "  %s len %d root %s\n", label, st.Length(), st.RootHash())

		for _, n := range st.Nodes {
			fmt.Fprintf(w, "    %s %d %d\n", n.Hash, n.Fee, n.SizeInBytes)
		}
	}

	for i, st := range stp.chainedSubtrees {
		writeSubtree(fmt.Sprintf("chained %d", i), st)
	}

	writeSubtree("current", stp.currentSubtree.Load())

	fmt.Fprintf(w, "  currentTxMap len %d\n", stp.currentTxMap.Length())

	for _, h := range c.allHashes {
		if inpoints, ok := stp.currentTxMap.Get(h); ok {
			fmt.Fprintf(w, "    %s %v\n", h, inpoints.ParentTxHashes)
		}
	}

	fmt.Fprintf(w, "  removeMap %v\n", removeMap)

	conflictingList := make([]chainhash.Hash, 0, len(conflicting))
	for h := range conflicting {
		conflictingList = append(conflictingList, h)
	}

	sortHashes(conflictingList)
	fmt.Fprintf(w, "  conflicting %v\n", conflictingList)

	for i, req := range notified {
		fmt.Fprintf(w, "  notified %d %s skip %v\n", i, req.Subtree.RootHash(), req.SkipNotification)
	}
}
