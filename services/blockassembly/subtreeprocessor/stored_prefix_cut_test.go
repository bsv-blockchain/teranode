package subtreeprocessor

import (
	"context"
	"fmt"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/stores/blob/null"
	"github.com/bsv-blockchain/teranode/stores/utxo/sql"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// Issue 2269, plan PR 3: with blockassembly_storedPrefixTemplate on, a mining template is cut at the
// first chained subtree whose root is not in the stored set; with it off nothing changes.

// storeRequests answers every store request of an unstarted processor and records the roots, so a
// test sees which subtrees were sent to the store and decides itself which count as stored.
type storeRequests struct {
	mu    sync.Mutex
	roots []chainhash.Hash
}

func (r *storeRequests) has(root chainhash.Hash) bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	for _, got := range r.roots {
		if got == root {
			return true
		}
	}

	return false
}

func (r *storeRequests) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()

	return len(r.roots)
}

// newStoredPrefixProcessor builds a processor with subtrees of itemsPerSubtree leaves, its block
// header set (templates are only published then) and set as its stored set. It is not started, so a
// test drives the processor's state directly.
func newStoredPrefixProcessor(t testing.TB, itemsPerSubtree int, set *StoredSet, mutate ...func(*SubtreeProcessor)) (*SubtreeProcessor, *storeRequests) {
	t.Helper()

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.BlockAssembly.InitialMerkleItemsPerSubtree = itemsPerSubtree

	log := &storeRequests{}
	newSubtreeChan := make(chan NewSubtreeRequest, 64)
	done := make(chan struct{})

	t.Cleanup(func() { close(done) })

	go func() {
		for {
			select {
			case req := <-newSubtreeChan:
				log.mu.Lock()
				log.roots = append(log.roots, *req.Subtree.RootHash())
				log.mu.Unlock()

				if req.ErrChan != nil {
					req.ErrChan <- nil
				}
			case <-done:
				return
			}
		}
	}()

	logger := ulogger.NewErrorTestLogger(t)
	subtreeStore, _ := null.New(logger)

	utxoStoreURL, err := url.Parse("sqlitememory:///test")
	require.NoError(t, err)

	utxoStore, err := sql.New(t.Context(), logger, tSettings, utxoStoreURL)
	require.NoError(t, err)

	var opts []Options
	if set != nil {
		opts = append(opts, WithStoredSet(set))
	}

	stp, err := NewSubtreeProcessor(t.Context(), ulogger.TestLogger{}, tSettings, subtreeStore, nil, utxoStore, newSubtreeChan, opts...)
	require.NoError(t, err)

	stp.InitCurrentBlockHeader(&model.BlockHeader{Version: 1, HashPrevBlock: &chainhash.Hash{}, HashMerkleRoot: &chainhash.Hash{}})

	for _, m := range mutate {
		m(stp)
	}

	return stp, log
}

// addStoredPrefixTxs adds n new transactions, so the chain grows by about n/itemsPerSubtree
// subtrees.
func addStoredPrefixTxs(t testing.TB, stp *SubtreeProcessor, n int) {
	t.Helper()

	parent := chainhash.HashH([]byte("stored-prefix-parent"))

	for i := range n {
		hash := chainhash.HashH([]byte{byte(i), byte(i >> 8), 's', 'p'})
		require.NoError(t, stp.addNode(subtreepkg.Node{Hash: hash, Fee: 1, SizeInBytes: 100},
			&subtreepkg.TxInpoints{ParentTxHashes: []chainhash.Hash{parent}}, true))
	}
}

func chainRoot(stp *SubtreeProcessor, i int) chainhash.Hash {
	return *stp.chainedSubtrees[i].RootHash()
}

// requireTemplateStored is the stored-set oracle: no template the processor publishes references a
// chained subtree outside the stored set, and it is a prefix of the chain.
func requireTemplateStored(t testing.TB, stp *SubtreeProcessor, set *StoredSet) {
	t.Helper()

	data := stp.GetPrecomputedMiningData()
	if data == nil {
		return
	}

	require.LessOrEqual(t, len(data.Subtrees), len(stp.chainedSubtrees))

	for i, st := range data.Subtrees {
		require.Same(t, stp.chainedSubtrees[i], st, "the template is a prefix of the chain")
		require.True(t, set.Has(*st.RootHash()), "template subtree %d is not stored", i)
	}
}

func TestStoredPrefix_FlagOffPublishesTheWholeChain(t *testing.T) {
	stp, _ := newStoredPrefixProcessor(t, 8, nil)
	addStoredPrefixTxs(t, stp, 40)
	require.GreaterOrEqual(t, len(stp.chainedSubtrees), 3)

	data := stp.GetPrecomputedMiningData()
	require.NotNil(t, data)
	require.Len(t, data.Subtrees, len(stp.chainedSubtrees), "no set: nothing is cut")
}

func TestStoredPrefix_TemplateStopsAtTheFirstSubtreeNotStored(t *testing.T) {
	set := NewStoredSet()
	stp, _ := newStoredPrefixProcessor(t, 8, set)
	addStoredPrefixTxs(t, stp, 40)
	require.GreaterOrEqual(t, len(stp.chainedSubtrees), 4)

	// the first and the third are stored; the second is not
	set.Add(chainRoot(stp, 0))
	set.Add(chainRoot(stp, 2))

	data := stp.GetPrecomputedMiningData()
	require.Len(t, data.Subtrees, 1, "the cut is at the first gap, not at the stored count")
	requireTemplateStored(t, stp, set)

	set.Add(chainRoot(stp, 1))
	require.Len(t, stp.GetPrecomputedMiningData().Subtrees, 3, "the template grows as the gap is filled")
	requireTemplateStored(t, stp, set)

	for i := range stp.chainedSubtrees {
		set.Add(chainRoot(stp, i))
	}

	require.Len(t, stp.GetPrecomputedMiningData().Subtrees, len(stp.chainedSubtrees))
}

func TestStoredPrefix_LimitZeroEmptyChainStillServesTheOpenSubtree(t *testing.T) {
	set := NewStoredSet()
	stp, _ := newStoredPrefixProcessor(t, 8, set)
	addStoredPrefixTxs(t, stp, 3)
	require.Empty(t, stp.chainedSubtrees)

	require.Nil(t, stp.GetPrecomputedMiningData(), "nothing is published before the first subtree completes")

	snapshot := stp.incompleteSubtreeSnapshot()
	require.NotNil(t, snapshot, "with nothing chained the open subtree is the template")
	require.Len(t, snapshot.Subtrees, 1)
}

func TestStoredPrefix_LimitZeroNonEmptyChainIsAnEmptyBlockNotTheOpenSubtree(t *testing.T) {
	set := NewStoredSet()
	stp, _ := newStoredPrefixProcessor(t, 8, set)
	addStoredPrefixTxs(t, stp, 20)
	require.NotEmpty(t, stp.chainedSubtrees)
	require.Greater(t, stp.currentSubtree.Load().Length(), 1, "the open subtree holds transactions")

	require.Empty(t, stp.GetPrecomputedMiningData().Subtrees, "nothing stored: the cut is at 0")

	// GetMiningCandidate asks for the snapshot when the slice is empty; it must refuse, or the
	// candidate would be the open subtree alone, which does not follow the chained ones
	require.Nil(t, stp.incompleteSubtreeSnapshot(), "the chain is non-empty")
}

// The guard reads chainedSubtreeCount, which the bulk builders resync by hand: after one, a stale 0
// would let the open subtree out alone.
func TestStoredPrefix_GuardHoldsAfterABulkBuild(t *testing.T) {
	set := NewStoredSet()
	stp, _ := newStoredPrefixProcessor(t, 8, set)

	nodes := make([]subtreepkg.Node, 20)
	for i := range nodes {
		nodes[i] = subtreepkg.Node{Hash: chainhash.HashH([]byte{byte(i), 'b'}), Fee: 1, SizeInBytes: 100}
	}

	require.NoError(t, stp.bulkBuildSubtrees(t.Context(), nodes, 8))
	require.NotEmpty(t, stp.chainedSubtrees)
	stp.updatePrecomputedMiningData()

	require.Empty(t, stp.GetPrecomputedMiningData().Subtrees)
	require.Nil(t, stp.incompleteSubtreeSnapshot())
}

func TestStoredPrefix_NoIncompleteSnapshotRoundTripWhileChained(t *testing.T) {
	stp, _ := newStoredPrefixProcessor(t, 8, NewStoredSet())
	addStoredPrefixTxs(t, stp, 20)

	// not started: a request to the processor goroutine would wait for its 5 s timeout
	start := time.Now()
	require.Nil(t, stp.GetIncompleteSubtreeMiningData(context.Background()))
	require.Less(t, time.Since(start), time.Second, "a refusal known in advance must not wait for the processor")
}

// A rechain builds new subtrees with new roots; the roots of the old ones are in the set, the new
// ones are not until stored.
func TestStoredPrefix_RechainNewRootsAreAbsentUntilStored(t *testing.T) {
	set := NewStoredSet()
	stp, requests := newStoredPrefixProcessor(t, 8, set)
	addStoredPrefixTxs(t, stp, 60)
	require.GreaterOrEqual(t, len(stp.chainedSubtrees), 5)

	for i := range stp.chainedSubtrees {
		set.Add(chainRoot(stp, i))
	}

	require.Len(t, stp.GetPrecomputedMiningData().Subtrees, len(stp.chainedSubtrees))

	oldRoots := make([]chainhash.Hash, len(stp.chainedSubtrees))
	for i := range oldRoots {
		oldRoots[i] = chainRoot(stp, i)
	}

	// a tx of the third subtree is removed: from there on the chain is rebuilt
	hole := stp.chainedSubtrees[2].Nodes[3].Hash
	require.NoError(t, stp.removeTxFromSubtrees(t.Context(), hole))
	stp.updatePrecomputedMiningData()

	require.Equal(t, oldRoots[0], chainRoot(stp, 0))
	require.Equal(t, oldRoots[1], chainRoot(stp, 1))
	require.NotEqual(t, oldRoots[2], chainRoot(stp, 2))

	require.Len(t, stp.GetPrecomputedMiningData().Subtrees, 2, "the new roots are absent until stored")
	requireTemplateStored(t, stp, set)

	// the rechain sent every rebuilt subtree to the store (audit: reChainSubtrees, through
	// addNode and processCompleteSubtree)
	require.Eventually(t, func() bool {
		for i := 2; i < len(stp.chainedSubtrees); i++ {
			if !requests.has(chainRoot(stp, i)) {
				return false
			}
		}

		return true
	}, 5*time.Second, 5*time.Millisecond, "every rebuilt chained subtree has a store request")
}

// BenchmarkGetPrecomputedMiningData_StoredPrefix is the cost the cut adds to a read of the published
// template, with the whole chain stored (the common case: nothing is cut, no copy is made).
func BenchmarkGetPrecomputedMiningData_StoredPrefix(b *testing.B) {
	for _, subtrees := range []int{10, 100, 500} {
		for _, on := range []bool{false, true} {
			name := "off"
			if on {
				name = "on"
			}

			b.Run(fmt.Sprintf("subtrees=%d/%s", subtrees, name), func(b *testing.B) {
				var set *StoredSet
				if on {
					set = NewStoredSet()
				}

				stp, _ := newStoredPrefixProcessor(b, 8, set)
				addStoredPrefixTxs(b, stp, subtrees*8)

				if on {
					for i := range stp.chainedSubtrees {
						set.Add(chainRoot(stp, i))
					}
				}

				b.ReportAllocs()
				b.ResetTimer()

				for range b.N {
					if stp.GetPrecomputedMiningData() == nil {
						b.Fatal("no template")
					}
				}
			})
		}
	}
}

// The cut is kept between reads while the set and the published data stand still; it must not
// outlive a change to either.
func TestStoredPrefix_ACachedCutFollowsTheSetAndThePublishedData(t *testing.T) {
	set := NewStoredSet()
	stp, _ := newStoredPrefixProcessor(t, 8, set)
	addStoredPrefixTxs(t, stp, 40)

	for i := range stp.chainedSubtrees {
		set.Add(chainRoot(stp, i))
	}

	full := len(stp.chainedSubtrees)
	require.Len(t, stp.GetPrecomputedMiningData().Subtrees, full)
	require.Same(t, stp.GetPrecomputedMiningData(), stp.GetPrecomputedMiningData(), "no copy per read")

	// roots dropped from the set (pruned) cut the template again
	keep := map[chainhash.Hash]struct{}{chainRoot(stp, 0): {}, chainRoot(stp, 1): {}}
	set.Retain(keep)
	require.Len(t, stp.GetPrecomputedMiningData().Subtrees, 2)

	// a new publication is not answered from the old cut
	set.Add(chainRoot(stp, 2))
	require.Len(t, stp.GetPrecomputedMiningData().Subtrees, 3)

	stp.updatePrecomputedMiningData()
	require.Len(t, stp.GetPrecomputedMiningData().Subtrees, 3)
}
