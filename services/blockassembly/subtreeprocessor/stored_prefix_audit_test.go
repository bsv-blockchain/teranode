package subtreeprocessor

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/stretchr/testify/require"
)

// Issue 2269, plan PR 3, audit: under the cut a chained subtree that no store request was ever made
// for is outside the stored set for good, and every template is empty. For each path that puts a
// subtree into chainedSubtrees, a store request is made for every subtree it chains:
//
//   - processCompleteSubtree (every addNode and addNodesDirectly, so a reset's reload, a block's
//     own subtrees and reChainSubtrees): sends the request at the point it chains
//   - parallelBuildRemainderSubtrees: sends in its side-effect pass
//   - enforceBlockMaxSize and the move-back build (bulkBuildSubtrees append without a send): the
//     chain is sent by storeAndAnnounceChainedSubtrees right after, in enforceBlockMaxSize and in
//     reorgBlocks (which refuses a reorg with nothing to move forward)
//
// The tests below pin each of them. reChainSubtrees is pinned in stored_prefix_cut_test.go.

// requestRecorder records the root of every request, and adds the ones keep accepts to the set, as a
// store worker does for the stores that finish.
type requestRecorder struct {
	mu    sync.Mutex
	roots map[chainhash.Hash]struct{}
	set   *StoredSet
	keep  func(root chainhash.Hash) bool
}

func (r *requestRecorder) consume(req NewSubtreeRequest) {
	root := *req.Subtree.RootHash()

	r.mu.Lock()
	if r.roots == nil {
		r.roots = make(map[chainhash.Hash]struct{})
	}

	r.roots[root] = struct{}{}
	r.mu.Unlock()

	if r.set != nil && (r.keep == nil || r.keep(root)) {
		r.set.Add(root)
	}

	answerStored(req)
}

func (r *requestRecorder) requireRequested(t *testing.T, chain []*subtreepkg.Subtree) {
	t.Helper()

	require.NotEmpty(t, chain)

	r.mu.Lock()
	defer r.mu.Unlock()

	for i, st := range chain {
		_, ok := r.roots[*st.RootHash()]
		require.True(t, ok, "chained subtree %d was never sent to the store", i)
	}
}

func TestStoredPrefixAudit_EnforceBlockMaxSizeSendsTheSubtreesItBuilds(t *testing.T) {
	set := NewStoredSet()
	stp, h := newBlockMaxSizeProcessor(t, 64, model.BlockHeaderSize+blockMaxSizeTestBudget, WithStoredSet(set))

	addBlockMaxSizeTestTxs(t, stp, sizes(200, blockMaxSizeTestTxSize)...)

	oldRoots := make(map[chainhash.Hash]struct{})
	for _, st := range stp.chainedSubtrees {
		oldRoots[*st.RootHash()] = struct{}{}
	}

	require.NoError(t, stp.enforceBlockMaxSize(t.Context()))

	sent := make(map[chainhash.Hash]struct{})
	for _, req := range h.requests() {
		sent[*req.Subtree.RootHash()] = struct{}{}
	}

	require.NotEmpty(t, stp.chainedSubtrees)

	for i, st := range stp.chainedSubtrees {
		_, rebuilt := oldRoots[*st.RootHash()]
		require.False(t, rebuilt, "the resize built new subtrees")

		_, ok := sent[*st.RootHash()]
		require.True(t, ok, "rebuilt subtree %d was never sent to the store", i)
	}

	// and the template waits for them
	require.Empty(t, stp.GetPrecomputedMiningData().Subtrees)
	require.Nil(t, stp.incompleteSubtreeSnapshot())
}

func TestStoredPrefixAudit_AResetsReloadSendsTheSubtreesItChains(t *testing.T) {
	set := NewStoredSet()
	stp, requests := newStoredPrefixProcessor(t, 8, set)

	stp.Start(context.Background())
	t.Cleanup(func() { stp.Stop(context.Background()) })

	reload := func() error {
		batch := make([]*utxo.UnminedTransaction, 40)
		nodes := make([]subtreepkg.Node, len(batch))
		inpoints := make([]subtreepkg.TxInpoints, len(batch))

		for i := range batch {
			nodes[i] = subtreepkg.Node{Hash: chainhash.HashH([]byte{byte(i), 'r', 'l'}), Fee: 1, SizeInBytes: 100}
			inpoints[i] = voutZeroInpoints(chainhash.HashH([]byte("reload-parent")))
			batch[i] = &utxo.UnminedTransaction{Node: &nodes[i], TxInpoints: &inpoints[i], CreatedAt: i}
		}

		return stp.AddNodesDirectly(batch, true)
	}

	resp := stp.Reset(stp.GetCurrentBlockHeader(), nil, nil, false, reload)
	require.NoError(t, resp.Err)

	chain := stp.GetChainedSubtrees()
	require.NotEmpty(t, chain)

	for i, st := range chain {
		root := *st.RootHash()
		require.Eventually(t, func() bool { return requests.has(root) }, 5*time.Second, time.Millisecond,
			"chained subtree %d was never sent to the store", i)
	}

	require.Empty(t, stp.GetPrecomputedMiningData().Subtrees, "nothing stored yet")
}

// reorgBlocks refuses a reorg with no block to move forward, so every move-back build is followed by
// the send of the whole chain before the marks.
func TestStoredPrefixAudit_AReorgSendsEverySubtreeItChains(t *testing.T) {
	set := NewStoredSet()
	rec := &requestRecorder{set: set, keep: func(root chainhash.Hash) bool { return root[0]%2 == 0 }}

	cfg := smallScenarioConfig()
	cfg.storedSet = set
	cfg.storeConsumer = rec.consume
	sc := newReorgScenario(t, cfg)

	require.NoError(t, sc.run(t))

	rec.requireRequested(t, sc.stp.chainedSubtrees)

	// and no template the processor holds goes past what is stored: the oracle
	requireTemplateStored(t, sc.stp, set)
	require.Less(t, len(sc.stp.GetPrecomputedMiningData().Subtrees), len(sc.stp.chainedSubtrees),
		"half of the roots are withheld, so the template is cut")
}
