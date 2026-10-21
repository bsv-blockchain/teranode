//go:build bvgaps

package blockvalidation

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/stores/blob"
	blobmemory "github.com/bsv-blockchain/teranode/stores/blob/memory"
	bloboptions "github.com/bsv-blockchain/teranode/stores/blob/options"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

// gap029FailingSubtreeSetStore is a real in-memory blob store whose Set fails for FileTypeSubtree,
// modelling the async write worker's store failing after the block has been queued.
type gap029FailingSubtreeSetStore struct {
	blob.Store

	failedSets atomic.Int32
}

func (s *gap029FailingSubtreeSetStore) Set(ctx context.Context, key []byte, fileType fileformat.FileType, value []byte, opts ...bloboptions.FileOption) error {
	if fileType == fileformat.FileTypeSubtree {
		s.failedSets.Add(1)
		return errors.NewProcessingError("gap029: simulated .subtree write failure")
	}

	return s.Store.Set(ctx, key, fileType, value, opts...)
}

// TestGapBV029_SubtreesSetNotCommittedAheadOfQueuedSubtreeWrites covers GAP-BV-029 (BV-QUICK-021).
//
// Spec-correct behaviour: a block must not be stored with subtrees_set=true unless all of its
// .subtree files are present, so the async route must either barrier the block's writes before
// AddBlock or repair afterwards. Here the async quick route (tryQuickValidation with a real write
// worker) validates a block while the store fails the .subtree write; whatever the route does with
// the block, a block that ends up stored with subtrees_set=true must have its .subtree file.
// Expected to fail today: commitBlock (quick_validate.go) calls AddBlock with WithSubtreesSet(true)
// without waiting for the queued jobs (catchup.go tryQuickValidation only waits on the write
// WaitGroup on the failure path), so the commit lands and the failed write leaves no file.
func TestGapBV029_SubtreesSetNotCommittedAheadOfQueuedSubtreeWrites(t *testing.T) {
	failing := &gap029FailingSubtreeSetStore{Store: blobmemory.New()}
	h := newPreBindHarness(t, failing)

	coinbase := preBindCoinbase(t, 0x29)
	parent := h.storeGenuineParent(0x29)
	child := preBindSpendOf(t, parent, 9_000)

	served := h.oneSubtreeBody(coinbase, child)

	block := h.newPreBindBlock(coinbase,
		[]*chainhash.Hash{served.RootHash()},
		composeBlockMerkleRoot(t, []chainhash.Hash{coinbaseSubstitutedRoot(t, served, coinbase)}),
		2)

	server, catchupCtx := h.newAbortServer()
	catchupCtx.blockUpTo = block

	writeJobsChan := make(chan *SubtreeWriteJob, 16)

	g, gCtx := errgroup.WithContext(h.ctx)
	g.Go(func() error { return h.bv.subtreeWriteWorker(gCtx, writeJobsChan) })

	_, quickErr := server.tryQuickValidation(h.ctx, block, catchupCtx, "peer", "http://peer", writeJobsChan, nil)
	// The body is honest, so the only acceptable failure is the injected write fault surfacing;
	// a consensus or corrupt verdict would mean the scenario never reached the commit.
	require.False(t, errors.IsBlockCorrupt(quickErr), "precondition: honest body must not be judged corrupt: %v", quickErr)
	require.False(t, errors.Is(quickErr, errors.ErrBlockInvalid), "precondition: honest body must not be judged invalid: %v", quickErr)

	close(writeJobsChan)
	// The worker's own error is expected (and ignored): the failed write is the scenario.
	_ = g.Wait()

	// Precondition: the writer really attempted (and failed) the .subtree write.
	require.Positive(t, failing.failedSets.Load(), "precondition: the worker must have attempted the .subtree write")

	_, meta, err := h.chain.GetBlockHeader(h.ctx, block.Hash())
	if err != nil {
		// Spec-correct alternative: the block was not committed ahead of its writes. That is only
		// acceptable if the write failure was surfaced to the caller rather than swallowed.
		require.Error(t, quickErr, "block not committed, yet quick validation reported no error for the failed .subtree write")
		return
	}

	if meta.SubtreesSet {
		exists, err := h.subtreeStore.Exists(h.ctx, served.RootHash()[:], fileformat.FileTypeSubtree)
		require.NoError(t, err)
		require.True(t, exists, "block stored with subtrees_set=true but its .subtree file was never written")
	}
}
