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

// TestGapBV029_SubtreesSetNotCommittedAheadOfQueuedSubtreeWrites covers GAP-BV-029 (BV-QUICK-021),
// bitcoin-sv/teranode#4978.
//
// A block must not be stored with subtrees_set=true unless all of its .subtree files are present.
// Here the async quick route (tryQuickValidation with a real write worker) validates an honest block
// while the store fails the .subtree write. quickValidateBlockAsync waits for the block's queued
// writes before commitBlock, sees the failed one, and refuses to commit; tryQuickValidation then
// aborts with the subtree-writes-not-landed marker rather than falling through.
//
// Mutation proof: removing the awaitSubtreeWrites call before commitBlock stores the block with
// subtrees_set and no file, and the test fails on the not-committed assertion.
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

	require.True(t, isSubtreeWritesNotLanded(quickErr), "tryQuickValidation must surface the failed .subtree write: %v", quickErr)

	_, meta, err := h.chain.GetBlockHeader(h.ctx, block.Hash())
	if err == nil {
		require.False(t, meta.SubtreesSet, "block stored with subtrees_set=true but its .subtree file was never written")
	}

	exists, err := h.chain.GetBlockExists(h.ctx, block.Hash())
	require.NoError(t, err)
	require.False(t, exists, "block must not be committed ahead of its failed .subtree write")
}
