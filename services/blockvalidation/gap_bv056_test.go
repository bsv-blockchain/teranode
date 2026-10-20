//go:build bvgaps

package blockvalidation

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

// gap056FailingUnlockStore is the real sqlitememory UTXO store except that the post-commit unlock
// (SetLocked(..., false)) fails.
type gap056FailingUnlockStore struct {
	utxo.Store

	unlockCalls int
}

func (s *gap056FailingUnlockStore) SetLocked(ctx context.Context, txHashes []chainhash.Hash, value bool) error {
	if !value {
		s.unlockCalls++
		return errors.NewProcessingError("gap056: simulated unlock failure after AddBlock")
	}

	return s.Store.SetLocked(ctx, txHashes, value)
}

// TestGapBV056_PostCommitUnlockFailureKeepsCommittedBlocksSubtreeFiles covers GAP-BV-056
// (BV-QUICK-015, BV-QUICK-021, BV-COMMIT-002).
//
// Spec-correct behaviour: once AddBlock has committed a block with subtrees_set and mined_set, any
// later error on the quick route (here the post-commit unlock SetLocked(false) failing) is a
// post-commit error. The block's .subtree files must never be deleted, and the unlock must be
// retried so the outputs do not stay locked. Expected to fail today: commitBlock
// (quick_validate.go) returns the unlock error, and tryQuickValidation (catchup.go, local-fault
// branch) then calls removeCatchupSubtreeFiles on the attempt's freshly written .subtree files and
// falls through to normal validation, which finds the block stored and does nothing.
func TestGapBV056_PostCommitUnlockFailureKeepsCommittedBlocksSubtreeFiles(t *testing.T) {
	h := newPreBindHarness(t, nil)

	failing := &gap056FailingUnlockStore{Store: h.utxoStore}
	h.bv.utxoStore = failing

	coinbase := preBindCoinbase(t, 0x56)
	parent := h.storeGenuineParent(0x56)
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

	_, _ = server.tryQuickValidation(h.ctx, block, catchupCtx, "peer", "http://peer", writeJobsChan, nil)

	close(writeJobsChan)
	require.NoError(t, g.Wait())

	// Preconditions: the unlock was attempted (so the failure was injected), and AddBlock committed
	// the block with subtrees_set.
	require.Positive(t, failing.unlockCalls, "precondition: the post-commit unlock must have been attempted")

	_, meta, err := h.chain.GetBlockHeader(h.ctx, block.Hash())
	require.NoError(t, err, "precondition: the block must have been committed by AddBlock")
	require.True(t, meta.SubtreesSet, "precondition: the quick route commits with subtrees_set=true")

	// Spec assertion 1: a committed block marked subtrees_set must still have its .subtree file.
	exists, err := h.subtreeStore.Exists(h.ctx, served.RootHash()[:], fileformat.FileTypeSubtree)
	require.NoError(t, err)
	require.True(t, exists, "committed block has subtrees_set=true but its .subtree file was deleted (post-commit error treated as local failure)")

	// Spec assertion 2: the outputs must not remain locked.
	created, err := h.utxoStore.Get(h.ctx, child.TxIDChainHash(), fields.Locked)
	require.NoError(t, err)
	require.False(t, created.Locked, "committed block's outputs must not stay locked after the unlock failure")
}
