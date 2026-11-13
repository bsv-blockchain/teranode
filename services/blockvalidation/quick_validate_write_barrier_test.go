package blockvalidation

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/stretchr/testify/require"
)

// awaitSubtreeWrites gates commitBlock on the async route (bitcoin-sv/teranode#4978). These cases
// pin its three outcomes; the end-to-end case through tryQuickValidation is
// TestGapBV029_SubtreesSetNotCommittedAheadOfQueuedSubtreeWrites.

func testBarrierBlock() *model.Block {
	return &model.Block{Header: &model.BlockHeader{HashPrevBlock: &chainhash.Hash{}, HashMerkleRoot: &chainhash.Hash{}}}
}

func TestAwaitSubtreeWrites_NilBarrier(t *testing.T) {
	require.NoError(t, awaitSubtreeWrites(context.Background(), testBarrierBlock(), nil))
}

func TestAwaitSubtreeWrites_AllLanded(t *testing.T) {
	barrier := &subtreeWriteBarrier{}
	barrier.Add(1)
	barrier.Done()

	require.NoError(t, awaitSubtreeWrites(context.Background(), testBarrierBlock(), barrier))
}

func TestAwaitSubtreeWrites_OneUnwritten(t *testing.T) {
	barrier := &subtreeWriteBarrier{}
	barrier.Add(1)
	barrier.unwritten.Store(true)
	barrier.Done()

	err := awaitSubtreeWrites(context.Background(), testBarrierBlock(), barrier)
	require.Error(t, err)
	require.True(t, isSubtreeWritesNotLanded(err))
}

// A job no worker will ever receive keeps the count above zero; a cancelled context must
// still release the caller, and with the marker, since the block's files are not known to be
// on disk.
func TestAwaitSubtreeWrites_ContextEndsFirst(t *testing.T) {
	barrier := &subtreeWriteBarrier{}
	barrier.Add(1)
	t.Cleanup(barrier.Done)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := awaitSubtreeWrites(ctx, testBarrierBlock(), barrier)
	require.Error(t, err)
	require.True(t, isSubtreeWritesNotLanded(err))
}
