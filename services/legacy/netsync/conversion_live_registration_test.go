package netsync

import (
	"bytes"
	"context"
	"testing"

	"github.com/bsv-blockchain/teranode/stores/blob/memory"
	"github.com/stretchr/testify/require"
)

// A copy that took over converts from its side file outside the admission slot, so a later copy
// can be admitted while it converts. That copy used to register over the live conversion and
// convert beside it, writing the same content-addressed subtree files at the same time (review
// of 2026-10-12). It is now drained, and the live conversion keeps the block.
func TestALiveConversionIsNotReplacedByALaterCopy(t *testing.T) {
	ctx := context.Background()
	store := memory.New()
	sm := newPipelineParkManager(t, store, 8)
	sm.blockPark.dir = t.TempDir()

	blk := wireBlockWithTxs(t, 40, false)
	pipelineHeaderFixture(t, sm, blk)
	proveBlockOrigin(t, sm, blk)

	body := blockBodyBytes(t, blk)
	hash := *blk.Hash()
	header := &blk.MsgBlock().Header
	n := sinkPayloadLen(body)

	// Stand in for the copy that took over and is converting from its side file.
	live := sm.startConversion(hash, nil)
	require.NotNil(t, live)

	converted, err := sm.pipelineBlockSink(hash, header, bytes.NewReader(body), n)
	require.NoError(t, err)
	require.False(t, converted, "the later copy does not convert beside the live one")
	require.Same(t, live, sm.conversionOf(hash), "the live conversion keeps its registration")
	require.True(t, sm.takeDrainedDuplicate(hash), "the later copy is accounted as drained")

	_, err = sm.blockPark.ReadConverted(ctx, hash)
	require.Error(t, err, "the later copy wrote no record")

	// Once the live conversion has been taken over, a copy may register in its place.
	require.True(t, live.takeOver())
	require.NotNil(t, sm.startConversion(hash, nil))
}
