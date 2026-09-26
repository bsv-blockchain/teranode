package netsync

import (
	"bytes"
	"context"
	"io"
	"os"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/stores/blob/memory"
	"github.com/stretchr/testify/require"
)

type sinkResult struct {
	converted bool
	err       error
}

// A second copy that completes while the first is still converting is the one kept. On
// 2026-09-25 a complete 4 GB copy of block 760,331 that arrived in 1m44s was drained because a copy
// at 2.7 MB/s had started first. The first copy must stop and remove what it wrote before the
// second converts, since the two share content-addressed subtree files.
func TestASecondCopyThatCompletesFirstIsTheOneKept(t *testing.T) {
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
	n := int64(len(body))

	// The first copy arrives slowly: half its bytes, then nothing until the test says so.
	slowR, slowW := io.Pipe()
	firstDone := make(chan sinkResult, 1)

	go func() {
		converted, err := sm.pipelineBlockSink(hash, header, slowR, n)
		firstDone <- sinkResult{converted, err}
	}()

	_, err := slowW.Write(body[:len(body)/2])
	require.NoError(t, err)
	require.Eventually(t, func() bool { return sm.conversionOf(hash) != nil }, 5*time.Second, 10*time.Millisecond)

	first := sm.conversionOf(hash)

	// The second copy arrives whole.
	secondDone := make(chan sinkResult, 1)

	go func() {
		converted, err := sm.raceDuplicateCopy(hash, header, bytes.NewReader(body), n, sm.pipelineBlockSink)
		secondDone <- sinkResult{converted, err}
	}()

	require.Eventually(t, first.yielding, 5*time.Second, 10*time.Millisecond, "the second copy takes over")

	// The first copy's next bytes let it notice, stop and drain.
	go func() {
		_, _ = slowW.Write(body[len(body)/2:])
		_ = slowW.Close()
	}()

	got := <-firstDone
	require.NoError(t, got.err)
	require.False(t, got.converted, "the first copy stopped")

	got = <-secondDone
	require.NoError(t, got.err)
	require.True(t, got.converted, "the second copy converted")

	record, err := sm.blockPark.ReadConverted(ctx, hash)
	require.NoError(t, err)
	require.NotEmpty(t, record.Subtrees)

	for _, h := range record.Subtrees {
		for _, ft := range []fileformat.FileType{fileformat.FileTypeSubtree, fileformat.FileTypeSubtreeData, fileformat.FileTypeSubtreeMeta} {
			exists, err := store.Exists(ctx, h[:], ft)
			require.NoError(t, err)
			require.True(t, exists, "the first copy's cleanup removed none of the second copy's %s files", ft)
		}
	}

	require.True(t, sm.takeDrainedDuplicate(hash), "the first copy is accounted as the drained one")
	requireNoSideFiles(t, sm.blockPark.dir)
}

// When the first copy has already read its last transaction, it wins and the second is dropped.
func TestAFirstCopyThatFinishesFirstIsTheOneKept(t *testing.T) {
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
	n := int64(len(body))

	// Stand in for a first copy that has claimed the finish.
	ctl := sm.startConversion(hash)
	require.True(t, ctl.finish())

	converted, err := sm.raceDuplicateCopy(hash, header, bytes.NewReader(body), n, sm.pipelineBlockSink)
	require.NoError(t, err)
	require.False(t, converted)
	require.True(t, sm.takeDrainedDuplicate(hash))

	_, err = sm.blockPark.ReadConverted(ctx, hash)
	require.Error(t, err, "the second copy wrote nothing")
	requireNoSideFiles(t, sm.blockPark.dir)
}

func TestConversionCtlHandsTheBlockToExactlyOneCopy(t *testing.T) {
	c := &conversionCtl{cleaned: make(chan struct{})}
	require.True(t, c.takeOver())
	require.False(t, c.finish(), "a conversion taken over cannot then finish")

	c = &conversionCtl{cleaned: make(chan struct{})}
	require.True(t, c.finish())
	require.False(t, c.takeOver(), "a conversion that claimed the finish cannot be taken over")
}

func requireNoSideFiles(t *testing.T, dir string) {
	t.Helper()

	entries, err := os.ReadDir(dir)
	require.NoError(t, err)

	for _, e := range entries {
		require.False(t, isDuplicateCopyFile(e.Name()), "side file %s left behind", e.Name())
	}
}
