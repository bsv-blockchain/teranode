package netsync

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/stores/blob"
	"github.com/bsv-blockchain/teranode/stores/blob/memory"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

// A test-strength note, not a bug, for every test below that reaches
// sm.blockPark.store directly (.Exists/.Get/.Set): the in-memory store
// (stores/blob/memory) ignores the subdirectory and hash-prefix options that
// parkOpts always passes in production, so a key built here without those
// options happens to land on the same string as one built with them. Against
// the real file store the park actually runs on, that would not hold, and the
// mismatched-record test in particular (TestBlockPark_ReadConvertedRefusesAMismatchedRecord)
// would pass for the wrong reason there — a plain not-found, rather than the
// hash-mismatch refusal it is meant to exercise. Nothing here should be read
// as proof of the on-disk layout; that is what the file-store-backed tests in
// block_park_test.go are for.
//
// TestPipelineSink_ParksARecordNotAPhantom is the defect this task closes. Before
// it, a pipelined block made the park adopt an entry for a body that was never
// written: the park charged its byte budget for a blob that did not exist and the
// drain would later fail to read it. The record must actually be on disk.
func TestPipelineSink_ParksARecordNotAPhantom(t *testing.T) {
	ctx := context.Background()
	store := memory.New()
	sm := newPipelineParkManager(t, store, 8)

	blk := wireBlockWithTxs(t, 20, false)
	pipelineHeaderFixture(t, sm, blk)

	header := &blk.MsgBlock().Header
	body := blockBodyBytes(t, blk)

	converted, err := sm.pipelineBlockSink(*blk.Hash(), header, bytes.NewReader(body), int64(len(body)))
	require.NoError(t, err, "a well-formed block below the checkpoint must convert cleanly")
	require.True(t, converted, "sanity: this test needs an actual conversion, or it asserts nothing about the park record")

	exists, err := sm.blockPark.store.Exists(ctx, blk.Hash()[:], fileformat.FileTypeBlock)
	require.NoError(t, err)
	require.True(t, exists, "the converted record must be on disk before anything adopts it")

	noWholeBlock, err := sm.blockPark.store.Exists(ctx, blk.Hash()[:], fileformat.FileTypeMsgBlock)
	require.NoError(t, err)
	require.False(t, noWholeBlock, "and the whole block must NOT be, or the pipeline saved nothing")
}

// TestPipelineSink_TheParkedRecordIsTinyCompared pins the reason the pipeline
// exists. The park's budget is charged per entry, so a record that is not
// dramatically smaller than the block buys nothing.
func TestPipelineSink_TheParkedRecordIsTinyCompared(t *testing.T) {
	ctx := context.Background()
	store := memory.New()
	sm := newPipelineParkManager(t, store, 8)

	blk := wireBlockWithTxs(t, 200, false)
	pipelineHeaderFixture(t, sm, blk)

	header := &blk.MsgBlock().Header
	body := blockBodyBytes(t, blk)

	converted, err := sm.pipelineBlockSink(*blk.Hash(), header, bytes.NewReader(body), int64(len(body)))
	require.NoError(t, err, "a well-formed block below the checkpoint must convert cleanly")
	require.True(t, converted, "sanity: this test needs an actual conversion, or it asserts nothing about the park record")

	raw, err := sm.blockPark.store.Get(ctx, blk.Hash()[:], fileformat.FileTypeBlock)
	require.NoError(t, err)

	require.Less(t, len(raw), len(body)/4,
		"a parked record must be a small fraction of the block it stands for, or parking it saves nothing")
}

// TestBlockPark_ReadConvertedRoundTrips pins that what the park writes is what the
// committer will later read.
func TestBlockPark_ReadConvertedRoundTrips(t *testing.T) {
	ctx := context.Background()
	store := memory.New()
	sm := newPipelineParkManager(t, store, 8)

	blk := wireBlockWithTxs(t, 20, false)
	pipelineHeaderFixture(t, sm, blk)

	header := &blk.MsgBlock().Header
	body := blockBodyBytes(t, blk)

	converted, err := sm.pipelineBlockSink(*blk.Hash(), header, bytes.NewReader(body), int64(len(body)))
	require.NoError(t, err, "a well-formed block below the checkpoint must convert cleanly")
	require.True(t, converted, "sanity: this test needs an actual conversion, or it asserts nothing about the park record")

	back, err := sm.blockPark.ReadConverted(ctx, *blk.Hash())
	require.NoError(t, err)
	require.NotNil(t, back)
	require.Equal(t, blk.Hash().String(), back.Header.Hash().String(),
		"the park must return the block the key names")
}

// TestBlockPark_ReadConvertedRefusesAMismatchedRecord pins the same check the
// whole-block read already makes: a blob stored under one hash whose content
// hashes to another is corruption, and must never be handed to the committer as
// if it were the block asked for.
func TestBlockPark_ReadConvertedRefusesAMismatchedRecord(t *testing.T) {
	ctx := context.Background()
	store := memory.New()
	sm := newPipelineParkManager(t, store, 8)

	blkA := wireBlockWithTxs(t, 20, false)
	pipelineHeaderFixture(t, sm, blkA)

	headerA := &blkA.MsgBlock().Header
	bodyA := blockBodyBytes(t, blkA)
	converted, err := sm.pipelineBlockSink(*blkA.Hash(), headerA, bytes.NewReader(bodyA), int64(len(bodyA)))
	require.NoError(t, err, "a well-formed block below the checkpoint must convert cleanly")
	require.True(t, converted, "sanity: this test needs an actual conversion, or it asserts nothing about the park record")

	raw, err := sm.blockPark.store.Get(ctx, blkA.Hash()[:], fileformat.FileTypeBlock)
	require.NoError(t, err)

	wrongKey := chainhash.Hash{0xde, 0xad}
	require.NoError(t, sm.blockPark.store.Set(ctx, wrongKey[:], fileformat.FileTypeBlock, raw))

	_, err = sm.blockPark.ReadConverted(ctx, wrongKey)
	require.Error(t, err)
	require.Contains(t, err.Error(), "hash")
}

// newPipelineParkManager is newPipelineManager plus an enabled park sharing the
// same store, following newPipelineManagerWithPark's construction
// (pipeline_parent_height_test.go). One store rather than two: Task 2 puts a
// converted record in the same blob store the subtree writer already uses —
// there is no second store for it — so every test that reads a converted
// record back through the park, rather than through the now-deleted
// pipelineVerified map, needs this rather than newPipelineManagerWithPark's
// two-store shape.
func newPipelineParkManager(t *testing.T, store blob.Store, maxItems int) *SyncManager {
	t.Helper()

	sm := newPipelineManager(t, store, maxItems)
	sm.blockPark = &blockPark{
		logger:       ulogger.TestLogger{},
		store:        store,
		storeTimeout: 5 * time.Second,
		entries:      make(map[chainhash.Hash]*parkedBlock),
		children:     make(map[chainhash.Hash][]chainhash.Hash),
		charged:      make(map[chainhash.Hash]int64),
	}

	return sm
}
