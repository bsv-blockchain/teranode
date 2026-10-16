package netsync

import (
	"bytes"
	"context"
	"io"
	"net/url"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	peerpkg "github.com/bsv-blockchain/teranode/services/legacy/peer"
	"github.com/bsv-blockchain/teranode/stores/blob"
	"github.com/bsv-blockchain/teranode/stores/blob/file"
	"github.com/bsv-blockchain/teranode/stores/blob/memory"
	"github.com/bsv-blockchain/teranode/stores/blob/options"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

// A failed delivery removes only the subtree files it created. A subtree file is keyed by its own
// root hash, so a corrupt redelivery of a block already parked shares every subtree before the
// corrupt transaction with the honest copy. Those files were already on disk; the failed delivery
// must leave them, or the parked block is left pointing at subtree data that no longer exists.
func TestAFailedRedeliveryKeepsTheParkedBlocksSubtreeFiles(t *testing.T) {
	stores := map[string]func(t *testing.T) blob.Store{
		"memory": func(t *testing.T) blob.Store { return memory.New() },
		// The file store takes the streamed data file path (PendingFile), the memory store the
		// buffered one, so both ways an existing data file can be met are covered.
		"file": func(t *testing.T) blob.Store {
			storeURL, err := url.Parse("file://" + t.TempDir())
			require.NoError(t, err)

			store, err := file.New(ulogger.TestLogger{}, storeURL, options.WithBlobDeletionScheduler(&recordingDeletionScheduler{}))
			require.NoError(t, err)

			return store
		},
	}

	for name, newStore := range stores {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			store := newStore(t)
			sm := newPipelineParkManager(t, store, 8)

			blk := wireBlockWithTxs(t, 40, false)
			pipelineHeaderFixture(t, sm, blk)
			proveBlockOrigin(t, sm, blk)

			body := blockBodyBytes(t, blk)
			hash := *blk.Hash()
			header := &blk.MsgBlock().Header
			n := sinkPayloadLen(body)

			converted, err := sm.pipelineBlockSink(hash, header, bytes.NewReader(body), n)
			require.NoError(t, err)
			require.True(t, converted)

			record, err := sm.blockPark.ReadConverted(ctx, hash)
			require.NoError(t, err)
			require.Greater(t, len(record.Subtrees), 1, "the corrupt copy must share subtrees with the honest one")

			// The final transaction's lock time is the body's last byte: changing it changes only
			// the final subtree, and the merkle root no longer matches.
			corrupt := append([]byte(nil), body...)
			corrupt[len(corrupt)-1] ^= 0xff

			converted, err = sm.pipelineBlockSink(hash, header, bytes.NewReader(corrupt), n)
			require.Error(t, err)
			require.False(t, converted)

			_, err = sm.blockPark.ReadConverted(ctx, hash)
			require.NoError(t, err, "the honest record survives")

			for _, h := range record.Subtrees {
				for _, ft := range []fileformat.FileType{fileformat.FileTypeSubtreeToCheck, fileformat.FileTypeSubtreeData, fileformat.FileTypeSubtreeMeta} {
					exists, err := store.Exists(ctx, h[:], ft)
					require.NoError(t, err)
					require.True(t, exists, "the failed delivery removed the parked block's %s file for subtree %s", ft, h)
				}
			}
		})
	}
}

// A copy that took over must not wait forever when the copy it took over from fails instead of
// yielding. The failing copy cleans up and returns through its error path, not through
// yieldToFasterCopy, and still has to tell the waiting copy that its files are gone.
func TestADuplicateConvertsWhenTheCopyItTookOverFromFails(t *testing.T) {
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

	owner := owingPeer(t, sm, hash, 253)
	secondDone := make(chan sinkResult, 1)

	go func() {
		converted, err := sm.raceDuplicateCopy(hash, header, peerpkg.NewDeliveryReader(bytes.NewReader(body), owner), n, sm.pipelineBlockSink)
		secondDone <- sinkResult{converted, err}
	}()

	require.Eventually(t, first.yielding, 5*time.Second, 10*time.Millisecond, "the second copy takes over")

	// The first copy is truncated: its read fails before it gets back to the yield check.
	require.NoError(t, slowW.CloseWithError(io.ErrUnexpectedEOF))

	got := <-firstDone
	require.Error(t, got.err)
	require.False(t, got.converted)

	select {
	case got = <-secondDone:
	case <-time.After(10 * time.Second):
		t.Fatal("the second copy is still waiting for a cleanup signal the failed copy never sent")
	}

	require.NoError(t, got.err)
	require.True(t, got.converted, "the second copy converted")

	_, err = sm.blockPark.ReadConverted(ctx, hash)
	require.NoError(t, err)
	requireNoSideFiles(t, sm.blockPark.dir)
}

// A redelivery of a parked block that converts cleanly and is then refused by the wire layer must
// not take the parked block with it. The wire layer refuses a body that left declared bytes
// unread after the sink succeeded (readBlockMessage's short-body check), and a peer can cause
// that by appending bytes after the last transaction. The redelivery's conversion overwrites the
// parked record with an identical one, so "this delivery converted" does not mean "this delivery
// owns what is under the hash".
func TestARefusedRedeliveryKeepsTheParkedBlock(t *testing.T) {
	ctx := context.Background()
	store := memory.New()
	sm := newPipelineParkManager(t, store, 8)

	blk := wireBlockWithTxs(t, 40, false)
	pipelineHeaderFixture(t, sm, blk)
	proveBlockOrigin(t, sm, blk)

	body := blockBodyBytes(t, blk)
	hash := *blk.Hash()
	header := &blk.MsgBlock().Header

	converted, err := sm.pipelineBlockSink(hash, header, bytes.NewReader(body), sinkPayloadLen(body))
	require.NoError(t, err)
	require.True(t, converted)
	require.True(t, sm.blockPark.AdoptWritten(parkedBlock{hash: hash, prevBlock: header.PrevBlock, wireSize: int64(len(body))}))

	record, err := sm.blockPark.ReadConverted(ctx, hash)
	require.NoError(t, err)

	// The same body with junk after it, more than the stream's read-ahead buffer can swallow.
	padded := append(append([]byte(nil), body...), make([]byte, 4<<20)...)
	lr := &io.LimitedReader{R: bytes.NewReader(padded), N: int64(len(padded))}

	converted, err = sm.pipelineBlockSink(hash, header, lr, sinkPayloadLen(padded))
	require.Error(t, err, "the sink refuses a body that does not end at its last transaction")
	require.True(t, errors.IsBlockCorrupt(err), "corrupt, not invalid: the block itself is fine")
	require.False(t, converted, "so the wire layer's cleanup has nothing to remove")

	// What readBlockMessage does with a sink failure.
	require.NoError(t, sm.pipelineBlockDelete(hash, converted))

	require.True(t, sm.blockPark.Has(hash), "the parked block is still parked")

	_, err = sm.blockPark.ReadConverted(ctx, hash)
	require.NoError(t, err, "the parked block's record survives")

	for _, h := range record.Subtrees {
		for _, ft := range []fileformat.FileType{fileformat.FileTypeSubtreeToCheck, fileformat.FileTypeSubtreeData, fileformat.FileTypeSubtreeMeta} {
			exists, err := store.Exists(ctx, h[:], ft)
			require.NoError(t, err)
			require.True(t, exists, "the refused redelivery removed the parked block's %s file for subtree %s", ft, h)
		}
	}
}
