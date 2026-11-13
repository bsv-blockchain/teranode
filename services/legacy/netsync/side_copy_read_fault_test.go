package netsync

import (
	"bytes"
	"context"
	"io"
	"io/fs"
	"syscall"
	"testing"
	"time"

	peerpkg "github.com/bsv-blockchain/teranode/services/legacy/peer"
	"github.com/bsv-blockchain/teranode/stores/blob/memory"
	"github.com/stretchr/testify/require"
)

// A side file is on this node's disk. A failed read of it was returned as the bare *fs.PathError,
// which isLocalSinkFault does not know as ours, and the read loop rejected and disconnected the
// peer that sent the copy.

// failingAfter reads from r until limit bytes have gone, then fails as a disk read fails.
type failingAfter struct {
	r     io.Reader
	limit int
	read  int
}

func (f *failingAfter) Read(p []byte) (int, error) {
	if f.read >= f.limit {
		return 0, &fs.PathError{Op: "read", Path: "side.copy", Err: syscall.EIO}
	}

	if len(p) > f.limit-f.read {
		p = p[:f.limit-f.read]
	}

	n, err := f.r.Read(p)
	f.read += n

	return n, err
}

// A read fault in the merkle root pass drops the side copy and keeps its peer: the honest copy
// converts.
func TestASideFileReadFaultInTheRootPassKeepsThePeer(t *testing.T) {
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

	owner := owingPeer(t, sm, hash, 246)

	sm.sideCopyReader = func(r io.Reader) io.Reader { return &failingAfter{r: r, limit: len(body) / 2} }

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

	secondDone := make(chan sinkResult, 1)

	go func() {
		r := peerpkg.NewDeliveryReader(bytes.NewReader(body), owner)
		converted, err := sm.raceDuplicateCopy(hash, header, r, n, sm.pipelineBlockSink)
		converted, err = sm.absorbLocalSinkFault(hash, r, converted, err)
		secondDone <- sinkResult{converted, err}
	}()

	// A copy that took over waits for the first copy's next bytes, so a takeover shows as no answer.
	var second sinkResult
	select {
	case second = <-secondDone:
	case <-time.After(10 * time.Second):
		require.Fail(t, "the side copy took over although its root pass could not read it", "yielding: %v", first.yielding())
	}

	require.NoError(t, second.err, "a read fault on our disk is not the peer's: the read loop keeps the peer")
	require.False(t, second.converted)
	require.True(t, sm.takeDrainedDuplicate(hash), "the side copy is accounted as drained")
	require.False(t, first.yielding(), "the honest conversion was never asked to stop")
	require.True(t, owner.Connected())

	go func() {
		_, _ = slowW.Write(body[len(body)/2:])
		_ = slowW.Close()
	}()

	got := <-firstDone
	require.NoError(t, got.err)
	require.True(t, got.converted, "the honest copy converts")

	_, err = sm.blockPark.ReadConverted(ctx, hash)
	require.NoError(t, err)
	requireNoSideFiles(t, sm.blockPark.dir)
}

// A read fault after the side copy took over is a storage fault of this node: absorbLocalSinkFault
// keeps the peer, and the block is asked for again.
func TestASideFileReadFaultAfterTheTakeoverKeepsThePeer(t *testing.T) {
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

	owner := owingPeer(t, sm, hash, 245)

	// The root pass reads the file well; the conversion's read fails half way.
	reads := 0
	sm.sideCopyReader = func(r io.Reader) io.Reader {
		reads++
		if reads == 1 {
			return r
		}

		return &failingAfter{r: r, limit: len(body) / 2}
	}

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

	secondDone := make(chan sinkResult, 1)

	go func() {
		r := peerpkg.NewDeliveryReader(bytes.NewReader(body), owner)
		converted, err := sm.raceDuplicateCopy(hash, header, r, n, sm.pipelineBlockSink)
		converted, err = sm.absorbLocalSinkFault(hash, r, converted, err)
		secondDone <- sinkResult{converted, err}
	}()

	require.Eventually(t, first.yielding, 5*time.Second, 10*time.Millisecond, "the second copy takes over")

	go func() {
		_, _ = slowW.Write(body[len(body)/2:])
		_ = slowW.Close()
	}()

	got := <-firstDone
	require.NoError(t, got.err)
	require.False(t, got.converted, "the first copy stopped")

	got = <-secondDone
	require.NoError(t, got.err, "a read fault on our disk is not the peer's: the read loop keeps the peer")
	require.False(t, got.converted)
	require.True(t, owner.Connected())

	_, err = sm.blockPark.ReadConverted(ctx, hash)
	require.Error(t, err, "no copy converted")
	requireNoSideFiles(t, sm.blockPark.dir)
}
