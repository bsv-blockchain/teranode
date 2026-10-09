package netsync

import (
	"bytes"
	"context"
	"io"
	"os"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/services/legacy/bsvutil"
	peerpkg "github.com/bsv-blockchain/teranode/services/legacy/peer"
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
	n := sinkPayloadLen(body)

	// The second copy comes from a peer the ledger says owes the block, so it may race.
	owner := owingPeer(t, sm, hash, 249)

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
		converted, err := sm.raceDuplicateCopy(hash, header, peerpkg.NewDeliveryReader(bytes.NewReader(body), owner), n, sm.pipelineBlockSink)
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
		for _, ft := range []fileformat.FileType{fileformat.FileTypeSubtreeToCheck, fileformat.FileTypeSubtreeData, fileformat.FileTypeSubtreeMeta} {
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
	n := sinkPayloadLen(body)

	// Stand in for a first copy that has claimed the finish.
	ctl := sm.startConversion(hash, nil)
	require.True(t, ctl.finish())

	owner := owingPeer(t, sm, hash, 250)

	converted, err := sm.raceDuplicateCopy(hash, header, peerpkg.NewDeliveryReader(bytes.NewReader(body), owner), n, sm.pipelineBlockSink)
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

// A copy from a peer that does not owe the block never interrupts the owner's conversion. The gate
// admits a body for any requested hash from any peer, so a peer that knew a requested block's
// header could otherwise make the honest multi-gigabyte stream stop and drain, then fail as corrupt
// on its own junk and come back to do it again. The owner's copy converts and parks; the other
// copy is drained, as every second copy was before the race existed.
func TestANonOwnersCopyNeverInterruptsTheOwnersConversion(t *testing.T) {
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

	owner := owingPeer(t, sm, hash, 251)
	stranger, _, _ := connectRacePeer(t, 252, 1000)

	sink := sm.admitPipelineSink(sm.pipelineBlockSink)

	// The owner's copy arrives slowly through the same admission path a peer's read loop uses.
	slowR, slowW := io.Pipe()
	firstDone := make(chan sinkResult, 1)

	go func() {
		converted, err := sink(hash, header, peerpkg.NewDeliveryReader(slowR, owner), n)
		firstDone <- sinkResult{converted, err}
	}()

	_, err := slowW.Write(body[:len(body)/2])
	require.NoError(t, err)
	require.Eventually(t, func() bool { return sm.conversionOf(hash) != nil }, 5*time.Second, 10*time.Millisecond)

	first := sm.conversionOf(hash)

	// A whole copy from a peer the ledger does not hold the request with.
	secondDone := make(chan sinkResult, 1)

	go func() {
		converted, err := sink(hash, header, peerpkg.NewDeliveryReader(bytes.NewReader(body), stranger), n)
		secondDone <- sinkResult{converted, err}
	}()

	var second sinkResult

	select {
	case second = <-secondDone:
	case <-time.After(5 * time.Second):
		t.Fatalf("the stranger's copy is still running; it took over the owner's conversion: %v", first.yielding())
	}

	require.NoError(t, second.err)
	require.False(t, second.converted, "the stranger's copy is drained")
	require.False(t, first.yielding(), "the stranger's copy did not take over")
	require.True(t, sm.takeDrainedDuplicate(hash), "the stranger's copy is accounted as drained")
	requireNoSideFiles(t, sm.blockPark.dir)

	go func() {
		_, _ = slowW.Write(body[len(body)/2:])
		_ = slowW.Close()
	}()

	got := <-firstDone
	require.NoError(t, got.err)
	require.True(t, got.converted, "the owner's copy converted")

	record, err := sm.blockPark.ReadConverted(ctx, hash)
	require.NoError(t, err)
	require.NotEmpty(t, record.Subtrees, "the owner's block is parked")
}

// A non-owner's copy that arrives before the owner's never takes the block's conversion slot. The
// gate admits a body for any requested hash from any peer, so a peer that knew a requested block's
// header could send a well-formed body with the wrong merkle root just ahead of the owner. That
// copy used to start converting: the owner's honest copy became the duplicate and downloaded in
// full to a side file, the junk reached its last transaction and claimed the finish, then failed
// its root check, and the owner's copy, unable to take over, was thrown away. The non-owner's
// copy is drained before it writes anything, and the owner's copy converts and parks.
func TestANonOwnersCopyArrivingFirstNeverTakesTheConversionSlot(t *testing.T) {
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

	owner := owingPeer(t, sm, hash, 254)
	stranger, _, _ := connectRacePeer(t, 255, 1000)

	// Well formed, but the final transaction's lock time is changed, so the merkle root is wrong.
	junk := append([]byte(nil), body...)
	junk[len(junk)-1] ^= 0xff

	sink := sm.admitPipelineSink(sm.pipelineBlockSink)

	// The stranger's whole junk copy is on the wire and its connection stays open, so a copy that
	// started converting would hold the slot.
	junkR, junkW := io.Pipe()
	junkDone := make(chan sinkResult, 1)

	go func() {
		converted, err := sink(hash, header, peerpkg.NewDeliveryReader(junkR, stranger), n)
		junkDone <- sinkResult{converted, err}
	}()

	_, err := junkW.Write(junk)
	require.NoError(t, err)

	require.Never(t, func() bool { return sm.conversionOf(hash) != nil || len(store.ListKeys()) > 0 },
		500*time.Millisecond, 10*time.Millisecond, "the stranger's copy took the conversion slot or wrote files")

	// The owner's copy arrives while the stranger's connection is still open.
	converted, err := sink(hash, header, peerpkg.NewDeliveryReader(bytes.NewReader(body), owner), n)
	require.NoError(t, err)
	require.True(t, converted, "the owner's copy converted")

	require.NoError(t, junkW.Close())

	var got sinkResult

	select {
	case got = <-junkDone:
	case <-time.After(5 * time.Second):
		t.Fatal("the stranger's copy is still running")
	}

	require.NoError(t, got.err, "the stranger's copy is drained with the connection kept")
	require.False(t, got.converted)
	require.True(t, sm.takeDrainedDuplicate(hash), "the stranger's copy is accounted as drained")
	requireNoSideFiles(t, sm.blockPark.dir)

	record, err := sm.blockPark.ReadConverted(ctx, hash)
	require.NoError(t, err)
	require.NotEmpty(t, record.Subtrees, "the owner's block is parked")
}

// owingPeer connects a peer and records in sm's download ledger that it owes hash.
func owingPeer(t *testing.T, sm *SyncManager, hash chainhash.Hash, idx uint8) *peerpkg.Peer {
	t.Helper()

	if sm.blockDownloads == nil {
		sm.blockDownloads = newBlockDownloadTracker(blockRequestAssignmentTTL)
	}

	p, _, _ := connectRacePeer(t, idx, 1000)
	require.True(t, sm.blockDownloads.Add(p, hash))

	return p
}

func requireNoSideFiles(t *testing.T, dir string) {
	t.Helper()

	entries, err := os.ReadDir(dir)
	require.NoError(t, err)

	for _, e := range entries {
		require.False(t, isDuplicateCopyFile(e.Name()), "side file %s left behind", e.Name())
	}
}

// midTxOffset is the offset in body of the middle of transaction i: the count varint, the
// transactions before i, and half of i.
func midTxOffset(t *testing.T, blk *bsvutil.Block, i int) int {
	t.Helper()

	txs := blk.Transactions()
	off := wire.VarIntSerializeSize(uint64(len(txs)))

	for _, tx := range txs[:i] {
		off += tx.MsgTx().SerializeSize()
	}

	size := txs[i].MsgTx().SerializeSize()
	require.Greater(t, size, 2)

	return off + size/2
}

// A takeover starts at the converting copy's next read, not at its next transaction. A slow
// peer that trickled the middle of a large transaction kept a complete copy waiting until the
// transaction ended. Here the slow copy stops in the middle of a transaction, and one more byte
// is enough to let the complete copy convert.
func TestATakeoverDoesNotWaitForTheSlowCopysNextTransaction(t *testing.T) {
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

	cut := midTxOffset(t, blk, 20)

	slowR, slowW := io.Pipe()
	firstDone := make(chan sinkResult, 1)

	go func() {
		converted, err := sm.pipelineBlockSink(hash, header, slowR, n)
		firstDone <- sinkResult{converted, err}
	}()

	_, err := slowW.Write(body[:cut])
	require.NoError(t, err)
	require.Eventually(t, func() bool { return sm.conversionOf(hash) != nil }, 5*time.Second, 10*time.Millisecond)

	first := sm.conversionOf(hash)

	secondDone := make(chan sinkResult, 1)

	go func() {
		converted, err := sm.raceDuplicateCopy(hash, header, peerpkg.NewDeliveryReader(bytes.NewReader(body), owner), n, sm.pipelineBlockSink)
		secondDone <- sinkResult{converted, err}
	}()

	require.Eventually(t, first.yielding, 5*time.Second, 10*time.Millisecond, "the second copy takes over")

	// One byte more, still in the middle of transaction 20.
	_, err = slowW.Write(body[cut : cut+1])
	require.NoError(t, err)

	select {
	case got := <-secondDone:
		require.NoError(t, got.err)
		require.True(t, got.converted, "the complete copy converted while the slow copy was in the middle of a transaction")
	case <-time.After(5 * time.Second):
		t.Fatal("the complete copy waited for the slow copy's next transaction")
	}

	go func() {
		_, _ = slowW.Write(body[cut+1:])
		_ = slowW.Close()
	}()

	got := <-firstDone
	require.NoError(t, got.err)
	require.False(t, got.converted)

	_, err = sm.blockPark.ReadConverted(ctx, hash)
	require.NoError(t, err)
	requireNoSideFiles(t, sm.blockPark.dir)
}

// A converting copy whose peer sends no byte at all cannot get to its next read. The complete copy
// waits takeoverStallTimeout for it and then disconnects that peer, which ends its read, and then
// converts. It used to wait until shutdown.
func TestATakeoverDisconnectsAConvertingCopyThatSendsNoByte(t *testing.T) {
	ctx := context.Background()
	store := memory.New()
	sm := newPipelineParkManager(t, store, 8)
	sm.blockPark.dir = t.TempDir()

	timer := make(chan time.Time, 1)

	var waited []time.Duration

	sm.takeoverAfter = func(d time.Duration) <-chan time.Time {
		waited = append(waited, d)

		return timer
	}

	blk := wireBlockWithTxs(t, 40, false)
	pipelineHeaderFixture(t, sm, blk)
	proveBlockOrigin(t, sm, blk)

	body := blockBodyBytes(t, blk)
	hash := *blk.Hash()
	header := &blk.MsgBlock().Header
	n := sinkPayloadLen(body)
	slowPeer := owingPeer(t, sm, hash, 246)
	owner := owingPeer(t, sm, hash, 247)

	slowR, slowW := io.Pipe()
	firstDone := make(chan sinkResult, 1)

	go func() {
		converted, err := sm.pipelineBlockSink(hash, header, peerpkg.NewDeliveryReader(slowR, slowPeer), n)
		firstDone <- sinkResult{converted, err}
	}()

	_, err := slowW.Write(body[:midTxOffset(t, blk, 20)])
	require.NoError(t, err)
	require.Eventually(t, func() bool { return sm.conversionOf(hash) != nil }, 5*time.Second, 10*time.Millisecond)

	first := sm.conversionOf(hash)
	require.Eventually(t, first.reading, 5*time.Second, 10*time.Millisecond, "the slow copy waits for its peer's next byte")

	secondDone := make(chan sinkResult, 1)

	go func() {
		converted, err := sm.raceDuplicateCopy(hash, header, peerpkg.NewDeliveryReader(bytes.NewReader(body), owner), n, sm.pipelineBlockSink)
		secondDone <- sinkResult{converted, err}
	}()

	require.Eventually(t, first.yielding, 5*time.Second, 10*time.Millisecond, "the second copy takes over")
	require.True(t, slowPeer.Connected(), "the slow peer is kept until the bound")

	timer <- time.Now()

	require.True(t, WaitUntil(func() bool { return !slowPeer.Connected() }, 5*time.Second), "at the bound the peer that sends no byte is disconnected")
	require.Equal(t, []time.Duration{takeoverStallTimeout}, waited)
	require.True(t, owner.Connected(), "the peer of the complete copy is kept")

	// The disconnect closes the socket the slow copy reads, as here.
	_ = slowW.CloseWithError(io.ErrClosedPipe)

	got := <-secondDone
	require.NoError(t, got.err)
	require.True(t, got.converted, "the complete copy converted")

	got = <-firstDone
	require.False(t, got.converted)

	_, err = sm.blockPark.ReadConverted(ctx, hash)
	require.NoError(t, err)
	requireNoSideFiles(t, sm.blockPark.dir)
}
