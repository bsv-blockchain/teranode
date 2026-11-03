package netsync

import (
	"bytes"
	"context"
	stderrors "errors"
	"io"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-chaincfg"
	txmap "github.com/bsv-blockchain/go-tx-map"
	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/services/legacy/bsvutil"
	peerpkg "github.com/bsv-blockchain/teranode/services/legacy/peer"
	"github.com/bsv-blockchain/teranode/stores/blob"
	"github.com/bsv-blockchain/teranode/stores/blob/memory"
	"github.com/bsv-blockchain/teranode/stores/blob/options"
	"github.com/bsv-blockchain/teranode/util/expiringmap"
	"github.com/stretchr/testify/require"
)

// recordWriteFailingStore is a fault injector around the real stores/blob/memory
// store: Set fails with a StorageError for FileTypeBlock only, which is the one
// write the park makes for a converted record (blockPark.WriteConvertedBlock).
// Every subtree write goes through the file storer's SetFromReader on the real
// store underneath, so the sink runs its whole conversion and fails only at the
// record. The memory store has no NewPendingFile, so the pending-file data path
// the production file store takes is not exercised here, as in every other
// memory-store test in this package.
type recordWriteFailingStore struct {
	blob.Store
}

func (s *recordWriteFailingStore) Set(ctx context.Context, key []byte, fileType fileformat.FileType, value []byte, opts ...options.FileOption) error {
	if fileType == fileformat.FileTypeBlock {
		return errors.NewStorageError("disk full")
	}

	return s.Store.Set(ctx, key, fileType, value, opts...)
}

// regtestStreamedBlock builds a block of txCount transactions that the streaming
// gate admits: its header carries regtest's target with a nonce that meets it,
// its parent is the manager's genesis, its merkle root is the one its body
// produces, and the ledger holds a request for its hash. The same construction
// as TestAShortRedeliveryThroughTheWireKeepsTheParkedBlock.
func regtestStreamedBlock(t *testing.T, sm *SyncManager, txCount int) (*wire.MsgBlock, chainhash.Hash) {
	t.Helper()

	msgBlock := wireBlockWithTxs(t, txCount, false).MsgBlock()
	blk := bsvutil.NewBlock(msgBlock)
	blk.SetHeight(500)
	pipelineHeaderFixture(t, sm, blk)

	msgBlock.Header.Bits = chaincfg.RegressionNetParams.PowLimitBits

	header := &msgBlock.Header
	for nonce := uint32(0); ; nonce++ {
		require.Less(t, nonce, uint32(1000), "regtest's target is met about every other nonce")

		header.Nonce = nonce
		hash := header.BlockHash()
		require.True(t, sm.blockDownloads.Add(nil, hash))

		if sm.streamingBlockGate(hash, header, 0) == nil {
			break
		}
	}

	return msgBlock, header.BlockHash()
}

// goodRunSubtreeRoots converts the same block on a fresh manager over a throwaway
// store and returns the subtree roots the block produces, so a test of a failed
// run can require every one of them to be absent from the failed store (the
// pattern TestPipelineSink_DeletesWhatItWroteOnAWrongMerkleRoot uses).
func goodRunSubtreeRoots(t *testing.T, msgBlock *wire.MsgBlock, hash chainhash.Hash) []chainhash.Hash {
	t.Helper()

	good := newPipelineParkManager(t, memory.New(), 8)
	body := blockBodyBytes(t, bsvutil.NewBlock(msgBlock))

	converted, err := good.pipelineBlockSink(hash, &msgBlock.Header, bytes.NewReader(body), sinkPayloadLen(body))
	require.NoError(t, err, "sanity: the good run must convert, or the failed run's cleanup is asserted against nothing")
	require.True(t, converted)

	record, err := good.blockPark.ReadConverted(context.Background(), hash)
	require.NoError(t, err)
	require.NotEmpty(t, record.Subtrees, "sanity: the good run must produce subtrees")

	roots := make([]chainhash.Hash, 0, len(record.Subtrees))
	for _, root := range record.Subtrees {
		roots = append(roots, *root)
	}

	return roots
}

// requireNothingOfTheBlockOnDisk asserts the end state of a failed conversion:
// no converted record under hash and none of the block's subtree files.
func requireNothingOfTheBlockOnDisk(t *testing.T, store blob.Store, hash chainhash.Hash, roots []chainhash.Hash) {
	t.Helper()

	ctx := context.Background()

	exists, err := store.Exists(ctx, hash[:], fileformat.FileTypeBlock)
	require.NoError(t, err)
	require.False(t, exists, "no converted record may be left under the block's hash")

	for _, root := range roots {
		for _, ft := range []fileformat.FileType{fileformat.FileTypeSubtreeToCheck, fileformat.FileTypeSubtree, fileformat.FileTypeSubtreeData, fileformat.FileTypeSubtreeMeta} {
			exists, err = store.Exists(ctx, root[:], ft)
			require.NoError(t, err)
			require.False(t, exists, "a failed conversion must leave no %s behind for subtree %s", ft, root)
		}
	}
}

// streamingPeerPair connects two peers over an in-memory pipe with the sync
// manager's streaming block path installed on the process-wide wire hooks. The
// receiver's listeners capture the on-disk message and each ping. The hooks are
// installed, and their cleanup registered, BEFORE the peers are made: cleanups
// run last-in first-out, so this order disconnects both peers and waits for
// their read loops before the hooks are cleared from under them.
func streamingPeerPair(t *testing.T, sm *SyncManager, idx uint8) (sender, receiver *peerpkg.Peer, onDisk chan *peerpkg.MsgBlockOnDisk, pings chan struct{}) {
	t.Helper()

	sm.installStreamingBlockPath(peerpkg.SetBlockBodyStreaming)
	t.Cleanup(func() { peerpkg.SetBlockBodyStreaming(nil, nil, nil) })
	peerpkg.RegisterStreamingBlockHandler()

	onDisk = make(chan *peerpkg.MsgBlockOnDisk, 4)
	pings = make(chan struct{}, 4)

	receiverCfg := peerpkg.Config{
		Listeners: peerpkg.MessageListeners{
			OnBlockOnDisk: func(_ *peerpkg.Peer, msg *peerpkg.MsgBlockOnDisk) {
				select {
				case onDisk <- msg:
				default:
				}
			},
			OnPing: func(*peerpkg.Peer, *wire.MsgPing) {
				select {
				case pings <- struct{}{}:
				default:
				}
			},
		},
		UserAgentName:    "btcdtest",
		UserAgentVersion: "1.0",
		ChainParams:      &chaincfg.MainNetParams,
	}
	senderCfg := peerpkg.Config{
		Listeners:        peerpkg.MessageListeners{},
		UserAgentName:    "btcdtest",
		UserAgentVersion: "1.0",
		ChainParams:      &chaincfg.MainNetParams,
	}

	receiver, sender, err := MakeConnectedPeers(t, receiverCfg, senderCfg, idx)
	require.NoError(t, err)

	t.Cleanup(func() {
		sender.DisconnectWithInfo("test over")
		receiver.DisconnectWithInfo("test over")
		sender.WaitForDisconnect()
		receiver.WaitForDisconnect()
	})

	return sender, receiver, onDisk, pings
}

// TestPeer_AStoreFaultAtTheSinkKeepsTheConnection is the gate for this step. A
// peer delivers a well-formed block over a real pipe, through go-wire's streaming
// handler and the installed sink, and this node's own record write fails. SV Node
// never charges a peer for a failure of this node's storage (validation.cpp
// AbortNode on a failed block write records state.Error, which is not
// state.IsInvalid, so BlockChecked never scores the peer); here the rest of the
// body is drained, the peer stays connected, the next message on the connection
// is handled, nothing of the block is left on disk, and the drain is marked for
// the on-disk handler to re-ask the block.
//
// Before absorbLocalSinkFault, the StorageError left the sink as a read error,
// peer.inHandler pushed a reject "malformed" and disconnected the peer.
func TestPeer_AStoreFaultAtTheSinkKeepsTheConnection(t *testing.T) {
	realStore := memory.New()
	store := &recordWriteFailingStore{Store: realStore}
	sm := newPipelineParkManager(t, store, 8)
	sm.blockDownloads = newBlockDownloadTracker(time.Hour)
	// Assigned before installStreamingBlockPath: that decides at install time
	// whether trackBlockStreams wraps the sink, and the waste counters below are
	// only fed through it.
	sm.streams = newStreamRegistry()

	msgBlock, hash := regtestStreamedBlock(t, sm, 40)
	roots := goodRunSubtreeRoots(t, msgBlock, hash)

	sender, receiver, onDisk, pings := streamingPeerPair(t, sm, 21)
	// The peer the read loop names as sending is the one asked for the block.
	require.True(t, sm.blockDownloads.Add(receiver, hash))

	sender.QueueMessage(msgBlock, nil)
	sender.QueueMessage(wire.NewMsgPing(42), nil)

	// The read loop is serial, so a handled ping proves the block was consumed in
	// full and the connection carried on past it.
	select {
	case <-pings:
	case <-time.After(5 * time.Second):
		t.Fatal("the message after the block was never handled: the read loop stopped at the store fault")
	}

	require.True(t, receiver.Connected(), "a failure of this node's own store must not disconnect the peer that delivered the block")

	var msg *peerpkg.MsgBlockOnDisk
	select {
	case msg = <-onDisk:
	default:
		t.Fatal("the block never reached OnBlockOnDisk, so the read loop treated the store fault as a read error")
	}

	require.Equal(t, hash, msg.Hash)
	require.False(t, msg.Converted, "a delivery whose record write failed must not report having converted")

	requireNothingOfTheBlockOnDisk(t, realStore, hash, roots)

	require.Equal(t, int64(1), sm.waste.localFaultDrained.Load(), "the drain is counted as this node's own fault")
	require.Equal(t, int64(0), sm.waste.dupDrained.Load(), "and not as a drained duplicate")
	require.Positive(t, sm.waste.bytesWasted.Load(), "the body's bytes were received and lost")
	require.True(t, sm.takeLocalFaultDrain(hash), "the mark is waiting for handleBlockOnDiskMsg")
}

// TestPipelineSink_ABodyCutMidTransactionIsADeliveryFaultNotAnInvalidBlock pins
// the HARDEN 1421 rule on the real sink: a connection that ends inside a
// transaction, before the declared payload length, says nothing about the block.
// The error leaves the sink as the bare io.ErrUnexpectedEOF, which is what
// peer.shouldHandleReadError compares by identity, so the peer is logged as
// disconnected rather than rejected as malformed; it carries neither the invalid
// nor the corrupt code, and the classifier does not read it as this node's fault
// either. Before this step block_tx_stream wrapped every read failure, EOF
// included, in NewBlockInvalidError.
func TestPipelineSink_ABodyCutMidTransactionIsADeliveryFaultNotAnInvalidBlock(t *testing.T) {
	store := memory.New()
	sm := newPipelineParkManager(t, store, 8)
	sm.blockDownloads = newBlockDownloadTracker(time.Hour)

	msgBlock, hash := regtestStreamedBlock(t, sm, 40)
	roots := goodRunSubtreeRoots(t, msgBlock, hash)
	body := blockBodyBytes(t, bsvutil.NewBlock(msgBlock))

	// Seven bytes short of the last transaction, which is at least ten bytes, so
	// the cut lands inside it; n still declares the whole body.
	cut := body[:len(body)-7]

	converted, err := sm.pipelineBlockSink(hash, &msgBlock.Header, bytes.NewReader(cut), sinkPayloadLen(body))
	require.False(t, converted)
	require.Error(t, err)
	require.Same(t, io.ErrUnexpectedEOF, err, "the read loop compares by identity (peer.go shouldHandleReadError), so the bare sentinel must come out")
	require.False(t, errors.Is(err, errors.ErrBlockInvalid), "a hang-up mid-body is not an invalid block")
	require.False(t, errors.IsBlockCorrupt(err), "nor a corrupt one: the declared body never finished arriving")
	require.False(t, isLocalSinkFault(err), "nor this node's fault")

	require.False(t, sm.blockPark.Has(hash))
	requireNothingOfTheBlockOnDisk(t, store, hash, roots)
}

// TestWire_AMidBodyEOFReachesTheReadLoopByIdentity drives the same cut body
// through go-wire's ReadMessageWithEncodingN and the registered handler, which is
// the route the peer's read loop takes. readBlockMessage used to wrap every sink
// error in a ProcessingError, so the identity the sink now preserves was lost one
// layer up and the peer was still rejected as malformed.
func TestWire_AMidBodyEOFReachesTheReadLoopByIdentity(t *testing.T) {
	store := memory.New()
	sm := newPipelineParkManager(t, store, 8)
	sm.blockDownloads = newBlockDownloadTracker(time.Hour)

	msgBlock, hash := regtestStreamedBlock(t, sm, 40)
	roots := goodRunSubtreeRoots(t, msgBlock, hash)

	sm.installStreamingBlockPath(peerpkg.SetBlockBodyStreaming)
	t.Cleanup(func() { peerpkg.SetBlockBodyStreaming(nil, nil, nil) })
	peerpkg.RegisterStreamingBlockHandler()

	var framed bytes.Buffer
	_, err := wire.WriteMessageN(&framed, msgBlock, wire.ProtocolVersion, wire.MainNet)
	require.NoError(t, err)

	message := framed.Bytes()
	cut := message[:len(message)-7]
	owner := owingPeer(t, sm, hash, 22)

	_, _, _, err = wire.ReadMessageWithEncodingN(peerpkg.NewDeliveryReader(bytes.NewReader(cut), owner), wire.ProtocolVersion, wire.MainNet, wire.BaseEncoding)
	require.Error(t, err)
	require.Same(t, io.ErrUnexpectedEOF, err, "the wire layer must hand the read loop the bare sentinel it compares by identity")

	require.False(t, sm.blockPark.Has(hash))
	requireNothingOfTheBlockOnDisk(t, store, hash, roots)
}

// TestPipelineSink_AResetMidTransactionIsADeliveryFaultNotAnInvalidBlock is the
// socket-error twin of the cut-body test above, on the real sink: the connection
// dies inside a transaction with a *net.OpError (a reset by the peer, a NAT timing
// out, the idle timer closing the socket) instead of a FIN. The error leaves the
// sink bare and by identity, because peer.shouldHandleReadError type-asserts
// *net.OpError directly and logs a network error instead of pushing a reject. It
// carries neither the invalid nor the corrupt code, and the classifier does not
// read it as this node's fault. Nothing of the block is left on disk.
func TestPipelineSink_AResetMidTransactionIsADeliveryFaultNotAnInvalidBlock(t *testing.T) {
	store := memory.New()
	sm := newPipelineParkManager(t, store, 8)
	sm.blockDownloads = newBlockDownloadTracker(time.Hour)

	msgBlock, hash := regtestStreamedBlock(t, sm, 40)
	roots := goodRunSubtreeRoots(t, msgBlock, hash)
	body := blockBodyBytes(t, bsvutil.NewBlock(msgBlock))

	// Seven bytes short of the last transaction, so the reset lands inside it; n
	// still declares the whole body.
	reset := connectionReset()
	src := &resetReader{r: bytes.NewReader(body[:len(body)-7]), err: reset}

	converted, err := sm.pipelineBlockSink(hash, &msgBlock.Header, src, sinkPayloadLen(body))
	require.False(t, converted)
	require.Error(t, err)
	require.Same(t, reset, err, "the read loop type-asserts *net.OpError (peer.go shouldHandleReadError), so the socket's own error must come out")
	require.False(t, errors.Is(err, errors.ErrBlockInvalid), "a reset mid-body is not an invalid block")
	require.False(t, errors.IsBlockCorrupt(err), "nor a corrupt one: the declared body never finished arriving")
	require.False(t, isLocalSinkFault(err), "nor this node's fault")

	require.False(t, sm.blockPark.Has(hash))
	requireNothingOfTheBlockOnDisk(t, store, hash, roots)
}

// TestWire_AMidBodyResetReachesTheReadLoopByIdentity drives the reset through
// go-wire's ReadMessageWithEncodingN and the registered handler, the route the
// peer's read loop takes. readBlockMessage passed only the two EOF sentinels
// through by identity and wrapped every other sink error in a ProcessingError, so
// the socket error the sink now preserves was lost one layer up and the peer was
// still rejected as malformed. A net.Pipe cannot produce a *net.OpError, so this
// is the highest layer the shape can be driven through in a test.
func TestWire_AMidBodyResetReachesTheReadLoopByIdentity(t *testing.T) {
	store := memory.New()
	sm := newPipelineParkManager(t, store, 8)
	sm.blockDownloads = newBlockDownloadTracker(time.Hour)

	msgBlock, hash := regtestStreamedBlock(t, sm, 40)
	roots := goodRunSubtreeRoots(t, msgBlock, hash)

	sm.installStreamingBlockPath(peerpkg.SetBlockBodyStreaming)
	t.Cleanup(func() { peerpkg.SetBlockBodyStreaming(nil, nil, nil) })
	peerpkg.RegisterStreamingBlockHandler()

	var framed bytes.Buffer
	_, err := wire.WriteMessageN(&framed, msgBlock, wire.ProtocolVersion, wire.MainNet)
	require.NoError(t, err)

	message := framed.Bytes()
	reset := connectionReset()
	src := &resetReader{r: bytes.NewReader(message[:len(message)-7]), err: reset}
	owner := owingPeer(t, sm, hash, 23)

	_, _, _, err = wire.ReadMessageWithEncodingN(peerpkg.NewDeliveryReader(src, owner), wire.ProtocolVersion, wire.MainNet, wire.BaseEncoding)
	require.Error(t, err)
	require.Same(t, reset, err, "the wire layer must hand the read loop the socket error it type-asserts")

	require.False(t, sm.blockPark.Has(hash))
	requireNothingOfTheBlockOnDisk(t, store, hash, roots)
}

// TestHandleBlockOnDiskMsg_ALocalFaultDrainLetsTheOtherOwnersOffAndLeavesNoMark
// pins the ledger outcome in headers-first mode: the delivering peer is released,
// every other owner is forgiven (not cancelled, so a copy still on the wire from
// them is admitted), the block carries no recentlyFailedBlocks mark (that mark
// skips a block for ten minutes, the wrong judgement for a store blip, as
// parkDispositionRetryLater already decided for a commit-time fault of ours), the
// drain mark is consumed, and nothing is parked. The deferred top-up then re-asks
// the block through the scheduler.
func TestHandleBlockOnDiskMsg_ALocalFaultDrainLetsTheOtherOwnersOffAndLeavesNoMark(t *testing.T) {
	sm := newPipelineParkManager(t, memory.New(), 8)
	sm.blockDownloads = newBlockDownloadTracker(time.Hour)
	sm.recentlyFailedBlocks = expiringmap.New[chainhash.Hash, struct{}](recentlyFailedBlocksTTL).WithMaxSize(recentlyFailedBlocksMaxTracked)
	sm.headersFirstMode.Store(true)

	h := chainhash.HashH([]byte("a block this node failed to store"))
	other := &peerpkg.Peer{}

	require.True(t, sm.blockDownloads.Add(nil, h))
	require.True(t, sm.blockDownloads.Add(other, h))
	sm.noteLocalFaultDrain(h)

	sm.handleBlockOnDiskMsg(&blockOnDiskMsg{body: peerpkg.BlockBody{Hash: h}, peer: nil})

	active, _ := sm.blockDownloads.ActiveOwners(h)
	require.Empty(t, active, "the other owner is let off the block")
	require.True(t, sm.blockDownloads.Requested(h), "but keeps ownership, so its copy still passes the gate")
	require.False(t, sm.blockDownloads.HasOwner(nil, h), "the delivering peer is released")

	_, failed := sm.recentlyFailedBlocks.Get(h)
	require.False(t, failed, "a store fault of ours must not mark the block as failed")

	require.False(t, sm.blockPark.Has(h))
	require.False(t, sm.takeLocalFaultDrain(h), "the mark was consumed by the handler")
	require.Equal(t, int64(0), sm.waste.dupDrained.Load())
	require.Equal(t, int64(1), sm.waste.localFaultDrained.Load())
}

// TestHandleBlockOnDiskMsg_ALocalFaultDrainInInvModeReAsksTheSamePeer pins the
// recovery above the final checkpoint, where headers-first mode is off and the
// deferred topUpHeaderBlocks returns at once. The block was requested from an inv
// the delivering peer will not repeat, so the handler asks that same peer again
// with a getdata: the fault was ours and the block is still there. A getblocks
// would not do, because PushGetBlocksMsg filters a repeat at an unmoved tip.
func TestHandleBlockOnDiskMsg_ALocalFaultDrainInInvModeReAsksTheSamePeer(t *testing.T) {
	sm := newPipelineParkManager(t, memory.New(), 8)
	sm.blockDownloads = newBlockDownloadTracker(time.Hour)
	sm.peerStates = txmap.NewSyncedMap[*peerpkg.Peer, *peerSyncState]()
	sm.headersFirstMode.Store(false)

	local, _, rec := connectRacePeer(t, 22, 0)

	h := chainhash.HashH([]byte("a block to ask the same peer for again"))
	require.True(t, sm.blockDownloads.Add(local, h))
	sm.noteLocalFaultDrain(h)

	sm.handleBlockOnDiskMsg(&blockOnDiskMsg{body: peerpkg.BlockBody{Hash: h}, peer: local})

	active, _ := sm.blockDownloads.ActiveOwners(h)
	require.Equal(t, []*peerpkg.Peer{local}, active, "the delivering peer owes the block again")

	require.Eventually(t, func() bool { return rec.count() == 1 }, 2*time.Second, 10*time.Millisecond, "one getdata for the block reaches the peer")
	require.Equal(t, []chainhash.Hash{h}, rec.all())
}

// TestAbsorbLocalSinkFault_LeavesAShutdownErrorToTheReadLoop pins the ctx guard.
// sm.ctx is the peer server's context, cancelled before the server disconnects
// its peers, so a store call cut by shutdown comes out of the sink as a
// StorageError whose chain cannot show the cancellation (teranode errors.New
// flattens a foreign cause to its message). The guard reads the context, not the
// chain: with sm.ctx cancelled the error is returned as it is, nothing is drained,
// logged as a store fault, counted or marked.
func TestAbsorbLocalSinkFault_LeavesAShutdownErrorToTheReadLoop(t *testing.T) {
	sm := newPipelineParkManager(t, memory.New(), 8)

	ctx, cancel := context.WithCancel(context.Background())
	sm.ctx = ctx

	h := chainhash.HashH([]byte("a block mid-conversion at shutdown"))
	rest := bytes.NewReader([]byte("the rest of the body"))
	storeErr := errors.NewStorageError("[subtreeWriter] failed writing", context.Canceled)

	// With sm.ctx live the same error is this node's fault: drained, counted, marked.
	converted, err := sm.absorbLocalSinkFault(h, rest, false, storeErr)
	require.False(t, converted)
	require.NoError(t, err)
	require.Zero(t, rest.Len(), "the rest of the body was drained")
	require.Equal(t, int64(1), sm.waste.localFaultDrained.Load())
	require.True(t, sm.takeLocalFaultDrain(h))

	cancel()

	rest = bytes.NewReader([]byte("the rest of the body"))

	converted, err = sm.absorbLocalSinkFault(h, rest, false, storeErr)
	require.False(t, converted)
	require.Same(t, storeErr, err, "at shutdown the error is left to the read loop; the server is closing every connection")
	require.Positive(t, rest.Len(), "nothing was drained")
	require.Equal(t, int64(1), sm.waste.localFaultDrained.Load(), "not counted again")
	require.False(t, sm.takeLocalFaultDrain(h), "not marked")
}

// TestIsLocalSinkFault_ReadsTheOutermostVerdict pins the classifier: the
// outermost teranode code is the verdict, because producers wrap foreign causes
// (an fs error inside a StorageError, io.ErrUnexpectedEOF inside a BlockInvalid)
// and teranode's errors.Is would match any link in the chain. Only the invalid
// and corrupt codes are the peer's; a non-teranode error is left for the read
// loop's identity checks; every other code is this node's.
func TestIsLocalSinkFault_ReadsTheOutermostVerdict(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"storage error", errors.NewStorageError("disk full"), true},
		{"processing error", errors.NewProcessingError("nil transaction"), true},
		{"subtree error", errors.NewSubtreeError("failed creating subtree"), true},
		{"tx error", errors.NewTxError("failed extending"), true},
		{"storage error wrapping a block invalid", errors.NewStorageError("outer", errors.NewBlockInvalidError("inner")), true},
		{"processing error wrapping context.Canceled with sm.ctx live", errors.NewProcessingError("outer", context.Canceled), true},
		{"block invalid", errors.NewBlockInvalidError("merkle root mismatch"), false},
		{"block invalid wrapping an unexpected EOF", errors.NewBlockInvalidError("failed reading", io.ErrUnexpectedEOF), false},
		{"block corrupt", errors.NewBlockCorruptError("bytes after the last transaction"), false},
		{"raw io.EOF", io.EOF, false},
		{"raw io.ErrUnexpectedEOF", io.ErrUnexpectedEOF, false},
		{"raw net.OpError", connectionReset(), false},
		{"plain error", stderrors.New("a socket error with no teranode code"), false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, isLocalSinkFault(tc.err))
		})
	}
}
