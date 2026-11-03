package netsync

import (
	"bytes"
	stderrors "errors"
	"io"
	"net"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-chaincfg"
	txmap "github.com/bsv-blockchain/go-tx-map"
	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	blockchain2 "github.com/bsv-blockchain/teranode/services/blockchain"
	peerpkg "github.com/bsv-blockchain/teranode/services/legacy/peer"
	"github.com/bsv-blockchain/teranode/stores/blob/memory"
	"github.com/bsv-blockchain/teranode/util/expiringmap"
	"github.com/stretchr/testify/require"
)

// rejectionCapture records what the two listeners this file asserts on were
// handed: the receiver's OnBlockBodyRejected and the sender's OnReject.
type rejectionCapture struct {
	mu       sync.Mutex
	rejected []*peerpkg.BlockBodyRejectedError
	rejects  []*wire.MsgReject
}

func (c *rejectionCapture) oneRejected(t *testing.T) *peerpkg.BlockBodyRejectedError {
	t.Helper()

	require.True(t, WaitUntil(func() bool {
		c.mu.Lock()
		defer c.mu.Unlock()

		return len(c.rejected) > 0
	}, 5*time.Second), "OnBlockBodyRejected was never invoked")

	c.mu.Lock()
	defer c.mu.Unlock()

	require.Len(t, c.rejected, 1, "one body, one rejection")

	return c.rejected[0]
}

func (c *rejectionCapture) oneReject(t *testing.T) *wire.MsgReject {
	t.Helper()

	require.True(t, WaitUntil(func() bool {
		c.mu.Lock()
		defer c.mu.Unlock()

		return len(c.rejects) > 0
	}, 5*time.Second), "the sender never received a reject")

	c.mu.Lock()
	defer c.mu.Unlock()

	require.Len(t, c.rejects, 1, "one body, one reject")

	return c.rejects[0]
}

// punishmentPeerPair connects two peers over an in-memory pipe with the sync
// manager's streaming block path installed, as streamingPeerPair does, and
// captures the receiver's OnBlockBodyRejected and the sender's OnReject. Same
// install-before-connect ordering, for the same reason: cleanups run last-in
// first-out, so the hooks are cleared only after both read loops have stopped.
func punishmentPeerPair(t *testing.T, sm *SyncManager, idx uint8) (sender, receiver *peerpkg.Peer, capture *rejectionCapture) {
	t.Helper()

	sm.installStreamingBlockPath(peerpkg.SetBlockBodyStreaming)
	t.Cleanup(func() { peerpkg.SetBlockBodyStreaming(nil, nil, nil) })
	peerpkg.RegisterStreamingBlockHandler()

	capture = &rejectionCapture{}

	receiverCfg := peerpkg.Config{
		Listeners: peerpkg.MessageListeners{
			OnBlockBodyRejected: func(_ *peerpkg.Peer, rejected *peerpkg.BlockBodyRejectedError) {
				capture.mu.Lock()
				defer capture.mu.Unlock()

				capture.rejected = append(capture.rejected, rejected)
			},
		},
		UserAgentName:    "btcdtest",
		UserAgentVersion: "1.0",
		ChainParams:      &chaincfg.MainNetParams,
	}
	senderCfg := peerpkg.Config{
		Listeners: peerpkg.MessageListeners{
			OnReject: func(_ *peerpkg.Peer, msg *wire.MsgReject) {
				capture.mu.Lock()
				defer capture.mu.Unlock()

				capture.rejects = append(capture.rejects, msg)
			},
		},
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

	return sender, receiver, capture
}

// disconnectsWithin reports whether p's WaitForDisconnect returns inside d.
func disconnectsWithin(p *peerpkg.Peer, d time.Duration) bool {
	done := make(chan struct{})

	go func() {
		p.WaitForDisconnect()
		close(done)
	}()

	select {
	case <-done:
		return true
	case <-time.After(d):
		return false
	}
}

// TestPeer_ARejectedBodyDropsTheAssociationPrimary is the gate for this step. A
// peer delivers, over a real pipe and through go-wire's streaming handler into
// the real pipeline sink, a body that is not the block its header commits to:
// one output value was changed after the header was mined, so the hash and the
// proof of work are untouched and the gate admits it, and the sink finds the
// computed merkle root is not the header's. The body arrives on a DATA1
// sub-peer of a multistream association, as every block body does under the
// default legacy_allowBlockPriority, and the primary is the peer the sync
// manager knows: registered in peerStates and owning the block in the download
// ledger.
//
// End state: the sender is told the block was rejected (a reject for command
// block, code invalid, naming the hash), the primary is disconnected and so is
// the sub-peer, the listener that owns the ban was handed a rejection whose body
// arrived in full and carries ErrBlockBodyMismatch but is NOT ProvenBad, because
// the streaming path never verified the wire checksum (so
// serverPeer.OnBlockBodyRejected records no ban), the primary's departure
// releases the block it owed, and nothing of the block is left on disk.
//
// Before this step the sink's refusal was wrapped as a ProcessingError and the
// read loop answered "malformed" and disconnected the sub-peer alone: the
// primary stayed sync peer with the block still owed, and no listener existed.
//
// The ban itself lives in the legacy package (serverPeer.OnBlockBodyRejected)
// and is pinned there against this exact error shape; here the predicate it
// rests on is asserted on what the listener received. The ledger release is
// netsync's handleDonePeerMsg, which production reaches through the peer
// server's peerDoneHandler and syncManager.DonePeer; this harness runs no
// blockHandler consumer, so it is called directly once the primary is down.
func TestPeer_ARejectedBodyDropsTheAssociationPrimary(t *testing.T) {
	store := memory.New()
	sm := newPipelineParkManager(t, store, 8)
	sm.blockDownloads = newBlockDownloadTracker(time.Hour)
	sm.streams = newStreamRegistry()

	msgBlock, hash := regtestStreamedBlock(t, sm, 40)
	roots := goodRunSubtreeRoots(t, msgBlock, hash)

	// The body is not the header's. Changed after the header was mined, so the
	// block hash is unchanged and the gate's hash and work checks pass; only
	// the root the sink computes differs. Length unchanged, so RequireEnd
	// passes and the refusal is the merkle root's, which is the ban marker's
	// site.
	msgBlock.Transactions[1].TxOut[0].Value++

	// The primary: a connected peer the sync manager knows, owning the block
	// in the ledger as the peer that asked for it. The ledger is replaced
	// first, because regtestStreamedBlock records the request under a nil
	// owner to get the header through the gate.
	primary, _, _ := connectRecordingPeer(t, 31, 1000)
	sm.peerStates = txmap.NewSyncedMap[*peerpkg.Peer, *peerSyncState]()
	sm.peerStates.Set(primary, &peerSyncState{
		syncCandidate: true,
		requestedTxns: expiringmap.New[chainhash.Hash, struct{}](time.Hour),
	})

	sm.blockDownloads = newBlockDownloadTracker(time.Hour)
	require.True(t, sm.blockDownloads.Add(primary, hash))
	require.Equal(t, 1, sm.blockDownloads.CountForPeer(primary), "sanity: the primary owes the block")

	sender, receiver, capture := punishmentPeerPair(t, sm, 32)

	// The receiver is the association's DATA1 sub-peer, the shape the body
	// arrives in under BlockPriority. Sub-peers are never in peerStates and
	// never own anything in the ledger.
	assoc := peerpkg.NewAssociation([]byte{0x01, 0x02, 0x03}, primary)
	primary.SetAssociation(assoc)
	require.True(t, assoc.AddStream(wire.StreamTypeData1, receiver))
	receiver.SetAssociation(assoc)
	receiver.SetStreamType(wire.StreamTypeData1)

	sender.QueueMessage(msgBlock, nil)

	require.True(t, disconnectsWithin(primary, 5*time.Second),
		"the association's primary must be disconnected: it is the peer netsync knows, and dropping only the DATA1 sub-peer leaves it sync peer with the block still owed")
	require.True(t, disconnectsWithin(receiver, 5*time.Second), "the sub-peer that carried the body must be disconnected too")

	rejected := capture.oneRejected(t)
	require.Equal(t, hash, rejected.Hash)
	require.True(t, errors.Is(rejected.Err, errors.ErrBlockInvalid), "a body that is not the header's is an invalid block")
	require.False(t, rejected.Truncated, "the whole declared payload arrived")
	require.True(t, rejected.MismatchInFull(), "the merkle root site carries ErrBlockBodyMismatch and the body arrived in full")
	require.False(t, rejected.ChecksumVerified, "go-wire hands the body over before its checksum check, so the streaming path has not verified it")
	require.False(t, rejected.ProvenBad(),
		"a mismatch on bytes whose checksum nobody verified must not be the predicate serverPeer.OnBlockBodyRejected bans on")

	reject := capture.oneReject(t)
	require.Equal(t, wire.CmdBlock, reject.Cmd, "the reject names the block command, not malformed")
	require.Equal(t, wire.RejectInvalid, reject.Code)
	require.Equal(t, hash, reject.Hash, "the reject names the block")

	// What the primary's done message does in production, run here directly.
	sm.handleDonePeerMsg(primary)

	require.Zero(t, sm.blockDownloads.CountForPeer(primary), "the departed primary's owed block must be released")
	require.False(t, sm.blockDownloads.Requested(hash), "nobody is owed the block once its only owner has gone, so the next pass asks someone else")

	requireNothingOfTheBlockOnDisk(t, store, hash, roots)
}

// framedBlock returns msgBlock as the bytes a peer writes on the wire: message
// header, then header, count and transactions.
func framedBlock(t *testing.T, msgBlock *wire.MsgBlock) []byte {
	t.Helper()

	var framed bytes.Buffer
	_, err := wire.WriteMessageN(&framed, msgBlock, wire.ProtocolVersion, wire.MainNet)
	require.NoError(t, err)

	return framed.Bytes()
}

// TestWire_OnlyABodyDeliveredInFullIsAMismatchInFull is the no-ban-on-EOF gate,
// driven through go-wire's ReadMessageWithEncodingN and the registered handler
// into the real pipeline sink, the route the peer's read loop takes.
//
// Three deliveries of one block whose third transaction duplicates its second
// (CVE-2012-2459, SV Node's bad-txns-duplicate, one of the three ban sites):
//
//   - delivered in full: the sink refuses at the duplicate, the handler drains
//     the rest, the rejection is MismatchInFull. It is still not ProvenBad,
//     because the wire checksum was never verified on this path (see
//     peerpkg.streamingBlockHandler);
//   - cut seven bytes short: the sink refuses at the duplicate exactly as
//     before, but the drain hits the end of the stream with bytes still owed,
//     so the rejection is Truncated and NOT MismatchInFull, whatever code the sink
//     chose. A body this node never saw the end of earns a disconnect and no
//     ban, which is what SV Node gets for free by deserialising the whole
//     message before CheckBlock;
//   - a well-formed block cut mid-transaction: no refusal at all; the error
//     leaves as the bare io.ErrUnexpectedEOF (the HARDEN 1421 rule, pinned by
//     TestWire_AMidBodyEOFReachesTheReadLoopByIdentity) and is not a
//     BlockBodyRejectedError, so OnBlockBodyRejected can never see it.
func TestWire_OnlyABodyDeliveredInFullIsAMismatchInFull(t *testing.T) {
	store := memory.New()
	sm := newPipelineParkManager(t, store, 8)
	sm.blockDownloads = newBlockDownloadTracker(time.Hour)

	sm.installStreamingBlockPath(peerpkg.SetBlockBodyStreaming)
	t.Cleanup(func() { peerpkg.SetBlockBodyStreaming(nil, nil, nil) })
	peerpkg.RegisterStreamingBlockHandler()

	withDuplicate, hash := regtestStreamedBlock(t, sm, 6)
	// After the header is mined, so the hash is unchanged. The duplicate is the
	// third transaction, not the last: the refusal must come with bytes still
	// to read, so the cut row's drain is what finds the stream short.
	withDuplicate.Transactions[2] = withDuplicate.Transactions[1]
	duplicateBody := framedBlock(t, withDuplicate)

	wellFormed, wellFormedHash := regtestStreamedBlock(t, sm, 40)
	wellFormedBody := framedBlock(t, wellFormed)

	// Both blocks are owed by the peer the bodies are read from.
	owner := owingPeer(t, sm, hash, 33)
	require.True(t, sm.blockDownloads.Add(owner, wellFormedHash))

	readThroughTheHandler := func(message []byte) error {
		_, _, _, err := wire.ReadMessageWithEncodingN(peerpkg.NewDeliveryReader(bytes.NewReader(message), owner), wire.ProtocolVersion, wire.MainNet, wire.BaseEncoding)

		return err
	}

	t.Run("a duplicate transaction in a body delivered in full is a mismatch in full", func(t *testing.T) {
		err := readThroughTheHandler(duplicateBody)
		require.Error(t, err)

		var rejected *peerpkg.BlockBodyRejectedError
		require.True(t, stderrors.As(err, &rejected), "the sink's refusal must reach the read loop typed, got %T", err)
		require.Equal(t, hash, rejected.Hash)
		require.True(t, errors.Is(rejected.Err, errors.ErrBlockInvalid))
		require.False(t, rejected.Truncated, "the whole declared payload was read")
		require.True(t, rejected.MismatchInFull(), "the duplicate site carries the marker and the body arrived in full")
		require.False(t, rejected.ProvenBad(), "the wire checksum was not verified, so no ban rests on it")
	})

	t.Run("the same body cut short is rejected but not a mismatch in full", func(t *testing.T) {
		err := readThroughTheHandler(duplicateBody[:len(duplicateBody)-7])
		require.Error(t, err)

		var rejected *peerpkg.BlockBodyRejectedError
		require.True(t, stderrors.As(err, &rejected), "the sink refused at the duplicate before the cut, so the refusal is typed, got %T", err)
		require.Equal(t, hash, rejected.Hash)
		require.True(t, errors.IsBlockBodyMismatch(rejected.Err), "the sink's own code is the marker: the bit, not the code, is what withholds the ban")
		require.True(t, rejected.Truncated, "the drain found the stream ended with bytes still owed")
		require.False(t, rejected.MismatchInFull(), "a body this node never saw the end of must not be banned for")
	})

	t.Run("a well-formed body cut mid-transaction is not a rejection at all", func(t *testing.T) {
		err := readThroughTheHandler(wellFormedBody[:len(wellFormedBody)-7])
		require.Error(t, err)
		require.Same(t, io.ErrUnexpectedEOF, err, "a hang-up mid-body leaves by identity for shouldHandleReadError")

		var rejected *peerpkg.BlockBodyRejectedError
		require.False(t, stderrors.As(err, &rejected), "a delivery fault is never a rejection, so the ban listener can never see it")
	})

	// A duplicate refused by the real sink leaves nothing behind either way.
	require.False(t, sm.blockPark.Has(hash))

	exists, err := store.Exists(sm.ctx, hash[:], fileformat.FileTypeBlock)
	require.NoError(t, err)
	require.False(t, exists, "no converted record may be left under a refused block's hash")
}

// stallingConn is a net.Conn whose writes hang once stall is called, which is
// what a socket does when the remote has stopped reading and the kernel send
// buffer is full. Close releases every hung write with an error, as closing the
// socket does. Reads are untouched, so the remote can keep sending.
type stallingConn struct {
	net.Conn

	stalled   chan struct{}
	released  chan struct{}
	stallOnce sync.Once
	closeOnce sync.Once
}

func newStallingConn(c net.Conn) *stallingConn {
	return &stallingConn{Conn: c, stalled: make(chan struct{}), released: make(chan struct{})}
}

// stall makes every Write from now on hang until Close.
func (c *stallingConn) stall() {
	c.stallOnce.Do(func() { close(c.stalled) })
}

func (c *stallingConn) Write(b []byte) (int, error) {
	select {
	case <-c.stalled:
		<-c.released

		return 0, net.ErrClosed
	default:
		return c.Conn.Write(b)
	}
}

func (c *stallingConn) Close() error {
	c.closeOnce.Do(func() { close(c.released) })

	return c.Conn.Close()
}

// connectStallingPeer is connectRecordingPeer with the local peer's writes
// routed through a stallingConn; stall hangs them from then on.
func connectStallingPeer(t *testing.T, idx uint8, lastBlock int32) (local, remote *peerpkg.Peer, rec *peerMsgRecorder, stall func()) {
	t.Helper()

	rec = &peerMsgRecorder{}
	chainParams := &chaincfg.MainNetParams

	remoteCfg := peerpkg.Config{
		Listeners: peerpkg.MessageListeners{
			OnReject: func(_ *peerpkg.Peer, msg *wire.MsgReject) { rec.recordReject(msg) },
		},
		UserAgentName:    "btcdtest",
		UserAgentVersion: "1.0",
		ChainParams:      chainParams,
	}
	localCfg := peerpkg.Config{
		UserAgentName:    "btcdtest",
		UserAgentVersion: "1.0",
		ChainParams:      chainParams,
	}

	conn1, conn2 := Pipe(
		&SimpleAddr{net: "tcp", addr: "10.0.0." + strconv.Itoa(int(idx)) + ":8333"},
		&SimpleAddr{net: "tcp", addr: "10.0.1." + strconv.Itoa(int(idx)) + ":8333"},
	)
	localConn := newStallingConn(conn2)

	remote, local, err := makeConnectedPeersOn(t, remoteCfg, localCfg, conn1, localConn)
	require.NoError(t, err)

	local.UpdateLastBlockHeight(lastBlock)

	t.Cleanup(func() {
		local.DisconnectWithInfo("test over")
		remote.DisconnectWithInfo("test over")
	})

	return local, remote, rec, localConn.stall
}

// TestSyncManager_APeerThatStopsReadingDoesNotStallTheDrain pins the drain's
// liveness against the peer it is punishing. A block judged invalid at the
// drain earns its peer a reject and the association dropped, and the drain runs
// on the in-order commit goroutine: every later commit waits behind it. The
// peer here has stopped reading our socket, the way a remote with a full
// receive window does, so a write to it never returns and no write deadline on
// the connection ends it.
//
// End state: the parent is committed, the judged block is gone from the park and
// written off, the drain step returned long before anything bounded the write
// to the peer, the peer is dropped anyway, and the remote never saw the reject,
// because the reject's one chance to leave was the socket that was closed under
// it. That last cost is accepted: a peer that will not read is told nothing.
//
// Before this fix the drain pushed the reject with wait=true and waited for the
// write on the commit goroutine, so a RUNNING-state peer that stopped reading
// held every commit until its socket died.
func TestSyncManager_APeerThatStopsReadingDoesNotStallTheDrain(t *testing.T) {
	h := newParkWiringHarnessInState(t, true, blockchain2.FSMStateRUNNING)

	stalledPeer, _, rec, stall := connectStallingPeer(t, 74, 1000)
	registerRacePeer(h.sm, stalledPeer)
	h.peer = stalledPeer

	child := h.blocks[1].MsgBlock().BlockHash()
	parent := h.blocks[0].MsgBlock().BlockHash()

	h.validation.failOnce(child, errors.NewBlockInvalidError("[ValidateBlock][%s] block is invalid", child.String()))

	require.NoError(t, h.deliver(t, 1))
	require.Equal(t, 1, h.sm.blockPark.Len(), "the child parks until its parent commits")

	// The remote stops reading from here on; the handshake and the delivery
	// above went through.
	stall()

	h.sm.parkCommits = make(chan parkCommit, parkSweepRPCBudget)
	require.NoError(t, h.deliver(t, 0))

	var commit parkCommit
	select {
	case commit = <-h.sm.parkCommits:
	default:
		t.Fatal("the parent's arrival posted no parkCommit")
	}

	// drainOneParkCommit's two steps, run off the test goroutine so a drain
	// that waits on the peer's socket is a failed assertion and not a hung
	// test. The drain commits the parent and judges the child.
	drained := make(chan struct{})

	go func() {
		defer close(drained)

		h.sm.blockPark.Restore(commit.entry)
		h.sm.scheduleDrain(commit.entry.prevBlock, commit.parentHeight)
	}()

	select {
	case <-drained:
	case <-time.After(time.Second):
		t.Fatal("the drain is still waiting on the punished peer's socket: the in-order commit goroutine must never wait for a peer to read")
	}

	h.requireCommitted(t, parent)
	require.Equal(t, 1, h.validation.callsFor(child), "the child reached block validation, which is what judged it")
	require.Zero(t, h.sm.blockPark.Len(), "a judged block does not stay parked")

	_, failed := h.sm.recentlyFailedBlocks.Get(child)
	require.True(t, failed, "a judged block is written off")

	require.True(t, disconnectsWithin(stalledPeer, 10*time.Second),
		"the peer that delivered a block judged invalid is dropped even though it never read the reject")
	require.False(t, rec.wasRejected(child), "the stall held: the reject died with the socket, which is the accepted cost of a peer that will not read")
}
