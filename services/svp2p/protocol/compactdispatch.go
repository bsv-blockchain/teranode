package protocol

import (
	"context"
	"io"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/services/svp2p/transport"
	"github.com/bsv-blockchain/teranode/ulogger"
)

// releaseTxnStream drains and closes an undelivered blocktxn payload OFF the
// peer loop.
//
// It must never run on Peer.Run's goroutine. payloadStream.Close drains the
// whole DECLARED payload with io.Copy and the transport sets no read deadline,
// so a peer that promises a large payload and then dribbles would otherwise
// hold the goroutine that services the idle timer, the ping ticker and ctx
// cancellation for as long as it liked. peer.go startIngest states the same
// rule for the block path, which refuses rather than drains for this reason.
//
// The goroutine is bounded by the connection, not by the peer's patience: the
// transport read loop stays parked until this drain ends, so no further message
// arrives, the idle timer ends the peer, and Peer.disconnect closes the socket —
// which fails the read inside io.Copy and releases this goroutine.
func releaseTxnStream(logger ulogger.Logger, ts *transport.TxnStream) {
	hash := ts.BlockHash()

	go func() {
		if err := ts.Close(); err != nil {
			logger.Debugf("[svp2p] blocktxn stream for %s released with: %v", hash, err)
		}
	}()
}

// CompactReady is one reconstructed compact block, handed back to the peer
// loop so the ingest runs where a plain block's ingest runs: on the peer's own
// ingest goroutine, off the loop that owns the idle timer (peer.go
// startIngest's own doc comment).
//
// It carries no SizeBytes counterpart. A compact block's payload is not the
// block's size — the bytes arrive from three different places (the cmpctblock
// itself, the index, and the blocktxn) and none of them declares the total —
// so the honest answer is that nobody knows it yet, and the field the ingestor
// reads is left at zero rather than filled with a guess.
type CompactReady struct {
	// Header is the block header the cmpctblock carried, already accepted
	// into the index.
	Header *wire.BlockHeader

	// Hash is Header.BlockHash(), passed alongside it because the caller
	// needs it for the ingest report and BlockHash() is not free.
	Hash chainhash.Hash

	// TxCount is the block's transaction count, which the compact block
	// declares as its slot count.
	TxCount uint64

	// TxReader is the assembled transaction stream. Closing it releases both
	// the assembler and the blocktxn stream underneath it, so the transport
	// read loop is freed on every exit path.
	TxReader io.ReadCloser
}

// SendCmpct dispatches NetMsgType::SENDCMPCT (net_processing.cpp
// ProcessSendCompactMessage:2390-2411) to the sending peer's sync state. It
// is the compactDispatcher half of Task 6; see that interface's own doc
// comment for why it is wired independent of SyncEnabled().
func (m *PeerManager) SendCmpct(sp *SyncPeer, msg *wire.MsgSendcmpct) {
	m.syncMu.Lock()
	defer m.syncMu.Unlock()

	sp.State.recordSendCmpct(msg)
}

// CompactBlock dispatches NetMsgType::CMPCTBLOCK, the port of
// ProcessCompactBlockMessage (net_processing.cpp:3693-3979) reduced to the
// branch this receive-only port takes: accept the header, decide whether the
// block is worth reconstructing, claim it, reconstruct what the index can
// fill, and either ask for the gaps or hand the finished block to the caller.
//
// It returns the messages to send, the reconstructed block when there is one,
// the misbehaviour delta the message earned, and an error only when the peer
// must be disconnected — the same four-part contract syncDispatcher.Headers
// uses, and for the same reason: this method performs no I/O (spec §4.3).
//
// THE ABOVE-THE-CEILING FALLBACKS (:3902-3923). A block more than
// MaxCompactBlockHeightAhead above our tip takes one of two branches in
// SVNode, and this port reaches the same end state through machinery it
// already has rather than by porting either branch literally:
//
//   - :3904-3911, the block is already in flight: push a getdata for it. Here
//     the scheduler owns getdata and re-offers the block on its own tick
//     (BlockDownloader.SendGetDataBlocks), so the same request goes out one
//     TickInterval later.
//   - :3913-3921, otherwise: "we want the same treatment as a header message"
//     and the announcement is reprocessed as a plain headers message. This
//     port's header accept below runs UNCONDITIONALLY, ahead of every guard,
//     so the header is already in the index and updateBlockAvailability has
//     already run by the time the ceiling refuses the block — which is the
//     whole of what that branch achieves.
//
// WHAT IS NOT PORTED. The optimistic reconstruction at :3883-3900, for a block
// already in flight from ANOTHER peer, needs a second partial block per hash
// and buys only a saved round trip.
//
// ctx is the peer loop's own context, which the assembled stream reads the
// index through; a disconnect cancels it, and the ingest with it.
func (m *PeerManager) CompactBlock(ctx context.Context, sp *SyncPeer, msg *wire.MsgCmpctBlock) ([]wire.Message, *CompactReady, int, error) {
	hash := msg.Header.BlockHash()

	// Read before syncMu is taken: txIndex() takes m.mu, and the two mutexes
	// are never held together (see the note on syncMu). A nil index is the
	// flag-off state, whatever legacy_compactBlocks says (SetTxIndex).
	idx := m.txIndex()

	if !m.tSettings.Legacy.CompactBlocks || idx == nil {
		m.logger.Debugf("[svp2p] ignoring cmpctblock for %s from %s: compact blocks are off", hash, peerAddrOf(sp))

		return nil, nil, 0, nil
	}

	out, score, claimed, err := m.claimCompactBlock(sp, &msg.Header, hash)
	if !claimed {
		return out, nil, score, err
	}

	// InitData walks the whole short ID list against the index, which hashes
	// every entry the index holds. It runs with no lock held, the same
	// contract txIndex()'s own doc comment states for both TxIndex methods.
	state, status, err := newCompactState(msg, idx)

	switch {
	case status == readInvalid:
		// net_processing.cpp:3849-3855: MarkBlockAsFailed, then
		// Misbehaving(pfrom, 100, "invalid-cmpctblk").
		m.failCompactBlock(sp, hash)

		return nil, nil, scoreInvalidBlock, errors.New(errors.ERR_NETWORK_PEER_MALICIOUS,
			"svp2p: peer sent an invalid compact block %s", hash, ErrCompactBlockInvalid)

	case status == readFailed:
		// net_processing.cpp:3856-3861 pushes a getdata for the block right
		// here and keeps it in flight. This port's scheduler owns getdata and
		// sends it on its own tick, so the block is released instead and goes
		// back on offer, which is the same outcome one tick later. No score:
		// SVNode does not punish this either ("Duplicate txindices").
		m.failCompactBlock(sp, hash)
		m.logger.Debugf("[svp2p] compact block for %s unreconstructable, falling back to getdata: %v", hash, err)

		return nil, nil, 0, nil
	}

	// net_processing.cpp:3864-3869 collects every slot the index could not
	// fill; :3870-3881 either asks for them or completes the block at once.
	if req := state.gapRequest(); req != nil {
		if !m.holdCompactBlock(sp, state) {
			m.logger.Debugf("[svp2p] dropping compact block %s from %s: the claim ended while it was reconstructed",
				hash, peerAddrOf(sp))

			return nil, nil, 0, nil
		}

		return []wire.Message{req}, nil, 0, nil
	}

	// No blocktxn was asked for, so this partial block goes straight to the
	// ingesting state: nothing may fill it again.
	if !m.handOffCompactBlock(sp, state) {
		m.logger.Debugf("[svp2p] dropping compact block %s from %s: the claim ended while it was reconstructed",
			hash, peerAddrOf(sp))

		return nil, nil, 0, nil
	}

	return nil, readyFrom(ctx, state, idx, nil), 0, nil
}

// claimCompactBlock is ProcessCompactBlockMessage's cs_main section
// (net_processing.cpp:3718-3845) up to and including MarkBlockAsInFlight: the
// header accept, the availability update, the guards wantCompact carries, and
// the claim itself. claimed is false when the announcement goes no further,
// in which case out, score and err are the verdict.
func (m *PeerManager) claimCompactBlock(sp *SyncPeer, header *wire.BlockHeader, hash chainhash.Hash) (out []wire.Message, score int, claimed bool, err error) {
	m.syncMu.Lock()
	defer m.syncMu.Unlock()

	if m.headerSync == nil || m.blockDownloader == nil || sp == nil || sp.State == nil {
		return nil, 0, false, nil
	}

	// net_processing.cpp:3721-3733: a header whose parent we do not hold is
	// NOT run through AcceptBlockHeader — "Doesn't connect (or is genesis),
	// instead of DoSing in AcceptBlockHeader, request deeper headers". The
	// peer is not scored for announcing a block we are simply behind on; we
	// ask for the headers that would connect it.
	if _, known := m.headerIndex.Lookup(header.PrevBlock); !known {
		// :3725 gates the push on !IsInitialBlockDownload(). This port's
		// proxy for that predicate is HeaderSync.tipIsNearAdjustedTime, the
		// same one BlockDownloader.maxDownloadTimeMicros uses and for the same
		// stated reason: the work half of IsInitialBlockDownload has no
		// counterpart here, so the age half carries it. Without the gate a
		// peer can make a catching-up node emit one full locator per
		// non-connecting announcement, which is a small message answered by a
		// large one.
		if !m.headerSync.tipIsNearAdjustedTime() {
			m.logger.Debugf("[svp2p] ignoring non-connecting cmpctblock %s from %s: still in initial sync",
				hash, peerAddrOf(sp))

			return nil, 0, false, nil
		}

		m.logger.Debugf("[svp2p] cmpctblock %s from %s does not connect, asking for headers", hash, peerAddrOf(sp))

		return []wire.Message{m.blockDownloader.getHeadersFor(chainhash.Hash{})}, 0, false, nil
	}

	// net_processing.cpp:3740-3762 ProcessNewBlockHeaders. A header already in
	// the index is answered from it, the duplicate short-circuit
	// AcceptBlockHeader takes at validation.cpp:6104-6117 and acceptHeaders
	// documents at length.
	node, known := m.headerIndex.Lookup(hash)

	if !known {
		inserted, headerScore, accepted, acceptErr := m.headerSync.acceptHeader(header, hash)
		if acceptErr != nil {
			return nil, 0, false, acceptErr
		}

		// net_processing.cpp:3743-3751: the refusal's own DoS value, applied
		// only when it is above zero, exactly as a bad headers entry is.
		if !accepted {
			m.logger.Debugf("[svp2p] peer %s sent an invalid header via cmpctblock %s", peerAddrOf(sp), hash)

			return nil, headerScore, false, nil
		}

		node = inserted
	}

	// net_processing.cpp:3789-3790 UpdateBlockAvailability, before any of the
	// guards below, so a block we decline to reconstruct still counts as
	// announced.
	sp.State.updateBlockAvailability(m.headerIndex, hash)

	if !m.blockDownloader.wantCompact(sp, node, m.activeTip) {
		m.logger.Debugf("[svp2p] ignoring cmpctblock %s at height %d from %s: not wanted at tip height %d",
			hash, node.Height, peerAddrOf(sp), m.activeTip.Height)

		return nil, 0, false, nil
	}

	// net_processing.cpp:3839-3844, "Peer sent us compact block we were
	// already syncing!" — see peerSyncState.compact for why this port reads it
	// as one partial block per peer rather than one per in-flight entry. Both
	// states count: a reconstruction still being ingested is as much "already
	// syncing" as one waiting on a blocktxn.
	if outstanding := sp.State.compact; outstanding != nil {
		m.logger.Debugf("[svp2p] ignoring cmpctblock %s from %s: already reconstructing %s",
			hash, peerAddrOf(sp), outstanding.hash)

		return nil, 0, false, nil
	}

	if ingesting := sp.State.compactIngest; ingesting != nil {
		m.logger.Debugf("[svp2p] ignoring cmpctblock %s from %s: still ingesting %s",
			hash, peerAddrOf(sp), ingesting.hash)

		return nil, 0, false, nil
	}

	// net_processing.cpp:3832-3845 MarkBlockAsInFlight. A false return means
	// the block was ALREADY in flight from this peer, which the source does not
	// read as a refusal — it reuses that QueuedBlock and hangs the partial
	// block off it (:3833-3838). wantCompact has already applied the :3826-3828
	// rule that decides whether a claim may be taken at all, so the return
	// value carries no further verdict here.
	m.blockDownloader.MarkBlockAsInFlight(sp, node, time.Now().UnixMicro())

	return nil, 0, true, nil
}

// holdCompactBlock records a partial block that is WAITING on a blocktxn. It
// reports false when the claim it belongs to has gone, in which case nothing is
// held and the announcement ends here.
//
// The re-check is not defensive noise. CompactBlock releases syncMu to run
// newCompactState, because TxIndex.Match hashes every entry the index holds and
// must not run under the lock. A disconnect, a sync-peer rotation, or another
// peer delivering the block inside that window ends the claim through clearPeer
// or removeFromFlight; writing the partial block afterwards would resurrect it
// for a claim that no longer exists, and a stranded partial block refuses every
// later cmpctblock from this peer for the life of the connection.
func (m *PeerManager) holdCompactBlock(sp *SyncPeer, state *compactState) bool {
	m.syncMu.Lock()
	defer m.syncMu.Unlock()

	if m.blockDownloader == nil || !m.blockDownloader.IsInFlightFrom(sp, state.hash) {
		return false
	}

	sp.State.compact = state

	return true
}

// handOffCompactBlock moves a partial block from "waiting on a blocktxn" to
// "being ingested", which is what stops a replayed blocktxn from filling it a
// second time. It is this port's stand-in for the MarkBlockAsReceived at
// net_processing.cpp:3646, which destroys the QueuedBlock the partial block
// hangs off before the block is processed. See peerSyncState.compactIngest.
// It reports false for the same reason holdCompactBlock does, and on the same
// window: the no-gaps path reaches it straight from newCompactState, with the
// lock released in between.
func (m *PeerManager) handOffCompactBlock(sp *SyncPeer, state *compactState) bool {
	m.syncMu.Lock()
	defer m.syncMu.Unlock()

	if m.blockDownloader == nil || !m.blockDownloader.IsInFlightFrom(sp, state.hash) {
		return false
	}

	sp.State.compact = nil
	sp.State.compactIngest = state

	return true
}

// failCompactBlock releases a claim the reconstruction could not use, which is
// net_processing.cpp's own MarkBlockAsFailed on both the INVALID and the
// FAILED branch (:3851, :3856-3861 by way of the getdata that follows it).
func (m *PeerManager) failCompactBlock(sp *SyncPeer, hash chainhash.Hash) {
	m.syncMu.Lock()
	defer m.syncMu.Unlock()

	sp.State.compact = nil
	sp.State.compactIngest = nil

	m.blockDownloader.BlockFailed(sp, hash, time.Now().UnixMicro())
}

// BlockTxn dispatches NetMsgType::BLOCKTXN, the port of
// ProcessBlockTxnMessage (net_processing.cpp:3576-3688). The reply arrives as
// a live stream rather than a decoded message (transport.TxnStream), so this
// method takes ownership of it: every path either closes it or hands it on
// inside the returned CompactReady.
//
// The four-part return is CompactBlock's, unchanged.
func (m *PeerManager) BlockTxn(ctx context.Context, sp *SyncPeer, ts *transport.TxnStream) ([]wire.Message, *CompactReady, int, error) {
	hash := ts.BlockHash()

	// Read before syncMu, like CompactBlock's own read: txIndex() takes m.mu,
	// and the two mutexes are never held together.
	idx := m.txIndex()

	state, status, err := m.fillCompactBlock(sp, hash, ts)

	switch {
	case state == nil:
		// net_processing.cpp:3602-3606: "Peer %d sent us block transactions
		// for block we weren't expecting" is logged and the message dropped.
		// There is no Misbehaving call on that path and no MarkBlockAsFailed —
		// the only score in this function is the READ_STATUS_INVALID branch
		// below. A blocktxn racing a claim this node released (a rotation, a
		// timeout, the handoff of a reply already accepted) is a timing
		// artefact, not evidence of malice.
		releaseTxnStream(m.logger, ts)

		m.logger.Debugf("[svp2p] peer %s sent us block transactions for block %s we weren't expecting",
			peerAddrOf(sp), hash)

		return nil, nil, 0, nil

	case status == readInvalid:
		// net_processing.cpp:3610-3616: MarkBlockAsFailed, then
		// Misbehaving(pfrom, 100, "invalid-cmpctblk-txns").
		//
		// The stream is deliberately NOT closed here. This return disconnects
		// the peer, and Peer.disconnect closes the connection, which releases
		// the parked read loop through sockClosed without reading a byte
		// (transport/conn.go serveStream). Draining first would run io.Copy
		// over the undelivered remainder on the peer loop, for a peer we are
		// about to drop — the refusal pattern peer.go startIngest documents.
		return nil, nil, scoreInvalidBlock, err

	case status != readOK:
		// Unreachable today: fill returns only readOK or readInvalid. Handled
		// rather than fallen through, because falling through would hand the
		// caller an ingest of a block fillCompactBlock has already failed and
		// put back on offer.
		releaseTxnStream(m.logger, ts)

		m.logger.Debugf("[svp2p] compact block for %s could not be filled: %v", hash, err)

		return nil, nil, 0, nil
	}

	// legacy_compactBlocks was turned off, or the index was withdrawn, between
	// the cmpctblock and this reply. The assembler would fail the block slot by
	// slot at openHeld; release it here instead and let the ordinary getdata
	// path take it. Not the peer's fault, so no score.
	if idx == nil {
		releaseTxnStream(m.logger, ts)

		m.failCompactBlock(sp, hash)
		m.logger.Debugf("[svp2p] compact block for %s abandoned: no transaction index", hash)

		return nil, nil, 0, nil
	}

	return nil, readyFrom(ctx, state, idx, ts), 0, nil
}

// fillCompactBlock is ProcessBlockTxnMessage's cs_main section
// (net_processing.cpp:3588-3646): find the partial block this reply belongs
// to, then FillBlock. A nil state means there was no outstanding request this
// reply answers.
//
// It latches the stream into the partial block rather than reading it (see
// compactState.fill): the transactions are consumed later, by the assembled
// stream, on the ingest goroutine.
func (m *PeerManager) fillCompactBlock(sp *SyncPeer, hash chainhash.Hash, ts *transport.TxnStream) (*compactState, readStatus, error) {
	m.syncMu.Lock()
	defer m.syncMu.Unlock()

	if m.blockDownloader == nil || sp == nil || sp.State == nil {
		return nil, readOK, nil
	}

	state := sp.State.compact

	// GetBlockDetails looks the reply up by (block hash, node id) and throws
	// when either misses. `requested` is the third condition C++ gets for
	// free: a partial block with no getblocktxn outstanding has nothing a
	// blocktxn could answer.
	if state == nil || state.hash != hash || !state.requested {
		return nil, readOK, nil
	}

	status, err := state.fill(ts.Count(), ts.Reader())
	if status != readOK {
		sp.State.compact = nil
		sp.State.compactIngest = nil

		m.blockDownloader.BlockFailed(sp, hash, time.Now().UnixMicro())

		return state, status, err
	}

	// The handoff, done HERE rather than by the caller so it is inside the same
	// critical section as the guard above: from this moment the partial block is
	// no longer reachable as `compact`, so a replayed blocktxn finds nothing and
	// takes the unsolicited path. This is the MarkBlockAsReceived at
	// net_processing.cpp:3646, which destroys the QueuedBlock before the block
	// is processed.
	sp.State.compact = nil
	sp.State.compactIngest = state

	return state, readOK, nil
}

// readyFrom builds the ingest request's half of a finished reconstruction.
// assembleTxs is the txs-only stream, because BlockIngestRequest carries the
// header separately and the ingestor expects the transactions alone, exactly
// as transport.BlockStream.TxReader hands them over for a plain block.
func readyFrom(ctx context.Context, state *compactState, idx TxIndex, gaps io.Closer) *CompactReady {
	header := state.header

	count, txs := state.assembleTxs(ctx, idx)

	return &CompactReady{
		Header:   &header,
		Hash:     state.hash,
		TxCount:  count,
		TxReader: newCompactStream(txs, gaps),
	}
}

// peerAddrOf names a peer for a log line, answering "unknown" for the test
// peers this package builds without a SyncPeer.
func peerAddrOf(sp *SyncPeer) string {
	if sp == nil {
		return "unknown"
	}

	return sp.Addr
}

// takeCompactStatus removes the INGESTING partial block for hash from this
// peer, and reports the status its assembly reached. found is false when no compact
// block from this peer produced the ingest being reported — the ordinary case,
// since most blocks arrive whole.
//
// Requires the caller to hold PeerManager's sync-state mutex.
func takeCompactStatus(sp *SyncPeer, hash chainhash.Hash) (status readStatus, found bool) {
	if sp == nil || sp.State == nil || sp.State.compactIngest == nil || sp.State.compactIngest.hash != hash {
		return readOK, false
	}

	status = sp.State.compactIngest.fillStatus()
	sp.State.compactIngest = nil

	return status, true
}

// compactStream closes the assembled reader AND the blocktxn stream
// underneath it. The assembler owns neither the socket nor the payload
// boundary: transport.TxnStream does, and it stays parked until it is closed
// (payloadstream.go). Handing the ingestor only the assembler would leave the
// read loop holding this connection for ever once the block was ingested.
type compactStream struct {
	txs  io.ReadCloser
	gaps io.Closer
}

func newCompactStream(txs io.ReadCloser, gaps io.Closer) *compactStream {
	return &compactStream{txs: txs, gaps: gaps}
}

func (c *compactStream) Read(p []byte) (int, error) { return c.txs.Read(p) }

// Close releases both halves and reports the assembler's own error, which is
// the one that says whether the block was assembled. The stream's drain error
// is not returned: an unread blocktxn tail is a bookkeeping detail of the
// socket, not a verdict on the block.
func (c *compactStream) Close() error {
	err := c.txs.Close()

	if c.gaps != nil {
		_ = c.gaps.Close()
	}

	return err
}
