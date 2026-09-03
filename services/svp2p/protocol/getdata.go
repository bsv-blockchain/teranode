package protocol

import (
	"context"
	"io"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/services/svp2p/transport"
)

// BlockTxFetcher is this package's whole view of the Teranode-side READ path,
// the mirror of BlockIngestor on the write side. Spec §4.4 forbids protocol
// from importing the bridge or any Teranode client, so the interface is
// declared here, where the getdata path consumes it, and satisfied in the
// svp2p package beside blockIngestor.
type BlockTxFetcher interface {
	// FetchBlock streams a block's legacy-wire bytes — the 80 byte header, the
	// transaction count varint, then the transactions — and reports the
	// declared length that goes into the message header.
	//
	// It is called TWICE for every block served, once to hash the payload and
	// once to write it (see transport.Conn.SendBlock), so it must be able to
	// deliver the same block twice and must report a block it no longer holds
	// as errors.ErrBlockNotFound rather than as a bare failure.
	FetchBlock(ctx context.Context, hash *chainhash.Hash) (io.ReadCloser, uint64, error)

	// FetchTx returns a transaction's serialized bytes. A transaction this
	// node does not hold in full is errors.ErrTxNotFound; every other error is
	// a lookup failure, and the two must stay distinguishable — one is
	// answered with notfound and the other must claim nothing.
	FetchTx(ctx context.Context, hash *chainhash.Hash) ([]byte, error)
}

// serveOutcome is what one inventory entry got. The values are deliberately
// distinct rather than a bool, because "we do not hold it", "our lookup
// failed" and "we hold it but cannot frame it" each get a different answer,
// and collapsing any two of them lies to the peer.
type serveOutcome int

const (
	// serveSent means the data is on the wire.
	serveSent serveOutcome = iota

	// serveAbsent means we do not hold it. Both a transaction and a block
	// become a notfound entry, which for the block is a deliberate divergence
	// from SVNode (see Serving.OnGetData).
	serveAbsent

	// serveFailed means our own store or send failed. Claim nothing: a
	// notfound here would tell the peer to stop asking for something we may
	// well have.
	serveFailed

	// serveUnframeable is the >4 GiB block for a peer below 70016, the
	// extended-header floor — SVNode's GetMaxPayloadLength(version) has the
	// same floor. It is answered with notfound like serveAbsent, and kept
	// distinct from it because the cause and the log line are different.
	serveUnframeable
)

// maxPendingGetData bounds the inventory entries one peer may have waiting to
// be served — the memory bound SVNode's own vRecvGetData does not have.
//
// It is exactly wire.MaxInvPerMsg, the cap go-wire's decoder already applies
// to a single inbound getdata, and that number is chosen for a reason: it
// guarantees no LEGAL single request is ever partially refused, which is the
// property that matters for correctness. A peer that pipelines more than one
// full message's worth faster than we can answer is refused the excess with a
// warning.
//
// Cost: an entry is a getDataItem plus the wire.InvVect it points at, about 56
// bytes, so a peer holding the cap costs roughly 2.8 MB — and only while it is
// draining. SVNode needs no such cap because its answer to the same pressure
// is the GetPausedForSending break plus disconnecting a misbehaving peer,
// neither of which is a memory bound.
const maxPendingGetData = wire.MaxInvPerMsg

// queueGetData classifies one getdata and appends it to this peer's pending
// queue — the port of pfrom->vRecvGetData. It runs on the Run goroutine and
// must never block it: answering a getdata can take minutes.
func (p *Peer) queueGetData(msg *wire.MsgGetData) {
	if p.cfg.Fetcher == nil {
		// No read path is wired (a depless server). SVNode and legacy would
		// both answer; there is nothing here to answer from.
		p.cfg.Logger.Debugf("[svp2p] ignoring getdata from %s: no read path is wired", p.cfg.Conn.RemoteAddr())

		return
	}

	items := p.cfg.Sync.GetData(p.cfg.SyncPeer, msg)
	if len(items) == 0 {
		return
	}

	p.getDataMu.Lock()

	room := maxPendingGetData - len(p.getData)
	refused := 0

	if len(items) > room {
		refused = len(items) - room
		items = items[:room]
	}

	p.getData = append(p.getData, items...)
	pending := len(p.getData)

	p.getDataMu.Unlock()

	if refused > 0 {
		p.cfg.Logger.Warnf("[svp2p] refused %d of %d getdata entries from %s: %d already pending, the per-peer cap is %d",
			refused, refused+len(items), p.cfg.Conn.RemoteAddr(), pending, maxPendingGetData)
	}

	// Non-blocking: the serve loop drains until the queue is empty, so a
	// signal already waiting covers whatever was just appended.
	select {
	case p.getDataWake <- struct{}{}:
	default:
	}
}

// nextGetData pops the front of the pending queue, the C++ `it++` over
// vRecvGetData.
func (p *Peer) nextGetData() (getDataItem, bool) {
	p.getDataMu.Lock()
	defer p.getDataMu.Unlock()

	if len(p.getData) == 0 {
		return getDataItem{}, false
	}

	item := p.getData[0]

	// The consumed entry is cleared as well as dropped, so a long-lived
	// backing array cannot pin the InvVects behind it.
	p.getData[0] = getDataItem{}
	p.getData = p.getData[1:]

	return item, true
}

func (p *Peer) getDataPending() bool {
	p.getDataMu.Lock()
	defer p.getDataMu.Unlock()

	return len(p.getData) > 0
}

// serveLoop drains the pending getdata queue one pass at a time.
//
// It is a goroutine of its own for the same reason startIngest is: hashing and
// streaming a multi-gigabyte block takes minutes, and the Run goroutine has to
// keep the idle timer honest, answer pings and observe shutdown while it
// happens.
func (p *Peer) serveLoop(ctx context.Context) {
	for {
		select {
		case <-p.getDataWake:
			for p.getDataPending() && ctx.Err() == nil {
				p.servePass(ctx)
			}

		case <-ctx.Done():
			return

		case <-p.gone:
			return
		}
	}
}

// servePass is one ProcessGetData call (net_processing.cpp:1163). It answers
// pending entries in the peer's own request order until it has handled ONE
// block-type entry, then returns with the remainder of the queue intact.
//
// What the break does NOT buy here, stated plainly because the C++ shape
// invites the wrong conclusion: it is not a bound on how long a peer occupies
// this goroutine. serveLoop runs the next pass immediately, so a peer that
// asked for MaxInvPerMsg blocks holds its OWN serve goroutine until the request
// is done. The C++ break yields to a shared message loop serving every peer;
// there is no shared loop here to yield to, so there is nothing to yield.
//
// What it does buy, and the only reasons it is worth porting:
//
//   - notfound scope. vNotFound is local to one ProcessGetData call, so the
//     break is what makes a notfound answer a pass instead of a whole request.
//   - The plug point for GetPausedForSending (net_processing.cpp:1176-1179),
//     which IS a real bound and is unported only because the transport cannot
//     yet report send-queue depth. It goes at the top of this loop.
//
// A peer's serve goroutine is bounded instead by maxPendingGetData and by the
// socket itself. Do not let this comment claim otherwise: an overstated safety
// property is how a later change convinces itself a bound exists.
//
// A notfound closes the PASS rather than the whole request, which is again the
// C++ shape: vNotFound is local to one ProcessGetData call, so a request whose
// misses straddle a block boundary draws one notfound per pass.
//
// That contradicts the plan text's "one trailing notfound", and the
// contradiction is in the plan, not here: once a pass can end at the first
// block-type entry, a single trailing notfound is UNREACHABLE for any request
// mixing blocks with misses, because the entries after the block have not been
// looked at when the pass ends. The plan's phrasing predates the pacing being
// ported. Recorded so this does not read as an oversight.
//
// Locking: nothing here holds PeerManager's sync-state mutex. ContinueInv takes
// it itself for the length of the call and performs no I/O. The package lock
// order is peer lock then manager lock, and this goroutine holds neither while
// it fetches or sends. That is not an optimisation: a block send is the most
// blocking thing this service does, and holding syncMu across one would stall
// every peer.
func (p *Peer) servePass(ctx context.Context) {
	notFound := wire.NewMsgNotFound()

	for {
		if ctx.Err() != nil {
			return
		}

		item, ok := p.nextGetData()
		if !ok {
			break
		}

		switch item.kind {
		case getDataTx:
			// The ONLY entry kind a miss puts in a notfound.
			if p.serveTx(ctx, item.inv) == serveAbsent {
				p.addNotFound(notFound, item.inv)
			}

		case getDataBlock:
			// Both "we do not hold it" and "we hold it but cannot frame it" are
			// answered, which is legacy's shape rather than SVNode's — SVNode
			// notfounds no block on any path. See Serving.OnGetData for why the
			// divergence is worth its cost. serveFailed is the one outcome that
			// answers nothing: our lookup broke, so we know nothing about
			// whether we hold it.
			if outcome := p.serveBlock(ctx, item.inv); outcome == serveAbsent || outcome == serveUnframeable {
				p.addNotFound(notFound, item.inv)
			}

		case getDataFilteredBlock, getDataUnsupported:
			p.cfg.Logger.Warnf("[svp2p] unsupported inventory type %s requested by %s, answering nothing for it",
				item.inv.Type, p.cfg.Conn.RemoteAddr())
		}

		// C++: the unconditional break after any IsBlockType entry
		// (net_processing.cpp:1448-1452), served or not.
		if item.kind.blockType() {
			break
		}
	}

	if len(notFound.InvList) > 0 {
		p.send([]wire.Message{notFound})
	}
}

// addNotFound appends one entry to the pass's notfound. AddInvVect only
// refuses past MaxInvPerMsg, which is the cap go-wire already applied to the
// inbound request, so this cannot fire; the error is still not dropped,
// because a notfound the encoder would refuse must not be built silently.
func (p *Peer) addNotFound(notFound *wire.MsgNotFound, inv *wire.InvVect) {
	if err := notFound.AddInvVect(inv); err != nil {
		p.cfg.Logger.Warnf("[svp2p] notfound entry for %s dropped: %v", p.cfg.Conn.RemoteAddr(), err)
	}
}

// serveTx answers one MSG_TX entry (net_processing.cpp:1382, legacy
// pushTxMsg at peer_server.go:1976).
//
// The bytes go out verbatim rather than through a wire.MsgTx round trip.
// Legacy deserialized and re-serialized (peer_server.go:2033,
// bsvutil.NewTxFromBytes then QueueMessageWithEncoding), which for a large
// transaction allocates the whole structure twice for no gain; the same
// reasoning legacy's own RawBlockMessage records
// (services/legacy/raw_block_message.go:10-20). FetchTx has already verified
// that these bytes hash to the requested txid, so sending them unchanged is
// also the only form that cannot disagree with the hash the peer asked for.
func (p *Peer) serveTx(ctx context.Context, inv *wire.InvVect) serveOutcome {
	raw, err := p.cfg.Fetcher.FetchTx(ctx, &inv.Hash)
	if err != nil {
		if errors.Is(err, errors.ErrTxNotFound) {
			return serveAbsent
		}

		p.cfg.Logger.Warnf("[svp2p] tx %s lookup for %s failed, answering nothing for it: %v",
			inv.Hash, p.cfg.Conn.RemoteAddr(), err)

		return serveFailed
	}

	if err := p.cfg.Conn.Send(&rawTxMsg{raw: raw}); err != nil {
		// A refused send is our queue's state, not a statement about what we
		// hold, so it must not become a notfound.
		p.cfg.Logger.Warnf("[svp2p] dropped tx %s to %s: %v", inv.Hash, p.cfg.Conn.RemoteAddr(), err)

		return serveFailed
	}

	return serveSent
}

// serveBlock answers one MSG_BLOCK entry (net_processing.cpp:1189) by
// streaming the body, then runs the continuation if this was the hash that
// closed a full getblocks inv.
//
// The block is never materialized: transport.Conn.SendBlock hashes one
// streaming pass for the message checksum and writes a second one to the
// socket, so the peer's ban-scored checksum check (net_processing.cpp:4995-5015)
// is satisfied without the io.ReadAll legacy needed
// (services/legacy/raw_block_message.go:27). blockPasses is what turns the
// single-use stream FetchBlock returns into the two passes that needs.
func (p *Peer) serveBlock(ctx context.Context, inv *wire.InvVect) serveOutcome {
	body, length, err := p.cfg.Fetcher.FetchBlock(ctx, &inv.Hash)
	if err != nil {
		if errors.Is(err, errors.ErrBlockNotFound) {
			// Answered with notfound, unlike SVNode, so the peer can release
			// its in-flight assignment instead of waiting out the per-block
			// download timeout. See Serving.OnGetData.
			p.cfg.Logger.Debugf("[svp2p] block %s requested by %s is not held here, answering notfound",
				inv.Hash, p.cfg.Conn.RemoteAddr())

			return serveAbsent
		}

		p.cfg.Logger.Warnf("[svp2p] block %s lookup for %s failed, answering nothing for it: %v",
			inv.Hash, p.cfg.Conn.RemoteAddr(), err)

		return serveFailed
	}

	// OPEN QUESTION 5, decided from the declared length before a single
	// payload byte is read: a block a basic message header cannot declare is
	// framed with the extended header (transport.Conn.SendBlock) for a peer
	// that negotiated transport.ExtendedPayloadVersion, and refused with
	// notfound for one that has not.
	//
	// SVNode frames a payload this large with an extended header for every
	// peer (protocol.cpp:220-237) and has no version floor of its own beyond
	// CMessageHeader::GetMaxPayloadLength(version), which is exactly this
	// check. Below that floor, the owner closed OQ5 as notfound-plus-warn: it
	// reaches the peer the same way an absent block does, but stays a
	// distinct outcome because this one is a temporary limitation of that
	// peer's negotiated version, not a statement about what we hold.
	//
	// Cost of deciding it here rather than before the fetch: FetchBlock reports
	// the declared length and issues the HTTP request in the same call, so this
	// closes an opened-but-unread body — one request, zero bytes read — for a
	// peer below the floor. Reading the length without a body needs a
	// size-only method on BlockTxFetcher; the booked follow-up that stores the
	// payload hash at ingest removes the need for either.
	if length > transport.MaxBlockFrameBytes && p.negotiatedVersion() < transport.ExtendedPayloadVersion {
		_ = body.Close()

		p.cfg.Logger.Warnf("[svp2p] block %s is %d bytes and %s negotiated version %d, below the extended-header floor; answering notfound",
			inv.Hash, length, p.cfg.Conn.RemoteAddr(), p.negotiatedVersion())

		return serveUnframeable
	}

	passes := &blockPasses{fetch: p.cfg.Fetcher, hash: inv.Hash, length: length, held: body}
	defer passes.discard()

	if err := p.cfg.Conn.SendBlock(ctx, transport.BlockSendRequest{Length: length, Open: passes.open}); err != nil {
		switch {
		case errors.Is(err, errors.ErrBlockNotFound):
			// Reorged out between the two passes. Nothing was written.
			return serveAbsent

		case errors.Is(err, transport.ErrBlockLengthMismatch):
			// The declared size and the bytes actually streamed disagree.
			// Nothing was written, which is the point: a frame built from
			// either number lies about the other, and every peer on the
			// network would ban-score the checksum.
			//
			// Logged at warn, with the hash, because there is a benign cause an
			// operator must be able to tell from corruption: a reorg between
			// the two passes. The transport's own message carries both byte
			// counts.
			p.cfg.Logger.Warnf("[svp2p] block %s was not served to %s, the declared size and the streamed bytes disagree — a reorg between the two read passes, or a store-integrity fault: %v",
				inv.Hash, p.cfg.Conn.RemoteAddr(), err)

		default:
			p.cfg.Logger.Warnf("[svp2p] block %s send to %s failed: %v", inv.Hash, p.cfg.Conn.RemoteAddr(), err)
		}

		return serveFailed
	}

	// C++: `if (inv.hash == pfrom->hashContinue)` … push an inv of
	// chainActive.Tip() "right after the last block so they don't wait for
	// other stuff first" (net_processing.cpp:1364-1377). Only after a block
	// that was actually sent, which is the C++ wasSent guard.
	p.send(p.cfg.Sync.ContinueInv(p.cfg.SyncPeer, inv.Hash))

	return serveSent
}

// blockPasses hands the block body to the two passes SendBlock makes over it,
// without fetching it three times: the caller has already opened the body to
// learn the declared length, so that first reader IS the first pass, and only
// the second pass re-fetches.
//
// The two calls are strictly ordered — SendBlock hashes the first, then hands
// the request to the writer goroutine over a channel, which is the
// happens-before edge that makes held safe without a mutex.
type blockPasses struct {
	fetch  BlockTxFetcher
	hash   chainhash.Hash
	length uint64
	held   io.ReadCloser
}

// open returns the held reader once, then re-fetches. The re-fetch verifies
// that the declared length has not changed, because the checksum was computed
// against the first one; the byte COUNT of the second pass is verified by
// SendBlock itself as it copies.
func (b *blockPasses) open(ctx context.Context) (io.ReadCloser, error) {
	if r := b.held; r != nil {
		b.held = nil

		return r, nil
	}

	body, length, err := b.fetch.FetchBlock(ctx, &b.hash)
	if err != nil {
		return nil, err
	}

	if length != b.length {
		_ = body.Close()

		return nil, errors.New(errors.ERR_INVALID_ARGUMENT,
			"svp2p: block %s declared %d bytes on the first pass and %d on the second", b.hash, b.length, length,
			transport.ErrBlockLengthMismatch)
	}

	return body, nil
}

// discard closes a held reader that no pass consumed, which happens when
// SendBlock refuses the request before it opens anything.
func (b *blockPasses) discard() {
	if b.held != nil {
		_ = b.held.Close()
		b.held = nil
	}
}

// rawTxMsg is a "tx" message whose payload is already serialized. Same shape,
// and same reason, as legacy's RawBlockMessage
// (services/legacy/raw_block_message.go): it skips the
// deserialize-then-reserialize round trip a wire.MsgTx would cost, and
// guarantees the bytes on the wire are exactly the bytes the store returned.
type rawTxMsg struct {
	raw []byte
}

var _ wire.Message = (*rawTxMsg)(nil)

func (m *rawTxMsg) Command() string { return wire.CmdTx }

func (m *rawTxMsg) BsvEncode(w io.Writer, _ uint32, _ wire.MessageEncoding) error {
	_, err := w.Write(m.raw)

	return err
}

// Bsvdecode is never called: this type only ever goes out.
func (m *rawTxMsg) Bsvdecode(io.Reader, uint32, wire.MessageEncoding) error {
	return errors.New(errors.ERR_INVALID_ARGUMENT, "svp2p: rawTxMsg cannot be decoded")
}

// MaxPayloadLength is the real encoded size, not an upper bound, so the send
// lane's byte budget charges this message what it actually costs.
func (m *rawTxMsg) MaxPayloadLength(uint32) uint64 { return uint64(len(m.raw)) }
