package peer

import (
	"bytes"
	stderrors "errors"
	"io"
	"sync"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/errors"
)

// blockBodySink consumes a block's body streamed off the wire. It is handed the
// already-parsed header rather than header bytes, because every consumer needs the
// header as structure: the coinbase substitution in the first subtree and the
// merkle root to compare against both come from it, and re-parsing bytes the wire
// layer has already parsed puts avoidable work on the read loop.
//
// The reader it receives starts at the transaction count varint, NOT at the
// header. A consumer that wants a byte-for-byte copy of the block must
// re-serialize the header itself; that is one 80-byte write against a body of
// hundreds of megabytes.
//
// Nil until the sync manager installs one, and a nil sink means every block is
// decoded, which is what keeps callers that never wire a store working unchanged.
//
// The bool it returns says whether THIS call actually converted the block —
// see BlockBody.Converted for why that must come from here rather than be
// inferred afterward from anything in a store.
var blockBodySink func(hash chainhash.Hash, header *wire.BlockHeader, r io.Reader, n int64) (bool, error)

// blockBodyGate answers whether a block's body may be streamed to disk, and is
// the only thing standing between a peer and this node's disk. It is
// installed by the sync manager, which is the only place that knows both the
// chain's difficulty limit and what this node actually asked for; this
// package has neither, and checking proof of work here against nothing but
// the header's own declared target bounds nothing; a peer can declare
// whatever target it likes. A nil gate means no streaming at all: the handler
// falls back to decoding, exactly as a nil blockBodySink already does, so a
// caller that wires a sink but forgets a gate gets the old safe behaviour
// rather than an open door.
//
// length is the payload length the peer declared in the message header, known
// before any byte of the body is read, so a body too large for this node's own
// block policy can be refused without reading it.
var blockBodyGate func(hash chainhash.Hash, header *wire.BlockHeader, length uint64) error

// blockBodyDelete removes a block body already written under hash. It is
// installed alongside blockBodySink and blockBodyGate. A body can be fully
// written and only then found unusable, for example a peer's stream ending
// short of what it declared, or a transaction count that will not parse. An
// orphaned body left on disk under a well-formed hash is worse than a failed
// download: a failed download is simply retried by the download walk, but
// nothing downstream of this handler knows to distrust bytes sitting on disk
// under a hash that looks legitimate. Nil until the sync manager installs it,
// same as the other two.
//
// converted is blockBodySink's own return value for THIS call, passed straight
// through rather than re-derived. A hash can be re-requested and re-delivered
// while an earlier, still-parked delivery for it is waiting on its parent —
// the streaming gate accepts any hash the download ledger still holds a
// request for, and ownership is released as soon as a delivery's sink call
// finishes — so an
// implementation that inferred "did this call convert something" by asking
// whether a converted record merely exists for hash would find the OTHER
// delivery's genuine, still-needed record and destroy it. converted is what
// lets the installed callback tell the two apart without asking.
var blockBodyDelete func(hash chainhash.Hash, converted bool) error

// streamingBlockHandler is a wire.SetExternalHandler implementation for the
// "block" message that decodes the block payload directly from the network
// reader, avoiding the default ReadMessageWithEncodingN behaviour of
// allocating the full payload as a []byte before calling Bsvdecode. On fat
// blocks (multi-GB testnet stress blocks) that buffer alone reached
// ~2.86 GB of legacy heap inuse during sync, the second-largest contributor
// to RSS after the per-tx scratch buffer.
//
// Note on the wire-level DoubleHash checksum: this handler never verifies it.
// go-wire calls an external handler and returns its result before it reaches
// the checksum check (go-wire v1.2.11 message.go:488-490 hand the payload
// reader to the handler; the comparison is at :504-511, on the non-streaming
// path only), and the handler's signature carries no checksum, so there is
// nothing here to compare the body against. Corruption is still caught
// downstream: the sink rebuilds the merkle root from the bytes it read and
// checks it against the header, so a damaged body is refused rather than
// stored. What is lost is the ability to tell a body damaged in transit from
// a body the peer built wrong: both reach the sink's merkle-root,
// duplicate-transaction or zero-count check and raise
// ERR_BLOCK_BODY_MISMATCH. SV Node can tell them apart, because it drops a
// message whose checksum fails (net_processing.cpp ProcessMessages, scoring one
// point only for a burst of more than 100 within 500 ms), and bans only a body
// that passed the checksum and still did not match.
//
// So a rejection from here never has ChecksumVerified set, and therefore is
// never ProvenBad: the peer is sent a reject and disconnected, and not banned.
// Banning on these bytes would ban an honest peer for 24 hours over a bit flip
// that TCP's 16-bit checksum missed. The root fix is in go-wire: pass the
// header's checksum to the external handler, so this handler can double-SHA256
// the payload as it streams, treat a mismatch as a delivery fault (drain,
// disconnect, no ban), and set ChecksumVerified when it matches.
//
// Whenever both a sink and a gate are installed, the body is never decoded at
// all, regardless of size: the header is read, put to the gate, and
// everything after it goes straight to the sink. That is what keeps a
// multi-gigabyte block from ever existing as a Go object on this path.
// Streaming used to mean "write the body to disk", which only paid for a
// block too large to hold in memory, so a size threshold used to decide which
// path a block took. Streaming now means "convert the block as it arrives",
// which pays at every size, so there is no threshold left to apply.
func streamingBlockHandler(r io.Reader, length uint64, totalBytes int) (int, wire.Message, []byte, error) {
	// Cap the inner reader so a malformed varint cannot read past the declared
	// payload boundary and desync the next ReadMessage call.
	lr := &io.LimitedReader{R: r, N: int64(length)}

	msg, err := readBlockMessage(lr, length)

	// Drain any unread payload bytes so the next ReadMessage starts on a clean
	// header boundary, and surface a short stream as an error. io.Copy on a
	// LimitedReader returns nil if the underlying reader EOFs before N reaches
	// 0, so lr.N is checked explicitly. This also runs after a gate rejection,
	// since readBlockMessage returns as soon as the gate refuses without
	// touching the rest of lr, so the bytes it never read still need draining
	// here for the connection to stay on a clean message boundary.
	var drainErr error

	if lr.N > 0 {
		if _, copyErr := io.Copy(io.Discard, lr); copyErr != nil {
			drainErr = copyErr
		} else if lr.N > 0 {
			drainErr = errors.NewProcessingError("streaming block: peer declared %d byte payload but stream ended with %d bytes unread", length, lr.N)
		}
	}

	if err == nil {
		err = drainErr
	}

	// A sink refusal is only a judgement on the whole body if the whole body
	// arrived. drainErr is non-nil exactly when the declared payload did not: the
	// copy failed, or the stream ended with bytes still owed. The bit travels on
	// the typed error so the peer server can refuse to ban for a body it never
	// saw the end of, whatever code the sink chose; see BlockBodyRejectedError.
	var rejected *BlockBodyRejectedError
	if drainErr != nil && stderrors.As(err, &rejected) {
		rejected.Truncated = true
	}

	// totalBytes accounts for the header already read by
	// ReadMessageWithEncodingN; add the full declared payload length so the
	// caller's bytesReceived counter stays consistent with the non-streaming
	// path regardless of how many bytes were actually consumed before erroring.
	return totalBytes + int(length), msg, nil, err
}

// readBlockMessage returns the block either decoded or as a body on disk,
// depending on whether a sink and a gate are installed.
//
// The whole-block decode below survives only for when no sink or gate is
// installed at all: the sync manager's park switched off, or its temp store
// one whose contents a restart could not enumerate (see newBlockPark's doc
// comment) — the one configuration this package cannot itself convert a
// block in. That is a real, if rare, running configuration, not a size
// threshold to weigh against.
func readBlockMessage(lr *io.LimitedReader, length uint64) (wire.Message, error) {
	if blockBodySink == nil || blockBodyGate == nil {
		msg := &wire.MsgBlock{}

		return msg, msg.Bsvdecode(lr, wire.ProtocolVersion, wire.BaseEncoding)
	}

	var header wire.BlockHeader
	if err := header.Deserialize(lr); err != nil {
		return nil, errors.NewProcessingError("streaming block: could not read the header", err)
	}

	hash := header.BlockHash()

	// blockBodyGate is what stops a peer filling this node's disk: nothing is
	// written until it returns nil. The sync manager's implementation of it
	// checks the header first (that it hashes to this hash, that its declared
	// target is not easier than the chain's own difficulty limit, and that it
	// meets that now-bounded target), and only then whether this node asked
	// for the hash, so a forged header is refused as the peer's fault whatever
	// hash it carries and only an honest one earns the quiet discard below.
	//
	// This runs before the park sees the block at all. A future change to the
	// park's admission path for a streamed body is expected to skip re-running
	// any of this, on the grounds that it already happened here; as written
	// today that admission path only knows about decoded blocks.
	if err := blockBodyGate(hash, &header, length); err != nil {
		// A block this node did not ask for is not the peer's fault, so it is
		// discarded rather than failed: the caller drains the body and the
		// connection carries on, as SV Node's does. Matched by type, never by
		// message text, so no other refusal can be mistaken for this one.
		var notRequested *BlockNotRequestedError
		if stderrors.As(err, &notRequested) {
			return &MsgBlockDiscarded{Hash: hash, Size: int64(length), Reason: "this node did not ask for it"}, nil
		}

		// A declared payload above this node's own block policy is this node's
		// configuration, not the peer's conduct: every peer serves the same
		// block, so dropping this one buys nothing. Discarded and drained like
		// the unrequested case, with the connection kept. Matched on the code,
		// which the gate raises for this one refusal.
		if errors.Is(err, errors.ErrBlockPolicyDeclined) {
			return &MsgBlockDiscarded{Hash: hash, Size: int64(length), Reason: "its declared size is above this node's own block policy"}, nil
		}

		// Everything else the gate refuses (a header that hashes to another
		// block, a target easier than the chain's floor, a hash that does not
		// meet its target) is the peer's doing and stays what it was: an error
		// the read loop answers with a "malformed" reject and a disconnect. No
		// ban: SV Node scores a high-hash header DoS(50), below its threshold.
		return nil, errors.NewProcessingError("streaming block %s: refused", hash, err)
	}

	// counted wraps the post-header stream only, so the first bytes it sees are
	// the transaction count. The count is taken from the passing bytes rather
	// than read here, because reading it here would mean putting it back. It
	// also carries the peer the bytes come from, for DeliveredBy.
	var counted countingReader

	counted.r = lr
	counted.from = DeliveredBy(lr.R)

	converted, err := blockBodySink(hash, &header, &counted, int64(length))
	if err != nil {
		// Every judgement the sink makes carries a teranode code outermost (the
		// producer rule on pipelineBlockSink, services/legacy/netsync). What
		// carries none is the connection's: a hang-up by FIN before the declared
		// length comes back as the bare io.ErrUnexpectedEOF, and a socket that
		// failed (a reset, a closed connection) as the bare *net.OpError the
		// socket returned. peer.shouldHandleReadError reads those by identity, the
		// sentinels with == and the OpError with a type assertion, and logs a
		// disconnect instead of pushing a reject for a "malformed" message. go-wire
		// and readMessageStreaming pass the handler's error through untouched, so
		// this wrap was the one place that identity was lost; it is kept for every
		// coded verdict and skipped for anything uncoded. The code is looked for
		// with errors.As, not errors.Is: teranode's (*Error).Is matches a
		// non-teranode target by message text, so errors.Is against a sentinel
		// would read any wrapped error mentioning EOF as the connection ending.
		var coded *errors.Error
		if !errors.As(err, &coded) {
			return nil, deleteOrphanedBody(hash, converted, err)
		}

		// The two codes that are the peer's (the producer rule again): a body
		// that is not the block its header commits to, or a delivery whose
		// length and transactions disagree. Typed, so the read loop sends a
		// reject naming the block and drops the whole association instead of
		// answering "malformed"; whether the host is then banned is the peer
		// server's decision and rests on BlockBodyRejectedError.ProvenBad.
		if errors.Is(err, errors.ErrBlockInvalid) || errors.IsBlockCorrupt(err) {
			return nil, deleteOrphanedBody(hash, converted, &BlockBodyRejectedError{Hash: hash, Err: err})
		}

		// Any other coded error is one this node's sink wrapper did not absorb.
		// While the sync manager is live that wrapper drains and keeps the peer
		// for every fault of this node's own; once it is shutting down it lets
		// any code through (absorbLocalSinkFault), and the server is already
		// closing every connection, so the "malformed" disconnect costs nothing.
		return nil, deleteOrphanedBody(hash, converted, errors.NewProcessingError("streaming block %s: could not store the body", hash, err))
	}

	// The sink returned success, but if the peer's declared payload still has
	// unread bytes, the body just written is shorter than declared: a
	// truncated body sitting under a well-formed hash. Caught here, before the
	// caller's generic drain runs, so the orphan can be deleted rather than
	// left behind uncounted.
	//
	// The pipeline sink must never reach this with converted true. Its
	// blockTxStream (services/legacy/netsync/block_tx_stream.go) reads through
	// a 256 KiB buffer that can pull bytes off lr ahead of what the
	// transactions consumed, so lr.N here is not a reliable measure on that
	// path, and a refusal here after converted true would hand
	// pipelineBlockDelete a record that may stand in for an earlier parked
	// copy of the same block, whose subtree files it then deletes. The sink
	// therefore holds the body to exactly the declared length itself
	// (blockTxStream.RequireEnd) and refuses before writing its record, so on
	// that path this check only ever fires with converted false.
	if lr.N > 0 {
		return nil, deleteOrphanedBody(hash, converted, errors.NewProcessingError(
			"streaming block %s: peer declared %d byte payload but the body ended early with %d bytes unread", hash, length, lr.N))
	}

	txCount, err := wire.ReadVarInt(bytes.NewReader(counted.first), wire.ProtocolVersion)
	if err != nil {
		return nil, deleteOrphanedBody(hash, converted, errors.NewProcessingError("streaming block %s: could not read the transaction count", hash, err))
	}

	return &MsgBlockOnDisk{BlockBody{
		Header:    header,
		TxCount:   txCount,
		Size:      int64(length),
		Hash:      hash,
		Converted: converted,
	}}, nil
}

// deleteOrphanedBody removes a body already written under hash before
// returning err. See blockBodyDelete's doc comment for why an orphaned body is
// worse than a failed download, and for what converted is and why it must
// come from THIS call's own blockBodySink return rather than be re-derived.
// The delete is best-effort: its own failure is swallowed rather than
// returned, because err is the reason the caller is failing in the first
// place and must not be masked by a secondary cleanup error.
func deleteOrphanedBody(hash chainhash.Hash, converted bool, err error) error {
	if blockBodyDelete != nil {
		_ = blockBodyDelete(hash, converted)
	}

	return err
}

// countingReader passes bytes straight through while keeping the first nine,
// which is enough to hold any transaction-count varint. That lets the count be
// read without buffering the body or reading the file back. from is the peer
// the body is arriving from, or nil when the reader the wire layer handed the
// handler carried none.
type countingReader struct {
	r     io.Reader
	first []byte
	from  *Peer
}

// deliveryReader is a peer's connection as it is handed to go-wire for one
// message, marked with the peer. go-wire passes the caller's reader to an
// external handler unchanged (go-wire v1.2.11 message.go:488-489 and
// :639-640), so the global block handler can learn which peer is sending
// without go-wire carrying any context of its own.
type deliveryReader struct {
	r    io.Reader
	from *Peer
}

func (d *deliveryReader) Read(p []byte) (int, error) {
	return d.r.Read(p)
}

// NewDeliveryReader returns r marked as bytes arriving from peer from. The
// peer's read loop wraps its connection in one for every message it reads; a
// test that drives a block sink directly uses it to say who is sending.
func NewDeliveryReader(r io.Reader, from *Peer) io.Reader {
	return &deliveryReader{r: r, from: from}
}

// DeliveredBy returns the peer a block body is arriving from: for the reader a
// block sink is handed, or for a reader NewDeliveryReader made. It returns nil
// for any other reader, so a caller must treat nil as "no peer known".
func DeliveredBy(r io.Reader) *Peer {
	switch v := r.(type) {
	case *countingReader:
		return v.from
	case *deliveryReader:
		return v.from
	default:
		return nil
	}
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)

	if len(c.first) < 9 && n > 0 {
		want := 9 - len(c.first)
		if want > n {
			want = n
		}

		c.first = append(c.first, p[:want]...)
	}

	return n, err
}

var registerStreamingBlockHandlerOnce sync.Once

// RegisterStreamingBlockHandler installs the streaming "block" handler with
// go-wire globally. Safe to call multiple times; the registration runs at
// most once. Call this once during legacy service startup, after any other
// wire-level configuration (e.g. wire.SetLimits).
func RegisterStreamingBlockHandler() {
	registerStreamingBlockHandlerOnce.Do(func() {
		wire.SetExternalHandler(wire.CmdBlock, streamingBlockHandler)
	})
}
