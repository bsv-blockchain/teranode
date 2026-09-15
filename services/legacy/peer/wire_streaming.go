package peer

import (
	"bytes"
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
var blockBodyGate func(hash chainhash.Hash, header *wire.BlockHeader) error

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
// the streaming gate accepts any hash asked for within the last hour, and
// ownership is released as soon as a delivery's sink call finishes — so an
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
// Note on the wire-level DoubleHash checksum: the default path verifies the
// peer-supplied checksum over the payload bytes. This handler skips it, so
// the early-rejection signal that checksum provides is lost. Integrity is
// preserved by the existing downstream validation in
// netsync.HandleBlockDirect — PoW via HasMetTargetDifficulty, merkle root
// reconstruction during subtree preparation, and per-tx parse + validate.
// Any payload corruption that a wire-level checksum would have caught also
// fails one of those downstream checks; what we give up is rejecting a bad
// block before paying the decode cost. Preserving the checksum under
// streaming would require a TeeReader → SHA-256 pass over multi-GB
// payloads, which is not justified given the downstream guarantees.
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
	// written until it returns nil. The sync manager's implementation of it is
	// expected to check, in order, that this node actually asked for this
	// hash, that the header's declared target is not easier than the chain's
	// own difficulty limit, and only then that the header meets that (now
	// bounded) target; the first check answers "was this asked for", and of
	// the other two the limit check is what makes the target check mean
	// anything, since without it a peer can simply declare a target it always
	// meets.
	//
	// This runs before the park sees the block at all. A future change to the
	// park's admission path for a streamed body is expected to skip re-running
	// any of this, on the grounds that it already happened here; as written
	// today that admission path only knows about decoded blocks.
	if err := blockBodyGate(hash, &header); err != nil {
		return nil, errors.NewProcessingError("streaming block %s: refused", hash, err)
	}

	// counted wraps the post-header stream only, so the first bytes it sees are
	// the transaction count. The count is taken from the passing bytes rather
	// than read here, because reading it here would mean putting it back.
	var counted countingReader

	counted.r = lr

	converted, err := blockBodySink(hash, &header, &counted, int64(length))
	if err != nil {
		return nil, deleteOrphanedBody(hash, converted, errors.NewProcessingError("streaming block %s: could not store the body", hash, err))
	}

	// The sink returned success, but if the peer's declared payload still has
	// unread bytes, the body just written is shorter than declared: a
	// truncated body sitting under a well-formed hash. Caught here, before the
	// caller's generic drain runs, so the orphan can be deleted rather than
	// left behind uncounted.
	//
	// NOTE for the pipeline sink specifically: this check is unreliable on
	// that path and nobody has fixed it here. blockTxStream
	// (services/legacy/netsync/block_tx_stream.go) wraps lr in its own
	// 256 KiB buffered reader, and a bufio.Reader reads ahead of whatever the
	// caller actually consumed — so for a block small enough to fit inside
	// that buffer, the read-ahead can already have pulled every remaining
	// byte off lr before this check ever runs, leaving lr.N at 0 whether or
	// not the sink's own logic was correct. A large block, where the buffer
	// cannot get ahead of the whole body, does not have this problem. That
	// makes this check fire (or not) by block size rather than by
	// correctness on the pipeline path. Correctness there is still held by
	// the merkle root comparison inside pipelineBlockSink itself, which does
	// not depend on this. Not fixed here: this is a note for whoever touches
	// this next, not a defect this change set is fixing.
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
// read without buffering the body or reading the file back.
type countingReader struct {
	r     io.Reader
	first []byte
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
