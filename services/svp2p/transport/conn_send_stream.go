package transport

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"hash"
	"io"
	"math"

	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/errors"
)

// MaxBlockFrameBytes is the largest block payload THIS SERVICE will frame with
// a basic message header — `magic(4) + command(12) + length(4) + checksum(4)`,
// whose length field is a uint32 (go-wire writeElements, message.go:404,
// v1.2.10).
//
// It is deliberately one BELOW what that field can hold, because 0xffffffff is
// RESERVED as the extended-message marker. go-wire's own writer draws the line
// in the same place — `if lenpUint32 >= math.MaxUint32` switches to the extmsg
// marker (go-wire WriteMessageWithEncodingN, message.go:391, v1.2.10) — and a
// payload of exactly 0xffffffff framed with a basic header is not merely
// unusual, it is misparsed:
//
//   - SVNode copes. CMessageHeader::Read (src/protocol.cpp:257-263) decides
//     extended by the COMMAND field being "extmsg", never by the length.
//
//   - go-wire does NOT. Its header reader takes the extended branch only when
//     the command IS "extmsg" (go-wire readMessageHeader, message.go:270,
//     v1.2.10), so for command "block" it leaves extLength at 0 — but the
//     payload read then applies `if length == 0xffffffff { length =
//     hdr.extLength }` UNCONDITIONALLY, on both the buffered path (go-wire
//     ReadMessageWithEncodingN, message.go:483-485, v1.2.10) and the streaming
//     one (message.go:634-636). The result is a zero-byte payload read, a
//     checksum computed over nothing and compared against ours, and a
//     desynchronised socket. Every peer on this stack — another svp2p node, or
//     services/legacy — is in that group.
//
//     Note the later `verifyChecksum := length != 0xffffffff && hdr.extLength
//     == 0` (message.go:647) does NOT rescue it: the substitution above has
//     already set length to 0, so the guard is true and the comparison runs.
//
// So the reservation is a real boundary and it belongs in the constant rather
// than in each comparison: a payload AT this value is servable, above it is not.
//
// A larger payload is framed with the extended header (extheader.go) when the
// peer negotiated 70016; otherwise it cannot be served.
const MaxBlockFrameBytes = uint64(math.MaxUint32) - 1

// ErrBlockTooLargeToFrame reports a block that no basic message header can
// declare. The caller answers the requesting peer with notfound.
var ErrBlockTooLargeToFrame = errors.New(errors.ERR_INVALID_ARGUMENT, "svp2p: block payload exceeds the basic message header limit")

// ErrBlockLengthMismatch reports that the payload the first pass measured is
// not the length the caller declared. Nothing is written when this is
// returned: a frame built from either number would carry a checksum or a
// length the other half contradicts.
var ErrBlockLengthMismatch = errors.New(errors.ERR_INVALID_ARGUMENT, "svp2p: block payload length disagrees with the declared length")

// BlockSendRequest is one outbound "block" message whose payload is streamed
// straight to the socket and never materialized.
type BlockSendRequest struct {
	// Length is the payload's declared byte count: the 80 byte block header,
	// the transaction count varint, and the transactions. It is written into
	// the message header's length field, and the payload is verified against
	// it on both passes.
	Length uint64

	// Open returns the payload bytes from the start. It is called TWICE — see
	// SendBlock for why — and every returned reader is closed by SendBlock.
	// A block that has gone away between the two calls must return an error
	// with its own type, which SendBlock passes through untouched so the
	// caller can tell "no longer held" from "the lookup failed".
	Open func(ctx context.Context) (io.ReadCloser, error)
}

// blockSend is the streamed lane's queue entry. It carries the caller's
// context because the second pass is opened on the writer goroutine.
type blockSend struct {
	ctx      context.Context
	req      BlockSendRequest
	checksum [4]byte
	extended bool
	done     chan error
}

// SendBlock frames one block message and streams its payload to the socket.
// It blocks until the send finishes or fails, so responses to one getdata stay
// in request order on the calling goroutine.
//
// # Why two passes over the payload
//
// SVNode verifies the payload checksum of every NON-extended message it
// receives (net_processing.cpp:4995-4998) and scores a mismatch against
// dInvalidChecksumFrequency (:5005-5015) — a ban-scored offence, not a log
// line. The checksum is zeroed only on the extmsg path (protocol.cpp:220-237).
// So every block below MaxBlockFrameBytes must go out with a correct
// DoubleHashB(payload)[0:4], and the header precedes the payload on the wire.
//
// The legacy service could only manage that by materializing the whole block
// first (services/legacy/raw_block_message.go:27, io.ReadAll, then
// wire.WriteMessage hashes the buffer), which is the step this service exists
// to drop: a multi-GB block must never be held in memory. Hashing one
// streaming pass and then streaming a second pass to the socket keeps memory
// flat and the checksum exact, at the cost of a second read of the block.
//
// The first pass runs on the CALLER's goroutine, not the writer's: it touches
// no socket, and hashing gigabytes on the single writer would stall every
// other message on this connection for no reason. Only the second pass is
// enqueued for writeLoop, because writeLoop is the sole writer to the socket
// and a header-plus-body written from any other goroutine would interleave
// with whatever it is framing and misalign the connection for good.
//
// # The between-pass guard
//
// The first pass must measure exactly Length bytes, or the send aborts with
// ErrBlockLengthMismatch before anything is written. If the second Open fails
// — the block was reorged out, or its body is no longer served — its error is
// returned unchanged and, again, nothing has been written. Both are safe: the
// caller may answer notfound and the connection is untouched.
//
// The one unrecoverable case is a second pass that ends EARLY, after the
// header is already on the wire. The peer is then waiting for bytes that will
// never arrive and every later frame is misaligned, so the connection is
// failed. A second pass that runs LONG needs no check: exactly Length bytes
// are copied, and those are the same Length bytes the first pass hashed unless
// the store handed back different content for the same block hash.
//
// # Send budget accounting
//
// A streamed block is charged NOTHING against Config.SendBudgetBytes, and
// does not use the priority lane. The budget is the net.cpp nSendSize versus
// GetSendBufferSize check: it bounds the bytes the send QUEUE holds in memory,
// and this payload is never in the queue — it goes from the reader to the
// socket. Charging Send's MaxPayloadLength estimate would refuse every block
// larger than the whole budget; the priority lane would put a multi-GB copy
// behind Config.WriteTimeout, which is a channel-handoff timeout, not a
// transfer budget. The real bound is the peer's own TCP receive window, which
// the copy blocks on — the same backpressure SVNode gets.
//
// The one-block-send-per-connection invariant is enforced by SendBlock being
// SYNCHRONOUS — it returns only once the writer has finished the frame — plus
// the fact that its only caller is a peer's single serve goroutine
// (protocol/getdata.go servePass, which serves at most one block per pass). A
// change that makes SendBlock asynchronous, or calls it from a second
// goroutine, breaks the invariant: nothing else re-establishes it, and two
// concurrent callers would each pay a full hashing pass and then queue two
// multi-gigabyte frames.
func (c *Conn) SendBlock(ctx context.Context, req BlockSendRequest) error {
	if req.Open == nil {
		return errors.New(errors.ERR_INVALID_ARGUMENT, "svp2p: block send has no payload opener")
	}

	// req.Length feeds int64(...) conversions on the write path below; the
	// caller is expected to stay within wire.MaxBlockPayload(), which is far
	// below this, but the guard is explicit rather than a bare conversion.
	if req.Length > math.MaxInt64 {
		return errors.New(errors.ERR_INVALID_ARGUMENT, "svp2p: block payload of %d bytes exceeds the maximum representable length", req.Length)
	}

	// Decided before anything is opened, so an unservable block costs no read.
	extended := req.Length > MaxBlockFrameBytes

	if extended && c.pver.Load() < ExtendedPayloadVersion {
		return errors.New(errors.ERR_INVALID_ARGUMENT,
			"svp2p: block payload of %d bytes needs the extended header and the peer negotiated version %d, below %d", req.Length, c.pver.Load(), ExtendedPayloadVersion, ErrBlockTooLargeToFrame)
	}

	var (
		checksum [4]byte
		err      error
	)

	if !extended {
		// Only basic frames carry a checksum (protocol.cpp:226-235), so only
		// they pay for the hashing pass.
		checksum, err = c.hashBlockPayload(ctx, req)
		if err != nil {
			return err
		}
	}

	bs := &blockSend{
		ctx:      ctx,
		req:      req,
		checksum: checksum,
		extended: extended,
		done:     make(chan error, 1),
	}

	// Queued on the same lane as every other send, so replies stay in the
	// order the caller made them, and charged nothing against the byte budget.
	select {
	case c.sendCh <- queuedMsg{block: bs}:
	case <-c.quit:
		return c.Err()
	case <-ctx.Done():
		return ctx.Err()
	}

	// Waited on without a deadline: the writer owns the socket for the whole
	// frame and cannot be abandoned half way. A connection failure closes quit
	// and the writer reports through done either way.
	select {
	case err := <-bs.done:
		return err
	case <-c.quit:
		return c.Err()
	}
}

// hashBlockPayload is the first pass: it streams the payload through
// double-SHA256 without buffering it, and verifies the byte count against the
// declared length. The four header bytes are the first four of that digest,
// which is exactly what go-wire puts in the header: `copy(hdr.checksum[:],
// chainhash.DoubleHashB(payload)[0:4])` (go-wire WriteMessageWithEncodingN,
// message.go:398, v1.2.10).
func (c *Conn) hashBlockPayload(ctx context.Context, req BlockSendRequest) ([4]byte, error) {
	var checksum [4]byte

	body, err := req.Open(ctx)
	if err != nil {
		return checksum, err
	}

	defer func() { _ = body.Close() }()

	inner := sha256.New()

	n, err := io.Copy(inner, body)
	if err != nil {
		return checksum, err
	}

	// Both counts and the caller's own identifier for the block belong in this
	// message: a reorg between the two passes is the expected BENIGN cause, and
	// an operator has to be able to tell it from a corrupt stored size.
	if uint64(n) != req.Length { //nolint:gosec // io.Copy never returns a negative count
		return checksum, errors.New(errors.ERR_INVALID_ARGUMENT,
			"svp2p: block payload measured %d bytes on the hashing pass but %d were declared", n, req.Length, ErrBlockLengthMismatch)
	}

	return frameChecksum(inner), nil
}

// frameChecksum finishes a streamed double-SHA256 into the four header bytes.
// Both passes go through it, so the pass-2 verification compares like with
// like: a change to the digest cannot make one pass disagree with the other.
func frameChecksum(inner hash.Hash) [4]byte {
	var checksum [4]byte

	outer := sha256.Sum256(inner.Sum(nil))
	copy(checksum[:], outer[0:4])

	return checksum
}

// writeBlock is the second pass, and runs ONLY on the writer goroutine. It
// reports the outcome to the waiting caller and returns whether the connection
// is still usable.
func (c *Conn) writeBlock(bs *blockSend) (ok bool) {
	body, err := bs.req.Open(bs.ctx)
	if err != nil {
		// Nothing has been written, so the connection is intact and the
		// caller's own error type survives for it to classify.
		bs.done <- err

		return true
	}

	defer func() { _ = body.Close() }()

	var hdr []byte
	if bs.extended {
		hdr = extBlockFrameHeader(c.cfg.Net, bs.req.Length)
	} else {
		hdr = blockFrameHeader(c.cfg.Net, bs.req.Length, bs.checksum)
	}

	n, err := c.nc.Write(hdr)

	c.sent.Add(uint64(n)) //nolint:gosec // byte count is never negative

	if err != nil {
		bs.done <- err

		// A partial header is as misaligned as a partial body.
		return n == 0
	}

	// The extended path carries a zero checksum on the wire
	// (protocol.cpp:226), so there is nothing to verify: copy straight
	// through and skip the second hashing pass entirely.
	if bs.extended {
		copied, err := io.CopyN(c.nc, body, int64(bs.req.Length))

		c.sent.Add(uint64(copied)) //nolint:gosec // byte count is never negative

		if err == nil {
			bs.done <- nil

			return true
		}

		bs.done <- errors.New(errors.ERR_ERROR,
			"svp2p: block send wrote %d of %d declared payload bytes, connection is no longer byte aligned", copied, bs.req.Length, err)

		return false
	}

	// Hashed AS IT IS COPIED, not merely counted. Copying Length bytes proves
	// pass 2 had the same NUMBER of bytes as pass 1, which is a weak proxy for
	// having the same bytes: a store that returns a same-length wrong body —
	// corruption, a bad cache key, a wrong-body bug — would put pass 1's
	// checksum on the wire over pass 2's content, and every SVNode peer
	// ban-scores that (net_processing.cpp:5005-5015). One SHA-256 over bytes
	// already being copied is cheap against the network write, and against the
	// pass-1 hash this design already pays.
	//
	// Pass 1 cannot be dropped in favour of this one: the header precedes the
	// payload on the wire, so the checksum must be KNOWN before the body is
	// written. Pass 2's hash is verification, never the source.
	verify := sha256.New()

	copied, err := io.CopyN(io.MultiWriter(c.nc, verify), body, int64(bs.req.Length)) //nolint:gosec // bounded by MaxBlockFrameBytes above

	c.sent.Add(uint64(copied)) //nolint:gosec // byte count is never negative

	if err == nil {
		if got := frameChecksum(verify); got != bs.checksum {
			// The frame on the wire now carries a checksum that cannot match
			// the bytes behind it, and it is already sent — there is no
			// recovering this connection, only refusing to keep using it.
			// Dropping the connection costs us one peer; leaving it open costs
			// us a ban score from every peer this happens with.
			bs.done <- errors.New(errors.ERR_ERROR,
				"svp2p: block payload changed between the hashing pass and the write pass, so the frame just sent carries checksum %x over content hashing to %x — the connection is being dropped rather than continued",
				bs.checksum, got)

			return false
		}

		bs.done <- nil

		return true
	}

	// The header declared Length bytes and fewer went out, so the peer is
	// waiting mid-message and every later frame on this socket is garbage.
	// io.CopyN reports io.EOF for a body that ended early.
	bs.done <- errors.New(errors.ERR_ERROR,
		"svp2p: block send wrote %d of %d declared payload bytes, connection is no longer byte aligned", copied, bs.req.Length, err)

	return false
}

// blockFrameHeader writes the 24 byte basic message header by hand, the way
// the inbound path hand-rolls readWireHeader: go-wire's
// WriteMessageWithEncodingN (go-wire message.go:329, v1.2.10) cannot be used
// because it materializes the whole payload before it can hash it: `var bw
// bytes.Buffer` then `msg.BsvEncode(&bw, ...)` (message.go:351-353), which is
// precisely what this lane exists to avoid. The field layout written below is
// exactly what its writeElements emits (message.go:404).
func blockFrameHeader(magic wire.BitcoinNet, length uint64, checksum [4]byte) []byte {
	hdr := make([]byte, wire.MessageHeaderSize)

	binary.LittleEndian.PutUint32(hdr[0:4], uint32(magic))
	copy(hdr[4:4+wire.CommandSize], wire.CmdBlock)
	binary.LittleEndian.PutUint32(hdr[16:20], uint32(length)) //nolint:gosec // bounded by MaxBlockFrameBytes
	copy(hdr[20:24], checksum[:])

	return hdr
}
