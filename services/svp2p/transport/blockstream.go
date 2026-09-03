package transport

import (
	"bytes"
	"encoding/binary"
	"io"

	"github.com/bsv-blockchain/go-wire"
)

// minTxPayloadBytes is the smallest byte count a serialized transaction can
// occupy: version 4 + input count varint 1 + output count varint 1 +
// nLockTime 4. It mirrors minTxPayload in go-wire msg_tx.go:72, which
// maxTxPerBlock in msg_block.go:40-42 uses to bound the transaction count on
// the buffered decode path.
const minTxPayloadBytes = 10

// wireHeader is the 24 byte message header, kept raw so a non-block message
// can be replayed into go-wire's own framing path unchanged. An extended
// header (protocol.cpp:220-237) additionally carries ext, the 20 extension
// bytes read past the basic header, also kept raw for replay.
type wireHeader struct {
	raw      [wire.MessageHeaderSize]byte
	magic    wire.BitcoinNet
	command  string
	length   uint64
	checksum [4]byte
	extended bool
	ext      [wire.CommandSize + 8]byte
}

// readWireHeader reads one message header. It deliberately stops before the
// payload so the read loop can pick the streaming path for "block" before any
// payload byte is materialized.
//
// protocol.cpp:262-266 CMessageHeader::Read keys the extended header on the
// COMMAND alone; the 0xffffffff marker is what the writer emits alongside it
// (protocol.cpp:222-228). Requiring BOTH here is a deliberate narrowing: a
// frame that names "extmsg" without the marker is not something SVNode ever
// writes, and reading the extension off such a header would let a peer choose
// how many bytes this node consumes. The real command and a 64-bit length
// follow in the extension.
func readWireHeader(r io.Reader) (int, wireHeader, error) {
	var h wireHeader

	n, err := io.ReadFull(r, h.raw[:])
	if err != nil {
		return n, h, err
	}

	h.magic = wire.BitcoinNet(binary.LittleEndian.Uint32(h.raw[0:4]))
	h.command = string(bytes.TrimRight(h.raw[4:4+wire.CommandSize], "\x00"))
	basicLen := binary.LittleEndian.Uint32(h.raw[16:20])
	copy(h.checksum[:], h.raw[20:24])
	h.length = uint64(basicLen)

	if h.command == wire.CmdExtMsg && basicLen == extLengthMarker {
		m, err := io.ReadFull(r, h.ext[:])
		n += m

		if err != nil {
			if err == io.EOF { //nolint:errorlint // io.ReadFull never wraps io.EOF
				err = io.ErrUnexpectedEOF
			}

			return n, h, err
		}

		h.extended = true
		h.command = string(bytes.TrimRight(h.ext[:wire.CommandSize], "\x00"))
		h.length = binary.LittleEndian.Uint64(h.ext[wire.CommandSize:])
	}

	return n, h, nil
}

// BlockStream is one inbound block payload that stays on the socket. The read
// loop decodes the 80 byte block header and the transaction count, then hands
// the connection to the consumer bounded to the declared payload length. The
// transactions are never buffered: on fat blocks (multi-GB testnet stress
// blocks) the payload buffer alone reached ~2.86 GB of legacy heap inuse
// during sync, the second-largest contributor to RSS after the per-tx scratch
// buffer.
//
// Note on the wire-level DoubleHash checksum: the buffered path in
// wire.ReadMessageWithEncodingN verifies the peer-supplied checksum over the
// payload bytes. This path skips it, so the early-rejection signal that
// checksum provides is lost. Integrity is preserved by downstream block
// validation — PoW, merkle root reconstruction, and per-tx parse plus
// validate. Any payload corruption that a wire-level checksum would have
// caught also fails one of those downstream checks; what we give up is
// rejecting a bad block before paying the decode cost. Preserving the checksum
// under streaming would require a TeeReader → SHA-256 pass over multi-GB
// payloads, which is not justified given the downstream guarantees. Same
// tradeoff as services/legacy/peer/wire_streaming.go streamingBlockHandler,
// scoped per connection instead of the process-global wire handler.
//
// The consumer owns the stream until it calls Close. Close is idempotent and
// safe from any goroutine.
type BlockStream struct {
	payloadStream

	header  wire.BlockHeader
	txCount uint64
}

// newBlockStream decodes the fixed part of a block payload and returns the
// stream positioned at the first transaction. It returns a stream even on
// error so the caller can account for the bytes consumed.
func newBlockStream(r io.Reader, length uint64, pver uint32) (*BlockStream, error) {
	b := &BlockStream{payloadStream: newPayloadStream(r, length)}

	if err := b.header.Bsvdecode(b.lr, pver, wire.BaseEncoding); err != nil {
		return b, err
	}

	count, err := wire.ReadVarInt(b.lr, pver)
	if err != nil {
		return b, err
	}

	if err := b.boundedCount(count, wire.CmdBlock); err != nil {
		return b, err
	}

	b.txCount = count

	return b, nil
}

// Header returns the decoded 80 byte block header.
func (b *BlockStream) Header() wire.BlockHeader { return b.header }

// TxCount returns the transaction count the peer declared.
func (b *BlockStream) TxCount() uint64 { return b.txCount }

// TxReader returns the transaction bytes, bounded to the declared payload
// length. It reports io.EOF at the payload boundary, so a payload that carries
// fewer transactions than TxCount surfaces as a decode error to the consumer.
func (b *BlockStream) TxReader() io.ReadCloser { return payloadReader{p: &b.payloadStream} }
