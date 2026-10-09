package peer

import (
	"fmt"
	"io"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/errors"
)

// BlockBody describes a block whose transactions were streamed straight to the
// park's blob store rather than decoded into memory.
//
// It exists because nothing between the wire and the park reads a transaction.
// The park's stateless check needs the header, to see the block hashes to what
// we asked for and meets its own target difficulty, and needs the transaction
// count to be non-zero. The park's byte budget needs the size, which the wire
// message declares. The arrival path needs the previous block hash, which is in
// the header. On mainnet 91% of blocks arrive out of order and take exactly that
// path, so for those the four-gigabyte object was built only to be serialised
// straight back out.
type BlockBody struct {
	// Header is the block's own 80-byte header, read off the wire.
	Header wire.BlockHeader

	// TxCount is the transaction count varint that followed it.
	TxCount uint64

	// Size is the serialized block size, taken from the wire message's declared
	// payload length rather than by walking anything.
	Size int64

	// Hash is Header.BlockHash(), computed once when the header was read.
	Hash chainhash.Hash

	// Converted reports whether the sink that accepted this body actually
	// converted it — wrote a park record derived from its subtrees, as the
	// pipeline sink does — rather than merely writing the whole body
	// byte-for-byte. It comes straight from the sink's own return value, set
	// at the one place that genuinely knows: the sink call itself. Nothing
	// downstream should ever try to answer this question by inference (for
	// example, by checking whether some blob happens to exist for this hash),
	// because a blob that exists for another reason — a stale leftover, a
	// racing duplicate delivery of the same hash — looks identical to one this
	// delivery actually produced.
	Converted bool
}

// MsgBlockOnDisk is a block message whose body was streamed to the park rather
// than decoded. It satisfies wire.Message so go-wire's external handler can
// return it in place of a *wire.MsgBlock.
//
// Its encode and decode methods are never called: this message is produced by
// streamingBlockHandler and consumed inside this process, never re-read from or
// written to a peer. They return an error rather than doing nothing quietly, so
// a caller that starts using them finds out immediately.
type MsgBlockOnDisk struct {
	BlockBody
}

// Bsvdecode is not supported. See the type comment.
func (m *MsgBlockOnDisk) Bsvdecode(io.Reader, uint32, wire.MessageEncoding) error {
	return errors.NewProcessingError("MsgBlockOnDisk cannot be decoded: its body is on disk, not on the wire")
}

// BsvEncode is not supported. See the type comment.
func (m *MsgBlockOnDisk) BsvEncode(io.Writer, uint32, wire.MessageEncoding) error {
	return errors.NewProcessingError("MsgBlockOnDisk cannot be encoded: its body is on disk, not in memory")
}

// Command reports the same wire command a decoded block does, so anything
// switching on the command string treats the two alike.
func (m *MsgBlockOnDisk) Command() string { return wire.CmdBlock }

// MaxPayloadLength defers to the decoded block's answer.
func (m *MsgBlockOnDisk) MaxPayloadLength(pver uint32) uint64 {
	return (&wire.MsgBlock{}).MaxPayloadLength(pver)
}

// BlockNotRequestedError is what a block body gate returns for a block this node
// did not ask for. It is not the peer's fault: SV Node never disconnects or scores
// a peer for an unrequested block, it just does not keep it. The streaming handler
// reads this type, by type and not by message, and discards the body instead of
// failing the message, so the connection carries on. Any other gate refusal is
// still an error and still disconnects the peer.
type BlockNotRequestedError struct {
	Hash chainhash.Hash
}

func (e *BlockNotRequestedError) Error() string {
	return fmt.Sprintf("[streamingBlockGate][%s] this node did not ask for this block", e.Hash)
}

// MsgBlockDiscarded is a block message whose body was read off the wire and thrown
// away for a reason that is this node's and not the peer's: the block was not asked
// for, or its declared size is above this node's own block policy. It carries only
// what a log line needs.
type MsgBlockDiscarded struct {
	Hash chainhash.Hash
	Size int64

	// Reason is the one clause the discard log line prints. Two producers set it
	// (readBlockMessage, from the gate's refusal type): a block nobody asked for,
	// and a declared payload the local block policy declines. The log used to say
	// "did not ask for" for both.
	Reason string
}

// BlockBodyRejectedError is what the streaming block handler returns when the
// installed sink refused a body as the peer's fault: the body is not the block
// its header commits to (ERR_BLOCK_INVALID), or its length and transactions
// disagree (ERR_BLOCK_CORRUPT). It exists so the read loop can match the refusal
// by type, never by message text, and treat it differently from a message it
// could not read at all: the peer is sent a reject naming the block and the
// whole association is disconnected, which is what rotates the sync peer and
// releases the blocks it owed (netsync handleDonePeerMsg, clearRequestedState).
//
// Every other sink error keeps today's path. This node's own faults are drained
// and kept inside the sink wrapper (netsync absorbLocalSinkFault) and never
// reach here; an uncoded connection error is passed through by identity; any
// other coded error is still a "malformed" disconnect.
type BlockBodyRejectedError struct {
	Hash chainhash.Hash
	Err  error

	// Truncated reports that the declared payload never fully arrived: after the
	// sink refused, the handler's drain of what was left hit the end of the
	// stream. A body that was not delivered in full was never judged in full, so
	// it earns no ban whatever code the sink chose. Set by streamingBlockHandler
	// after the drain, which is the one place that knows; the sink cannot, and
	// readBlockMessage returns before the drain runs.
	Truncated bool

	// ChecksumVerified reports that the payload's double-SHA256 was compared
	// with the checksum in the message header and matched. Nothing sets it
	// today: go-wire hands the body to the streaming handler before it checks
	// the checksum, and does not pass the checksum to the handler (see
	// streamingBlockHandler). It is false, so ProvenBad is false, until that
	// changes.
	ChecksumVerified bool
}

func (e *BlockBodyRejectedError) Error() string {
	return fmt.Sprintf("block %s: the body was rejected: %v", e.Hash, e.Err)
}

func (e *BlockBodyRejectedError) Unwrap() error { return e.Err }

// ProvenBad is the one predicate a ban is allowed to rest on, and the legacy
// peer server (serverPeer.OnBlockBodyRejected) bans on nothing else: the body
// arrived in full, the sink raised ERR_BLOCK_BODY_MISMATCH, which only its
// three SV Node DoS(100) parity sites do (a merkle root the header does not
// carry, a duplicate transaction, no coinbase), and the wire checksum over the
// payload was verified. SV Node reaches CheckBlock only after the whole message
// is deserialised, so a short delivery never scores there either; Truncated
// gives teranode the same property on a path that judges the body as it
// streams. SV Node also checks the message checksum first and drops a message
// that fails it (net_processing.cpp ProcessMessages, "CHECKSUM ERROR"), scoring
// one point only for a burst of more than 100 within 500 ms, so a body damaged
// in transit never reaches CheckBlock.
// ChecksumVerified is that half, and it is never true today: see
// streamingBlockHandler.
func (e *BlockBodyRejectedError) ProvenBad() bool {
	return e != nil && e.ChecksumVerified && e.MismatchInFull()
}

// MismatchInFull reports that the body arrived in full and the sink raised
// ERR_BLOCK_BODY_MISMATCH at one of its three sites: the delivery half of
// ProvenBad, without the checksum half.
func (e *BlockBodyRejectedError) MismatchInFull() bool {
	return e != nil && !e.Truncated && errors.IsBlockBodyMismatch(e.Err)
}

// Bsvdecode always fails: the body was discarded, not kept.
func (m *MsgBlockDiscarded) Bsvdecode(io.Reader, uint32, wire.MessageEncoding) error {
	return errors.NewProcessingError("MsgBlockDiscarded cannot be decoded: its body was discarded")
}

// BsvEncode always fails: there is no body to send.
func (m *MsgBlockDiscarded) BsvEncode(io.Writer, uint32, wire.MessageEncoding) error {
	return errors.NewProcessingError("MsgBlockDiscarded cannot be encoded: its body was discarded")
}

// Command returns the protocol command the message arrived as.
func (m *MsgBlockDiscarded) Command() string { return wire.CmdBlock }

// MaxPayloadLength is the same bound a block message has.
func (m *MsgBlockDiscarded) MaxPayloadLength(pver uint32) uint64 {
	return (&wire.MsgBlock{}).MaxPayloadLength(pver)
}

// SetBlockBodyStreaming installs all three callbacks the streaming path needs,
// or clears all three when any of them is nil.
//
// One call rather than several because the three callbacks are only safe
// together. A sink with no gate is a store anybody can fill; a sink with no
// delete leaves an orphaned body behind on every write that fails after it.
// The handler already refuses to stream unless a sink and a gate are both
// present, and this makes the same rule true of how they are installed rather
// than only of how they are read.
//
// The gate is handed the declared payload length as well as the header, so it
// can refuse a body on its size before the first byte of it is read.
func SetBlockBodyStreaming(
	sink func(hash chainhash.Hash, header *wire.BlockHeader, r io.Reader, n int64) (bool, error),
	gate func(hash chainhash.Hash, header *wire.BlockHeader, length uint64) error,
	del func(hash chainhash.Hash, converted bool) error,
) {
	if sink == nil || gate == nil || del == nil {
		blockBodySink, blockBodyGate, blockBodyDelete = nil, nil, nil

		return
	}

	blockBodySink, blockBodyGate, blockBodyDelete = sink, gate, del
}
