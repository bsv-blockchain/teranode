package peer

import (
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

// SetBlockBodyStreaming installs all three callbacks the streaming path needs,
// or clears all three when any of them is nil.
//
// One call rather than three because the three are only safe together. A sink
// with no gate is a store anybody can fill; a sink with no delete leaves an
// orphaned body behind on every write that fails after it. The handler already
// refuses to stream unless a sink and a gate are both present, and this makes
// the same rule true of how they are installed rather than only of how they are
// read.
func SetBlockBodyStreaming(
	sink func(hash chainhash.Hash, r io.Reader, n int64) error,
	gate func(hash chainhash.Hash, header *wire.BlockHeader) error,
	del func(hash chainhash.Hash) error,
) {
	if sink == nil || gate == nil || del == nil {
		blockBodySink, blockBodyGate, blockBodyDelete = nil, nil, nil

		return
	}

	blockBodySink, blockBodyGate, blockBodyDelete = sink, gate, del
}

// SetBlockBodySink installs the sink alone. Prefer SetBlockBodyStreaming, which
// is the only way to install a sink that is actually reachable: the handler
// checks for a gate too, so a sink installed on its own changes nothing. This
// exists for tests that exercise the sink in isolation.
func SetBlockBodySink(f func(hash chainhash.Hash, r io.Reader, n int64) error) {
	blockBodySink = f
}

// SetBlockBodyGate installs the gate that decides whether a block's body may
// be streamed to disk. The sync manager calls this where it installs
// blockBodySink (via SetBlockBodySink), since a store reachable with no gate
// in front of it is a store anybody can fill; the two must be installed, and
// removed, together. See blockBodyGate's doc comment for what the gate is
// required to check.
func SetBlockBodyGate(f func(hash chainhash.Hash, header *wire.BlockHeader) error) {
	blockBodyGate = f
}

// SetBlockBodyDelete installs the callback that removes a body already
// written under a hash, for the handler to call when it fails after the sink
// has already returned success. See blockBodyDelete's doc comment for why an
// orphaned body is worse than a failed download.
func SetBlockBodyDelete(f func(hash chainhash.Hash) error) {
	blockBodyDelete = f
}
