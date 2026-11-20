package transport

import (
	"io"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-wire"
)

// TxnStream is one inbound blocktxn payload that stays on the socket, the
// compact-block sibling of BlockStream. blockencodings.h:84-113
// BlockTransactions is the block hash, a compactsize transaction count, then
// the transactions the peer's getblocktxn asked for.
//
// A blocktxn reply carries whole transactions, so on a block whose short IDs
// mostly missed the recent-tx index it approaches the size of the block
// itself. It therefore takes the streaming path for the same reason a block
// does: materializing the payload would put a multi-GB buffer on the heap.
//
// The buffered wire.MsgBlockTxn decode is never used on the receive path.
//
// The wire-level checksum tradeoff is BlockStream's, unchanged: the streaming
// path skips it, and the assembled block is validated downstream — PoW, merkle
// root reconstruction, and per-tx parse plus validate. A blocktxn whose bytes
// were corrupted fails merkle root reconstruction, which is what makes the
// missing early-rejection signal affordable here as well.
//
// The consumer owns the stream until it calls Close. Close is idempotent and
// safe from any goroutine.
type TxnStream struct {
	payloadStream

	blockHash chainhash.Hash
	count     uint64
}

// newTxnStream decodes the fixed part of a blocktxn payload and returns the
// stream positioned at the first transaction. It returns a stream even on
// error so the caller can account for the bytes consumed.
func newTxnStream(r io.Reader, length uint64, pver uint32) (*TxnStream, error) {
	t := &TxnStream{payloadStream: newPayloadStream(r, length)}

	// blockencodings.h:99 READWRITE(blockhash): the 32 byte hash comes first.
	if _, err := io.ReadFull(t.lr, t.blockHash[:]); err != nil {
		return t, err
	}

	// blockencodings.h:101 READWRITE(COMPACTSIZE(txn_size)).
	count, err := wire.ReadVarInt(t.lr, pver)
	if err != nil {
		return t, err
	}

	if err := t.boundedCount(count, wire.CmdBlockTxn); err != nil {
		return t, err
	}

	t.count = count

	return t, nil
}

// BlockHash returns the block the peer says these transactions belong to. The
// consumer must check it against the compact block it requested: a reply for
// another block is not the one it asked for.
func (t *TxnStream) BlockHash() chainhash.Hash { return t.blockHash }

// Count returns the transaction count the peer declared.
func (t *TxnStream) Count() uint64 { return t.count }

// Reader returns the transaction bytes, wire-encoded back to back and bounded
// to the declared payload length. It reports io.EOF at the payload boundary,
// so a payload that carries fewer transactions than Count surfaces as a decode
// error to the consumer.
func (t *TxnStream) Reader() io.ReadCloser { return payloadReader{p: &t.payloadStream} }
