package netsync

import (
	"bufio"
	"io"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/pkg/txstream"
)

// errBlockTxStreamDone reports that every declared transaction has been read. It
// is a normal end, not a failure, and is distinct from a truncated body.
//
// HAZARD: teranode's (*errors.Error).Is matches purely on the numeric error
// CODE (errors/errors.go), not on identity or message. This sentinel is a
// NewProcessingError, so any OTHER error in this file built with
// NewProcessingError becomes indistinguishable from clean exhaustion to a
// caller doing errors.Is(err, errBlockTxStreamDone) — a real failure would
// read as "nothing left to read," and a corrupt block would be treated as a
// complete one. Nothing else in this file may use NewProcessingError. Give
// every other failure here its own, different code (NewBlockInvalidError,
// as used below, is fine — it's code 11, this sentinel is code 4).
var errBlockTxStreamDone = errors.NewProcessingError("block transaction stream exhausted")

// maxBlockTxCount is the absolute ceiling on the peer-declared transaction count.
//
// It sits inside uint32 deliberately: the duplicate map the sink allocates from
// this count is txmap.NewSplitSwissMapUint64, which takes a uint32, and a count of
// 1<<32 would wrap to zero and silently produce a map that dedups nothing.
// 1<<31 is above any block this network will carry and still half of what uint32
// holds.
const maxBlockTxCount = 1 << 31

// minSerializedTxSize is the smallest a serialized transaction can possibly be:
// 4 version, 1 input count, 1 output count, 4 locktime. No real transaction is
// this small, which is the point — it is a floor for arithmetic, not an estimate.
const minSerializedTxSize = 10

// blockTxStreamReadBufferSize is the buffer between the socket and the decoder.
// Large enough that a transaction of ordinary size is assembled without a syscall
// per field, small enough that one per in-flight block is not a memory concern.
const blockTxStreamReadBufferSize = 256 * 1024

// blockTxStream yields a block's transactions one at a time from a reader
// positioned at the transaction count varint.
//
// It exists so that a block is never a whole object in memory. The count is read
// eagerly because the subtree partition is decided before the first transaction
// arrives; the transactions are read lazily and nothing here retains them.
type blockTxStream struct {
	r       *bufio.Reader
	txCount uint64
	read    uint64
	// src counts every byte pulled from the caller's reader, read-ahead included, so the bytes
	// actually consumed are src.n less what r still holds buffered.
	src *countingSource
	// bodyLen is the declared length of what r reads: the body from the transaction count on.
	bodyLen int64
	// txs parses each transaction and computes its id from the bytes as they are read, rather
	// than serializing it again.
	txs *txstream.Reader
}

// newBlockTxStream reads the declared transaction count and positions the stream
// at the first transaction. payloadLen is the declared length of what r reads,
// the body from the transaction count on (the block's wire payload less its
// 80-byte header). It is what makes the count checkable rather than merely
// bounded, and what RequireEnd holds the body to.
func newBlockTxStream(r io.Reader, payloadLen int64) (*blockTxStream, error) {
	src := &countingSource{r: r}
	br := bufio.NewReaderSize(src, blockTxStreamReadBufferSize)

	count, err := wire.ReadVarInt(br, wire.ProtocolVersion)
	if err != nil {
		// NewBlockInvalidError, not NewProcessingError: a body too short to carry
		// a transaction count is a malformed block from the peer, not a local
		// processing fault, and giving it errBlockTxStreamDone's code would make
		// errors.Is match it against clean exhaustion (see the sentinel's comment).
		return nil, errors.NewBlockInvalidError("[blockTxStream] could not read the transaction count", err)
	}

	if count == 0 {
		return nil, errors.NewBlockInvalidError("[blockTxStream] block has no transactions, not even a coinbase")
	}

	// The body's own length is a real, cheap bound on the count — a
	// transaction cannot be smaller than minSerializedTxSize, so a body this
	// long cannot hold more than payloadLen/minSerializedTxSize of them, and a
	// larger claim is a lie catchable for free — but it does NOT stop the
	// allocation amplification below. payloadLen is itself peer-declared
	// (int64(length) off the wire message header, never measured), and the
	// wire payload ceiling is 4,000,000,000 bytes (services/legacy/config.go
	// maxWireBlockPayload), so this check alone still lets a peer declare a
	// 400,000,000-transaction body it never sends. What actually stops the
	// amplification is that the caller no longer sizes anything from this
	// count: see newPipelineDedupMap in pipeline_sink.go.
	if payloadLen > 0 && count > uint64(payloadLen)/minSerializedTxSize {
		return nil, errors.NewBlockInvalidError("[blockTxStream] block declares %d transactions, more than its %d-byte body can hold", count, payloadLen)
	}

	if count > maxBlockTxCount {
		return nil, errors.NewBlockInvalidError("[blockTxStream] block declares %d transactions, above the %d limit", count, uint64(maxBlockTxCount))
	}

	return &blockTxStream{r: br, txCount: count, txs: txstream.NewReader(br), src: src, bodyLen: payloadLen}, nil
}

// TxCount is the transaction count the peer declared, including the coinbase.
func (s *blockTxStream) TxCount() uint64 {
	return s.txCount
}

// Next returns the next transaction and its hash, or errBlockTxStreamDone once
// every declared transaction has been read.
//
// The hash is computed here and handed on, rather than left for a caller to
// recompute: go-bt's TxIDChainHash re-serializes the whole transaction on a cache
// miss and does not populate its own cache, so a second caller asking for it pays
// the full cost again.
func (s *blockTxStream) Next() (*bt.Tx, *chainhash.Hash, error) {
	tx, hash, _, err := s.NextStreamed(nil)

	return tx, hash, err
}

// NextStreamed is Next with the transaction's outputs streamed as they are read. Once a
// transaction's inputs are read, beforeOutputs is called with it; when it returns a writer, every
// byte of the outputs and the lock time is copied to that writer as it is read, and the script of
// an OP_FALSE OP_RETURN output goes only there and is not kept (txstream.DataOutputScript stands
// in for it). It also returns the transaction's size on the wire.
//
// A failure of beforeOutputs, or of the writer, is returned as it is: it is this node's fault, not
// the peer's, so it must not read as an invalid block.
func (s *blockTxStream) NextStreamed(beforeOutputs txstream.BeforeOutputs) (*bt.Tx, *chainhash.Hash, int64, error) {
	if s.read >= s.txCount {
		return nil, nil, 0, errBlockTxStreamDone
	}

	var (
		localErr error
		sink     *errRecordingWriter
	)

	opts := txstream.Options{SkipDataScripts: true}
	if beforeOutputs != nil {
		opts.BeforeOutputs = func(tx *bt.Tx) (io.Writer, error) {
			w, err := beforeOutputs(tx)
			if err != nil {
				localErr = err

				return nil, err
			}

			if w == nil {
				return nil, nil
			}

			sink = &errRecordingWriter{w: w}

			return sink, nil
		}
	}

	tx, hash, size, err := s.txs.Next(opts)
	if err != nil {
		if localErr != nil {
			return nil, nil, 0, localErr
		}

		if sink != nil && sink.err != nil {
			return nil, nil, 0, errors.NewStorageError("[blockTxStream] failed writing transaction %d of the %d declared", s.read, s.txCount, sink.err)
		}

		return nil, nil, 0, errors.NewBlockInvalidError("[blockTxStream] failed reading transaction %d of the %d declared", s.read, s.txCount, err)
	}

	s.read++

	return tx, hash, size, nil
}

// RequireEnd fails unless the declared transactions used exactly the declared body length and the
// body ends there. Both ways a delivery can differ from its declaration are refused: bytes left
// after the last transaction, and a body that stops short of the declared length, which the
// connection closing after the last transaction presents as a clean io.EOF (an io.LimitedReader
// passes the underlying EOF through while bytes are still owed).
//
// The wire layer refuses both too, but only after the sink has reported success (readBlockMessage,
// services/legacy/peer/wire_streaming.go). By then the converted record is written, possibly over
// a parked copy of the same block, and the refusal's cleanup, pipelineBlockDelete, takes that
// parked copy's subtree files with it, because they are keyed by root and shared by every copy.
func (s *blockTxStream) RequireEnd() error {
	// Corrupt, not invalid, here and below: the transactions matched the header, so a length
	// that disagrees says nothing about the block, only about this delivery of it.
	if consumed := s.src.n - int64(s.r.Buffered()); consumed != s.bodyLen {
		return errors.NewBlockCorruptError("[blockTxStream] the %d declared transactions used %d bytes of a body declared as %d", s.txCount, consumed, s.bodyLen)
	}

	_, err := s.r.ReadByte()

	switch {
	case err == nil:
		// Only reachable when the caller's reader is not bounded at the declared length.
		return errors.NewBlockCorruptError("[blockTxStream] body carries bytes after its %d declared transactions", s.txCount)
	case err == io.EOF:
		return nil
	default:
		return errors.NewBlockCorruptError("[blockTxStream] failed reading past the last of the %d declared transactions", s.txCount, err)
	}
}

// countingSource counts the bytes read through it.
type countingSource struct {
	r io.Reader
	n int64
}

func (c *countingSource) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.n += int64(n)

	return n, err
}

// errRecordingWriter remembers the first error its writer returned, so a failed write can be told
// apart from a failed read.
type errRecordingWriter struct {
	w   io.Writer
	err error
}

func (e *errRecordingWriter) Write(p []byte) (int, error) {
	n, err := e.w.Write(p)
	if err != nil && e.err == nil {
		e.err = err
	}

	return n, err
}
