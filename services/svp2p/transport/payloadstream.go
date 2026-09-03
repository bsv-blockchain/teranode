package transport

import (
	"io"
	"sync"

	"github.com/bsv-blockchain/teranode/errors"
)

// ErrBlockStreamClosed is returned by a stream's reader once the stream is
// closed.
var ErrBlockStreamClosed = errors.New(errors.ERR_INVALID_ARGUMENT, "svp2p: block stream is closed")

// payloadStream is the part BlockStream and TxnStream share: one declared
// payload that stays on the socket, bounded by a LimitedReader, drained to the
// frame boundary on Close. The two differ only in the fixed prefix they decode
// before the consumer's reader begins.
//
// The consumer owns the stream until it calls Close. Close is idempotent and
// safe from any goroutine.
type payloadStream struct {
	length   uint64
	extended bool

	// mu serializes consumer reads against the drain that Close performs,
	// because Close may run on a goroutine other than the reading one.
	mu       sync.Mutex
	lr       *io.LimitedReader
	closed   bool
	drainErr error

	done      chan struct{}
	closeOnce sync.Once
}

// streamedPayload is what BlockStream and TxnStream have in common as far as
// the read loop is concerned: the payload core underneath them.
type streamedPayload interface{ core() *payloadStream }

// core is how BlockStream and TxnStream satisfy streamedPayload; both embed
// payloadStream, so both promote this method.
func (p *payloadStream) core() *payloadStream { return p }

// newPayloadStream binds a stream to the declared payload length. Capping the
// reader at that length is what stops a malformed varint from reading past the
// message boundary and desyncing the next header. Mirrors
// services/legacy/peer/wire_streaming.go streamingBlockHandler.
func newPayloadStream(r io.Reader, length uint64) payloadStream {
	return payloadStream{
		length: length,
		lr:     &io.LimitedReader{R: r, N: int64(length)}, //nolint:gosec // length is bounded by MaxBlockPayload
		done:   make(chan struct{}),
	}
}

// Length returns the payload length the peer declared. It is known before a
// single transaction byte is read, which makes it the honest weight for an
// admission budget keyed on payload size — the consumer cannot derive that
// from the fixed prefix or the transaction count.
func (p *payloadStream) Length() uint64 { return p.length }

// Extended reports whether this payload arrived framed with the extended
// message header (protocol.cpp:220-237), i.e. it exceeded MaxBlockFrameBytes.
func (p *payloadStream) Extended() bool { return p.extended }

// Close drains any unread payload bytes so the connection stays aligned on the
// next message header, and releases the read loop. It is idempotent: every
// call returns the same result. Close waits for any in-flight read to return,
// so a reader blocked on a silent peer holds Close until the socket closes.
func (p *payloadStream) Close() error {
	p.closeOnce.Do(func() {
		p.mu.Lock()
		p.closed = true
		p.drainErr = p.drainLocked()
		p.mu.Unlock()

		close(p.done)
	})

	p.mu.Lock()
	defer p.mu.Unlock()

	return p.drainErr
}

// drainLocked mirrors services/legacy/peer/wire_streaming.go
// streamingBlockHandler: io.Copy on a LimitedReader returns nil if the
// underlying reader EOFs before N reaches 0, so lr.N must be checked
// explicitly. Otherwise an undersized stream would silently desync every
// later read.
func (p *payloadStream) drainLocked() error {
	if p.lr.N <= 0 {
		return nil
	}

	if _, err := io.Copy(io.Discard, p.lr); err != nil {
		return err
	}

	if p.lr.N > 0 {
		return errors.New(errors.ERR_NETWORK_INVALID_RESPONSE,
			"svp2p: peer declared %d byte payload but stream ended with %d bytes unread", p.length, p.lr.N)
	}

	return nil
}

// consumed reports the payload bytes taken off the socket so far.
func (p *payloadStream) consumed() uint64 {
	p.mu.Lock()
	defer p.mu.Unlock()

	return p.length - uint64(p.lr.N) //nolint:gosec // lr.N is non-negative and never exceeds length
}

func (p *payloadStream) read(b []byte) (int, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return 0, ErrBlockStreamClosed
	}

	return p.lr.Read(b)
}

// payloadReader is the consumer's view of the remaining payload bytes.
//
// It is an io.ReadCloser, not a bare io.Reader, so a consumer that only ever
// sees the reader can still release the stream: Close forwards to the stream's
// own idempotent Close. That matters because the read loop stays parked on the
// connection until the stream closes, and a consumer handed only the reader
// would otherwise have no way to release it.
type payloadReader struct{ p *payloadStream }

func (t payloadReader) Read(b []byte) (int, error) { return t.p.read(b) }

// Close releases the whole stream, not just this reader: it is the same
// idempotent stream Close, so closing the reader drains any unread payload and
// frees the parked read loop.
func (t payloadReader) Close() error { return t.p.Close() }

// boundedCount rejects a declared transaction count the remaining payload
// bytes cannot possibly hold. go-wire rejects above maxTxPerBlock, which is
// (MaxBlockPayload/minTxPayload) + 1 — the quotient PLUS ONE, msg_block.go:40-42.
// Here the divisor applies to the unread remainder of this payload, so the
// bound is tighter than the buffered path's and never looser: the remainder can
// at most equal MaxBlockPayload, which leaves this bound one below theirs even
// then. Consumers size their ingest from the count, so this must not admit a
// number the buffered path would have refused.
func (p *payloadStream) boundedCount(count uint64, kind string) error {
	if count > uint64(p.lr.N)/minTxPayloadBytes { //nolint:gosec // lr.N is non-negative
		return errors.New(errors.ERR_NETWORK_INVALID_RESPONSE,
			"svp2p: %s declares %d transactions in %d remaining payload bytes", kind, count, p.lr.N)
	}

	return nil
}
