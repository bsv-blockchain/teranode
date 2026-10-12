package peer

import (
	"fmt"
	"io"
	"sync"

	"github.com/bsv-blockchain/go-wire"
)

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
func streamingBlockHandler(r io.Reader, length uint64, totalBytes int) (int, wire.Message, []byte, error) {
	// Cap the inner decoder so a malformed varint cannot read past the
	// declared payload boundary and desync the next ReadMessage call.
	lr := &io.LimitedReader{R: r, N: int64(length)}

	msg := &wire.MsgBlock{}
	decodeErr := msg.Bsvdecode(lr, wire.ProtocolVersion, wire.BaseEncoding)

	// Drain any unread payload bytes so the next ReadMessage starts on a
	// clean header boundary, and surface a short-stream as an error.
	// io.Copy on a LimitedReader returns nil if the underlying reader
	// EOFs before N reaches 0; we must check lr.N explicitly to detect
	// that case, otherwise a successful Bsvdecode followed by an
	// undersized stream would silently desync subsequent reads.
	var drainErr error
	if lr.N > 0 {
		if _, err := io.Copy(io.Discard, lr); err != nil {
			drainErr = err
		} else if lr.N > 0 {
			drainErr = fmt.Errorf("streaming block: peer declared %d byte payload but stream ended with %d bytes unread", length, lr.N)
		}
	}

	// Decode errors take precedence — they describe the actual failure,
	// whereas drainErr is a downstream symptom of the truncated stream.
	err := decodeErr
	if err == nil {
		err = drainErr
	}

	// totalBytes accounts for the header already read by
	// ReadMessageWithEncodingN; add the full declared payload length so
	// the caller's bytesReceived counter stays consistent with the
	// non-streaming path regardless of how many bytes Bsvdecode actually
	// consumed before erroring.
	return totalBytes + int(length), msg, nil, err
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
