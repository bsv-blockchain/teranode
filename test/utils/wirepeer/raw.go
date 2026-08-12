package wirepeer

import (
	"crypto/sha256"
	"encoding/binary"
	"net"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/daemon"
	"github.com/stretchr/testify/require"
)

// RawConn is a bare TCP connection to the legacy wire listener, for tests whose
// whole point is to send bytes a conforming encoder would never produce:
// unknown commands, empty payloads, bad checksums, oversized length prefixes.
// Upstream equivalents include bsv-empty-payload.py, bsv-empty-msg-cmd.py and
// the protoconf violation tests.
//
// Anything that can be expressed as a valid wire.Message belongs on Peer
// instead; this type does no handshake and no bookkeeping.
type RawConn struct {
	conn  net.Conn
	magic uint32
}

// DialRaw opens an un-negotiated connection to the daemon's legacy listener.
func DialRaw(t *testing.T, td *daemon.TestDaemon) *RawConn {
	t.Helper()

	addr := listenAddr(t, td)

	conn, err := net.DialTimeout("tcp", addr, 10*time.Second)
	require.NoError(t, err, "dial legacy listener at %s", addr)

	return &RawConn{
		conn:  conn,
		magic: uint32(td.Settings.ChainCfgParams.Net),
	}
}

// WriteFrame writes a wire message header followed by payload, computing a
// correct length and checksum. Use it to send a well-framed message with an
// arbitrary command name or payload body.
func (r *RawConn) WriteFrame(t *testing.T, command string, payload []byte) {
	t.Helper()

	r.WriteBytes(t, frame(t, r.magic, command, payload, uint32(len(payload))))
}

// WriteFrameWithLength writes a frame whose declared payload length is
// deliberately wrong, for tests that check how the node handles a length prefix
// that disagrees with the bytes that follow.
func (r *RawConn) WriteFrameWithLength(t *testing.T, command string, payload []byte, declaredLen uint32) {
	t.Helper()

	r.WriteBytes(t, frame(t, r.magic, command, payload, declaredLen))
}

// frame builds a 24-byte wire header followed by payload. declaredLen is the
// length written into the header, which callers can make disagree with the
// bytes that follow. The command is written into a 12-byte field padded with
// NULs, so passing "" produces the all-zero command name that
// bsv-empty-msg-cmd.py sends.
func frame(t *testing.T, magic uint32, command string, payload []byte, declaredLen uint32) []byte {
	t.Helper()

	require.LessOrEqual(t, len(command), 12, "wire command names are at most 12 bytes")

	header := make([]byte, 24)
	binary.LittleEndian.PutUint32(header[0:4], magic)
	copy(header[4:16], command)
	binary.LittleEndian.PutUint32(header[16:20], declaredLen)
	copy(header[20:24], checksum(payload))

	return append(header, payload...)
}

// WriteBytes writes raw bytes with no framing whatsoever.
func (r *RawConn) WriteBytes(t *testing.T, b []byte) {
	t.Helper()

	require.NoError(t, r.conn.SetWriteDeadline(time.Now().Add(10*time.Second)))

	_, err := r.conn.Write(b)
	require.NoError(t, err, "write %d bytes", len(b))
}

// ReadSome reads up to max bytes, returning what arrived before the deadline.
// A short or empty read is a normal outcome here, not an error: for many of
// these tests the expected node behaviour is to say nothing and hang up.
func (r *RawConn) ReadSome(t *testing.T, max int, timeout time.Duration) []byte {
	t.Helper()

	require.NoError(t, r.conn.SetReadDeadline(time.Now().Add(timeout)))

	buf := make([]byte, max)

	n, err := r.conn.Read(buf)
	if err != nil {
		return buf[:n]
	}

	return buf[:n]
}

// TryWriteBytes writes raw bytes and reports whether the write succeeded, rather
// than failing the test.
//
// For tests that deliberately push until the node reacts: a flood may legitimately
// be cut short by a disconnect, and where that happens is the observation rather
// than an error. Use WriteBytes when a failed write really is a test failure.
func (r *RawConn) TryWriteBytes(b []byte) bool {
	if err := r.conn.SetWriteDeadline(time.Now().Add(10 * time.Second)); err != nil {
		return false
	}

	_, err := r.conn.Write(b)

	return err == nil
}

// ClosedWithin reports whether the node closed the connection within timeout.
//
// It exists because a plain read cannot tell the two outcomes apart: ReadSome
// returns no bytes both for a connection the node has closed and for one it is
// simply keeping quiet on, which are opposite answers for a test about timeouts.
// The difference is in the error, so this inspects it - a timeout means still
// open, anything else including EOF means gone.
//
// It asserts nothing beyond being able to set a deadline, so it can be used to
// require either outcome. Note that a "still open" answer costs the full timeout,
// so keep the window short when that is the expected result.
func (r *RawConn) ClosedWithin(t *testing.T, timeout time.Duration) bool {
	t.Helper()

	require.NoError(t, r.conn.SetReadDeadline(time.Now().Add(timeout)))

	buf := make([]byte, 1024)

	for {
		_, err := r.conn.Read(buf)
		if err == nil {
			// Data arrived. Keep reading: the question is whether the connection
			// ends before the deadline, not whether it is silent.
			continue
		}

		if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
			return false
		}

		return true
	}
}

// ExpectClosed asserts the node closes the connection within timeout, which is
// the expected response to most malformed input.
func (r *RawConn) ExpectClosed(t *testing.T, timeout time.Duration) {
	t.Helper()

	if !r.ClosedWithin(t, timeout) {
		t.Fatalf("wirepeer: connection still open after %s; expected the node to close it", timeout)
	}
}

// Close closes the connection.
func (r *RawConn) Close() {
	_ = r.conn.Close()
}

// checksum returns the first 4 bytes of the double-SHA256 of payload, as the
// wire message header requires.
func checksum(payload []byte) []byte {
	first := sha256.Sum256(payload)
	second := sha256.Sum256(first[:])

	return second[:4]
}
