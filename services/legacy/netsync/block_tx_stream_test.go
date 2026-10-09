package netsync

import (
	"bytes"
	"io"
	"net"
	"syscall"
	"testing"

	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/stretchr/testify/require"
)

// streamTxBytes builds one serialized transaction and returns it with its hash.
// streamTx is the package's existing fixture, at block_stream_builder_test.go:27.
func streamTxBytes(t *testing.T, seed int) ([]byte, string) {
	t.Helper()

	tx, hash := streamTx(t, seed)

	return tx.Bytes(), hash.String()
}

// TestBlockTxStream_YieldsTheCountThenEveryTransaction is the contract. The count
// must be available before the first transaction, because the builder partitions
// from it before anything arrives.
func TestBlockTxStream_YieldsTheCountThenEveryTransaction(t *testing.T) {
	var buf bytes.Buffer

	require.NoError(t, wire.WriteVarInt(&buf, wire.ProtocolVersion, 3))

	want := make([]string, 0, 3)

	for i := 1; i <= 3; i++ {
		raw, hash := streamTxBytes(t, i)
		want = append(want, hash)

		_, err := buf.Write(raw)
		require.NoError(t, err)
	}

	s, err := newBlockTxStream(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
	require.NoError(t, err)
	require.Equal(t, uint64(3), s.TxCount(), "the count must be readable before any transaction")

	got := make([]string, 0, 3)

	for {
		_, hash, err := s.Next()
		if errors.Is(err, errBlockTxStreamDone) {
			break
		}

		require.NoError(t, err)
		got = append(got, hash.String())
	}

	require.Equal(t, want, got, "every transaction, in block order")
}

// TestBlockTxStream_RefusesATruncatedStream pins the case a malicious or broken
// peer produces: a declared count the body does not deliver. Stopping short must
// be an error, never a short block treated as complete. Here the body arrived to
// exactly its declared length and still owes four transactions, so the message
// disagrees with itself: that is the peer's, as a corrupt delivery, the same code
// RequireEnd gives a length that disagrees with the transactions.
func TestBlockTxStream_RefusesATruncatedStream(t *testing.T) {
	var buf bytes.Buffer

	require.NoError(t, wire.WriteVarInt(&buf, wire.ProtocolVersion, 5))

	raw, _ := streamTxBytes(t, 1)
	_, err := buf.Write(raw)
	require.NoError(t, err)

	s, err := newBlockTxStream(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
	require.NoError(t, err)

	_, _, err = s.Next()
	require.NoError(t, err)

	_, _, err = s.Next()
	require.Error(t, err, "a body that runs out mid-transaction must fail, not silently stop")
	require.NotErrorIs(t, err, errBlockTxStreamDone,
		"a truncated body must be an error, not a clean end of stream")
	require.True(t, errors.IsBlockCorrupt(err),
		"a body delivered to its declared length that still owes transactions is a corrupt delivery, the peer's")
	require.False(t, errors.Is(err, errors.ErrBlockInvalid), "and says nothing about the block itself")
}

// TestBlockTxStream_ABodyCutShortOfItsDeclaredLengthIsADeliveryFault pins the
// other way a stream stops early: the connection ends before the declared payload
// length is reached. That is a hang-up, not a bad body (HARDEN 1421: a delivery
// fault never carries the invalid code), so the bare io.ErrUnexpectedEOF comes
// out, which is what peer.shouldHandleReadError compares by identity.
func TestBlockTxStream_ABodyCutShortOfItsDeclaredLengthIsADeliveryFault(t *testing.T) {
	var buf bytes.Buffer

	require.NoError(t, wire.WriteVarInt(&buf, wire.ProtocolVersion, 2))

	raw, _ := streamTxBytes(t, 1)
	_, err := buf.Write(raw)
	require.NoError(t, err)

	// Half of the second transaction, under a declaration of both in full.
	cut := append([]byte(nil), raw[:len(raw)/2]...)
	_, err = buf.Write(cut)
	require.NoError(t, err)

	s, err := newBlockTxStream(bytes.NewReader(buf.Bytes()), int64(buf.Len()+len(raw)-len(cut)))
	require.NoError(t, err)

	_, _, err = s.Next()
	require.NoError(t, err)

	_, _, err = s.Next()
	require.Same(t, io.ErrUnexpectedEOF, err, "a body cut before its declared length is a hang-up, returned by identity")
	require.NotErrorIs(t, err, errBlockTxStreamDone)
}

// resetReader hands out its bytes and then fails with the error a connection that
// died produces instead of a clean FIN: a *net.OpError, which is what net.Conn
// returns for a reset by the peer, a closed socket or a NAT timing out. A net.Pipe
// cannot produce one (closing it gives io.EOF or io.ErrClosedPipe), so this is how
// the shape is driven in a test.
type resetReader struct {
	r   io.Reader
	err error
}

func (r *resetReader) Read(p []byte) (int, error) {
	n, err := r.r.Read(p)
	if err == io.EOF {
		return n, r.err
	}

	return n, err
}

// connectionReset is the error a hung-up TCP socket hands the reader: non-temporary
// by Go's definition (syscall.Errno.Temporary is true only for EINTR, EMFILE, ENFILE
// and the timeout errnos), so peer.shouldHandleReadError logs it as a network error
// rather than answering it with a reject.
func connectionReset() *net.OpError {
	return &net.OpError{Op: "read", Net: "tcp", Err: syscall.ECONNRESET}
}

// TestBlockTxStream_AResetMidTransactionIsADeliveryFault pins the second way a
// connection ends: not a FIN, which the decoder sees as EOF and endOfBody judges by
// where the body stopped, but a socket error inside a transaction. That error is
// the connection's, never the block's, so it leaves the stream bare and by identity
// (peer.shouldHandleReadError type-asserts *net.OpError directly), with neither the
// invalid nor the corrupt code and not read as this node's fault either. Before
// this fix every non-EOF read failure was wrapped in NewBlockInvalidError, so a
// reset was answered with a reject for a "malformed" block.
func TestBlockTxStream_AResetMidTransactionIsADeliveryFault(t *testing.T) {
	var buf bytes.Buffer

	require.NoError(t, wire.WriteVarInt(&buf, wire.ProtocolVersion, 2))

	raw, _ := streamTxBytes(t, 1)
	_, err := buf.Write(raw)
	require.NoError(t, err)

	// Half of the second transaction, under a declaration of both in full, then the reset.
	cut := append([]byte(nil), raw[:len(raw)/2]...)
	_, err = buf.Write(cut)
	require.NoError(t, err)

	reset := connectionReset()
	src := &resetReader{r: bytes.NewReader(buf.Bytes()), err: reset}

	s, err := newBlockTxStream(src, int64(buf.Len()+len(raw)-len(cut)))
	require.NoError(t, err)

	_, _, err = s.Next()
	require.NoError(t, err)

	_, _, err = s.Next()
	require.Error(t, err)
	require.Same(t, reset, err, "the socket's own error must come out by identity; the read loop type-asserts it")
	require.NotErrorIs(t, err, errBlockTxStreamDone)
	require.False(t, errors.Is(err, errors.ErrBlockInvalid), "a reset mid-body says nothing about the block")
	require.False(t, errors.IsBlockCorrupt(err), "nor about this delivery's declaration: it never finished arriving")
	require.False(t, isLocalSinkFault(err), "nor is it this node's fault")
}

// TestBlockTxStream_AResetBeforeTheCountIsADeliveryFault pins the same rule at the
// count read, the other place the stream reads from the socket and judges the
// failure, which used to stamp any non-EOF read error as a badly encoded varint.
func TestBlockTxStream_AResetBeforeTheCountIsADeliveryFault(t *testing.T) {
	reset := connectionReset()

	_, err := newBlockTxStream(&resetReader{r: bytes.NewReader(nil), err: reset}, 100)
	require.Error(t, err)
	require.Same(t, reset, err, "the connection died before a byte of the body arrived")
	require.False(t, errors.Is(err, errors.ErrBlockInvalid))
	require.False(t, errors.IsBlockCorrupt(err))
}

// TestBlockTxStream_RefusesABodyTooShortForACount pins the collision the
// reviewer found: errBlockTxStreamDone and a body too short to even carry the
// transaction count varint were both built with NewProcessingError, and
// teranode's errors.Is matches on code alone, so the two were
// indistinguishable to a caller doing errors.Is(err, errBlockTxStreamDone). A
// body that ends before its declared length even holds a count must read as a
// failure, never as "nothing left to read." Nothing of the 100 declared bytes
// arrived, so it is the connection ending, by identity, not an invalid block.
func TestBlockTxStream_RefusesABodyTooShortForACount(t *testing.T) {
	_, err := newBlockTxStream(bytes.NewReader(nil), 100)
	require.Error(t, err, "an empty body cannot even hold a transaction count")
	require.NotErrorIs(t, err, errBlockTxStreamDone,
		"a count-read failure must not collide with clean exhaustion on error code")
	require.Same(t, io.ErrUnexpectedEOF, err, "no byte of the declared body arrived: the connection ended")
}

// TestBlockTxStream_ADeclaredBodyWithNoRoomForACountIsCorrupt pins the boundary of
// the rule above: a message whose declared payload is exactly the header, so the
// body is complete at zero bytes and holds no count, is a malformed message from
// the peer, not a hang-up.
func TestBlockTxStream_ADeclaredBodyWithNoRoomForACountIsCorrupt(t *testing.T) {
	_, err := newBlockTxStream(bytes.NewReader(nil), 0)
	require.Error(t, err)
	require.True(t, errors.IsBlockCorrupt(err), "the body arrived in full and carries no count: the declaration is wrong, the peer's")
}

// TestBlockTxStream_StopsAtTheDeclaredCount pins the other end: trailing bytes
// beyond the declared count are not read as transactions.
func TestBlockTxStream_StopsAtTheDeclaredCount(t *testing.T) {
	var buf bytes.Buffer

	require.NoError(t, wire.WriteVarInt(&buf, wire.ProtocolVersion, 1))

	raw, _ := streamTxBytes(t, 1)
	_, err := buf.Write(raw)
	require.NoError(t, err)

	extra, _ := streamTxBytes(t, 2)
	_, err = buf.Write(extra)
	require.NoError(t, err)

	s, err := newBlockTxStream(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
	require.NoError(t, err)

	_, _, err = s.Next()
	require.NoError(t, err)

	_, _, err = s.Next()
	require.ErrorIs(t, err, errBlockTxStreamDone, "one transaction was declared, so one is read")
}

// TestBlockTxStream_RefusesACountItsBodyCannotHold is the guard that matters. The
// caller sizes a duplicate map from this count before any transaction arrives, so
// a huge claim against a tiny body must be refused at the claim, not discovered
// when the body runs out.
func TestBlockTxStream_RefusesACountItsBodyCannotHold(t *testing.T) {
	var buf bytes.Buffer

	require.NoError(t, wire.WriteVarInt(&buf, wire.ProtocolVersion, 1<<20))

	_, err := newBlockTxStream(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
	require.Error(t, err)
	require.Contains(t, err.Error(), "body can hold")
}

// TestBlockTxStream_RefusesACountAboveTheAbsoluteCeiling pins the second bound,
// which exists because the duplicate map's constructor takes a uint32: a count
// above that range would wrap and produce a map that dedups nothing. A payloadLen
// large enough to clear the body bound is passed so this test exercises the
// ceiling and not the other guard.
func TestBlockTxStream_RefusesACountAboveTheAbsoluteCeiling(t *testing.T) {
	var buf bytes.Buffer

	require.NoError(t, wire.WriteVarInt(&buf, wire.ProtocolVersion, 1<<40))

	_, err := newBlockTxStream(bytes.NewReader(buf.Bytes()), 1<<62)
	require.Error(t, err)
	require.Contains(t, err.Error(), "above the")
}

// TestBlockTxStream_RefusesAnEmptyBlock pins the coinbase floor: every block has
// at least one transaction, so a zero count is a malformed body and not an empty
// success.
func TestBlockTxStream_RefusesAnEmptyBlock(t *testing.T) {
	var buf bytes.Buffer

	require.NoError(t, wire.WriteVarInt(&buf, wire.ProtocolVersion, 0))

	_, err := newBlockTxStream(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
	require.Error(t, err)
	require.Contains(t, err.Error(), "no transactions")
}
