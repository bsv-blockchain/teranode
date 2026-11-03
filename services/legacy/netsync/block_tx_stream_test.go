package netsync

import (
	"bytes"
	"io"
	"net"
	"syscall"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
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
// exactly its declared length and stops inside the second of its two transactions,
// so the message disagrees with itself: that is the peer's, as a corrupt delivery,
// the same code RequireEnd gives a length that disagrees with the transactions. The
// second transaction is begun rather than missing so the body is long enough for its
// count (minSerializedTxSize) and the stream gets as far as reading it.
func TestBlockTxStream_RefusesATruncatedStream(t *testing.T) {
	var buf bytes.Buffer

	require.NoError(t, wire.WriteVarInt(&buf, wire.ProtocolVersion, 2))

	raw, _ := streamTxBytes(t, 1)
	_, err := buf.Write(raw)
	require.NoError(t, err)

	second, _ := streamTxBytes(t, 2)
	_, err = buf.Write(second[:len(second)/2])
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

// The same disagreement with the body ending exactly at a transaction boundary: the stream sees
// a clean io.EOF where the next transaction should start, rather than one inside a transaction,
// and that is still a corrupt delivery, not a clean end.
func TestBlockTxStream_RefusesAStreamThatEndsBetweenTransactionsShortOfItsCount(t *testing.T) {
	var buf bytes.Buffer

	require.NoError(t, wire.WriteVarInt(&buf, wire.ProtocolVersion, 4))

	for i := 1; i <= 3; i++ {
		raw, _ := streamTxBytes(t, i)
		_, err := buf.Write(raw)
		require.NoError(t, err)
	}

	bodyLen := int64(buf.Len())
	require.GreaterOrEqual(t, bodyLen, int64(4*minSerializedTxSize), "the body must be long enough for its count, so the stream reads to its end")

	s, err := newBlockTxStream(bytes.NewReader(buf.Bytes()), bodyLen)
	require.NoError(t, err)

	for i := 0; i < 3; i++ {
		_, _, err = s.Next()
		require.NoError(t, err)
	}

	_, _, err = s.Next()
	require.Error(t, err)
	require.NotErrorIs(t, err, errBlockTxStreamDone)
	require.True(t, errors.IsBlockCorrupt(err), "a body delivered to its declared length that still owes a transaction is a corrupt delivery: %v", err)
	require.False(t, errors.Is(err, errors.ErrBlockInvalid))
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

// A transaction with no inputs or no outputs is refused as it is read, before the caller records it
// in the block's duplicate map. It parses in 10 bytes, so without this a peer could make the map
// grow by one entry for every 10 bytes of body sent before the merkle root was checked (issue
// 4945). It is an invalid block and the peer's fault, but not one of the three refusals that may
// ban (ErrBlockBodyMismatch).
func TestBlockTxStream_RefusesATransactionWithNoInputsOrNoOutputs(t *testing.T) {
	noInputs := bt.NewTx()
	require.NoError(t, noInputs.PayToAddress("1BitcoinEaterAddressDontSendf59kuE", 1000))

	noOutputs, _ := streamTx(t, 1)
	noOutputs.Outputs = nil

	neither := bt.NewTx()

	for name, tx := range map[string]*bt.Tx{"no inputs": noInputs, "no outputs": noOutputs, "neither": neither} {
		t.Run(name, func(t *testing.T) {
			first, _ := streamTx(t, 2)

			var buf bytes.Buffer

			require.NoError(t, wire.WriteVarInt(&buf, wire.ProtocolVersion, 2))
			buf.Write(first.Bytes())
			buf.Write(tx.Bytes())

			// Declared long enough for its count, so the stream reaches the transaction itself.
			s, err := newBlockTxStream(&buf, minSerializedTxSize*2+1)
			require.NoError(t, err)

			_, _, err = s.Next()
			require.NoError(t, err)

			_, _, err = s.Next()
			require.Error(t, err)
			require.Contains(t, err.Error(), "it needs at least one", "refused by the check, not by the parser")
			require.True(t, errors.Is(err, errors.ErrBlockInvalid), "no block carrying it can be valid: %v", err)
			require.False(t, errors.Is(err, errors.ErrBlockBodyMismatch), "and it is not a refusal that bans")
			require.NotErrorIs(t, err, errBlockTxStreamDone)
		})
	}
}

// A transaction with no inputs is refused once its inputs are read, before the caller is asked for
// a writer for its outputs: nothing of it reaches the subtree data file.
func TestBlockTxStream_RefusesNoInputsBeforeItsOutputsAreWritten(t *testing.T) {
	noInputs := bt.NewTx()
	require.NoError(t, noInputs.PayToAddress("1BitcoinEaterAddressDontSendf59kuE", 1000))

	var buf bytes.Buffer

	require.NoError(t, wire.WriteVarInt(&buf, wire.ProtocolVersion, 1))
	buf.Write(noInputs.Bytes())

	s, err := newBlockTxStream(&buf, minSerializedTxSize)
	require.NoError(t, err)

	asked := false

	_, _, _, err = s.NextStreamed(func(*bt.Tx) (io.Writer, error) {
		asked = true

		return io.Discard, nil
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "has no inputs")
	require.True(t, errors.Is(err, errors.ErrBlockInvalid), "no block carrying it can be valid: %v", err)
	require.False(t, errors.Is(err, errors.ErrBlockBodyMismatch), "and it is not a refusal that bans")
	require.False(t, asked, "the outputs were never given a writer")
}

// The count is held to minSerializedTxSize, the smallest valid transaction, not to the 10 bytes an
// input-less, output-less one parses in: a body of 60 bytes per declared transaction passes, one
// byte less is refused at the claim.
func TestBlockTxStream_HoldsTheCountToTheSmallestValidTransaction(t *testing.T) {
	const count = 1000

	var buf bytes.Buffer

	require.NoError(t, wire.WriteVarInt(&buf, wire.ProtocolVersion, count))

	_, err := newBlockTxStream(bytes.NewReader(buf.Bytes()), count*minSerializedTxSize)
	require.NoError(t, err)

	_, err = newBlockTxStream(bytes.NewReader(buf.Bytes()), count*minSerializedTxSize-1)
	require.Error(t, err)
	require.Contains(t, err.Error(), "body can hold")
}

// A block on the wire whose transaction comes in the extended form is a corrupt delivery. Its id,
// and so the header, still match, because an id is defined over the standard form, but SV Node
// rejects the encoding, and the previous outputs it carries would land in the .subtreeData file.
// Nothing reaches the subtree writer (review of 2026-10-12).
func TestBlockTxStream_RefusesATransactionInTheExtendedForm(t *testing.T) {
	tx, _ := streamTx(t, 1)

	for _, in := range tx.Inputs {
		in.PreviousTxSatoshis = 1000
		in.PreviousTxScript = bscript.NewFromBytes([]byte{bscript.OpTRUE})
	}

	raw := tx.ExtendedBytes()
	require.NotEqual(t, tx.Bytes(), raw)

	var buf bytes.Buffer

	require.NoError(t, wire.WriteVarInt(&buf, wire.ProtocolVersion, 1))

	_, err := buf.Write(raw)
	require.NoError(t, err)

	s, err := newBlockTxStream(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
	require.NoError(t, err)

	var written bytes.Buffer

	_, _, _, err = s.NextStreamed(func(*bt.Tx) (io.Writer, error) {
		return &written, nil
	})
	require.Error(t, err)
	require.True(t, errors.IsBlockCorrupt(err), "a delivery fault, the peer's: %v", err)
	require.False(t, errors.Is(err, errors.ErrBlockInvalid), "and says nothing about the block itself")
	require.Zero(t, written.Len(), "nothing reaches the subtree file")
}
