package netsync

import (
	"bytes"
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
// be an error, never a short block treated as complete.
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
}

// TestBlockTxStream_RefusesABodyTooShortForACount pins the collision the
// reviewer found: errBlockTxStreamDone and a body too short to even carry the
// transaction count varint were both built with NewProcessingError, and
// teranode's errors.Is matches on code alone, so the two were
// indistinguishable to a caller doing errors.Is(err, errBlockTxStreamDone). A
// peer that sends a body too short to carry a count must read as a failure,
// never as "nothing left to read."
func TestBlockTxStream_RefusesABodyTooShortForACount(t *testing.T) {
	_, err := newBlockTxStream(bytes.NewReader(nil), 100)
	require.Error(t, err, "an empty body cannot even hold a transaction count")
	require.NotErrorIs(t, err, errBlockTxStreamDone,
		"a count-read failure must not collide with clean exhaustion on error code")
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
