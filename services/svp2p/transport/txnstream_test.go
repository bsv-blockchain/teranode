package transport

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math"
	"net"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-wire"
	"github.com/stretchr/testify/require"
)

// testMsgBlockTxn builds a blocktxn reply carrying txCount transactions, the
// shape blockencodings.h:84-113 BlockTransactions serializes.
func testMsgBlockTxn(t *testing.T, hash chainhash.Hash, txCount int) *wire.MsgBlockTxn {
	t.Helper()

	m := &wire.MsgBlockTxn{BlockHash: hash}

	for i := 0; i < txCount; i++ {
		require.NoError(t, m.AddTransaction(testMsgTx(uint32(i)))) //nolint:gosec // small test loop counter
	}

	return m
}

// txnPayload is the whole blocktxn payload: hash, count varint, transactions.
func txnPayload(t *testing.T, m *wire.MsgBlockTxn) []byte {
	t.Helper()

	var buf bytes.Buffer

	require.NoError(t, m.BsvEncode(&buf, wire.ProtocolVersion, wire.BaseEncoding))

	return buf.Bytes()
}

// txnTxRegion is the part of the payload that follows the 32 byte block hash
// and the count varint: exactly what Reader must deliver.
func txnTxRegion(t *testing.T, m *wire.MsgBlockTxn) []byte {
	t.Helper()

	var buf bytes.Buffer

	for _, tx := range m.Transactions {
		require.NoError(t, tx.Serialize(&buf))
	}

	return buf.Bytes()
}

func recvTxns(t *testing.T, c *Conn) *TxnStream {
	t.Helper()

	select {
	case ts, open := <-c.InboundTxns():
		require.True(t, open, "inbound txns channel closed")
		require.NotNil(t, ts)

		return ts
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for a txn stream")

		return nil
	}
}

// requireConnFailsWithoutTxns asserts the read loop rejected a blocktxn frame
// outright: no stream reached the consumer and the connection died.
func requireConnFailsWithoutTxns(t *testing.T, c *Conn) {
	t.Helper()

	select {
	case ts, open := <-c.InboundTxns():
		require.False(t, open, "a rejected blocktxn must not be delivered, got %v", ts)
	case <-time.After(5 * time.Second):
		t.Fatal("inbound txns channel did not close")
	}

	<-c.Done()
	require.Error(t, c.Err())
}

func TestTxnStreamRoundTrip(t *testing.T) {
	a, b := net.Pipe()
	cb := New(b, testConfig())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cb.Start(ctx)

	hash := chainhash.Hash{0xaa, 0xbb, 0xcc}
	m := testMsgBlockTxn(t, hash, 3)

	go func() {
		_ = wire.WriteMessage(a, m, wire.ProtocolVersion, wire.MainNet)
	}()

	ts := recvTxns(t, cb)
	require.Equal(t, hash, ts.BlockHash())
	require.Equal(t, uint64(3), ts.Count())
	require.False(t, ts.Extended())
	require.Equal(t, uint64(len(txnPayload(t, m))), ts.Length())

	got, err := io.ReadAll(ts.Reader())
	require.NoError(t, err)
	require.Equal(t, txnTxRegion(t, m), got)

	require.NoError(t, ts.Close())
}

// An empty blocktxn is well formed: the peer had nothing to send back.
func TestTxnStreamEmptyRoundTrip(t *testing.T) {
	a, b := net.Pipe()
	cb := New(b, testConfig())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cb.Start(ctx)

	m := testMsgBlockTxn(t, chainhash.Hash{0x01}, 0)

	go func() {
		_ = wire.WriteMessage(a, m, wire.ProtocolVersion, wire.MainNet)
		_ = wire.WriteMessage(a, wire.NewMsgPing(9), wire.ProtocolVersion, wire.MainNet)
	}()

	ts := recvTxns(t, cb)
	require.Equal(t, uint64(0), ts.Count())

	got, err := io.ReadAll(ts.Reader())
	require.NoError(t, err)
	require.Empty(t, got)

	require.NoError(t, ts.Close())

	recvPing(t, cb, 9)
}

// Close drains whatever the consumer left, so the connection stays aligned on
// the next message header.
func TestTxnStreamCloseDrainsToTheBoundary(t *testing.T) {
	a, b := net.Pipe()
	cb := New(b, testConfig())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cb.Start(ctx)

	m := testMsgBlockTxn(t, chainhash.Hash{0x77}, 5)

	go func() {
		_ = wire.WriteMessage(a, m, wire.ProtocolVersion, wire.MainNet)
		_ = wire.WriteMessage(a, wire.NewMsgPing(31), wire.ProtocolVersion, wire.MainNet)
	}()

	ts := recvTxns(t, cb)

	_, err := io.ReadFull(ts.Reader(), make([]byte, 4))
	require.NoError(t, err)

	require.NoError(t, ts.Close())

	recvPing(t, cb, 31)
}

func TestTxnStreamReadAfterCloseFails(t *testing.T) {
	a, b := net.Pipe()
	cb := New(b, testConfig())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cb.Start(ctx)

	m := testMsgBlockTxn(t, chainhash.Hash{0x02}, 2)

	go func() {
		_ = wire.WriteMessage(a, m, wire.ProtocolVersion, wire.MainNet)
	}()

	ts := recvTxns(t, cb)
	require.NoError(t, ts.Close())

	_, err := ts.Reader().Read(make([]byte, 8))
	require.ErrorIs(t, err, ErrBlockStreamClosed)
}

// A payload that ends before the declared length leaves the socket
// misaligned, so Close must report it rather than return nil.
func TestTxnStreamShortPayloadIsReportedByClose(t *testing.T) {
	a, b := net.Pipe()
	cb := New(b, testConfig())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cb.Start(ctx)

	payload := txnPayload(t, testMsgBlockTxn(t, chainhash.Hash{0x03}, 2))

	go func() {
		_, _ = a.Write(frameHeader(wire.MainNet, wire.CmdBlockTxn, uint32(len(payload))+64, [4]byte{})) //nolint:gosec // test payload is small
		_, _ = a.Write(payload)
		_ = a.Close()
	}()

	ts := recvTxns(t, cb)

	_, _ = io.Copy(io.Discard, ts.Reader())

	require.Error(t, ts.Close())
}

func TestTxnStreamOversizedPayloadRejected(t *testing.T) {
	a, b := net.Pipe()
	cb := New(b, testConfig())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cb.Start(ctx)

	oversized := wire.MaxBlockPayload() + 1
	require.LessOrEqual(t, oversized, uint64(^uint32(0)), "the test needs the oversized length to fit the header field")

	go func() {
		_, _ = a.Write(frameHeader(wire.MainNet, wire.CmdBlockTxn, uint32(oversized), [4]byte{})) //nolint:gosec // bounded by the assertion above
	}()

	requireConnFailsWithoutTxns(t, cb)
	require.Contains(t, cb.Err().Error(), "exceeds")
	require.Contains(t, cb.Err().Error(), wire.CmdBlockTxn)
}

func TestTxnStreamWrongNetworkRejected(t *testing.T) {
	a, b := net.Pipe()
	cb := New(b, testConfig())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cb.Start(ctx)

	payload := txnPayload(t, testMsgBlockTxn(t, chainhash.Hash{0x04}, 1))

	go func() {
		_, _ = a.Write(frameHeader(wire.TestNet, wire.CmdBlockTxn, uint32(len(payload)), payloadChecksum(payload))) //nolint:gosec // test payload is small
		_, _ = a.Write(payload)
	}()

	requireConnFailsWithoutTxns(t, cb)
	require.Contains(t, cb.Err().Error(), "other network")
	require.Contains(t, cb.Err().Error(), wire.CmdBlockTxn)
}

// The declared transaction count is bounded by the payload bytes that remain,
// the same way newBlockStream bounds a block's count.
func TestTxnStreamImpossibleCountRejected(t *testing.T) {
	a, b := net.Pipe()
	cb := New(b, testConfig())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cb.Start(ctx)

	var payload bytes.Buffer

	hash := chainhash.Hash{0x05}
	_, _ = payload.Write(hash[:])
	require.NoError(t, wire.WriteVarInt(&payload, wire.ProtocolVersion, 500000))

	go func() {
		_, _ = a.Write(frameHeader(wire.MainNet, wire.CmdBlockTxn, uint32(payload.Len()), payloadChecksum(payload.Bytes()))) //nolint:gosec // test payload is small
		_, _ = a.Write(payload.Bytes())
	}()

	requireConnFailsWithoutTxns(t, cb)
	require.Contains(t, cb.Err().Error(), "declares 500000 transactions")
}

// The whole declared payload is charged to the byte counter, header included,
// exactly as a completed block is.
func TestTxnStreamAccountsFullPayloadBytes(t *testing.T) {
	a, b := net.Pipe()
	cb := New(b, testConfig())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cb.Start(ctx)

	m := testMsgBlockTxn(t, chainhash.Hash{0x06}, 4)
	payload := txnPayload(t, m)

	go func() {
		_ = wire.WriteMessage(a, m, wire.ProtocolVersion, wire.MainNet)
		_ = wire.WriteMessage(a, wire.NewMsgPing(44), wire.ProtocolVersion, wire.MainNet)
	}()

	ts := recvTxns(t, cb)

	// Close without reading a transaction byte: the drain must still charge
	// the whole payload.
	require.NoError(t, ts.Close())

	recvPing(t, cb, 44)

	pingFrame := wire.MessageHeaderSize + 8
	want := uint64(wire.MessageHeaderSize + len(payload) + pingFrame) //nolint:gosec // test payload is small

	require.Equal(t, want, cb.BytesReceived())
}

// The read loop stays parked on the connection while a txn stream is open, so
// a local Close must release it and tear the connection down.
func TestTxnStreamOpenAtTeardownReleasesTheReadLoop(t *testing.T) {
	a, b := net.Pipe()
	defer a.Close()

	cb := New(b, testConfig())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cb.Start(ctx)

	m := testMsgBlockTxn(t, chainhash.Hash{0x07}, 3)

	go func() {
		_ = wire.WriteMessage(a, m, wire.ProtocolVersion, wire.MainNet)
	}()

	ts := recvTxns(t, cb)

	require.NoError(t, cb.Close())

	select {
	case <-cb.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("the read loop stayed parked on the open txn stream")
	}

	require.ErrorIs(t, cb.Err(), ErrConnClosed)

	_ = ts.Close()
}

// An extended blocktxn is the second command the streaming path accepts
// (protocol.cpp:220-263), and it is delivered with its 64-bit length.
func TestReadLoop_ExtendedBlockTxnIsStreamed(t *testing.T) {
	logger := &recordingLogger{}

	a, b := net.Pipe()
	defer a.Close()

	cfg := testConfig()
	cfg.ProtocolVersion = ExtendedPayloadVersion
	cfg.MaxBlockPayload = 8 << 30
	cfg.Logger = logger

	c := New(b, cfg)
	c.Start(context.Background())

	defer c.Close()

	length := uint64(math.MaxUint32) + 1

	go func() {
		_, _ = a.Write(extFrameHeader(wire.MainNet, wire.CmdBlockTxn, length))
		_, _ = io.CopyN(a, zeroReader{}, 200)
		_ = a.Close()
	}()

	ts := recvTxns(t, c)
	require.Equal(t, length, ts.Length())
	require.True(t, ts.Extended())

	_ = ts.Close()

	require.True(t, logger.contains("extended blocktxn frame"), "an accepted extended blocktxn must be logged: %v", logger.lines)
	require.True(t, logger.contains(fmt.Sprintf("%d bytes", length)), "the log line must name the declared length: %v", logger.lines)
}

// A peer below 70016 may not frame a blocktxn extended either (version.h:51).
func TestReadLoop_ExtendedBlockTxnFromOldPeerDisconnects(t *testing.T) {
	a, b := net.Pipe()
	defer a.Close()

	cfg := testConfig()
	cfg.ProtocolVersion = ExtendedPayloadVersion - 1

	c := New(b, cfg)
	c.Start(context.Background())

	go func() { _, _ = a.Write(extFrameHeader(wire.MainNet, wire.CmdBlockTxn, uint64(math.MaxUint32)+1)) }()

	<-c.Done()
	require.ErrorIs(t, c.Err(), ErrExtendedVersion)
}

// Widening the extended path to blocktxn must not widen it any further: every
// other command is still refused, the BIP152 siblings included.
func TestReadLoop_ExtendedNonStreamedCommandsStillRefused(t *testing.T) {
	for _, cmd := range []string{wire.CmdInv, wire.CmdTx, wire.CmdHeaders, wire.CmdCmpctBlock, wire.CmdGetBlockTxn} {
		t.Run(cmd, func(t *testing.T) {
			a, b := net.Pipe()
			defer a.Close()

			cfg := testConfig()
			cfg.ProtocolVersion = ExtendedPayloadVersion

			c := New(b, cfg)
			c.Start(context.Background())

			go func() { _, _ = a.Write(extFrameHeader(wire.MainNet, cmd, uint64(math.MaxUint32)+1)) }()

			<-c.Done()
			require.ErrorIs(t, c.Err(), ErrExtendedNonBlock)
		})
	}
}

// A basic blocktxn frame must stay silent, or every compact-block gap fill on
// a busy node writes a line.
func TestReadLoop_BasicBlockTxnIsNotLoggedAsExtended(t *testing.T) {
	logger := &recordingLogger{}

	a, b := net.Pipe()
	defer a.Close()

	cfg := testConfig()
	cfg.Logger = logger

	cb := New(b, cfg)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cb.Start(ctx)

	m := testMsgBlockTxn(t, chainhash.Hash{0x08}, 1)

	go func() {
		_ = wire.WriteMessage(a, m, wire.ProtocolVersion, wire.MainNet)
	}()

	ts := recvTxns(t, cb)
	require.NoError(t, ts.Close())

	require.False(t, logger.contains("extended"), "a basic frame must not be logged as extended: %v", logger.lines)
}

// The association merges InboundTxns the way it merges InboundBlocks, from
// whichever stream the peer sent the reply on.
func TestAssociation_MergesInboundTxns(t *testing.T) {
	cases := []struct {
		name       string
		streamType wire.StreamType
	}{
		{name: "general", streamType: wire.StreamTypeGeneral},
		{name: "data1", streamType: wire.StreamTypeData1},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			general, generalRemote := pipeConn(t, wire.StreamTypeGeneral, wire.ProtocolVersion)

			a := NewAssociation(general, []byte{0x01})

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			a.Start(ctx)

			remote := generalRemote

			if tc.streamType != wire.StreamTypeGeneral {
				data1, data1Remote := pipeConn(t, wire.StreamTypeData1, wire.ProtocolVersion)
				require.NoError(t, a.Attach(data1))

				remote = data1Remote
			}

			hash := chainhash.Hash{0x09}
			m := testMsgBlockTxn(t, hash, 2)

			go func() {
				_ = wire.WriteMessage(remote, m, wire.ProtocolVersion, wire.MainNet)
			}()

			select {
			case ts, open := <-a.InboundTxns():
				require.True(t, open, "the merged txns channel closed")
				require.Equal(t, hash, ts.BlockHash())
				require.Equal(t, uint64(2), ts.Count())
				require.NoError(t, ts.Close())
			case <-time.After(5 * time.Second):
				t.Fatal("no txn stream reached the association")
			}

			require.NoError(t, a.Close())
		})
	}
}

// Teardown closes the merged txns channel with the rest.
func TestAssociation_TeardownClosesInboundTxns(t *testing.T) {
	general, generalRemote := pipeConn(t, wire.StreamTypeGeneral, wire.ProtocolVersion)

	a := NewAssociation(general, []byte{0x02})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	a.Start(ctx)

	require.NoError(t, generalRemote.Close())

	select {
	case <-a.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("the association did not tear down")
	}

	select {
	case _, open := <-a.InboundTxns():
		require.False(t, open, "the merged txns channel stayed open")
	case <-time.After(5 * time.Second):
		t.Fatal("the merged txns channel did not close")
	}
}
