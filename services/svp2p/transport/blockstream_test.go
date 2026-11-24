package transport

import (
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-wire"
	"github.com/stretchr/testify/require"
)

func testMsgTx(seq uint32) *wire.MsgTx {
	tx := wire.NewMsgTx(1)
	tx.AddTxIn(wire.NewTxIn(wire.NewOutPoint(&chainhash.Hash{byte(seq)}, seq), []byte{0x51, byte(seq)}))
	tx.AddTxOut(wire.NewTxOut(int64(1000+seq), []byte{0x76, 0xa9, byte(seq)}))

	return tx
}

func testMsgBlock(t *testing.T, txCount int) *wire.MsgBlock {
	t.Helper()

	hdr := wire.NewBlockHeader(1, &chainhash.Hash{0x11}, &chainhash.Hash{0x22}, 0x1d00ffff, 0x9988)
	hdr.Timestamp = time.Unix(1700000000, 0)

	blk := wire.NewMsgBlock(hdr)

	for i := 0; i < txCount; i++ {
		require.NoError(t, blk.AddTransaction(testMsgTx(uint32(i)))) //nolint:gosec // small test loop counter
	}

	return blk
}

// txRegion returns the payload bytes that follow the 80 byte block header and
// the transaction count varint: exactly what TxReader must deliver.
func txRegion(t *testing.T, blk *wire.MsgBlock) []byte {
	t.Helper()

	var buf bytes.Buffer

	for _, tx := range blk.Transactions {
		require.NoError(t, tx.Serialize(&buf))
	}

	return buf.Bytes()
}

func blockPayload(t *testing.T, blk *wire.MsgBlock) []byte {
	t.Helper()

	var buf bytes.Buffer

	require.NoError(t, blk.BsvEncode(&buf, wire.ProtocolVersion, wire.BaseEncoding))

	return buf.Bytes()
}

func payloadChecksum(payload []byte) [4]byte {
	var c [4]byte

	copy(c[:], chainhash.DoubleHashB(payload)[0:4])

	return c
}

// frameHeader builds a 24 byte message header from parts, so a test can
// declare a length or a checksum the payload does not honour. It returns
// bytes rather than writing, because a peer that rejects the frame hangs up
// mid-write and the writing goroutine must not call require.
func frameHeader(magic wire.BitcoinNet, cmd string, declaredLen uint32, checksum [4]byte) []byte {
	hdr := make([]byte, wire.MessageHeaderSize)

	binary.LittleEndian.PutUint32(hdr[0:4], uint32(magic))
	copy(hdr[4:4+wire.CommandSize], cmd)
	binary.LittleEndian.PutUint32(hdr[16:20], declaredLen)
	copy(hdr[20:24], checksum[:])

	return hdr
}

// writeRawFrame writes a well-framed message with a caller-chosen payload.
func writeRawFrame(t *testing.T, w io.Writer, magic wire.BitcoinNet, cmd string, payload []byte) {
	t.Helper()

	hdr := frameHeader(magic, cmd, uint32(len(payload)), payloadChecksum(payload)) //nolint:gosec // test payloads are small

	_, err := w.Write(hdr)
	require.NoError(t, err)

	_, err = w.Write(payload)
	require.NoError(t, err)
}

// requireConnFailsWithoutBlock asserts the read loop rejected a block frame
// outright: no stream reached the consumer and the connection died.
func requireConnFailsWithoutBlock(t *testing.T, c *Conn) {
	t.Helper()

	select {
	case bs, open := <-c.InboundBlocks():
		require.False(t, open, "a rejected block must not be delivered, got %v", bs)
	case <-time.After(5 * time.Second):
		t.Fatal("inbound block channel did not close")
	}

	<-c.Done()
	require.Error(t, c.Err())
}

func recvBlock(t *testing.T, c *Conn) *BlockStream {
	t.Helper()

	select {
	case bs, open := <-c.InboundBlocks():
		require.True(t, open, "inbound block channel closed")
		require.NotNil(t, bs)

		return bs
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for a block stream")

		return nil
	}
}

func recvPing(t *testing.T, c *Conn, nonce uint64) {
	t.Helper()

	select {
	case msg, open := <-c.Inbound():
		require.True(t, open, "inbound channel closed")

		ping, ok := msg.(*wire.MsgPing)
		require.True(t, ok, "expected a ping, got %T", msg)
		require.Equal(t, nonce, ping.Nonce)
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for the message after the block")
	}
}

func TestBlockStreamRoundTrip(t *testing.T) {
	a, b := net.Pipe()
	cb := New(b, testConfig())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cb.Start(ctx)

	blk := testMsgBlock(t, 3)

	go func() {
		_ = wire.WriteMessage(a, blk, wire.ProtocolVersion, wire.MainNet)
	}()

	bs := recvBlock(t, cb)

	gotHdr := bs.Header()

	require.Equal(t, blk.Header.BlockHash(), gotHdr.BlockHash())
	require.Equal(t, blk.Header.Timestamp.Unix(), gotHdr.Timestamp.Unix())
	require.Equal(t, blk.Header.Bits, gotHdr.Bits)
	require.Equal(t, blk.Header.Nonce, gotHdr.Nonce)
	require.Equal(t, uint64(3), bs.TxCount())

	got, err := io.ReadAll(bs.TxReader())
	require.NoError(t, err)
	require.Equal(t, txRegion(t, blk), got)

	require.NoError(t, bs.Close())
}

func TestBlockStreamNoBlockOnInbound(t *testing.T) {
	a, b := net.Pipe()
	cb := New(b, testConfig())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cb.Start(ctx)

	blk := testMsgBlock(t, 1)

	go func() {
		_ = wire.WriteMessage(a, blk, wire.ProtocolVersion, wire.MainNet)
		_ = wire.WriteMessage(a, wire.NewMsgPing(7), wire.ProtocolVersion, wire.MainNet)
	}()

	bs := recvBlock(t, cb)

	select {
	case msg := <-cb.Inbound():
		t.Fatalf("block leaked onto Inbound() as %T", msg)
	default:
	}

	require.NoError(t, bs.Close())
	recvPing(t, cb, 7)
}

// A payload that declares more transactions than it carries must surface the
// shortfall to the consumer while leaving the connection byte aligned.
func TestBlockStreamTruncatedPayloadKeepsAlignment(t *testing.T) {
	a, b := net.Pipe()
	cb := New(b, testConfig())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cb.Start(ctx)

	blk := testMsgBlock(t, 1)
	payload := blockPayload(t, blk)

	// Byte 80 is the transaction count varint; claim three, carry one.
	require.Equal(t, byte(1), payload[80])
	payload[80] = 3

	go func() {
		writeRawFrame(t, a, wire.MainNet, wire.CmdBlock, payload)
		_ = wire.WriteMessage(a, wire.NewMsgPing(99), wire.ProtocolVersion, wire.MainNet)
	}()

	bs := recvBlock(t, cb)
	require.Equal(t, uint64(3), bs.TxCount())

	var first wire.MsgTx
	require.NoError(t, first.Bsvdecode(bs.TxReader(), wire.ProtocolVersion, wire.BaseEncoding))

	var second wire.MsgTx
	require.Error(t, second.Bsvdecode(bs.TxReader(), wire.ProtocolVersion, wire.BaseEncoding),
		"a transaction beyond the payload boundary must fail")

	require.NoError(t, bs.Close())

	recvPing(t, cb, 99)
}

func TestBlockStreamEarlyCloseKeepsAlignment(t *testing.T) {
	a, b := net.Pipe()
	cb := New(b, testConfig())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cb.Start(ctx)

	blk := testMsgBlock(t, 4)

	go func() {
		_ = wire.WriteMessage(a, blk, wire.ProtocolVersion, wire.MainNet)
		_ = wire.WriteMessage(a, wire.NewMsgPing(11), wire.ProtocolVersion, wire.MainNet)
	}()

	bs := recvBlock(t, cb)

	var first wire.MsgTx
	require.NoError(t, first.Bsvdecode(bs.TxReader(), wire.ProtocolVersion, wire.BaseEncoding))

	// Abandon the remaining three transactions.
	require.NoError(t, bs.Close())

	recvPing(t, cb, 11)
}

func TestBlockStreamCloseWithoutReadingKeepsAlignment(t *testing.T) {
	a, b := net.Pipe()
	cb := New(b, testConfig())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cb.Start(ctx)

	blk := testMsgBlock(t, 5)

	go func() {
		_ = wire.WriteMessage(a, blk, wire.ProtocolVersion, wire.MainNet)
		_ = wire.WriteMessage(a, wire.NewMsgPing(12), wire.ProtocolVersion, wire.MainNet)
	}()

	bs := recvBlock(t, cb)
	require.NoError(t, bs.Close())

	recvPing(t, cb, 12)
}

func TestBlockStreamReadAfterCloseFails(t *testing.T) {
	a, b := net.Pipe()
	cb := New(b, testConfig())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cb.Start(ctx)

	blk := testMsgBlock(t, 2)

	go func() {
		_ = wire.WriteMessage(a, blk, wire.ProtocolVersion, wire.MainNet)
	}()

	bs := recvBlock(t, cb)
	require.NoError(t, bs.Close())

	_, err := bs.TxReader().Read(make([]byte, 8))
	require.ErrorIs(t, err, ErrBlockStreamClosed)
}

// Close is reachable from any goroutine and must be idempotent.
func TestBlockStreamCloseIdempotentAndConcurrent(t *testing.T) {
	t.Parallel()

	a, b := net.Pipe()
	cb := New(b, testConfig())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cb.Start(ctx)

	blk := testMsgBlock(t, 2)

	go func() {
		_ = wire.WriteMessage(a, blk, wire.ProtocolVersion, wire.MainNet)
		_ = wire.WriteMessage(a, wire.NewMsgPing(13), wire.ProtocolVersion, wire.MainNet)
	}()

	bs := recvBlock(t, cb)

	var wg sync.WaitGroup

	errs := make([]error, 4)

	for i := 0; i < 4; i++ {
		wg.Add(1)

		go func(i int) {
			defer wg.Done()

			errs[i] = bs.Close()
		}(i)
	}

	wg.Wait()

	for _, err := range errs {
		require.NoError(t, err)
	}

	recvPing(t, cb, 13)
}

// The read loop blocks until the consumer closes the stream: a slow consumer
// delays every later message on the connection.
func TestBlockStreamBlocksReadLoop(t *testing.T) {
	a, b := net.Pipe()
	cb := New(b, testConfig())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cb.Start(ctx)

	blk := testMsgBlock(t, 1)

	go func() {
		_ = wire.WriteMessage(a, blk, wire.ProtocolVersion, wire.MainNet)
		_ = wire.WriteMessage(a, wire.NewMsgPing(21), wire.ProtocolVersion, wire.MainNet)
	}()

	bs := recvBlock(t, cb)

	select {
	case msg := <-cb.Inbound():
		t.Fatalf("read loop advanced past an open block stream: %T", msg)
	case <-time.After(250 * time.Millisecond):
	}

	require.NoError(t, bs.Close())

	recvPing(t, cb, 21)
}

// A consumer that never closes the stream must not wedge Conn shutdown.
func TestBlockStreamAbandonedReleasedByConnClose(t *testing.T) {
	a, b := net.Pipe()
	cb := New(b, testConfig())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cb.Start(ctx)

	blk := testMsgBlock(t, 2)

	go func() {
		_ = wire.WriteMessage(a, blk, wire.ProtocolVersion, wire.MainNet)
	}()

	_ = recvBlock(t, cb)

	require.NoError(t, cb.Close())

	select {
	case <-cb.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("conn did not shut down with an abandoned block stream")
	}

	select {
	case _, open := <-cb.InboundBlocks():
		require.False(t, open)
	case <-time.After(5 * time.Second):
		t.Fatal("inbound block channel did not close")
	}
}

// The peer declares more payload than it sends and then hangs up: the drain
// cannot reach the boundary, so Close reports the shortfall.
func TestBlockStreamShortStreamSurfacesError(t *testing.T) {
	a, b := net.Pipe()
	cb := New(b, testConfig())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cb.Start(ctx)

	blk := testMsgBlock(t, 2)
	payload := blockPayload(t, blk)

	go func() {
		hdr := frameHeader(wire.MainNet, wire.CmdBlock, uint32(len(payload)+512), payloadChecksum(payload)) //nolint:gosec // test payload is small

		_, _ = a.Write(hdr)
		_, _ = a.Write(payload)
		_ = a.Close()
	}()

	bs := recvBlock(t, cb)

	_, _ = io.Copy(io.Discard, bs.TxReader())

	first := bs.Close()
	require.Error(t, first, "a short stream must surface as an error")

	// Close is idempotent on the error path too: the read loop calls it again
	// and must see the same failure.
	require.Equal(t, first, bs.Close())

	select {
	case <-cb.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("conn did not shut down after a short block stream")
	}

	require.Error(t, cb.Err())
}

func TestBlockStreamMalformedHeaderFailsConn(t *testing.T) {
	a, b := net.Pipe()
	cb := New(b, testConfig())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cb.Start(ctx)

	go func() {
		// 40 bytes cannot hold an 80 byte block header.
		payload := make([]byte, 40)

		_, _ = a.Write(frameHeader(wire.MainNet, wire.CmdBlock, uint32(len(payload)), payloadChecksum(payload))) //nolint:gosec // fixed test size
		_, _ = a.Write(payload)
	}()

	requireConnFailsWithoutBlock(t, cb)
}

// The buffered path rejects a transaction count above
// MaxBlockPayload/minTxPayload. The streaming path must not admit a count the
// buffered path would have refused, because consumers size their ingest from
// TxCount.
func TestBlockStreamAbsurdTxCountRejected(t *testing.T) {
	a, b := net.Pipe()
	cb := New(b, testConfig())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cb.Start(ctx)

	const declaredLen = 1_000_000

	require.Less(t, uint64(declaredLen), wire.MaxBlockPayload(), "the length guard must not be what rejects this frame")

	blk := testMsgBlock(t, 0)

	var prefix bytes.Buffer

	require.NoError(t, blk.Header.BsvEncode(&prefix, wire.ProtocolVersion, wire.BaseEncoding))
	require.NoError(t, wire.WriteVarInt(&prefix, wire.ProtocolVersion, 500_000))

	// 500,000 transactions cannot fit in the ~999,911 payload bytes that
	// follow: at minTxPayloadBytes each they need at least 5,000,000.
	require.Less(t, uint64(500_000), uint64(declaredLen-prefix.Len()), "the count must be under the remaining byte count, so only the tx bound can reject it")

	go func() {
		_, _ = a.Write(frameHeader(wire.MainNet, wire.CmdBlock, declaredLen, payloadChecksum(prefix.Bytes())))
		_, _ = a.Write(prefix.Bytes())
	}()

	requireConnFailsWithoutBlock(t, cb)
	require.Contains(t, cb.Err().Error(), "declares 500000 transactions")
}

func TestBlockStreamOversizedPayloadRejected(t *testing.T) {
	a, b := net.Pipe()
	cb := New(b, testConfig())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cb.Start(ctx)

	oversized := wire.MaxBlockPayload() + 1
	require.LessOrEqual(t, oversized, uint64(^uint32(0)), "the test needs the oversized length to fit the header field")

	go func() {
		_, _ = a.Write(frameHeader(wire.MainNet, wire.CmdBlock, uint32(oversized), [4]byte{})) //nolint:gosec // bounded by the assertion above
	}()

	requireConnFailsWithoutBlock(t, cb)
	require.Contains(t, cb.Err().Error(), "exceeds")
}

func TestBlockStreamWrongNetworkRejected(t *testing.T) {
	a, b := net.Pipe()
	cb := New(b, testConfig())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cb.Start(ctx)

	payload := blockPayload(t, testMsgBlock(t, 1))

	go func() {
		_, _ = a.Write(frameHeader(wire.TestNet, wire.CmdBlock, uint32(len(payload)), payloadChecksum(payload))) //nolint:gosec // test payload is small
		_, _ = a.Write(payload)
	}()

	requireConnFailsWithoutBlock(t, cb)
	require.Contains(t, cb.Err().Error(), "other network")
}

// Replaying the header into wire.ReadMessageWithEncodingN must keep the
// checksum verification the buffered path performs. A corrupt non-block frame
// has to fail the connection.
func TestNonBlockCorruptChecksumFailsConn(t *testing.T) {
	a, b := net.Pipe()
	cb := New(b, testConfig())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cb.Start(ctx)

	var payload bytes.Buffer

	require.NoError(t, wire.NewMsgPing(77).BsvEncode(&payload, wire.ProtocolVersion, wire.BaseEncoding))

	bad := payloadChecksum(payload.Bytes())
	bad[0] ^= 0xff

	go func() {
		_, _ = a.Write(frameHeader(wire.MainNet, wire.CmdPing, uint32(payload.Len()), bad)) //nolint:gosec // test payload is small
		_, _ = a.Write(payload.Bytes())
	}()

	select {
	case msg, open := <-cb.Inbound():
		require.False(t, open, "a frame with a bad checksum must not be delivered, got %v", msg)
	case <-time.After(5 * time.Second):
		t.Fatal("inbound channel did not close")
	}

	<-cb.Done()
	require.ErrorContains(t, cb.Err(), "checksum")
}

func TestBlockStreamAccountsFullPayloadBytes(t *testing.T) {
	a, b := net.Pipe()
	cb := New(b, testConfig())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cb.Start(ctx)

	blk := testMsgBlock(t, 3)
	payload := blockPayload(t, blk)

	go func() {
		_ = wire.WriteMessage(a, blk, wire.ProtocolVersion, wire.MainNet)
		_ = wire.WriteMessage(a, wire.NewMsgPing(31), wire.ProtocolVersion, wire.MainNet)
	}()

	bs := recvBlock(t, cb)
	require.NoError(t, bs.Close())

	recvPing(t, cb, 31)

	// Block frame plus the 32 byte ping frame, whatever the consumer read.
	want := uint64(wire.MessageHeaderSize+len(payload)) + uint64(wire.MessageHeaderSize+8)
	require.Equal(t, want, cb.BytesReceived())
}
