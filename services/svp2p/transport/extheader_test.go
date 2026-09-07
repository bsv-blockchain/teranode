package transport

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

// protocol.cpp:220-237: extended header = basic header with command "extmsg",
// length 0xffffffff, zero checksum, then command(12) + payload length(8).
func TestReadWireHeader_ParsesExtendedBlockHeader(t *testing.T) {
	want := uint64(math.MaxUint32) + 1
	hdr := extBlockFrameHeader(wire.MainNet, want)
	require.Len(t, hdr, extHeaderSize)

	n, h, err := readWireHeader(bytes.NewReader(hdr))
	require.NoError(t, err)
	require.Equal(t, extHeaderSize, n)
	require.True(t, h.extended)
	require.Equal(t, wire.CmdBlock, h.command)
	require.Equal(t, want, h.length)
	require.Equal(t, wire.MainNet, h.magic)
	require.Equal(t, [4]byte{}, h.checksum)
}

func TestReadWireHeader_BasicHeaderUnchanged(t *testing.T) {
	hdr := blockFrameHeader(wire.MainNet, 1000, [4]byte{1, 2, 3, 4})

	n, h, err := readWireHeader(bytes.NewReader(hdr))
	require.NoError(t, err)
	require.Equal(t, wire.MessageHeaderSize, n)
	require.False(t, h.extended)
	require.Equal(t, uint64(1000), h.length)
	require.Equal(t, [4]byte{1, 2, 3, 4}, h.checksum)
}

// An "extmsg" command whose length is not 0xffffffff is not an extended
// header; it is a malformed basic message and is read as such.
func TestReadWireHeader_ExtmsgCommandWithoutMarkerIsBasic(t *testing.T) {
	raw := make([]byte, wire.MessageHeaderSize)
	binary.LittleEndian.PutUint32(raw[0:4], uint32(wire.MainNet))
	copy(raw[4:16], wire.CmdExtMsg)
	binary.LittleEndian.PutUint32(raw[16:20], 7)

	_, h, err := readWireHeader(bytes.NewReader(raw))
	require.NoError(t, err)
	require.False(t, h.extended)
	require.Equal(t, wire.CmdExtMsg, h.command)
}

func TestReadWireHeader_TruncatedExtensionIsAnError(t *testing.T) {
	hdr := extBlockFrameHeader(wire.MainNet, uint64(math.MaxUint32)+1)

	_, _, err := readWireHeader(bytes.NewReader(hdr[:30]))
	require.ErrorIs(t, err, io.ErrUnexpectedEOF)
}

// An extended block streams to the consumer with its 64-bit length.
func TestReadLoop_ExtendedBlockIsStreamed(t *testing.T) {
	a, b := net.Pipe()
	defer a.Close()

	c := New(b, Config{Net: wire.MainNet, ProtocolVersion: 70016, SendBudgetBytes: 1 << 20, RecvQueueLen: 4, WriteTimeout: time.Second, MaxBlockPayload: 8 << 30})
	c.Start(context.Background())
	defer c.Close()

	length := uint64(math.MaxUint32) + 1
	go func() {
		_, _ = a.Write(extBlockFrameHeader(wire.MainNet, length))
		_, _ = io.CopyN(a, zeroReader{}, 200) // only the first bytes; the test reads the declared length, not the body
		_ = a.Close()                         // let bs.Close()'s drain see EOF instead of blocking on bytes never sent
	}()

	select {
	case bs := <-c.InboundBlocks():
		require.Equal(t, length, bs.Length())
		_ = bs.Close()
	case <-time.After(5 * time.Second):
		t.Fatal("no block stream delivered")
	}
}

// A peer below 70016 may not send extended messages (version.h:51).
func TestReadLoop_ExtendedFromOldPeerDisconnects(t *testing.T) {
	a, b := net.Pipe()
	defer a.Close()

	c := New(b, Config{Net: wire.MainNet, ProtocolVersion: 70015, SendBudgetBytes: 1 << 20, RecvQueueLen: 4, WriteTimeout: time.Second})
	c.Start(context.Background())

	go func() { _, _ = a.Write(extBlockFrameHeader(wire.MainNet, uint64(math.MaxUint32)+1)) }()

	<-c.Done()
	require.ErrorIs(t, c.Err(), ErrExtendedVersion)
}

// protocol.cpp:220-237 only frames a payload extended once it exceeds uint32
// max, so a non-block message is never legitimately framed this way — every
// extended non-block header is refused, whatever length it declares.
func TestReadLoop_ExtendedNonBlockDisconnects(t *testing.T) {
	cases := []struct {
		name   string
		length uint64
	}{
		{name: "over the advertised receive limit", length: uint64(math.MaxUint32) + 1},
		{name: "a small declared length", length: 100},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			a, b := net.Pipe()
			defer a.Close()

			c := New(b, Config{Net: wire.MainNet, ProtocolVersion: 70016, SendBudgetBytes: 1 << 20, RecvQueueLen: 4, WriteTimeout: time.Second})
			c.Start(context.Background())

			hdr := extBlockFrameHeader(wire.MainNet, tc.length)
			copy(hdr[24:24+wire.CommandSize], make([]byte, wire.CommandSize))
			copy(hdr[24:24+wire.CommandSize], wire.CmdInv)

			go func() { _, _ = a.Write(hdr) }()

			<-c.Done()
			require.ErrorIs(t, c.Err(), ErrExtendedNonBlock)
		})
	}
}

type zeroReader struct{}

func (zeroReader) Read(p []byte) (int, error) {
	for i := range p {
		p[i] = 0
	}

	return len(p), nil
}

// countingReader yields n bytes of a repeating pattern without allocating
// them, so the 4 GiB round trip below never materializes a payload.
type countingReader struct {
	left uint64
	pos  uint64
}

func (r *countingReader) Read(p []byte) (int, error) {
	if r.left == 0 {
		return 0, io.EOF
	}

	n := uint64(len(p))
	if n > r.left {
		n = r.left
	}

	for i := uint64(0); i < n; i++ {
		p[i] = byte(r.pos + i)
	}

	r.pos += n
	r.left -= n

	return int(n), nil
}

type nopCloser struct{ io.Reader }

func (nopCloser) Close() error { return nil }

// spec §9: MaxBlockFrameBytes (basic), MaxBlockFrameBytes+1 == 0xffffffff
// (extended, the reserved marker), MaxBlockFrameBytes+2 == 4 GiB+1 (extended,
// genuinely beyond a uint32). Bodies are generated, never materialized; the
// receiver drains and counts.
func TestSendBlock_RoundTripsAcrossTheExtendedBoundary(t *testing.T) {
	for _, length := range []uint64{MaxBlockFrameBytes, MaxBlockFrameBytes + 1, MaxBlockFrameBytes + 2} {
		t.Run(fmt.Sprintf("%d", length), func(t *testing.T) {
			// The first two lengths each stream about 4 GiB through a loopback
			// socket, which is the whole cost of this test. They confirm where
			// the basic path ends; the third is the extended path itself and
			// always runs.
			if testing.Short() && length <= MaxBlockFrameBytes+1 {
				t.Skip("4 GiB basic-path round trip, skipped under -short")
			}

			ln, err := net.Listen("tcp", "127.0.0.1:0")
			require.NoError(t, err)
			defer ln.Close()

			go func() {
				nc, err := net.Dial("tcp", ln.Addr().String())
				require.NoError(t, err)

				c := New(nc, Config{Net: wire.MainNet, ProtocolVersion: ExtendedPayloadVersion, SendBudgetBytes: 1 << 20, RecvQueueLen: 4, WriteTimeout: time.Minute})
				c.Start(context.Background())

				err = c.SendBlock(context.Background(), BlockSendRequest{
					Length: length,
					Open:   func(context.Context) (io.ReadCloser, error) { return nopCloser{&countingReader{left: length}}, nil },
				})
				require.NoError(t, err)
			}()

			nc, err := ln.Accept()
			require.NoError(t, err)

			rx := New(nc, Config{Net: wire.MainNet, ProtocolVersion: ExtendedPayloadVersion, SendBudgetBytes: 1 << 20, RecvQueueLen: 4, WriteTimeout: time.Minute, MaxBlockPayload: 8 << 30})
			rx.Start(context.Background())
			defer rx.Close()

			bs := <-rx.InboundBlocks()
			require.Equal(t, length, bs.Length())
			require.Equal(t, length > MaxBlockFrameBytes, bs.Extended())

			// newBlockStream already decoded the block header and tx-count
			// varint off the socket before handing the stream back, so the
			// remaining reader delivers fewer than Length bytes; account for
			// what was already consumed to check the round trip end to end.
			preConsumed := bs.consumed()

			n, err := io.Copy(io.Discard, bs.TxReader())
			require.NoError(t, err)
			require.Equal(t, length, preConsumed+uint64(n))
		})
	}
}

// A peer below ExtendedPayloadVersion cannot receive an extended frame:
// SendBlock refuses as before.
func TestSendBlock_ExtendedRefusedForOldPeer(t *testing.T) {
	a, _ := net.Pipe()
	c := New(a, Config{Net: wire.MainNet, ProtocolVersion: ExtendedPayloadVersion - 1, SendBudgetBytes: 1 << 20, RecvQueueLen: 4, WriteTimeout: time.Second})
	c.Start(context.Background())
	defer c.Close()

	err := c.SendBlock(context.Background(), BlockSendRequest{
		Length: MaxBlockFrameBytes + 2,
		Open:   func(context.Context) (io.ReadCloser, error) { return nopCloser{&countingReader{}}, nil },
	})
	require.ErrorIs(t, err, ErrBlockTooLargeToFrame)
}

// failAfterConn is a net.Conn stand-in whose Write starts failing once it has
// accepted `after` bytes. It exists so a body write failure on the extended
// path can be forced deterministically, in-process, without racing the read
// loop's own failure detection on a real or piped socket (closing either end
// of a net.Pipe or a real TCP connection kills both directions at once, so
// the read loop can observe the failure and unblock SendBlock via c.quit
// before writeBlock's own error path is ever reached).
type failAfterConn struct {
	net.Conn
	after int
	wrote int
}

func (f *failAfterConn) Write(p []byte) (int, error) {
	if f.wrote >= f.after {
		return 0, errors.New(errors.ERR_ERROR, "svp2p: test write failure")
	}

	n := len(p)
	if f.wrote+n > f.after {
		n = f.after - f.wrote
	}

	f.wrote += n

	return n, nil
}

// TestWriteBlock_ExtendedWriteFailureWrapsCleanly is the extended-path
// analogue of TestSendBlockShortSecondPassFailsTheConnection: a body write
// that fails partway through must report a wrapped error whose message
// renders cleanly. It calls writeBlock directly, bypassing SendBlock and
// Start's read loop, so the failure it observes is exactly and only the one
// writeBlock's own error path produces.
func TestWriteBlock_ExtendedWriteFailureWrapsCleanly(t *testing.T) {
	nc := &failAfterConn{after: extHeaderSize + 5}
	c := &Conn{nc: nc, cfg: Config{Net: wire.MainNet}}

	length := MaxBlockFrameBytes + 2
	bs := &blockSend{
		ctx: context.Background(),
		req: BlockSendRequest{
			Length: length,
			Open:   func(context.Context) (io.ReadCloser, error) { return nopCloser{&countingReader{left: length}}, nil },
		},
		extended: true,
		done:     make(chan error, 1),
	}

	ok := c.writeBlock(bs)
	require.False(t, ok, "a body write that ends early must fail the connection")

	err := <-bs.done
	require.Error(t, err)
	require.NotContains(t, err.Error(), "%!", "the wrapped error must render, not an unmatched verb")
}

// recordingLogger keeps the Debugf lines a Conn writes, so a test can assert
// on them. Every other level goes to the embedded TestLogger.
type recordingLogger struct {
	ulogger.TestLogger

	mu    sync.Mutex
	lines []string
}

func (l *recordingLogger) Debugf(format string, args ...interface{}) {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.lines = append(l.lines, fmt.Sprintf(format, args...))
}

func (l *recordingLogger) contains(substr string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()

	for _, line := range l.lines {
		if strings.Contains(line, substr) {
			return true
		}
	}

	return false
}

// The live regtest run found the extended-header path completely silent, which
// left no way to tell an extended frame from a basic one in a log. Accepting
// one now writes a single debug line naming the peer and the declared length.
func TestReadLoop_ExtendedBlockIsLogged(t *testing.T) {
	logger := &recordingLogger{}

	a, b := net.Pipe()
	defer a.Close()

	c := New(b, Config{Net: wire.MainNet, ProtocolVersion: 70016, SendBudgetBytes: 1 << 20, RecvQueueLen: 4, WriteTimeout: time.Second, MaxBlockPayload: 8 << 30, Logger: logger})
	c.Start(context.Background())

	defer c.Close()

	length := uint64(math.MaxUint32) + 1

	go func() {
		_, _ = a.Write(extBlockFrameHeader(wire.MainNet, length))
		_, _ = io.CopyN(a, zeroReader{}, 200)
		_ = a.Close()
	}()

	select {
	case bs := <-c.InboundBlocks():
		_ = bs.Close()
	case <-time.After(5 * time.Second):
		t.Fatal("no block stream delivered")
	}

	require.True(t, logger.contains("extended block frame"), "an accepted extended frame must be logged: %v", logger.lines)
	require.True(t, logger.contains(fmt.Sprintf("%d bytes", length)), "the log line must name the declared length: %v", logger.lines)
}

// A basic block header is the ordinary case and must stay silent, or every
// block on a busy node writes a line.
func TestReadLoop_BasicBlockIsNotLoggedAsExtended(t *testing.T) {
	logger := &recordingLogger{}

	a, b := net.Pipe()
	defer a.Close()

	c := New(b, Config{Net: wire.MainNet, ProtocolVersion: 70016, SendBudgetBytes: 1 << 20, RecvQueueLen: 4, WriteTimeout: time.Second, MaxBlockPayload: 8 << 30, Logger: logger})
	c.Start(context.Background())

	defer c.Close()

	const length = 200

	go func() {
		var hdr [wire.MessageHeaderSize]byte

		binary.LittleEndian.PutUint32(hdr[0:4], uint32(wire.MainNet))
		copy(hdr[4:4+wire.CommandSize], wire.CmdBlock)
		binary.LittleEndian.PutUint32(hdr[16:20], length)

		_, _ = a.Write(hdr[:])
		_, _ = io.CopyN(a, zeroReader{}, length)
		_ = a.Close()
	}()

	select {
	case bs := <-c.InboundBlocks():
		_ = bs.Close()
	case <-time.After(5 * time.Second):
		t.Fatal("no block stream delivered")
	}

	require.False(t, logger.contains("extended block frame"), "a basic frame must not be logged as extended: %v", logger.lines)
}

// TestWriteBlock_BasicWriteFailureWrapsCleanly is the basic-path analogue of
// TestWriteBlock_ExtendedWriteFailureWrapsCleanly. The two paths report the
// same misalignment with the same wrapped cause, so the same rendering rule
// applies to both.
func TestWriteBlock_BasicWriteFailureWrapsCleanly(t *testing.T) {
	nc := &failAfterConn{after: wire.MessageHeaderSize + 5}
	c := &Conn{nc: nc, cfg: Config{Net: wire.MainNet}}

	length := uint64(1 << 20)
	bs := &blockSend{
		ctx: context.Background(),
		req: BlockSendRequest{
			Length: length,
			Open:   func(context.Context) (io.ReadCloser, error) { return nopCloser{&countingReader{left: length}}, nil },
		},
		done: make(chan error, 1),
	}

	ok := c.writeBlock(bs)
	require.False(t, ok, "a body write that ends early must fail the connection")

	err := <-bs.done
	require.Error(t, err)
	require.NotContains(t, err.Error(), "%!", "the wrapped error must render, not an unmatched verb")
}
