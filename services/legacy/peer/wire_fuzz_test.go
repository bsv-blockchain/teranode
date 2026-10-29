package peer

import (
	"bytes"
	"encoding/binary"
	"io"
	"net"
	"runtime"
	"strings"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/stretchr/testify/require"
)

// maxWireBlockPayloadForFuzz mirrors the maxWireBlockPayload constant that
// services/legacy/Server.go hands to wire.SetLimits during startup. It is
// unexported in package legacy, so it is repeated here — as
// wire_streaming_test.go already does — so that the fuzz targets read frames
// under the same limits production runs under.
const maxWireBlockPayloadForFuzz = 4_000_000_000

// maxFuzzPayload caps the declared payload length the fuzz targets will build
// a frame around.
//
// This cap is load-bearing, not tidiness. ReadMessageWithEncodingN bounds a
// declared length only by the per-message-type MaxPayloadLength, which for
// `tx` and `block` is the excessive block size — 4 GB. It then does
// `make([]byte, length)` before reading. A fuzzer allowed to drive the length
// field directly would therefore OOM the runner rather than find a bug. The
// hostile-length case is a fixed-shape question, not a search problem, so it
// belongs in TestReadWireMessage_HostileDeclaredLengthIsBounded below, where
// it can be asserted precisely.
const maxFuzzPayload = 16 * 1024

// framedCommands are the commands the framing fuzzer selects from. The list is
// deliberately limited to messages a peer can send us unsolicited or in
// response to our own requests, which is the surface an attacker controls.
var framedCommands = []string{
	wire.CmdBlock, wire.CmdTx, wire.CmdInv, wire.CmdGetData, wire.CmdNotFound,
	wire.CmdHeaders, wire.CmdGetHeaders, wire.CmdGetBlocks, wire.CmdAddr,
	wire.CmdPing, wire.CmdPong, wire.CmdReject, wire.CmdProtoconf,
	wire.CmdFeeFilter, wire.CmdMerkleBlock, wire.CmdVerAck, wire.CmdSendHeaders,
	wire.CmdAuthch, wire.CmdAuthresp, wire.CmdExtMsg,
}

// buildFrame wraps payload in a well-formed 24-byte bitcoin wire header:
// magic (4, LE) | command (12, NUL-padded) | length (4, LE) | checksum (4).
//
// The framing has to be correct or the fuzzer is useless. Random bytes fail
// the magic check in the first four bytes and never reach a decoder, so a
// target that fuzzed a raw stream would spend its entire budget confirming
// that garbage is not mainnet. Building an honest frame around the fuzzed
// bytes puts the mutation budget where the parsing is.
func buildFrame(command string, payload []byte) []byte {
	frame := make([]byte, wire.MessageHeaderSize, wire.MessageHeaderSize+len(payload))

	binary.LittleEndian.PutUint32(frame[0:4], uint32(wire.MainNet))
	copy(frame[4:4+wire.CommandSize], command)
	binary.LittleEndian.PutUint32(frame[16:20], uint32(len(payload)))
	copy(frame[20:24], chainhash.DoubleHashB(payload)[0:4])

	return append(frame, payload...)
}

// FuzzReadWireMessage feeds arbitrary payloads through the framed read path a
// legacy peer connection actually uses — wire.ReadMessageWithEncodingN, under
// the limits and the external block handler that services/legacy installs at
// startup. Peer bytes are wholly attacker-controlled, so a panic or an
// unbounded allocation anywhere along this path is a remote denial of service.
//
// Note on overlap: go-wire carries its own FuzzMessageDecode, which calls each
// message type's Bsvdecode directly. What that cannot cover, and this does, is
// the framing layer as Teranode configures it — header parsing, the
// per-command payload bounds, checksum verification, and the dispatch into
// Teranode's own streaming block handler, which replaces the buffered path for
// `block` and is not upstream code at all.
func FuzzReadWireMessage(f *testing.F) {
	wire.SetLimits(maxWireBlockPayloadForFuzz)
	RegisterStreamingBlockHandler()

	// A varint claiming 0xFFFFFFFFFFFFFFFF items with nothing behind it —
	// the shape that finds unbounded `make([]T, count)` in a decoder.
	hugeCount := []byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}

	for i := range framedCommands {
		f.Add(uint8(i), []byte{})
		f.Add(uint8(i), hugeCount)
		f.Add(uint8(i), make([]byte, 80)) // a bare block-header's worth of zeros
	}

	// A real, well-formed block payload, so the fuzzer starts from something
	// that decodes rather than only from rejects.
	var valid bytes.Buffer
	_, err := wire.WriteMessageN(&valid, makeTestBlock(f, 2, 32), wire.ProtocolVersion, wire.MainNet)
	require.NoError(f, err)
	f.Add(uint8(0), valid.Bytes()[wire.MessageHeaderSize:])

	f.Fuzz(func(_ *testing.T, cmdIdx uint8, payload []byte) {
		if len(payload) > maxFuzzPayload {
			payload = payload[:maxFuzzPayload]
		}

		command := framedCommands[int(cmdIdx)%len(framedCommands)]
		frame := buildFrame(command, payload)

		// Must not panic, hang or allocate without bound. An error return on
		// garbage is the expected outcome.
		_, _, _, _ = wire.ReadMessageWithEncodingN(bytes.NewReader(frame),
			wire.ProtocolVersion, wire.MainNet, wire.BaseEncoding)
	})
}

// FuzzStreamingBlockFraming is the property target for Teranode's own
// streaming block handler: after it has handled a `block` frame whose declared
// length is honest, the stream must be positioned exactly on the next header
// boundary — whatever the payload contained.
//
// That is the contract wire_streaming.go documents and relies on. The handler
// caps Bsvdecode with an io.LimitedReader and then drains whatever the decoder
// did not consume, precisely so that a malformed block cannot desync every
// subsequent message on the connection. A desync is worse than a decode
// failure: the peer stays connected and every later read is parsed at the
// wrong offset, so the failure surfaces far from its cause.
//
// The check is a valid `ping` placed immediately behind the fuzzed block. If
// the block frame leaves the stream misaligned by even one byte, the ping
// fails to decode and the property fails.
func FuzzStreamingBlockFraming(f *testing.F) {
	wire.SetLimits(maxWireBlockPayloadForFuzz)
	RegisterStreamingBlockHandler()

	const pingNonce = 0x0123456789abcdef

	f.Add([]byte{})
	f.Add(make([]byte, 80))                                             // header, no tx count
	f.Add([]byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}) // hostile varint

	var valid bytes.Buffer
	_, err := wire.WriteMessageN(&valid, makeTestBlock(f, 3, 48), wire.ProtocolVersion, wire.MainNet)
	require.NoError(f, err)
	f.Add(valid.Bytes()[wire.MessageHeaderSize:])

	f.Fuzz(func(t *testing.T, payload []byte) {
		if len(payload) > maxFuzzPayload {
			payload = payload[:maxFuzzPayload]
		}

		var stream bytes.Buffer
		// The declared length is honest — len(payload) bytes really do
		// follow. A frame that lies about its length is unrecoverable for
		// any parser and is not what this property is about.
		stream.Write(buildFrame(wire.CmdBlock, payload))

		_, err := wire.WriteMessageN(&stream, wire.NewMsgPing(pingNonce),
			wire.ProtocolVersion, wire.MainNet)
		require.NoError(t, err)

		// The block read is allowed to fail — garbage in, error out.
		_, _, _, _ = wire.ReadMessageWithEncodingN(&stream,
			wire.ProtocolVersion, wire.MainNet, wire.BaseEncoding)

		// The ping read is not allowed to fail. This is the property.
		_, msg, _, err := wire.ReadMessageWithEncodingN(&stream,
			wire.ProtocolVersion, wire.MainNet, wire.BaseEncoding)
		require.NoErrorf(t, err,
			"block frame with a %d-byte payload desynced the stream: the following ping did not decode",
			len(payload))

		ping, ok := msg.(*wire.MsgPing)
		require.Truef(t, ok, "expected *wire.MsgPing after the block frame, got %T", msg)
		require.Equal(t, uint64(pingNonce), ping.Nonce,
			"ping decoded at the wrong offset — the block frame consumed the wrong number of bytes")
	})
}

// TestReadWireMessage_HostileDeclaredLengthIsBounded pins the case the fuzzer
// is deliberately not allowed to explore (see maxFuzzPayload): a header that
// claims a huge payload with nothing behind it must be rejected without
// allocating proportionally to that untrusted field.
//
// The claim sits just under each type's own MaxPayloadLength, which for tx and
// block is the 4 GB excessive block size, so go-wire's per-type bound does not
// refuse it. go-wire's buffered ReadMessageWithEncodingN allocates the whole
// declared payload before reading, so the handshake read must refuse the length
// first (ReadHandshakeMessage), and the post-handshake streaming read must never
// buffer it. A regression here is a one-packet OOM of the legacy service from
// any peer that can open a connection.
func TestReadWireMessage_HostileDeclaredLengthIsBounded(t *testing.T) {
	wire.SetLimits(maxWireBlockPayloadForFuzz)
	RegisterStreamingBlockHandler()

	const maxAlloc = uint64(8 << 20) // 8 MB — far above a bounded reject, far below the claim

	readers := []struct {
		name string
		read func(r io.Reader) error
	}{
		{name: "handshake", read: func(r io.Reader) error {
			_, _, _, err := ReadHandshakeMessage(r, nil, wire.ProtocolVersion, wire.MainNet, wire.BaseEncoding)
			return err
		}},
		{name: "streaming", read: func(r io.Reader) error {
			_, _, err := wire.ReadMessageStreamingN(r, wire.ProtocolVersion, wire.MainNet, wire.BaseEncoding)
			return err
		}},
	}

	for _, tc := range []struct {
		command  string
		declared uint32
	}{
		{wire.CmdTx, maxWireBlockPayloadForFuzz - 1000},
		{wire.CmdBlock, maxWireBlockPayloadForFuzz - 1000},
		{wire.CmdReject, 0xF0000000},
		{wire.CmdInv, 0xF0000000},
		{wire.CmdHeaders, 0xF0000000},
	} {
		// A well-formed header claiming gigabytes, followed by nothing at all.
		frame := buildFrame(tc.command, nil)
		binary.LittleEndian.PutUint32(frame[16:20], tc.declared)

		for _, rd := range readers {
			t.Run(tc.command+"/"+rd.name, func(t *testing.T) {
				alloc := allocDeltaBytes(func() {
					require.Errorf(t, rd.read(bytes.NewReader(frame)), "a %s header claiming %d bytes with no payload must error", tc.command, tc.declared)
				})

				require.Lessf(t, alloc, maxAlloc,
					"reading a %d-byte %s frame allocated %d bytes — the payload buffer is sized from the untrusted length field",
					len(frame), tc.command, alloc)
			})
		}
	}
}

// TestReadHandshakeMessage_ReadsEveryHandshakeMessage: the length bound refuses
// only what no handshake message can be, so each message a connection may open
// with, and the streamack answering a createstream, still reads in full.
func TestReadHandshakeMessage_ReadsEveryHandshakeMessage(t *testing.T) {
	me := wire.NewNetAddressIPPort(net.ParseIP("10.0.0.1"), 8333, 0)
	you := wire.NewNetAddressIPPort(net.ParseIP("10.0.0.2"), 8333, 0)

	version := wire.NewMsgVersion(me, you, 42, 0)
	version.UserAgent = "/" + strings.Repeat("u", wire.MaxUserAgentLen-2) + "/"

	for _, msg := range []wire.Message{
		version,
		wire.NewMsgCreateStream(bytes.Repeat([]byte{1}, 16), wire.StreamTypeData1, wire.BlockPriorityStreamPolicy),
		wire.NewMsgStreamAck(bytes.Repeat([]byte{1}, 16), wire.StreamTypeData1),
	} {
		t.Run(msg.Command(), func(t *testing.T) {
			var frame bytes.Buffer
			_, err := wire.WriteMessageN(&frame, msg, wire.ProtocolVersion, wire.MainNet)
			require.NoError(t, err)

			n, got, payload, err := ReadHandshakeMessage(bytes.NewReader(frame.Bytes()), nil, wire.ProtocolVersion, wire.MainNet, wire.BaseEncoding)
			require.NoError(t, err)
			require.Equal(t, frame.Len(), n, "the byte count covers the header and the payload")
			require.Equal(t, msg.Command(), got.Command())
			require.Equal(t, frame.Bytes()[wire.MessageHeaderSize:], payload)
		})
	}
}

// TestReadHandshakeMessage_RefusesALengthNoHandshakeMessageCanHave pins the
// refusal itself: it is the peer's protocol violation, raised before a byte of
// the payload is read.
func TestReadHandshakeMessage_RefusesALengthNoHandshakeMessageCanHave(t *testing.T) {
	frame := buildFrame(wire.CmdVersion, nil)
	binary.LittleEndian.PutUint32(frame[16:20], uint32(maxHandshakePayload(wire.ProtocolVersion)+1)) //nolint:gosec // a few hundred bytes

	n, msg, _, err := ReadHandshakeMessage(bytes.NewReader(frame), nil, wire.ProtocolVersion, wire.MainNet, wire.BaseEncoding)
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.ErrNetworkInvalidResponse), "the peer's violation, not this node's fault: %v", err)
	require.Nil(t, msg)
	require.Equal(t, wire.MessageHeaderSize, n, "only the header was read")
}

// allocDeltaBytes runs fn and returns the bytes allocated during it.
// TotalAlloc is monotonic so the delta is GC-stable, and the call is
// single-goroutine so the reading is not racy. Mirrors the allocDelta helper
// used by the block parser's allocation-bound test in package model.
func allocDeltaBytes(fn func()) uint64 {
	var before, after runtime.MemStats

	runtime.ReadMemStats(&before)
	fn()
	runtime.ReadMemStats(&after)

	return after.TotalAlloc - before.TotalAlloc
}

// TestReadWireMessage_HostileCountInSmallFrameIsBounded pins the other half of
// the hostile-input question: a frame whose declared length is small and
// honest, but whose payload carries a CompactSize count or byte length far
// larger than the payload could ever back. A decoder that sizes an allocation
// from that field before reading what it describes turns a frame of a few dozen
// bytes into a multi-gigabyte allocation, under the 4 GB limits this service
// installs, from any connected peer.
//
// Every case runs through both entry points a legacy peer uses: the buffered
// ReadMessageWithEncodingN of the handshake and the streaming
// ReadMessageStreamingN of the post-handshake read loop. The block cases go
// through Teranode's own streaming block handler with no sink installed, which
// is its Bsvdecode fallback. The count cases are bounded since go-wire v1.4.0,
// the byte-length cases (scripts, the reject string) since v1.5.0. The
// corrupted-checksum case pins, on the streaming path, that a decode failing
// before the checksum is reached is still cheap; the buffered path checks the
// checksum before decoding.
func TestReadWireMessage_HostileCountInSmallFrameIsBounded(t *testing.T) {
	wire.SetLimits(maxWireBlockPayloadForFuzz)
	RegisterStreamingBlockHandler()

	const maxAlloc = uint64(8 << 20) // 8 MB — far above a bounded reject, far below the claims

	varInt := func(b *bytes.Buffer, v uint64) {
		require.NoError(t, wire.WriteVarInt(b, wire.ProtocolVersion, v))
	}

	// txWithOneInput writes a tx version and one input with an empty script, i.e.
	// everything before the output count.
	txWithOneInput := func(b *bytes.Buffer) {
		b.Write([]byte{1, 0, 0, 0}) // version
		varInt(b, 1)                // input count
		b.Write(make([]byte, 36))   // previous outpoint
		varInt(b, 0)                // signature script length
		b.Write(make([]byte, 4))    // sequence
	}

	cases := []struct {
		name     string
		command  string
		payload  func(b *bytes.Buffer)
		badCksum bool
	}{
		{name: "tx input count", command: wire.CmdTx, payload: func(b *bytes.Buffer) {
			b.Write([]byte{1, 0, 0, 0})
			varInt(b, 200_000_000)
		}},
		{name: "tx input count, corrupted checksum", command: wire.CmdTx, badCksum: true, payload: func(b *bytes.Buffer) {
			b.Write([]byte{1, 0, 0, 0})
			varInt(b, 200_000_000)
		}},
		{name: "tx signature script length", command: wire.CmdTx, payload: func(b *bytes.Buffer) {
			b.Write([]byte{1, 0, 0, 0})
			varInt(b, 1)
			b.Write(make([]byte, 36))
			varInt(b, 200_000_000)
		}},
		{name: "tx output script length", command: wire.CmdTx, payload: func(b *bytes.Buffer) {
			txWithOneInput(b)
			varInt(b, 1)             // output count
			b.Write(make([]byte, 8)) // value
			varInt(b, 1_000_000_000)
		}},
		{name: "block transaction count", command: wire.CmdBlock, payload: func(b *bytes.Buffer) {
			b.Write(make([]byte, 80))
			varInt(b, 399_000_000)
		}},
		{name: "block transaction script length", command: wire.CmdBlock, payload: func(b *bytes.Buffer) {
			b.Write(make([]byte, 80))
			varInt(b, 1)
			b.Write([]byte{1, 0, 0, 0})
			varInt(b, 1)
			b.Write(make([]byte, 36))
			varInt(b, 200_000_000)
		}},
		{name: "reject command string length", command: wire.CmdReject, payload: func(b *bytes.Buffer) {
			varInt(b, 1_000_000_000)
		}},
	}

	readers := []struct {
		name string
		read func(r io.Reader) error
	}{
		{name: "buffered", read: func(r io.Reader) error {
			_, _, _, err := wire.ReadMessageWithEncodingN(r, wire.ProtocolVersion, wire.MainNet, wire.BaseEncoding)
			return err
		}},
		{name: "streaming", read: func(r io.Reader) error {
			_, _, err := wire.ReadMessageStreamingN(r, wire.ProtocolVersion, wire.MainNet, wire.BaseEncoding)
			return err
		}},
	}

	for _, tc := range cases {
		var payload bytes.Buffer
		tc.payload(&payload)

		frame := buildFrame(tc.command, payload.Bytes())
		if tc.badCksum {
			frame[20] ^= 0xff
		}

		for _, rd := range readers {
			t.Run(tc.name+"/"+rd.name, func(t *testing.T) {
				alloc := allocDeltaBytes(func() {
					require.Errorf(t, rd.read(bytes.NewReader(frame)),
						"a %d-byte %s frame whose payload cannot back its declared count must error", len(frame), tc.command)
				})

				require.Lessf(t, alloc, maxAlloc,
					"reading a %d-byte %s frame allocated %d bytes — a decoder sized an allocation from an untrusted count",
					len(frame), tc.command, alloc)
			})
		}
	}
}
