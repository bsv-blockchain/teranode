package peer

import (
	"encoding/binary"
	"io"
	"net"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-chaincfg"
	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// frame builds one wire message whose framing is right: the magic, the command, the length the
// payload really has and a correct checksum.
func frame(magic wire.BitcoinNet, command string, payload []byte) []byte {
	header := make([]byte, 24)
	binary.LittleEndian.PutUint32(header[0:4], uint32(magic))
	copy(header[4:16], command)
	binary.LittleEndian.PutUint32(header[16:20], uint32(len(payload)))
	copy(header[20:24], chainhash.DoubleHashB(payload)[:4])

	return append(header, payload...)
}

// readFrom runs one read of the peer's own loop (streaming or not) over bytes the remote end
// writes and then closes, and returns the error the read loop would see.
func readFrom(t *testing.T, streaming bool, written []byte) error {
	t.Helper()

	params := chaincfg.MainNetParams
	p := newPeerBase(ulogger.TestLogger{}, test.CreateBaseTestSettings(t), &Config{ChainParams: &params}, true)

	local, remote := net.Pipe()
	t.Cleanup(func() {
		_ = local.Close()
		_ = remote.Close()
	})

	p.conn = local

	go func() {
		_, _ = remote.Write(written)
		_ = remote.Close()
	}()

	var err error
	if streaming {
		_, _, err = p.readMessageStreaming(wire.BaseEncoding)
	} else {
		_, _, err = p.readMessage(wire.BaseEncoding)
	}

	return err
}

// A well-framed message that is shorter than its command needs is a malformed message, not the
// peer hanging up: the read loop answers a *wire.MessageError with a reject.
func TestAShortPayloadIsAMalformedMessageNotAHangUp(t *testing.T) {
	params := chaincfg.MainNetParams

	for name, written := range map[string][]byte{
		"reject with no payload":   frame(params.Net, wire.CmdReject, nil),
		"ping with a short nonce":  frame(params.Net, wire.CmdPing, []byte{1, 2, 3}),
		"addr with a missing list": frame(params.Net, wire.CmdAddr, nil),
		"version with no payload":  frame(params.Net, wire.CmdVersion, nil),
	} {
		for _, streaming := range []bool{true, false} {
			t.Run(name, func(t *testing.T) {
				err := readFrom(t, streaming, written)

				var messageErr *wire.MessageError
				require.ErrorAs(t, err, &messageErr, "got %v", err)
				require.NotEqual(t, io.EOF, err)
				require.NotEqual(t, io.ErrUnexpectedEOF, err)
			})
		}
	}
}

// When the connection itself ends the error is left as it was, so a socket that closes between
// messages, or inside one, is still read as the peer leaving.
func TestAConnectionThatEndsStillReadsAsThePeerLeaving(t *testing.T) {
	params := chaincfg.MainNetParams

	whole := frame(params.Net, wire.CmdPing, []byte{1, 2, 3, 4, 5, 6, 7, 8})

	for _, streaming := range []bool{true, false} {
		// closed between messages
		require.Equal(t, io.EOF, readFrom(t, streaming, nil))

		// closed inside the header
		require.ErrorIs(t, readFrom(t, streaming, whole[:10]), io.ErrUnexpectedEOF)

		// closed inside the payload
		require.ErrorIs(t, readFrom(t, streaming, whole[:28]), io.ErrUnexpectedEOF)
	}
}
