package peer

import (
	"bytes"
	"io"
	"net"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-chaincfg"
	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// A panic anywhere under the post-handshake read, in go-wire's decoders or in
// the block sink the streaming handler calls on the same goroutine, is driven
// by bytes a remote peer chose. It must come back as a read error, which the
// read loop answers with a disconnect, and not take the whole process down.
func TestReadMessageStreamingRecoversAPanicAsAnError(t *testing.T) {
	// A runtime fault panics with an error value, anything else with whatever it
	// was given; neither reaches the error.
	for _, tc := range []struct {
		name  string
		value any
		want  string
	}{
		{name: "string", value: "sink exploded", want: "sink exploded"},
		{name: "error", value: io.ErrClosedPipe, want: io.ErrClosedPipe.Error()},
	} {
		t.Run(tc.name, func(t *testing.T) {
			payload, _ := serialisedBlock(t, 3)

			var blk wire.MsgBlock
			require.NoError(t, blk.Deserialize(bytes.NewReader(payload)))

			RegisterStreamingBlockHandler()

			restore := installTestSink(t,
				func(chainhash.Hash, *wire.BlockHeader, io.Reader, int64) (bool, error) {
					panic(tc.value)
				},
				permissiveGate,
				func(chainhash.Hash, bool) error { return nil },
			)
			t.Cleanup(restore)

			params := chaincfg.MainNetParams
			p := newPeerBase(ulogger.TestLogger{}, test.CreateBaseTestSettings(t), &Config{ChainParams: &params}, true)

			local, remote := net.Pipe()
			t.Cleanup(func() {
				_ = local.Close()
				_ = remote.Close()
			})

			p.conn = local

			go func() { _, _ = wire.WriteMessageN(remote, &blk, p.ProtocolVersion(), params.Net) }()

			var (
				msg wire.Message
				err error
			)

			require.NotPanics(t, func() { _, msg, err = p.readMessageStreaming(wire.BaseEncoding) })
			require.Error(t, err)
			require.True(t, errors.Is(err, errors.ErrNetworkInvalidResponse), "a panic decoding what the peer sent is answered as the peer's: %v", err)
			require.NotContains(t, err.Error(), tc.want, "the panic text stays in this node's log; the read loop echoes the error to the peer")
			require.Nil(t, msg)
		})
	}
}
