package peer

import (
	"bytes"
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

// The block sink is installed globally and go-wire calls the handler with no
// peer, so the peer the body comes from travels on the reader. A block read
// through a peer's own read loop must reach the sink with that peer, or the
// sync manager cannot tell an owed copy from anybody else's.
func TestReadMessageStreamingTellsTheSinkWhichPeerIsSending(t *testing.T) {
	payload, hash := serialisedBlock(t, 3)

	var blk wire.MsgBlock
	require.NoError(t, blk.Deserialize(bytes.NewReader(payload)))

	RegisterStreamingBlockHandler()

	var got *Peer

	restore := installTestSink(t,
		func(_ chainhash.Hash, _ *wire.BlockHeader, r io.Reader, _ int64) (bool, error) {
			got = DeliveredBy(r)
			_, err := io.Copy(io.Discard, r)

			return false, err
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

	_, msg, err := p.readMessageStreaming(wire.BaseEncoding)
	require.NoError(t, err)

	onDisk, ok := msg.(*MsgBlockOnDisk)
	require.True(t, ok)
	require.Equal(t, hash, onDisk.Hash)
	require.Same(t, p, got, "the sink is told which peer is sending")
}

// A reader that carries no peer says so with nil, never with some other peer.
func TestDeliveredByIsNilForAPlainReader(t *testing.T) {
	require.Nil(t, DeliveredBy(bytes.NewReader(nil)))

	p := &Peer{}
	require.Same(t, p, DeliveredBy(NewDeliveryReader(bytes.NewReader(nil), p)))
}
