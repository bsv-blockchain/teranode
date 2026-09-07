package svp2p

import (
	"context"
	"encoding/binary"
	"net/url"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	inmemorykafka "github.com/bsv-blockchain/teranode/util/kafka/in_memory_kafka"
	"github.com/stretchr/testify/require"
)

// buildV1TxMetaBatch builds a single-entry v1 txmeta wire batch — the same
// format services/svp2p/bridge/txmeta_test.go's own builder produces, kept
// as a small local copy here since this file tests the WIRING (Kafka ->
// Server -> PeerManager.RelayTxs -> a real socket), not the decoder itself,
// which bridge's own tests already cover exhaustively.
func buildV1TxMetaBatch(t *testing.T, hash chainhash.Hash, data meta.Data) []byte {
	t.Helper()

	metaBytes, err := data.MetaBytes()
	require.NoError(t, err)

	buf := make([]byte, 4, 4+len(hash)+1+4+len(metaBytes))
	binary.LittleEndian.PutUint32(buf, 1)
	buf = append(buf, hash[:]...)
	buf = append(buf, 0) // WireActionADD

	lenBuf := make([]byte, 4)
	binary.LittleEndian.PutUint32(lenBuf, uint32(len(metaBytes))) //nolint:gosec // test data
	buf = append(buf, lenBuf...)
	buf = append(buf, metaBytes...)

	return buf
}

// TestServerRelaysTxMetaToPeers is the txmeta-topic counterpart of
// TestServerRelaysBlocksFinalToPeers: a message produced onto the txmeta
// topic reaches a real, connected peer as a tx inv, proving the whole chain
// from bridge/kafka.go's decode, through txrelay.go's batcher, to
// PeerManager.RelayTxs' real socket send. newTestServer's blockchain client
// (LocalClient) reports FSM RUNNING unconditionally, so the RUNNING gate is
// open for the whole test — the gate itself is covered independently by
// services/svp2p/bridge's TestStartTxMetaConsumer_RunningGateFlipsBothDirections.
func TestServerRelaysTxMetaToPeers(t *testing.T) {
	srv, _ := newTestServer(t)

	const topic = "txmeta-server-wiring-test"

	kafkaURL, err := url.Parse("memory://localhost:9092/" + topic)
	require.NoError(t, err)
	srv.settings.Kafka.TxMetaConfig = kafkaURL

	t.Cleanup(func() { inmemorykafka.GetSharedBroker().DropTopic(topic) })

	cancel := startServer(t, srv)
	defer cancel()

	defer func() { require.NoError(t, srv.Stop(context.Background())) }()

	netMagic := srv.settings.ChainCfgParams.Net

	peer := dialRelayPeer(t, srv.manager.ListenAddrs()[0], netMagic)
	peer.completeHandshake(t, false)

	hash := chainhash.Hash{0xAB}
	payload := buildV1TxMetaBatch(t, hash, meta.Data{Fee: 500, SizeInBytes: 250})

	require.Eventually(t, func() bool {
		return inmemorykafka.GetSharedBroker().HasConsumer(topic)
	}, 5*time.Second, 10*time.Millisecond, "the txmeta consumer never subscribed")

	require.NoError(t, inmemorykafka.GetSharedBroker().Produce(context.Background(), topic, nil, payload))

	invMsg, ok := peer.readUntil(t, wire.CmdInv).(*wire.MsgInv)
	require.True(t, ok)
	require.Len(t, invMsg.InvList, 1)
	require.Equal(t, wire.InvTypeTx, invMsg.InvList[0].Type)
	require.Equal(t, hash, invMsg.InvList[0].Hash)
}
