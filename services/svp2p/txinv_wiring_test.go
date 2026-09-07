package svp2p

import (
	"context"
	"io"
	"net/url"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/svp2p/bridge"
	"github.com/bsv-blockchain/teranode/services/svp2p/protocol"
	"github.com/bsv-blockchain/teranode/ulogger"
	inmemorykafka "github.com/bsv-blockchain/teranode/util/kafka/in_memory_kafka"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// noopBlockIngestor satisfies protocol.BlockIngestor just enough to let
// ConfigureSync build the header-sync/block-download machines this file's
// test needs (BlockDownloader.OnInv/RequestTxs) — the tx-inv round trip has
// no dependency on block ingestion itself, and this test sends no block
// inv, so Ingest is never actually called; a full bridge.Bridge (and the
// real validator/subtree-validation/block-assembly stack behind it,
// sync_integration_test.go's own weight) buys nothing here.
type noopBlockIngestor struct{}

func (noopBlockIngestor) WatchProgress(r io.ReadCloser) protocol.IngestProgress {
	return &noopProgress{r: r, at: time.Now()}
}

func (noopBlockIngestor) Ingest(context.Context, protocol.BlockIngestRequest) protocol.IngestOutcome {
	return protocol.IngestOutcome{}
}

type noopProgress struct {
	r  io.ReadCloser
	at time.Time
}

func (p *noopProgress) Read(b []byte) (int, error) { return p.r.Read(b) }
func (p *noopProgress) Close() error               { return p.r.Close() }
func (p *noopProgress) BytesRead() uint64          { return 0 }
func (p *noopProgress) LastProgress() time.Time    { return p.at }

// fakeAlwaysRunningClient satisfies just the one blockchain.ClientI method
// bridge.StartLegacyInvConsumer's poller calls (IsFSMCurrentState), so this
// test needs no real blockchain service. Embedding the interface leaves
// every other method nil — fine, since nothing else on this client is ever
// called by the code path under test.
type fakeAlwaysRunningClient struct {
	blockchain.ClientI
}

func (fakeAlwaysRunningClient) IsFSMCurrentState(context.Context, blockchain.FSMStateType) (bool, error) {
	return true, nil
}

// TestPeerManagerAndBridgeKafkaRoundTripTxInv is Task 16's own round trip,
// proved at the composition seam Server.go wires with no logic of its own:
// a real protocol.PeerManager, wired directly to bridge.StartLegacyInvProducer/
// StartLegacyInvConsumer exactly the way Server.Start composes them
// (protocol.SyncConfig.TxInvProducer <- *bridge.LegacyInvProducer,
// PeerManager.InvFromKafka <- bridge.StartLegacyInvConsumer's callback).
//
// This is deliberately NOT built on the full *svp2p.Server
// (newTestServer/startServer): OnInv's tx branch only runs once
// BlockDownloader exists, which ConfigureSync only builds when a
// BlockIngestor is injected — and the Server only injects a real one behind
// the full block-ingestion dependency set (Deps.complete(): a real
// validator, subtree/block validation and block assembly,
// sync_integration_test.go's own weight). None of that is what this round
// trip exercises; wiring a real PeerManager directly against the bridge
// Kafka producer and consumer proves the actual seam (the interface/
// callback composition) without paying for infrastructure the test never
// uses. The individual halves are already proven with real sockets/real
// Kafka in protocol/tx_inv_test.go and bridge/legacy_inv_test.go; this test
// is what neither of those alone can prove — that Server.go's own wiring of
// the two packages together actually round-trips.
func TestPeerManagerAndBridgeKafkaRoundTripTxInv(t *testing.T) {
	logger := ulogger.TestLogger{}

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.ClientName = "svp2p-txinv-wiring-test"

	const topic = "legacy-inv-composition-wiring-test"

	kafkaURL, err := url.Parse("memory://localhost:9092/" + topic)
	require.NoError(t, err)
	tSettings.Kafka.LegacyInvConfig = kafkaURL

	t.Cleanup(func() { inmemorykafka.GetSharedBroker().DropTopic(topic) })

	genesis := tSettings.ChainCfgParams.GenesisBlock.Header

	idx, err := protocol.NewHeaderIndex(&genesis)
	require.NoError(t, err)

	banList, err := protocol.NewBanList("")
	require.NoError(t, err)

	m := protocol.NewPeerManager(logger, tSettings, banList)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	producer, err := bridge.StartLegacyInvProducer(ctx, logger, tSettings)
	require.NoError(t, err)
	require.NotNil(t, producer)

	t.Cleanup(func() { require.NoError(t, producer.Stop()) })

	require.NoError(t, m.ConfigureSync(protocol.SyncConfig{
		Index:         idx,
		Ingestor:      noopBlockIngestor{},
		TxInvProducer: producer,
	}))

	require.NoError(t, m.Start(ctx, []string{"127.0.0.1:0"}))
	t.Cleanup(func() { _ = m.Stop() })

	// fakeAlwaysRunningClient reports FSM RUNNING unconditionally, keeping
	// the consumer's per-message RUNNING gate open for the whole test; the
	// gate itself is proved independently in bridge/legacy_inv_test.go.
	consumer, err := bridge.StartLegacyInvConsumer(ctx, logger, tSettings, fakeAlwaysRunningClient{}, m.InvFromKafka)
	require.NoError(t, err)
	require.NotNil(t, consumer)
	t.Cleanup(func() { require.NoError(t, consumer.Close()) })

	require.Eventually(t, func() bool {
		return inmemorykafka.GetSharedBroker().HasConsumer(topic)
	}, 5*time.Second, 10*time.Millisecond, "the legacy inv consumer never subscribed")

	netMagic := tSettings.ChainCfgParams.Net

	far := dialRelayPeer(t, m.ListenAddrs()[0], netMagic)
	far.completeHandshake(t, false)

	hash := chainhash.Hash{0xCD}
	invMsg := wire.NewMsgInv()
	require.NoError(t, invMsg.AddInvVect(wire.NewInvVect(wire.InvTypeTx, &hash)))
	far.write(t, invMsg)

	gdmsg, ok := far.readUntil(t, wire.CmdGetData).(*wire.MsgGetData)
	require.True(t, ok)
	require.Len(t, gdmsg.InvList, 1)
	require.Equal(t, hash, gdmsg.InvList[0].Hash)
	require.Equal(t, wire.InvTypeTx, gdmsg.InvList[0].Type)
}
