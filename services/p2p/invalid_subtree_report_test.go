package p2p

import (
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/util/kafka"
	kafkamessage "github.com/bsv-blockchain/teranode/util/kafka/kafka_message"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

// These tests guard the attribution order of ReportInvalidSubtree: the peer
// that served the invalid bytes (explicit peer ID, then the owner of the
// DataHub URL) is charged, never the gossip announcer recorded in
// subtreePeerMap while a serving identity is known. Issue: an attacker
// relaying block/subtree announcements with its own DataHub URL and 404ing
// the fetches could previously drain the announcer's reputation at no cost.

const testSubtreeHash = "aa8ec1a5bb804bfbb64e07d044dc0b0632714b16f5b0410aa1acf115ad4a8d3d"

func requireFailures(t *testing.T, reg *blockchain.CentralizedPeerRegistry, peerID string, want int64) {
	t.Helper()

	info, ok := reg.Get(peerID)
	require.True(t, ok, "peer %s must be registered", peerID)
	require.Equal(t, want, info.InteractionFailures, "InteractionFailures for peer %s", peerID)
}

func TestReportInvalidSubtree_ChargesServingPeerNotAnnouncer(t *testing.T) {
	s, reg := newServerWithLocalRegistry(t)

	announcer := mustNewPeerID(t).String()
	attacker := mustNewPeerID(t).String()
	reg.Register(&blockchain.PeerInfo{ID: announcer, DataHubURL: "http://honest:8090"})
	reg.Register(&blockchain.PeerInfo{ID: attacker, DataHubURL: "http://attacker:8090"})

	// The honest miner announced the subtree via gossip...
	s.storePeerMapEntry(&s.subtreePeerMap, testSubtreeHash, announcer, time.Now().UTC())

	// ...but the invalid bytes were served from the attacker's DataHub, and the
	// reporter says so explicitly.
	err := s.ReportInvalidSubtree(t.Context(), testSubtreeHash, "http://attacker:8090", attacker, "peer_cannot_provide_subtree")
	require.NoError(t, err)

	requireFailures(t, reg, attacker, 1)
	requireFailures(t, reg, announcer, 0)

	_, ok := s.subtreePeerMap.Load(testSubtreeHash)
	require.False(t, ok, "subtreePeerMap entry must be cleaned up after the report")
}

func TestReportInvalidSubtree_ResolvesURLOwnerWhenNoPeerID(t *testing.T) {
	s, reg := newServerWithLocalRegistry(t)

	announcer := mustNewPeerID(t).String()
	server := mustNewPeerID(t).String()
	reg.Register(&blockchain.PeerInfo{ID: announcer, DataHubURL: "http://honest:8090"})
	reg.Register(&blockchain.PeerInfo{ID: server, DataHubURL: "http://server:8090"})

	s.storePeerMapEntry(&s.subtreePeerMap, testSubtreeHash, announcer, time.Now().UTC())

	err := s.ReportInvalidSubtree(t.Context(), testSubtreeHash, "http://server:8090", "", "malformed_transaction_data")
	require.NoError(t, err)

	requireFailures(t, reg, server, 1)
	requireFailures(t, reg, announcer, 0)
}

func TestReportInvalidSubtree_UnresolvableURLDoesNotChargeAnnouncer(t *testing.T) {
	s, reg := newServerWithLocalRegistry(t)

	announcer := mustNewPeerID(t).String()
	reg.Register(&blockchain.PeerInfo{ID: announcer, DataHubURL: "http://honest:8090"})

	s.storePeerMapEntry(&s.subtreePeerMap, testSubtreeHash, announcer, time.Now().UTC())

	// The bytes came from a URL no registered peer owns; the announcer merely
	// gossiped the hash and must not absorb the failure.
	err := s.ReportInvalidSubtree(t.Context(), testSubtreeHash, "http://unknown:8090", "", "peer_cannot_provide_subtree")
	require.NoError(t, err)

	requireFailures(t, reg, announcer, 0)

	_, ok := s.subtreePeerMap.Load(testSubtreeHash)
	require.False(t, ok, "the report must consume the subtreePeerMap entry even when nobody is charged")
}

func TestReportInvalidSubtree_ExplicitPeerIDWithoutURL(t *testing.T) {
	s, reg := newServerWithLocalRegistry(t)

	server := mustNewPeerID(t).String()
	reg.Register(&blockchain.PeerInfo{ID: server, DataHubURL: "http://server:8090"})

	err := s.ReportInvalidSubtree(t.Context(), testSubtreeHash, "", server, "malformed_transaction_data")
	require.NoError(t, err)

	requireFailures(t, reg, server, 1)
}

func TestReportInvalidSubtree_UndecodablePeerIDChargesNobody(t *testing.T) {
	s, reg := newServerWithLocalRegistry(t)

	announcer := mustNewPeerID(t).String()
	reg.Register(&blockchain.PeerInfo{ID: announcer, DataHubURL: "http://honest:8090"})

	s.storePeerMapEntry(&s.subtreePeerMap, testSubtreeHash, announcer, time.Now().UTC())

	err := s.ReportInvalidSubtree(t.Context(), testSubtreeHash, "http://attacker:8090", "not-a-valid-peer-id", "peer_cannot_provide_subtree")
	require.NoError(t, err)

	requireFailures(t, reg, announcer, 0)

	_, ok := s.subtreePeerMap.Load(testSubtreeHash)
	require.False(t, ok, "the report must consume the subtreePeerMap entry even when the peer ID cannot be decoded")
}

func TestReportInvalidSubtree_FallsBackToAnnouncerWhenNoURL(t *testing.T) {
	s, reg := newServerWithLocalRegistry(t)

	announcer := mustNewPeerID(t).String()
	reg.Register(&blockchain.PeerInfo{ID: announcer, DataHubURL: "http://honest:8090"})

	s.storePeerMapEntry(&s.subtreePeerMap, testSubtreeHash, announcer, time.Now().UTC())

	err := s.ReportInvalidSubtree(t.Context(), testSubtreeHash, "", "", "contains_invalid_transaction")
	require.NoError(t, err)

	requireFailures(t, reg, announcer, 1)
}

// TestInvalidSubtreeHandler_UsesPeerIDFromKafkaMessage exercises the Kafka
// consumer path end-to-end: the peer ID carried in
// KafkaInvalidSubtreeTopicMessage reaches ReportInvalidSubtree and wins over
// the gossip announcer in subtreePeerMap.
func TestInvalidSubtreeHandler_UsesPeerIDFromKafkaMessage(t *testing.T) {
	s, reg := newServerWithLocalRegistry(t)

	announcer := mustNewPeerID(t).String()
	attacker := mustNewPeerID(t).String()
	reg.Register(&blockchain.PeerInfo{ID: announcer, DataHubURL: "http://honest:8090"})
	reg.Register(&blockchain.PeerInfo{ID: attacker, DataHubURL: "http://attacker:8090"})

	s.storePeerMapEntry(&s.subtreePeerMap, testSubtreeHash, announcer, time.Now().UTC())

	msgBytes, err := proto.Marshal(&kafkamessage.KafkaInvalidSubtreeTopicMessage{
		SubtreeHash: testSubtreeHash,
		PeerUrl:     "http://attacker:8090",
		PeerId:      attacker,
		Reason:      "peer_cannot_provide_subtree",
	})
	require.NoError(t, err)

	// nil blockchainClient means "not syncing", so the handler proceeds.
	err = s.invalidSubtreeHandler(t.Context())(&kafka.KafkaMessage{Value: msgBytes})
	require.NoError(t, err)

	requireFailures(t, reg, attacker, 1)
	requireFailures(t, reg, announcer, 0)
}
