package blockchain

import (
	"context"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/services/blockchain/blockchain_api"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

func sampleLegacyPeerInfo() *LegacyPeerInfo {
	return &LegacyPeerInfo{
		Inbound:         true,
		ProtocolVersion: 70016,
		ServiceFlags:    0x25,
		PingMicros:      42000,
		TimeOffsetSecs:  -3,
		StartingHeight:  912000,
		IsSyncPeer:      true,
		TimeConnected:   time.Unix(1750000000, 0).UTC(),
	}
}

// TestRegister_StoresLegacyPeerInfo checks that a wire-protocol registration
// keeps its legacy block, and that the registry clones it rather than aliasing
// the caller's struct.
func TestRegister_StoresLegacyPeerInfo(t *testing.T) {
	r := NewCentralizedPeerRegistry(DefaultBanConfig())

	legacy := sampleLegacyPeerInfo()
	r.Register(&PeerInfo{
		ID:               "legacy:203.0.113.7:8333",
		TransportType:    blockchain_api.TransportType_TRANSPORT_WIRE_PROTOCOL,
		TransportTypeSet: true,
		NetworkAddress:   "203.0.113.7:8333",
		ClientName:       "/Bitcoin SV:1.0.16/",
		Height:           912345,
		Legacy:           legacy,
	})

	// Mutating the caller's struct must not reach the stored entry.
	legacy.ProtocolVersion = 1

	got, ok := r.Get("legacy:203.0.113.7:8333")
	require.True(t, ok)
	require.NotNil(t, got.Legacy)
	require.Equal(t, uint32(70016), got.Legacy.ProtocolVersion)
	require.True(t, got.Legacy.Inbound)
	require.True(t, got.Legacy.IsSyncPeer)
	require.Equal(t, int32(912000), got.Legacy.StartingHeight)
	require.Equal(t, int64(-3), got.Legacy.TimeOffsetSecs)
	require.Equal(t, time.Unix(1750000000, 0).UTC(), got.Legacy.TimeConnected)
}

// TestRegister_LegacyFalseBooleansSurvive is a regression test for the
// merge-semantics trap: Register only copies fields carrying "meaningful new
// data", and Inbound=false / IsSyncPeer=false are both meaningful. The nested
// pointer must be replaced wholesale, not field-by-field.
func TestRegister_LegacyFalseBooleansSurvive(t *testing.T) {
	r := NewCentralizedPeerRegistry(DefaultBanConfig())

	r.Register(&PeerInfo{
		ID:               "legacy:203.0.113.7:8333",
		TransportType:    blockchain_api.TransportType_TRANSPORT_WIRE_PROTOCOL,
		TransportTypeSet: true,
		Legacy:           &LegacyPeerInfo{Inbound: true, IsSyncPeer: true},
	})

	r.Register(&PeerInfo{
		ID:               "legacy:203.0.113.7:8333",
		TransportType:    blockchain_api.TransportType_TRANSPORT_WIRE_PROTOCOL,
		TransportTypeSet: true,
		Legacy:           &LegacyPeerInfo{Inbound: false, IsSyncPeer: false},
	})

	got, ok := r.Get("legacy:203.0.113.7:8333")
	require.True(t, ok)
	require.NotNil(t, got.Legacy)
	require.False(t, got.Legacy.Inbound, "Inbound=false must overwrite Inbound=true")
	require.False(t, got.Legacy.IsSyncPeer, "IsSyncPeer=false must overwrite IsSyncPeer=true")
}

// TestRegister_NilLegacyLeavesStoredValue checks that a libp2p-shaped update
// (nil Legacy) does not wipe a stored legacy block.
func TestRegister_NilLegacyLeavesStoredValue(t *testing.T) {
	r := NewCentralizedPeerRegistry(DefaultBanConfig())

	r.Register(&PeerInfo{
		ID:     "legacy:203.0.113.7:8333",
		Legacy: sampleLegacyPeerInfo(),
	})
	r.Register(&PeerInfo{
		ID:     "legacy:203.0.113.7:8333",
		Height: 912999,
	})

	got, ok := r.Get("legacy:203.0.113.7:8333")
	require.True(t, ok)
	require.NotNil(t, got.Legacy, "a nil Legacy update must not clear the stored block")
	require.Equal(t, uint32(70016), got.Legacy.ProtocolVersion)
	require.Equal(t, uint32(912999), got.Height)
}

// TestLegacyPeerInfo_ProtoRoundTrip checks the gRPC conversion both ways,
// including the nil case.
func TestLegacyPeerInfo_ProtoRoundTrip(t *testing.T) {
	in := &PeerInfo{
		ID:               "legacy:203.0.113.7:8333",
		TransportType:    blockchain_api.TransportType_TRANSPORT_WIRE_PROTOCOL,
		TransportTypeSet: true,
		Legacy:           sampleLegacyPeerInfo(),
	}

	out := protoToPeerInfo(ulogger.TestLogger{}, peerInfoToProto(in))
	require.NotNil(t, out.Legacy)
	require.Equal(t, *in.Legacy, *out.Legacy)

	noLegacy := protoToPeerInfo(ulogger.TestLogger{}, peerInfoToProto(&PeerInfo{ID: "p1"}))
	require.Nil(t, noLegacy.Legacy)
}

// TestLegacyPeerInfo_TimeConnectedIsNotConnectedAt is a regression test for the
// registry-assigned-ConnectedAt trap: Register stamps ConnectedAt with its own
// clock, so the true wire connect time must survive in Legacy.TimeConnected.
func TestLegacyPeerInfo_TimeConnectedIsNotConnectedAt(t *testing.T) {
	r := NewCentralizedPeerRegistry(DefaultBanConfig())

	wireConnectedAt := time.Now().Add(-6 * time.Hour).UTC().Truncate(time.Second)
	r.Register(&PeerInfo{
		ID:          "legacy:203.0.113.7:8333",
		ConnectedAt: wireConnectedAt,
		Legacy:      &LegacyPeerInfo{TimeConnected: wireConnectedAt},
	})

	got, ok := r.Get("legacy:203.0.113.7:8333")
	require.True(t, ok)
	require.NotNil(t, got.Legacy)
	require.Equal(t, wireConnectedAt, got.Legacy.TimeConnected)
	require.True(t, got.ConnectedAt.After(wireConnectedAt),
		"Register stamps ConnectedAt with its own clock, so it must differ")
}

// TestCleanup_ReapsDisconnectedLegacyPeer checks the TTL path: a legacy entry
// with IsConnected=false and a stale LastSeen must be removed.
func TestCleanup_ReapsDisconnectedLegacyPeer(t *testing.T) {
	r := NewCentralizedPeerRegistry(DefaultBanConfig())

	r.Register(&PeerInfo{
		ID:               "legacy:203.0.113.7:8333",
		TransportType:    blockchain_api.TransportType_TRANSPORT_WIRE_PROTOCOL,
		TransportTypeSet: true,
		Legacy:           sampleLegacyPeerInfo(),
	})
	require.NoError(t, NewLocalPeerRegistryClient(r).UpdateConnectionState(
		context.Background(), "legacy:203.0.113.7:8333", false))

	expired, _ := r.Cleanup(0, -time.Second)
	require.Equal(t, 1, expired)

	_, ok := r.Get("legacy:203.0.113.7:8333")
	require.False(t, ok)
}
