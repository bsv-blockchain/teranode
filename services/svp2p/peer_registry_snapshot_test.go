package svp2p

import (
	"context"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/services/svp2p/protocol"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

func testProtocolSnapshot() protocol.PeerSnapshot {
	return protocol.PeerSnapshot{
		Addr:            "203.0.113.7:8333",
		Inbound:         true,
		UserAgent:       "/Bitcoin SV:1.0.16/",
		ProtocolVersion: 70016,
		StartingHeight:  912000,
		BytesSent:       100,
		BytesReceived:   500,
		ConnectedAt:     time.Unix(1750000000, 0).UTC(),
		LastRecv:        time.Unix(1750000100, 0).UTC(),
		Services:        wire.ServiceFlag(0x25),
		SyncStarted:     true,
	}
}

// TestRegistrySnapshotFrom_MapsEveryField checks every registry field the
// protocol snapshot can supply arrives, and that the two fields svp2p does not
// track (ping round trip, clock offset) stay zero.
func TestRegistrySnapshotFrom_MapsEveryField(t *testing.T) {
	snap, ok := registrySnapshotFrom(testProtocolSnapshot())
	require.True(t, ok)

	require.Equal(t, "legacy:203.0.113.7:8333", snap.id)
	require.Equal(t, "203.0.113.7:8333", snap.addr)
	require.Equal(t, "/Bitcoin SV:1.0.16/", snap.userAgent)
	require.Equal(t, uint32(912000), snap.height)
	require.Equal(t, uint64(100), snap.bytesSent)
	require.Equal(t, uint64(500), snap.bytesReceived)
	require.Equal(t, time.Unix(1750000100, 0).UTC(), snap.lastRecv)

	require.True(t, snap.legacy.Inbound)
	require.Equal(t, uint32(70016), snap.legacy.ProtocolVersion)
	require.Equal(t, uint64(0x25), snap.legacy.ServiceFlags)
	require.Equal(t, int32(912000), snap.legacy.StartingHeight)
	require.True(t, snap.legacy.IsSyncPeer)
	require.Equal(t, time.Unix(1750000000, 0).UTC(), snap.legacy.TimeConnected)

	require.Zero(t, snap.legacy.PingMicros, "svp2p does not record the ping round trip")
	require.Zero(t, snap.legacy.TimeOffsetSecs, "svp2p does not retain the version clock offset")
}

// TestRegistrySnapshotFrom_SyncPeerFlag checks the header-sync flag travels.
func TestRegistrySnapshotFrom_SyncPeerFlag(t *testing.T) {
	ps := testProtocolSnapshot()
	ps.SyncStarted = false

	snap, ok := registrySnapshotFrom(ps)
	require.True(t, ok)
	require.False(t, snap.legacy.IsSyncPeer)
}

// TestRegistrySnapshotFrom_SkipsUnusableAddress checks a peer whose address
// cannot be split into host and port produces no registry entry.
func TestRegistrySnapshotFrom_SkipsUnusableAddress(t *testing.T) {
	ps := testProtocolSnapshot()
	ps.Addr = "not-an-address"

	_, ok := registrySnapshotFrom(ps)
	require.False(t, ok)
}

// TestRegistrySnapshotFrom_NegativeStartingHeightClampsToZero checks a
// pre-handshake or nonsense starting height never underflows the uint32
// registry height.
func TestRegistrySnapshotFrom_NegativeStartingHeightClampsToZero(t *testing.T) {
	ps := testProtocolSnapshot()
	ps.StartingHeight = -1

	snap, ok := registrySnapshotFrom(ps)
	require.True(t, ok)
	require.Zero(t, snap.height)
}

// TestRegistryPeerSnapshots_NilManagerReturnsNil checks the snapshot source
// answers nil — "no data", not "no peers" — before the manager exists.
func TestRegistryPeerSnapshots_NilManagerReturnsNil(t *testing.T) {
	s := &Server{}
	require.Nil(t, s.registryPeerSnapshots())
}

// TestRun_ReconcilesImmediatelyAndStopsOnContextCancel checks the loop does
// one reconcile before its first tick and exits when the context ends.
func TestRun_ReconcilesImmediatelyAndStopsOnContextCancel(t *testing.T) {
	calls := make(chan struct{}, 8)

	sync := newPeerRegistrySync(ulogger.TestLogger{}, testSyncSettings(), nil,
		func() []registryPeerSnapshot {
			calls <- struct{}{}
			return nil
		})
	sync.interval = time.Hour

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})

	go func() {
		sync.run(ctx)
		close(done)
	}()

	select {
	case <-calls:
	case <-time.After(5 * time.Second):
		t.Fatal("the loop never reconciled")
	}

	cancel()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("the loop did not stop on context cancel")
	}
}
