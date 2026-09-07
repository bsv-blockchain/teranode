package protocol

import (
	"testing"
	"time"

	"github.com/bsv-blockchain/go-wire"
	"github.com/stretchr/testify/require"
)

// TestSnapshots_CarriesServicesAndSyncFlag checks the two registry-facing
// snapshot fields: Services arrives from the peer's version message, and
// SyncStarted mirrors the peer's fSyncStarted under the sync-state mutex.
func TestSnapshots_CarriesServicesAndSyncFlag(t *testing.T) {
	m := startedManagerWith(t, nil, nil)

	far := dialScripted(t, nodeAddr(t, m, "127.0.0.1"))

	version := remoteVersion(4321)
	version.Services = wire.SFNodeNetwork
	far.completeOutboundHandshakeAs(t, version)

	require.Eventually(t, func() bool {
		snaps := m.Snapshots()
		return len(snaps) == 1 && snaps[0].Services == wire.SFNodeNetwork
	}, 5*time.Second, 20*time.Millisecond)

	snaps := m.Snapshots()
	require.Len(t, snaps, 1)
	require.False(t, snaps[0].SyncStarted, "no header sync round has started")

	sp := onlySyncPeerState(t, m)

	m.syncMu.Lock()
	sp.fSyncStarted = true
	m.syncMu.Unlock()

	snaps = m.Snapshots()
	require.Len(t, snaps, 1)
	require.True(t, snaps[0].SyncStarted)
}
