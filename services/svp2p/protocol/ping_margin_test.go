package protocol

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// SVNode pings every 2 minutes and drops a peer after 20 minutes of silence
// (net.h PING_INTERVAL, TIMEOUT_INTERVAL): a tenfold margin. svp2p's idle
// window is legacy_peerIdleTimeout, 125 s by default, so a 2-minute cadence
// leaves 5 s for the pong — any slow link trips the idle timer. The cadence
// must shrink to fit the window it is keeping alive.
func TestEffectivePingInterval(t *testing.T) {
	tests := []struct {
		name string
		idle time.Duration
		want time.Duration
	}{
		{name: "default 125 s window halves the cadence", idle: 125 * time.Second, want: 62500 * time.Millisecond},
		{name: "generous window keeps SVNode cadence", idle: 20 * time.Minute, want: 2 * time.Minute},
		{name: "exactly twice the cadence keeps it", idle: 4 * time.Minute, want: 2 * time.Minute},
		{name: "no idle timeout keeps the cadence", idle: 0, want: 2 * time.Minute},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, effectivePingInterval(tc.idle, pingInterval))
		})
	}
}

// The manager must hand the derived cadence to every peer it builds.
func TestManagerDerivesPingIntervalFromIdleTimeout(t *testing.T) {
	genesis := syncGenesis()

	idx, err := NewHeaderIndex(genesis)
	require.NoError(t, err)

	m := syncTestManager(t, idx, &recordingIngestor{})
	m.tSettings.Legacy.PeerIdleTimeout = 125 * time.Second

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	require.NoError(t, m.Start(ctx, []string{"127.0.0.1:0"}))

	defer func() { require.NoError(t, m.Stop()) }()

	far := dialScripted(t, m.ListenAddrs()[0])
	defer func() { _ = far.nc.Close() }()

	far.completeOutboundHandshake(t)

	var built *Peer

	require.Eventually(t, func() bool {
		m.mu.Lock()
		defer m.mu.Unlock()

		for p := range m.peers {
			built = p
			return true
		}

		return false
	}, 10*time.Second, 20*time.Millisecond)

	require.Equal(t, 125*time.Second, built.cfg.IdleTimeout)
	require.Equal(t, 62500*time.Millisecond, built.cfg.PingInterval)
}
