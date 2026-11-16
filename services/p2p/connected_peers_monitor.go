package p2p

import (
	"context"
	"time"
)

// connectedPeersPollInterval controls how often prometheusP2PConnectedPeers is
// refreshed from the live peer set.
const connectedPeersPollInterval = 5 * time.Second

// startConnectedPeersMonitor runs for the lifetime of ctx, keeping
// prometheusP2PConnectedPeers in sync with the live peer set on its own
// ticker. It is intentionally independent of the NAT-diagnostics logging
// goroutine started elsewhere in Start(): tying the gauge update to that
// goroutine's ticker meant the metric went stale between its (much longer)
// ticks and would stop updating entirely if that goroutine ever exited or
// was refactored away. The returned channel is closed once the monitor
// goroutine has exited, so callers (mainly tests) can synchronize on
// shutdown.
func (s *Server) startConnectedPeersMonitor(ctx context.Context, interval time.Duration) <-chan struct{} {
	done := make(chan struct{})

	go func() {
		defer close(done)

		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		s.updateConnectedPeersGauge(ctx)

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				s.updateConnectedPeersGauge(ctx)
			}
		}
	}()

	return done
}

// updateConnectedPeersGauge sets prometheusP2PConnectedPeers to the number of
// DIRECTLY connected peers, using the same definition as the
// connected_peers_count field in node_status (see getNodeStatusMessage in
// Server.go): the registry also holds gossiped/disconnected peers, so only
// entries with IsConnected set are counted. A nil registry (e.g. before
// NewServer has finished wiring the service) is a no-op, and a failed lookup
// leaves the gauge at its previous value rather than publishing a wrong one.
func (s *Server) updateConnectedPeersGauge(ctx context.Context) {
	if s.peerRegistry == nil {
		return
	}

	peers, err := s.peerRegistry.ListPeers(ctx, nil, 0, 0, false, false)
	if err != nil {
		s.logger.Warnf("[updateConnectedPeersGauge] ListPeers failed: %v", err)
		return
	}

	connected := 0

	for _, p := range peers {
		if p.IsConnected {
			connected++
		}
	}

	prometheusP2PConnectedPeers.Set(float64(connected))
}
