package legacy

import (
	"context"
	"time"

	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/blockchain/blockchain_api"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/ulogger"
)

// legacyPeerIDPrefix namespaces a wire-protocol peer inside the shared peer
// registry. A libp2p ID never carries this prefix, so a legacy entry is
// self-identifying in logs, filters and the dashboard.
const legacyPeerIDPrefix = "legacy:"

// defaultPeerRegistrySyncInterval applies when the configured interval is
// missing or not positive.
const defaultPeerRegistrySyncInterval = 10 * time.Second

// defaultRegistryRPCTimeout bounds a single registry call. It matches the p2p
// sync coordinator's rpcTimeout, which bounds the identical RPCs.
const defaultRegistryRPCTimeout = 5 * time.Second

// legacyRegistryID builds the registry key for a wire-protocol peer address.
//
// For an outbound peer the address is the one we dialled, so it is stable
// across reconnects and ban state and history survive a peer that flaps. An
// inbound peer is keyed on its remote address, which carries an ephemeral
// source port (see peer.Peer.addr, set from conn.RemoteAddr), so each inbound
// reconnect produces a new entry rather than reusing the old one. The dashboard
// bounds how long a disconnected legacy entry stays listed for that reason.
func legacyRegistryID(addr string) string {
	return legacyPeerIDPrefix + addr
}

// peerSnapshot is the subset of legacy peer state the registry needs. The
// reconcile loop compares consecutive snapshots, so an idle peer costs no RPC.
type peerSnapshot struct {
	id            string
	addr          string
	userAgent     string
	height        uint32
	bytesSent     uint64
	bytesReceived uint64
	lastRecv      time.Time
	legacy        blockchain.LegacyPeerInfo
}

// registrationEqual reports whether two snapshots carry identical registration
// data. The byte counters are excluded on purpose: they travel as deltas
// through UpdatePeerMetrics, never through RegisterPeer.
func (s peerSnapshot) registrationEqual(other peerSnapshot) bool {
	return s.addr == other.addr &&
		s.userAgent == other.userAgent &&
		s.height == other.height &&
		s.legacy == other.legacy
}

// peerRegistrySync mirrors connected legacy peers into the centralized peer
// registry, so the dashboard can show them beside libp2p peers. It is a
// read-only visibility path: nothing here feeds a sync, catchup or
// peer-selection decision.
type peerRegistrySync struct {
	logger     ulogger.Logger
	registry   blockchain.PeerRegistryClientI
	interval   time.Duration
	rpcTimeout time.Duration
	snapshot   func() []peerSnapshot
	lastSeen   map[string]peerSnapshot

	// adoptedExisting records that the startup pass over pre-existing registry
	// entries has run. See adoptExistingEntries.
	adoptedExisting bool
}

// newPeerRegistrySync builds the reconcile loop. The snapshot function must
// return nil to mean "no data available", and an empty slice to mean "no peers
// connected"; the two cases are handled differently.
func newPeerRegistrySync(logger ulogger.Logger, tSettings *settings.Settings,
	registry blockchain.PeerRegistryClientI, snapshot func() []peerSnapshot) *peerRegistrySync {
	interval := defaultPeerRegistrySyncInterval
	if tSettings != nil && tSettings.Legacy.PeerRegistrySyncInterval > 0 {
		interval = tSettings.Legacy.PeerRegistrySyncInterval
	}

	return &peerRegistrySync{
		logger:     logger,
		registry:   registry,
		interval:   interval,
		rpcTimeout: defaultRegistryRPCTimeout,
		snapshot:   snapshot,
		lastSeen:   make(map[string]peerSnapshot),
	}
}

// boundedContext derives a per-RPC deadline. The registry client adds no
// deadline of its own, so without this an unresponsive blockchain service
// stalls a tick for as long as the gRPC connection takes to give up — a visible
// gap in the dashboard at a 10 second cadence. The p2p sync coordinator bounds
// the identical calls the same way.
func (p *peerRegistrySync) boundedContext(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, p.rpcTimeout)
}

// run reconciles on every tick until ctx is cancelled.
func (p *peerRegistrySync) run(ctx context.Context) {
	ticker := time.NewTicker(p.interval)
	defer ticker.Stop()

	p.logger.Infof("[LegacyPeerRegistry] started, interval %s", p.interval)

	// Reconcile once up front so a node with live legacy peers is not shown as
	// empty for a whole interval. If the internal server is not answering yet,
	// the snapshot is nil and this tick is skipped.
	p.reconcile(ctx)

	for {
		select {
		case <-ctx.Done():
			p.logger.Infof("[LegacyPeerRegistry] stopped")
			return
		case <-ticker.C:
			p.reconcile(ctx)
		}
	}
}

// reconcile pushes one snapshot of connected legacy peers into the registry.
func (p *peerRegistrySync) reconcile(ctx context.Context) {
	peers := p.snapshot()
	if peers == nil {
		// A nil snapshot means the internal legacy server could not answer: a
		// full query channel, or a reply that timed out. It does NOT mean every
		// peer went away, so leave lastSeen untouched and retry next tick.
		p.logger.Debugf("[LegacyPeerRegistry] no peer snapshot available this tick")
		return
	}

	current := make(map[string]peerSnapshot, len(peers))

	for _, snap := range peers {
		current[snap.id] = snap

		previous, known := p.lastSeen[snap.id]

		if !known || !snap.registrationEqual(previous) {
			legacyCopy := snap.legacy
			info := &blockchain.PeerInfo{
				ID:               snap.id,
				TransportType:    blockchain_api.TransportType_TRANSPORT_WIRE_PROTOCOL,
				TransportTypeSet: true,
				ClientName:       snap.userAgent,
				NetworkAddress:   snap.addr,
				Height:           snap.height,
				Legacy:           &legacyCopy,
			}

			rpcCtx, cancel := p.boundedContext(ctx)
			err := p.registry.RegisterPeer(rpcCtx, info)
			cancel()

			if err != nil {
				p.logger.Warnf("[LegacyPeerRegistry] RegisterPeer %s failed: %v", snap.id, err)
				continue
			}
		}

		if !known {
			rpcCtx, cancel := p.boundedContext(ctx)
			err := p.registry.UpdateConnectionState(rpcCtx, snap.id, true)
			cancel()

			if err != nil {
				p.logger.Warnf("[LegacyPeerRegistry] connect %s failed: %v", snap.id, err)
				continue
			}
		}

		// Baseline the byte deltas. A registry entry can outlive our tracking:
		// a vanished peer leaves lastSeen but its entry survives until TTL
		// cleanup. UpdatePeerMetrics ADDS deltas, so a peer we are seeing for
		// the first time must baseline against what the registry already holds,
		// or its running total gets added on top of itself.
		sentBaseline, recvBaseline := previous.bytesSent, previous.bytesReceived
		haveBaseline := known

		if !known {
			rpcCtx, cancel := p.boundedContext(ctx)
			stored, found, err := p.registry.GetPeer(rpcCtx, snap.id)
			cancel()

			switch {
			case err != nil:
				p.logger.Warnf("[LegacyPeerRegistry] byte baseline %s failed: %v", snap.id, err)
			case found:
				sentBaseline, recvBaseline = stored.BytesSent, stored.BytesReceived
				haveBaseline = true
			}
		}

		sentDelta := byteDelta(snap.bytesSent, sentBaseline, haveBaseline)
		recvDelta := byteDelta(snap.bytesReceived, recvBaseline, haveBaseline)

		// What gets recorded as pushed is tracked per dimension. A rejected
		// call must not be rebaselined away: the next tick has to retry it,
		// otherwise one transient RPC failure drops those bytes for good.
		accepted := snap

		if sentDelta > 0 || recvDelta > 0 {
			rpcCtx, cancel := p.boundedContext(ctx)
			err := p.registry.UpdatePeerMetrics(rpcCtx, snap.id, 0, sentDelta, recvDelta,
				false, false, false, 0)
			cancel()

			if err != nil {
				p.logger.Warnf("[LegacyPeerRegistry] metrics %s failed: %v", snap.id, err)

				// Carry the old baseline forward so the next tick reports this
				// tick's bytes as well as its own.
				accepted.bytesSent, accepted.bytesReceived = sentBaseline, recvBaseline
				if !haveBaseline {
					accepted.bytesSent, accepted.bytesReceived = 0, 0
				}
			}
		}

		if snap.lastRecv.After(previous.lastRecv) {
			rpcCtx, cancel := p.boundedContext(ctx)
			err := p.registry.UpdateLastMessageTime(rpcCtx, snap.id)
			cancel()

			if err != nil {
				p.logger.Warnf("[LegacyPeerRegistry] last message %s failed: %v", snap.id, err)

				accepted.lastRecv = previous.lastRecv
			}
		}

		p.lastSeen[snap.id] = accepted
	}

	p.adoptExistingEntries(ctx, current)

	for id := range p.lastSeen {
		if _, present := current[id]; present {
			continue
		}

		rpcCtx, cancel := p.boundedContext(ctx)
		err := p.registry.UpdateConnectionState(rpcCtx, id, false)
		cancel()

		if err != nil {
			p.logger.Warnf("[LegacyPeerRegistry] disconnect %s failed: %v", id, err)
			continue
		}

		// Drop the tracking entry so no later tick registers this peer again.
		// Register refreshes LastSeen, which feeds registry TTL cleanup; a peer
		// that kept being registered would never age out.
		delete(p.lastSeen, id)
	}
}

// adoptExistingEntries clears the connected flag on wire-protocol entries this
// loop has never seen and the current snapshot does not contain. It runs once,
// on the first tick that produced real data.
//
// The disconnect sweep only knows peers the loop has tracked itself, so after a
// restart lastSeen is empty and an entry left connected by the previous process
// is in neither current nor lastSeen. Nothing else clears it either: the p2p
// connection sweep is scoped to libp2p transports precisely because it cannot
// see wire peers. Without this pass such an entry reports as a connected legacy
// peer until the registry TTL expires it, which is measured in hours.
//
// The caller must only invoke this with a snapshot that really came from the
// legacy server; a nil snapshot means "no data", not "nothing is connected".
func (p *peerRegistrySync) adoptExistingEntries(ctx context.Context, current map[string]peerSnapshot) {
	if p.adoptedExisting {
		return
	}

	transport := blockchain_api.TransportType_TRANSPORT_WIRE_PROTOCOL

	rpcCtx, cancel := p.boundedContext(ctx)
	peers, err := p.registry.ListPeers(rpcCtx, &transport, 0, 0, false, false)
	cancel()

	if err != nil {
		// Retry on the next tick rather than marking the pass done.
		p.logger.Warnf("[LegacyPeerRegistry] startup adoption failed: %v", err)
		return
	}

	for _, info := range peers {
		if info == nil || !info.IsConnected {
			continue
		}

		if _, live := current[info.ID]; live {
			continue
		}

		rpcCtx, cancel := p.boundedContext(ctx)
		stateErr := p.registry.UpdateConnectionState(rpcCtx, info.ID, false)
		cancel()

		if stateErr != nil {
			p.logger.Warnf("[LegacyPeerRegistry] clearing stale %s failed: %v", info.ID, stateErr)
			continue
		}

		p.logger.Infof("[LegacyPeerRegistry] cleared stale connected entry %s", info.ID)
	}

	p.adoptedExisting = true
}

// byteDelta converts an absolute counter into bytes not yet reported.
//
// With no baseline the whole current total is new. A counter that went backwards
// means the peer's connection object was replaced — legacy counters only ever
// climb within one connection — so the current total again belongs entirely to
// the new connection. Both cases return current, which never wraps uint64.
func byteDelta(current, previous uint64, haveBaseline bool) uint64 {
	if !haveBaseline || current < previous {
		return current
	}

	return current - previous
}
