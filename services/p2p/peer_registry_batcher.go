package p2p

import (
	"context"
	"sync"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/blockchain/blockchain_api"
	"github.com/bsv-blockchain/teranode/ulogger"
)

const (
	// defaultRegistryBatchInterval is how often coalesced peer-registry updates
	// are flushed when p2p_peer_registry_batch_interval is unset.
	defaultRegistryBatchInterval = time.Second

	// registryBatcherMaxPending bounds the number of distinct peers with
	// updates waiting for the next flush. Beyond this, new peers' updates are
	// dropped (and counted) instead of growing the map — the backpressure
	// valve against a gossip flood from many spoofed peer IDs.
	registryBatcherMaxPending = 10_000

	// registryReassertTTL is how long a RegisterPeer / UpdateConnectionState
	// assertion is considered fresh. Within this window a peer that keeps
	// gossiping does not get re-registered or re-marked connected on every
	// flush; only meaningful new registration data (client name, height,
	// block hash, DataHub URL) forces a RegisterPeer earlier.
	registryReassertTTL = time.Minute

	// registryAssertStatePruneAge is how long an idle peer's assert-state
	// entry survives before the flush loop prunes it. Must exceed
	// registryReassertTTL; a pruned peer is simply re-registered on its next
	// message.
	registryAssertStatePruneAge = 10 * time.Minute

	// registryFlushTimeout bounds a single flush cycle so a wedged registry
	// cannot hang the flush goroutine forever. Updates still pending after a
	// timeout are dropped; the next cycle starts from the freshly coalesced
	// state.
	registryFlushTimeout = 30 * time.Second
)

// pendingPeerUpdate accumulates every registry-affecting observation for one
// peer between flushes. Registration fields keep the latest non-zero value
// (matching the registry's own merge semantics), bytes accumulate, and the
// boolean intents are sticky until flushed.
type pendingPeerUpdate struct {
	clientName       string
	height           uint32
	blockHash        *chainhash.Hash
	dataHubURL       string
	storage          string
	markConnected    bool
	touchLastMessage bool
	bytesReceived    uint64
}

// hasInfo reports whether the update carries registration data worth pushing
// even when the peer was registered recently.
func (u *pendingPeerUpdate) hasInfo() bool {
	return u.clientName != "" || u.height > 0 || u.blockHash != nil || u.dataHubURL != ""
}

// registryAssertState remembers when RegisterPeer / UpdateConnectionState were
// last successfully sent for a peer, so repeat gossip does not re-issue them.
type registryAssertState struct {
	registeredAt time.Time
	connectedAt  time.Time
}

// peerRegistryBatcher coalesces the per-message peer-registry writes issued by
// the gossip handlers (RegisterPeer, UpdateConnectionState,
// UpdateLastMessageTime, UpdatePeerMetrics, UpdateStorage) into at most one
// small batch of RPCs per peer per flush interval. Handlers enqueue under a
// mutex and return immediately; a single background goroutine performs the
// actual gRPC calls. This removes registry latency from the gossip hot path
// and caps the RPC amplification of a message flood at a constant per peer
// per interval.
//
// A flushInterval <= 0 puts the batcher in synchronous mode: every enqueue
// flushes inline and start/stop are no-ops. Used by tests that assert registry
// state immediately after invoking a handler.
type peerRegistryBatcher struct {
	logger        ulogger.Logger
	registry      blockchain.PeerRegistryClientI
	ctx           context.Context
	flushInterval time.Duration

	mu           sync.Mutex
	pending      map[string]*pendingPeerUpdate
	dropped      uint64
	lastAsserted map[string]registryAssertState

	// flushMu serializes flush cycles (ticker, stop, and synchronous mode).
	flushMu sync.Mutex

	stopCh   chan struct{}
	doneCh   chan struct{}
	stopOnce sync.Once
}

func newPeerRegistryBatcher(ctx context.Context, logger ulogger.Logger, registry blockchain.PeerRegistryClientI, flushInterval time.Duration) *peerRegistryBatcher {
	return &peerRegistryBatcher{
		logger:        logger,
		registry:      registry,
		ctx:           ctx,
		flushInterval: flushInterval,
		pending:       make(map[string]*pendingPeerUpdate),
		lastAsserted:  make(map[string]registryAssertState),
		stopCh:        make(chan struct{}),
		doneCh:        make(chan struct{}),
	}
}

// start launches the background flush goroutine. No-op in synchronous mode.
func (b *peerRegistryBatcher) start() {
	if b.flushInterval <= 0 {
		close(b.doneCh)
		return
	}

	go func() {
		defer close(b.doneCh)

		ticker := time.NewTicker(b.flushInterval)
		defer ticker.Stop()

		for {
			select {
			case <-b.ctx.Done():
				return
			case <-b.stopCh:
				return
			case <-ticker.C:
				b.flushOnce()
			}
		}
	}()
}

// stop terminates the flush goroutine and performs a final best-effort flush
// so updates observed just before shutdown are not lost.
func (b *peerRegistryBatcher) stop() {
	b.stopOnce.Do(func() {
		close(b.stopCh)
		<-b.doneCh
		b.flushOnce()
	})
}

// enqueue merges an update for peerID into the pending map, applying
// backpressure when the map is full. Returns false when the update was
// dropped.
func (b *peerRegistryBatcher) enqueue(peerID string, apply func(*pendingPeerUpdate)) bool {
	b.mu.Lock()
	u, ok := b.pending[peerID]
	if !ok {
		if len(b.pending) >= registryBatcherMaxPending {
			b.dropped++
			b.mu.Unlock()
			return false
		}
		u = &pendingPeerUpdate{}
		b.pending[peerID] = u
	}
	apply(u)
	b.mu.Unlock()

	if b.flushInterval <= 0 {
		b.flushOnce()
	}
	return true
}

// enqueueRegister records the peer's latest registration data, optionally
// marking it as directly connected. Mirrors addPeer/addConnectedPeer.
func (b *peerRegistryBatcher) enqueueRegister(peerID, clientName string, height uint32, blockHash *chainhash.Hash, dataHubURL string, connected bool) {
	b.enqueue(peerID, func(u *pendingPeerUpdate) {
		if clientName != "" {
			u.clientName = clientName
		}
		if height > 0 {
			u.height = height
		}
		if blockHash != nil {
			u.blockHash = blockHash
		}
		if dataHubURL != "" {
			u.dataHubURL = dataHubURL
		}
		if connected {
			u.markConnected = true
		}
	})
}

// enqueueLastMessage records that a wire message was received from the peer.
func (b *peerRegistryBatcher) enqueueLastMessage(peerID string) {
	b.enqueue(peerID, func(u *pendingPeerUpdate) {
		u.touchLastMessage = true
	})
}

// enqueueBytesReceived accumulates a received-bytes delta for the peer.
func (b *peerRegistryBatcher) enqueueBytesReceived(peerID string, n uint64) {
	b.enqueue(peerID, func(u *pendingPeerUpdate) {
		u.bytesReceived += n
	})
}

// enqueueStorage records the peer's latest advertised storage mode.
func (b *peerRegistryBatcher) enqueueStorage(peerID, storage string) {
	if storage == "" {
		return
	}
	b.enqueue(peerID, func(u *pendingPeerUpdate) {
		u.storage = storage
	})
}

// forget clears the peer's assert state so its next message re-registers it.
// Called when the peer is removed from the registry (disconnect, ban); without
// this the batcher would keep skipping RegisterPeer while the registry no
// longer has the entry.
func (b *peerRegistryBatcher) forget(peerID string) {
	b.mu.Lock()
	delete(b.lastAsserted, peerID)
	delete(b.pending, peerID)
	b.mu.Unlock()
}

// flushOnce swaps out the pending map and pushes one batch of RPCs per peer.
// RPC order per peer matches the old inline path: RegisterPeer first (the
// registry ignores updates for unknown peers), then connection state, last
// message time, metrics, and storage.
func (b *peerRegistryBatcher) flushOnce() {
	b.flushMu.Lock()
	defer b.flushMu.Unlock()

	b.mu.Lock()
	pending := b.pending
	b.pending = make(map[string]*pendingPeerUpdate)
	dropped := b.dropped
	b.dropped = 0
	b.mu.Unlock()

	if dropped > 0 {
		b.logger.Warnf("[peerRegistryBatcher] dropped updates for %d peers (pending map full at %d entries)", dropped, registryBatcherMaxPending)
	}

	if len(pending) == 0 {
		b.pruneAssertState()
		return
	}

	ctx, cancel := context.WithTimeout(b.ctx, registryFlushTimeout)
	defer cancel()

	now := time.Now()
	rpcErrs := 0

	for peerID, u := range pending {
		if ctx.Err() != nil {
			b.logger.Warnf("[peerRegistryBatcher] flush aborted with %d peers unflushed: %v", len(pending), ctx.Err())
			return
		}

		b.mu.Lock()
		st := b.lastAsserted[peerID]
		b.mu.Unlock()

		sendRegister := u.hasInfo() || now.Sub(st.registeredAt) > registryReassertTTL
		sendConnected := u.markConnected && now.Sub(st.connectedAt) > registryReassertTTL

		if sendRegister {
			info := &blockchain.PeerInfo{
				ID:               peerID,
				TransportType:    blockchain_api.TransportType_TRANSPORT_HTTP,
				TransportTypeSet: true,
				ClientName:       u.clientName,
				Height:           u.height,
				BlockHash:        u.blockHash,
				DataHubURL:       u.dataHubURL,
			}
			if err := b.registry.RegisterPeer(ctx, info); err != nil {
				rpcErrs++
				continue // registry ignores updates for unknown peers, skip the rest
			}
			st.registeredAt = now
		}

		if sendConnected {
			if err := b.registry.UpdateConnectionState(ctx, peerID, true); err != nil {
				rpcErrs++
			} else {
				st.connectedAt = now
			}
		}

		if u.touchLastMessage {
			if err := b.registry.UpdateLastMessageTime(ctx, peerID); err != nil {
				rpcErrs++
			}
		}

		if u.bytesReceived > 0 {
			if err := b.registry.UpdatePeerMetrics(ctx, peerID, 0, 0, u.bytesReceived, false, false, false, 0); err != nil {
				rpcErrs++
			}
		}

		if u.storage != "" {
			if err := b.registry.UpdateStorage(ctx, peerID, u.storage); err != nil {
				rpcErrs++
			}
		}

		if sendRegister || sendConnected {
			b.mu.Lock()
			b.lastAsserted[peerID] = st
			b.mu.Unlock()
		}
	}

	if rpcErrs > 0 {
		b.logger.Warnf("[peerRegistryBatcher] flush completed with %d failed registry RPCs across %d peers", rpcErrs, len(pending))
	}

	b.pruneAssertState()
}

// pruneAssertState drops assert-state entries for peers idle longer than
// registryAssertStatePruneAge, bounding the map to recently active peers.
func (b *peerRegistryBatcher) pruneAssertState() {
	cutoff := time.Now().Add(-registryAssertStatePruneAge)

	b.mu.Lock()
	for peerID, st := range b.lastAsserted {
		if st.registeredAt.Before(cutoff) && st.connectedAt.Before(cutoff) {
			delete(b.lastAsserted, peerID)
		}
	}
	b.mu.Unlock()
}
