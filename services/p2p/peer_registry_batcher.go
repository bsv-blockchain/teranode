package p2p

import (
	"context"
	"sync"
	"sync/atomic"
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
	// message. The map is additionally count-bounded at
	// registryBatcherMaxPending: when full, entries staler than
	// registryReassertTTL are swept and, if it is still full, new assertions
	// are simply not recorded (the peer re-registers next flush) — so a flood
	// of spoofed peer IDs cannot grow the map without bound.
	registryAssertStatePruneAge = 10 * time.Minute

	// registryTombstoneAge is how long a removal tombstone is retained. It
	// only needs to outlive an in-flight flush cycle (registryFlushTimeout);
	// the "next message must re-register" guarantee comes from deleting the
	// peer's lastAsserted entry, not from the tombstone.
	registryTombstoneAge = 2 * registryFlushTimeout

	// registryFlushTimeout bounds a single flush cycle so a wedged registry
	// cannot hang the flush goroutine forever. Updates still pending after a
	// timeout are dropped; the next cycle starts from the freshly coalesced
	// state.
	registryFlushTimeout = 30 * time.Second
)

// pendingPeerUpdate accumulates every registry-affecting observation for one
// peer between flushes. Registration fields keep the most meaningful value
// (highest height, latest non-empty strings), bytes accumulate, and the
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

// merge folds another observation into u. Heights are advertised tips; with
// concurrent topic workers the enqueue order no longer matches message order,
// so the highest height (and its paired block hash) wins rather than the
// last-enqueued one.
//
// The monotonicity guarantee is scoped to a single flush window: two
// near-simultaneous messages split across a flush boundary can still reach
// the registry out of order (registry Register overwrites Height
// unconditionally), leaving a transiently regressed height until the peer's
// next message. That is deliberate — a hard monotonic clamp (batcher- or
// registry-side) would also suppress legitimate decreases, e.g. a peer
// reorging to a shorter chain or restarting from a lower height, which the
// old strictly-ordered path propagated immediately.
func (u *pendingPeerUpdate) merge(from *pendingPeerUpdate) {
	if from.clientName != "" {
		u.clientName = from.clientName
	}
	if from.height > u.height {
		u.height = from.height
		if from.blockHash != nil {
			u.blockHash = from.blockHash
		}
	} else if from.height == u.height && from.blockHash != nil {
		u.blockHash = from.blockHash
	}
	if from.dataHubURL != "" {
		u.dataHubURL = from.dataHubURL
	}
	if from.storage != "" {
		u.storage = from.storage
	}
	if from.markConnected {
		u.markConnected = true
	}
	if from.touchLastMessage {
		u.touchLastMessage = true
	}
	u.bytesReceived += from.bytesReceived
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
// state immediately after invoking a handler; NewServer clamps the configured
// interval so production never runs synchronously.
type peerRegistryBatcher struct {
	logger        ulogger.Logger
	registry      blockchain.PeerRegistryClientI
	ctx           context.Context
	flushInterval time.Duration

	mu           sync.Mutex
	pending      map[string]*pendingPeerUpdate
	dropped      uint64
	lastAsserted map[string]registryAssertState
	// removed holds tombstones for peers dropped via forget(). A tombstone
	// makes an in-flight flush skip the peer (so a removed peer is not
	// resurrected by updates coalesced before the removal) and is cleared by
	// the peer's next enqueued observation. Count-bounded like pending and
	// lastAsserted: when full, expired tombstones are swept and, if still
	// full, the tombstone is skipped (the lastAsserted deletion in forget
	// still guarantees the peer's next message re-registers it).
	removed map[string]time.Time
	// removedDuringFlush records forgets that arrive while a flush cycle is
	// processing its snapshot. Unlike removed, it is NOT cleared by a peer's
	// re-enqueue, so a forget → re-enqueue interleaving inside one flush
	// cannot let the loop push the peer's stale pre-removal snapshot; the
	// fresh post-removal data flushes next cycle. Nil when no flush is
	// running; reset at the end of each cycle.
	removedDuringFlush map[string]struct{}
	// assertForgottenDuringFlush records forgetAssertState calls that arrive
	// while a flush cycle is processing its snapshot, so the cycle's re-record
	// step does not resurrect the pre-forget assert state it read earlier.
	// Nil when no flush is running; reset at the end of each cycle. Bounded at
	// registryBatcherMaxPending; a dropped entry only means the reconciler's
	// clear can be masked for up to registryReassertTTL before self-healing.
	assertForgottenDuringFlush map[string]struct{}

	// flushMu serializes flush cycles (ticker, stop, and synchronous mode).
	flushMu sync.Mutex

	started  atomic.Bool
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
		removed:       make(map[string]time.Time),
		stopCh:        make(chan struct{}),
		doneCh:        make(chan struct{}),
	}
}

// start launches the background flush goroutine. No-op in synchronous mode.
func (b *peerRegistryBatcher) start() {
	if b.flushInterval <= 0 {
		return
	}
	if !b.started.CompareAndSwap(false, true) {
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
				// Detach periodic flushes from parent-ctx cancellation so a
				// flush already in progress when shutdown starts completes
				// (bounded by registryFlushTimeout inside flushOnce).
				b.flushOnce(context.WithoutCancel(b.ctx))
			}
		}
	}()
}

// stop terminates the flush goroutine and performs a final best-effort flush
// so updates observed just before shutdown are not lost. The whole call is
// bounded by ctx — the service manager's per-service stop budget — mirroring
// kafka.StopProducerCtx; when the budget runs out the final flush is skipped
// (or cut short) rather than overrunning it. Safe to call whether or not
// start ran (Start can fail before reaching the batcher).
func (b *peerRegistryBatcher) stop(ctx context.Context) {
	b.stopOnce.Do(func() {
		close(b.stopCh)
		if b.started.Load() {
			select {
			case <-b.doneCh:
			case <-ctx.Done():
				b.logger.Warnf("[peerRegistryBatcher] stop budget exhausted waiting for in-flight flush; skipping final flush: %v", ctx.Err())
				return
			}
		}
		b.flushOnce(ctx)
	})
}

// enqueue merges an observation for peerID into the pending map, applying
// backpressure when the map is full. Returns false when the update was
// dropped.
func (b *peerRegistryBatcher) enqueue(peerID string, from *pendingPeerUpdate) bool {
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
	u.merge(from)
	// A fresh observation supersedes a pending removal tombstone.
	delete(b.removed, peerID)
	b.mu.Unlock()

	if b.flushInterval <= 0 {
		b.flushOnce(context.WithoutCancel(b.ctx))
	}
	return true
}

// enqueueRegister records the peer's latest registration data, optionally
// marking it as directly connected. Mirrors addPeer/addConnectedPeer.
func (b *peerRegistryBatcher) enqueueRegister(peerID, clientName string, height uint32, blockHash *chainhash.Hash, dataHubURL string, connected bool) {
	b.enqueue(peerID, &pendingPeerUpdate{
		clientName:    clientName,
		height:        height,
		blockHash:     blockHash,
		dataHubURL:    dataHubURL,
		markConnected: connected,
	})
}

// enqueueLastMessage records that a wire message was received from the peer.
func (b *peerRegistryBatcher) enqueueLastMessage(peerID string) {
	b.enqueue(peerID, &pendingPeerUpdate{touchLastMessage: true})
}

// enqueueBytesReceived accumulates a received-bytes delta for the peer.
func (b *peerRegistryBatcher) enqueueBytesReceived(peerID string, n uint64) {
	b.enqueue(peerID, &pendingPeerUpdate{bytesReceived: n})
}

// enqueueStorage records the peer's latest advertised storage mode.
func (b *peerRegistryBatcher) enqueueStorage(peerID, storage string) {
	if storage == "" {
		return
	}
	b.enqueue(peerID, &pendingPeerUpdate{storage: storage})
}

// forgetAssertState drops only the peer's reassert memory, so the next flush
// re-registers it and re-asserts its connection state. Used when the registry's
// IsConnected flag is cleared out-of-band by the connection-state reconciler:
// without this, the batcher could skip re-asserting connected=true for up to
// registryReassertTTL after the peer reconnects. Pending updates and tombstones
// are untouched — the peer is not being removed. The flush-scoped set stops an
// in-flight flushOnce from resurrecting the pre-forget snapshot it read before
// this call (it re-records assert state after its RPCs complete).
//
// A pending markConnected enqueued before the reconciler's clear may still
// flush afterwards and re-assert true for a peer that just disconnected. That
// is deliberate convergence, not a bug: zeroing connectedAt here guarantees
// the re-assert is actually sent (not skipped as recently asserted), the
// wrong-way flag lasts at most one batch interval, and the next reconcile
// pass clears it again — this time with nothing pending.
func (b *peerRegistryBatcher) forgetAssertState(peerID string) {
	b.mu.Lock()
	delete(b.lastAsserted, peerID)
	if b.assertForgottenDuringFlush != nil && len(b.assertForgottenDuringFlush) < registryBatcherMaxPending {
		b.assertForgottenDuringFlush[peerID] = struct{}{}
	}
	b.mu.Unlock()
}

// forget clears the peer's batcher state and leaves a removal tombstone.
// Called when the peer is removed from the registry (disconnect, ban): pending
// updates for a removed peer are stale, an in-flight flush must not resurrect
// it, and its next message must re-register it rather than being skipped as
// recently asserted.
func (b *peerRegistryBatcher) forget(peerID string) {
	now := time.Now()

	b.mu.Lock()
	delete(b.lastAsserted, peerID)
	delete(b.pending, peerID)
	if _, exists := b.removed[peerID]; !exists && len(b.removed) >= registryBatcherMaxPending {
		// Sweep expired tombstones to make room; if the map is still full the
		// tombstone is skipped — bounded memory wins, and the lastAsserted
		// deletion above still forces re-registration on the next message.
		cutoff := now.Add(-registryTombstoneAge)
		for id, removedAt := range b.removed {
			if removedAt.Before(cutoff) {
				delete(b.removed, id)
			}
		}
	}
	if _, exists := b.removed[peerID]; exists || len(b.removed) < registryBatcherMaxPending {
		b.removed[peerID] = now
	}
	if b.removedDuringFlush != nil && len(b.removedDuringFlush) < registryBatcherMaxPending {
		b.removedDuringFlush[peerID] = struct{}{}
	}
	b.mu.Unlock()
}

// flushOnce swaps out the pending map and pushes one batch of RPCs per peer.
// RPC order per peer matches the old inline path: RegisterPeer first (the
// registry ignores updates for unknown peers), then connection state, last
// message time, metrics, and storage. The cycle is bounded by the caller's
// ctx capped at registryFlushTimeout: periodic flushes pass a detached ctx
// (they must survive parent cancellation during shutdown), the final flush in
// stop passes the service stop budget.
func (b *peerRegistryBatcher) flushOnce(ctx context.Context) {
	b.flushMu.Lock()
	defer b.flushMu.Unlock()

	b.mu.Lock()
	pending := b.pending
	b.pending = make(map[string]*pendingPeerUpdate)
	dropped := b.dropped
	b.dropped = 0
	if len(pending) > 0 {
		// Track forgets that race this cycle: a peer's re-enqueue clears its
		// persistent tombstone, but must not let this loop push the peer's
		// stale pre-removal snapshot.
		b.removedDuringFlush = make(map[string]struct{})
		b.assertForgottenDuringFlush = make(map[string]struct{})
	}
	b.mu.Unlock()

	defer func() {
		b.mu.Lock()
		b.removedDuringFlush = nil
		b.assertForgottenDuringFlush = nil
		b.mu.Unlock()
	}()

	if dropped > 0 {
		b.logger.Warnf("[peerRegistryBatcher] dropped %d peer updates (pending map full at %d peers)", dropped, registryBatcherMaxPending)
	}

	if len(pending) == 0 {
		b.pruneAssertState()
		return
	}

	ctx, cancel := context.WithTimeout(ctx, registryFlushTimeout)
	defer cancel()

	now := time.Now()
	rpcErrs := 0
	unflushed := len(pending)

	for peerID, u := range pending {
		if ctx.Err() != nil {
			b.logger.Warnf("[peerRegistryBatcher] flush cut short (%v) with updates for %d of %d peers unflushed", ctx.Err(), unflushed, len(pending))
			return
		}

		b.mu.Lock()
		isRemoved := b.isRemovedLocked(peerID)
		st := b.lastAsserted[peerID]
		b.mu.Unlock()

		unflushed--

		// The peer was removed after these updates were coalesced; pushing
		// them now would resurrect it in the registry.
		if isRemoved {
			continue
		}

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
				b.requeue(peerID, u)
				continue // registry ignores updates for unknown peers, retry next flush
			}
			st.registeredAt = now
		}

		// Registration is applied; from here each failed update is collected
		// and requeued for the next flush so accumulated byte deltas,
		// last-message freshness, and storage intents are not silently lost
		// when individual RPCs fail against a degraded registry.
		failed := &pendingPeerUpdate{}

		if sendConnected {
			if err := b.registry.UpdateConnectionState(ctx, peerID, true); err != nil {
				rpcErrs++
				failed.markConnected = true
			} else {
				st.connectedAt = now
			}
		}

		if u.touchLastMessage {
			if err := b.registry.UpdateLastMessageTime(ctx, peerID); err != nil {
				rpcErrs++
				failed.touchLastMessage = true
			}
		}

		if u.bytesReceived > 0 {
			if err := b.registry.UpdatePeerMetrics(ctx, peerID, 0, 0, u.bytesReceived, false, false, false, 0); err != nil {
				rpcErrs++
				failed.bytesReceived = u.bytesReceived
			}
		}

		if u.storage != "" {
			if err := b.registry.UpdateStorage(ctx, peerID, u.storage); err != nil {
				rpcErrs++
				failed.storage = u.storage
			}
		}

		if failed.markConnected || failed.touchLastMessage || failed.bytesReceived > 0 || failed.storage != "" {
			b.requeue(peerID, failed)
		}

		if sendRegister || sendConnected {
			b.mu.Lock()
			// Re-check the tombstones: a forget() may have raced the RPCs
			// above, and recording the assertion would suppress the peer's
			// re-registration for registryReassertTTL after its next message.
			if !b.isRemovedLocked(peerID) {
				// A forgetAssertState() may also have raced the RPCs. Zero the
				// whole snapshot, including the halves this cycle sent: the
				// reconciler's clear may have landed AFTER this cycle's
				// UpdateConnectionState(true), in which case the registry holds
				// false and keeping connectedAt would suppress the re-assert on
				// the peer's next message for registryReassertTTL. The batcher
				// cannot tell from inside the cycle which write landed last, so
				// forgetting everything is the only safe reading; the cost is
				// one redundant RegisterPeer + UpdateConnectionState on the
				// peer's next message, bounded by real reconciler clears.
				if _, forgotten := b.assertForgottenDuringFlush[peerID]; forgotten {
					st = registryAssertState{}
				}
				b.recordAssertStateLocked(peerID, st)
			}
			b.mu.Unlock()
		}
	}

	if rpcErrs > 0 {
		b.logger.Warnf("[peerRegistryBatcher] flush completed with %d failed registry RPCs across %d peers", rpcErrs, len(pending))
	}

	b.pruneAssertState()
}

// isRemovedLocked reports whether the peer has a removal tombstone, either
// persistent or flush-scoped. Caller must hold b.mu.
func (b *peerRegistryBatcher) isRemovedLocked(peerID string) bool {
	if _, r := b.removed[peerID]; r {
		return true
	}
	if b.removedDuringFlush != nil {
		if _, r := b.removedDuringFlush[peerID]; r {
			return true
		}
	}
	return false
}

// requeue puts the failed portion of a peer's coalesced update back into the
// pending map — the whole update after a failed RegisterPeer, or just the
// failed intents when individual follow-up RPCs error — so accumulated bytes,
// last-message freshness, and storage intents are retried on the next flush
// instead of being silently dropped. Called from flushOnce only; must not
// trigger a synchronous flush.
func (b *peerRegistryBatcher) requeue(peerID string, u *pendingPeerUpdate) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.isRemovedLocked(peerID) {
		return
	}
	existing, ok := b.pending[peerID]
	if !ok {
		if len(b.pending) >= registryBatcherMaxPending {
			b.dropped++
			return
		}
		existing = &pendingPeerUpdate{}
		b.pending[peerID] = existing
	}
	// Merge the failed batch under the newer observations: existing wins on
	// conflicts, which merge() achieves by folding existing into the old
	// update last.
	merged := &pendingPeerUpdate{}
	merged.merge(u)
	merged.merge(existing)
	*existing = *merged
}

// recordAssertStateLocked stores a peer's assert state, enforcing the count
// bound: when the map is full, entries staler than registryReassertTTL are
// swept first, and if it is still full the assertion is not recorded — the
// peer just re-registers on the next flush, keeping the RPC rate capped by
// the pending-map bound while memory stays bounded under a spoofed-ID flood.
// Caller must hold b.mu.
func (b *peerRegistryBatcher) recordAssertStateLocked(peerID string, st registryAssertState) {
	if _, exists := b.lastAsserted[peerID]; !exists && len(b.lastAsserted) >= registryBatcherMaxPending {
		b.evictStaleAssertStateLocked(time.Now().Add(-registryReassertTTL))
		if len(b.lastAsserted) >= registryBatcherMaxPending {
			return
		}
	}
	b.lastAsserted[peerID] = st
}

// evictStaleAssertStateLocked removes assert-state entries whose assertions
// are all older than cutoff. Caller must hold b.mu.
func (b *peerRegistryBatcher) evictStaleAssertStateLocked(cutoff time.Time) {
	for peerID, st := range b.lastAsserted {
		if st.registeredAt.Before(cutoff) && st.connectedAt.Before(cutoff) {
			delete(b.lastAsserted, peerID)
		}
	}
}

// pruneAssertState drops assert-state entries older than
// registryAssertStatePruneAge and removal tombstones older than
// registryTombstoneAge, bounding both maps to recently active peers.
func (b *peerRegistryBatcher) pruneAssertState() {
	now := time.Now()

	b.mu.Lock()
	b.evictStaleAssertStateLocked(now.Add(-registryAssertStatePruneAge))
	tombstoneCutoff := now.Add(-registryTombstoneAge)
	for peerID, removedAt := range b.removed {
		if removedAt.Before(tombstoneCutoff) {
			delete(b.removed, peerID)
		}
	}
	b.mu.Unlock()
}
