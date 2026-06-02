package blockchain

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/stores/blob"
	"github.com/bsv-blockchain/teranode/stores/blob/options"
	"github.com/bsv-blockchain/teranode/ulogger"
)

// peerRegistryBlobKey is the fixed key used to address the persisted registry
// envelope inside the configured blob.Store. Using a constant means there is
// always exactly one persisted record per store regardless of how many
// blockchain processes share it.
var peerRegistryBlobKey = []byte("peer-registry")

// persistedRegistry is the on-disk JSON envelope. Versioned so format changes
// (e.g. adding more fields, splitting files) can be handled without losing the
// existing operator state.
type persistedRegistry struct {
	Version   int                          `json:"version"`
	SavedAt   time.Time                    `json:"saved_at"`
	Peers     []*PeerInfo                  `json:"peers"`
	BanScores map[string]persistedBanEntry `json:"ban_scores,omitempty"`
}

// persistedBanEntry mirrors the in-memory banEntry but is exported for JSON.
// We don't expose banEntry directly because it is intentionally package-private.
type persistedBanEntry struct {
	Score     int32     `json:"score"`
	Banned    bool      `json:"banned"`
	BanUntil  time.Time `json:"ban_until"`
	LastDecay time.Time `json:"last_decay"`
	Reasons   []string  `json:"reasons,omitempty"`
}

const persistedRegistryVersion = 1

// errFutureRegistryVersion is returned by loadPeerRegistry when the blob's
// envelope.Version is newer than this binary supports. Callers (currently
// only Load) use this sentinel to differentiate the version-mismatch path
// from generic read errors and to gate persistence so the operator's
// future-version blob is not overwritten on the next save tick.
var errFutureRegistryVersion = errors.New(errors.ERR_PROCESSING, "peer registry blob is from a newer Version than this binary supports")

// savePeerRegistry marshals the registry envelope to JSON and writes it to the
// configured blob.Store. The store is responsible for atomic publication
// (local-fs backend uses temp-file + rename; remote backends use their own
// strong consistency primitives).
func savePeerRegistry(ctx context.Context, store blob.Store, peers []*PeerInfo, banScores map[string]persistedBanEntry) error {
	envelope := persistedRegistry{
		Version:   persistedRegistryVersion,
		SavedAt:   time.Now().UTC(),
		Peers:     peers,
		BanScores: banScores,
	}

	data, err := json.Marshal(&envelope)
	if err != nil {
		return errors.NewProcessingError("marshal peer registry", err)
	}

	if err = store.Set(ctx, peerRegistryBlobKey, fileformat.FileTypePeerRegistry, data,
		options.WithAllowOverwrite(true)); err != nil {
		return errors.NewProcessingError("write peer registry to blob store", err)
	}
	return nil
}

// loadPeerRegistry reads and deserialises the peer registry from the blob store,
// discarding peer entries whose LastSeen timestamp is older than ttl. Banned
// peers are exempt from the TTL filter — bans must outlive idle gaps,
// otherwise restarts would silently clear in-flight bans.
//
// Three non-fatal situations are handled:
//   - Key missing: returns empty state (first startup is fine).
//   - Stored blob is corrupt JSON: archives the bad blob to a timestamped
//     sidecar key (peer-registry.corrupt-<unixnano>) so operators can inspect
//     it post-mortem, logs an error via the supplied logger, then returns
//     empty state. The corrupt blob is deleted so the next save can write to
//     the primary key cleanly. archiveKey reports the exact sidecar key the
//     test seam (lastCorruptArchiveKey) records.
//   - Envelope.Version is newer than this binary supports: returns
//     errFutureRegistryVersion; Load uses this to gate further persistence so
//     the operator's blob is not overwritten on the next save tick.
func loadPeerRegistry(ctx context.Context, logger ulogger.Logger, store blob.Store, ttl time.Duration) (peers []*PeerInfo, bans map[string]persistedBanEntry, archiveKey string, err error) {
	exists, err := store.Exists(ctx, peerRegistryBlobKey, fileformat.FileTypePeerRegistry)
	if err != nil {
		return nil, nil, "", errors.NewProcessingError("check peer registry blob existence", err)
	}
	if !exists {
		return []*PeerInfo{}, nil, "", nil
	}

	data, err := store.Get(ctx, peerRegistryBlobKey, fileformat.FileTypePeerRegistry)
	if err != nil {
		return nil, nil, "", errors.NewProcessingError("read peer registry from blob store", err)
	}

	var envelope persistedRegistry
	if err = json.Unmarshal(data, &envelope); err != nil {
		// Corrupt JSON in the blob store. Archive the bad bytes to a sidecar
		// key before deletion so operators have something to inspect, then
		// surface an ERROR-level log line — silent data loss here would mean
		// a node "successfully" started while having destroyed reputation /
		// ban history.
		// Nanosecond granularity so two corruption events in the same second
		// don't collide on the sidecar key (which would clobber via
		// WithAllowOverwrite=true). Format: peer-registry.corrupt-<unixnano>.
		archiveKey = fmt.Sprintf("peer-registry.corrupt-%d", time.Now().UTC().UnixNano())
		if archiveErr := store.Set(ctx, []byte(archiveKey), fileformat.FileTypePeerRegistry, data,
			options.WithAllowOverwrite(true)); archiveErr != nil {
			logger.Errorf("peer registry: corrupt blob detected (%v); FAILED to archive to %s: %v; original will be deleted",
				err, archiveKey, archiveErr)
		} else {
			logger.Errorf("peer registry: corrupt blob detected (%v); archived to %s for forensics; starting with empty registry",
				err, archiveKey)
		}
		_ = store.Del(ctx, peerRegistryBlobKey, fileformat.FileTypePeerRegistry)
		return []*PeerInfo{}, nil, archiveKey, nil
	}

	// Reject envelopes from a newer format than this binary understands.
	// Silently dropping unknown fields would let a downgrade lose data — a
	// future Version=2 might split bans into a sub-message, for example.
	// We return a sentinel so Load can disable subsequent Save calls and
	// preserve the operator's blob.
	if envelope.Version > persistedRegistryVersion {
		return nil, nil, "", errors.NewProcessingError(
			fmt.Sprintf("envelope version %d, this binary supports up to version %d",
				envelope.Version, persistedRegistryVersion), errFutureRegistryVersion)
	}

	cutoff := time.Now().Add(-ttl)
	live := make([]*PeerInfo, 0, len(envelope.Peers))
	for _, p := range envelope.Peers {
		// TTL filter uses LastSeen only (not peerActivity / LastMessageTime) because
		// Save snapshots LastSeen on every successful interaction path, so it is the
		// most reliable freshness signal in persisted state. LastMessageTime is only
		// bumped by the explicit UpdateLastMessageTime RPC, which is a subset of the
		// events that refresh LastSeen — using the max of both at load would keep
		// entries alive slightly longer but adds no correctness benefit.
		if p.IsBanned || p.LastSeen.After(cutoff) {
			live = append(live, p)
		}
	}

	return live, envelope.BanScores, "", nil
}

// Save persists the current registry state to the supplied blob.Store. Safe to
// call concurrently — saveMu serializes the snapshot+write so a slow earlier
// save can't overwrite a newer one inside the store.
//
// Save returns nil without writing when saveDisabled is set. This happens
// after Load detects a future-version blob: persisting an empty Version=1
// envelope over the operator's future-version data would destroy exactly what
// the version check was meant to protect.
func (r *CentralizedPeerRegistry) Save(ctx context.Context, store blob.Store) error {
	if store == nil {
		return nil
	}
	if r.saveDisabled.Load() {
		return nil
	}

	r.saveMu.Lock()
	defer r.saveMu.Unlock()

	r.mu.RLock()
	peers := make([]*PeerInfo, 0, len(r.peers))
	for _, p := range r.peers {
		peerCopy := *p
		// Deep-copy BlockHash so the snapshot doesn't share the underlying
		// [32]byte with the live entry. Mirrors Register's pattern and
		// guarantees no aliasing even if future code starts mutating the
		// array in place rather than swapping the pointer.
		if p.BlockHash != nil {
			hashCopy := *p.BlockHash
			peerCopy.BlockHash = &hashCopy
		}
		peers = append(peers, &peerCopy)
	}
	bans := make(map[string]persistedBanEntry, len(r.banScores))
	for id, entry := range r.banScores {
		reasonsCopy := append([]string(nil), entry.Reasons...)
		bans[id] = persistedBanEntry{
			Score:     entry.Score,
			Banned:    entry.Banned,
			BanUntil:  entry.BanUntil,
			LastDecay: entry.LastDecay,
			Reasons:   reasonsCopy,
		}
	}
	r.mu.RUnlock()

	return savePeerRegistry(ctx, store, peers, bans)
}

// Load reads the registry from the supplied blob.Store and replaces the
// current in-memory state. Stale peer entries (older than ttl, not banned)
// are dropped on load. Ban-score entries that have already expired
// (BanUntil in the past) are discarded; everything else is restored so a
// node restart does not reset in-flight bans.
//
// When the persisted envelope is from a Version newer than this binary
// supports, Load returns errFutureRegistryVersion AND sets saveDisabled —
// subsequent Save / StartPeriodicSave calls become no-ops to preserve the
// operator's blob until they can decide whether to roll forward or restore a
// backup.
func (r *CentralizedPeerRegistry) Load(ctx context.Context, store blob.Store, ttl time.Duration) error {
	if store == nil {
		return nil
	}

	peers, bans, archiveKey, err := loadPeerRegistry(ctx, r.log(), store, ttl)
	if archiveKey != "" {
		key := archiveKey
		r.lastCorruptArchiveKey.Store(&key)
	}
	if err != nil {
		if errors.Is(err, errFutureRegistryVersion) {
			// Block all subsequent persistence — the operator's blob must not
			// be overwritten by the empty Version=1 envelope a save would
			// produce. saveDisabled is one-way; a fresh registry is the only
			// way to clear it.
			r.saveDisabled.Store(true)
			r.log().Errorf("peer registry: %v — persistence disabled until restart", err)
		}
		return err
	}

	now := time.Now()

	r.mu.Lock()
	defer r.mu.Unlock()

	r.peers = make(map[string]*PeerInfo, len(peers))
	for _, p := range peers {
		entry := *p
		r.peers[entry.ID] = &entry
	}

	r.banScores = make(map[string]*banEntry, len(bans))
	for id, b := range bans {
		// Drop ban entries whose window already closed before this load.
		if b.Banned && now.After(b.BanUntil) {
			continue
		}
		// Anchor LastDecay if missing so the next AddBanScore call doesn't
		// retroactively decay across the entire restart gap.
		lastDecay := b.LastDecay
		if lastDecay.IsZero() {
			lastDecay = now
		}
		r.banScores[id] = &banEntry{
			Score:     b.Score,
			Banned:    b.Banned,
			BanUntil:  b.BanUntil,
			LastDecay: lastDecay,
			Reasons:   append([]string(nil), b.Reasons...),
		}
	}

	// Reconcile PeerInfo.IsBanned / BanScore with the surviving banScores
	// map. A peer can carry IsBanned=true on disk while its corresponding
	// ban entry has just expired (and therefore got dropped above);
	// without this sync, selection and cleanup paths would treat the peer
	// as banned even though IsBannedPeer() returns false.
	for id, p := range r.peers {
		entry, ok := r.banScores[id]
		switch {
		case !ok:
			p.IsBanned = false
			p.BanScore = 0
		default:
			p.IsBanned = entry.Banned
			p.BanScore = entry.Score
		}
	}

	return nil
}
