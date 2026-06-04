package blockchain

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/services/blockchain/blockchain_api"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

// TestCentralizedPeerRegistry_Persistence_AllFields verifies that Save then Load
// preserves every PeerInfo field, not just Height and TransportType.
func TestCentralizedPeerRegistry_Persistence_AllFields(t *testing.T) {
	store := newTestBlobStore(t)
	ctx := context.Background()

	blockHash, err := chainhash.NewHashFromStr("000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f")
	require.NoError(t, err)

	r := NewCentralizedPeerRegistry(DefaultBanConfig())

	// Register a peer with minimal fields, then manually set all the counters
	// to known values so we can verify round-tripping.
	r.Register(&PeerInfo{
		ID:             "full-peer",
		TransportType:  blockchain_api.TransportType_TRANSPORT_WIRE_PROTOCOL,
		ClientName:     "test-node/0.1",
		Height:         12345,
		DataHubURL:     "http://datahub.example.com:8090",
		NetworkAddress: "192.168.1.100:8333",
		Storage:        "aerospike",
		BlockHash:      blockHash,
	})

	r.mu.Lock()
	p := r.peers["full-peer"]
	p.BytesSent = 999888
	p.BytesReceived = 777666
	p.InteractionAttempts = 50
	p.InteractionSuccesses = 45
	p.InteractionFailures = 5
	p.MaliciousCount = 1
	p.ReputationScore = 42.5
	p.AvgResponseTimeMs = 250
	p.IsBanned = true
	p.BanScore = 80
	// Pair the IsBanned flag with a real ban entry so Load's reconciliation
	// step keeps it set; otherwise IsBanned would be cleared because no
	// matching banScores entry exists.
	r.banScores["full-peer"] = &banEntry{
		Score:    80,
		Banned:   true,
		BanUntil: time.Now().Add(24 * time.Hour),
	}
	fixedTime := time.Date(2025, 6, 15, 12, 0, 0, 0, time.UTC)
	p.ConnectedAt = fixedTime
	p.LastMessageTime = fixedTime.Add(1 * time.Minute)
	p.LastInteractionAttempt = fixedTime.Add(2 * time.Minute)
	p.LastInteractionSuccess = fixedTime.Add(3 * time.Minute)
	p.LastInteractionFailure = fixedTime.Add(4 * time.Minute)
	p.LastSeen = fixedTime.Add(5 * time.Minute)
	r.mu.Unlock()

	require.NoError(t, r.Save(ctx, store))

	r2 := NewCentralizedPeerRegistry(DefaultBanConfig())
	require.NoError(t, r2.Load(ctx, store, 365*24*time.Hour))
	require.Equal(t, 1, r2.Count())

	got, ok := r2.Get("full-peer")
	require.True(t, ok)

	require.Equal(t, "full-peer", got.ID)
	require.Equal(t, blockchain_api.TransportType_TRANSPORT_WIRE_PROTOCOL, got.TransportType)
	require.Equal(t, "test-node/0.1", got.ClientName)
	require.Equal(t, uint32(12345), got.Height)
	require.Equal(t, "http://datahub.example.com:8090", got.DataHubURL)
	require.Equal(t, "192.168.1.100:8333", got.NetworkAddress)
	require.Equal(t, "aerospike", got.Storage)

	require.NotNil(t, got.BlockHash)
	require.Equal(t, blockHash.String(), got.BlockHash.String())

	require.Equal(t, uint64(999888), got.BytesSent)
	require.Equal(t, uint64(777666), got.BytesReceived)
	require.Equal(t, int64(50), got.InteractionAttempts)
	require.Equal(t, int64(45), got.InteractionSuccesses)
	require.Equal(t, int64(5), got.InteractionFailures)
	require.Equal(t, int64(1), got.MaliciousCount)
	require.Equal(t, 42.5, got.ReputationScore)
	require.Equal(t, int64(250), got.AvgResponseTimeMs)

	require.True(t, got.IsBanned)
	require.Equal(t, int32(80), got.BanScore)

	require.True(t, got.ConnectedAt.Equal(fixedTime))
	require.True(t, got.LastMessageTime.Equal(fixedTime.Add(1*time.Minute)))
	require.True(t, got.LastInteractionAttempt.Equal(fixedTime.Add(2*time.Minute)))
	require.True(t, got.LastInteractionSuccess.Equal(fixedTime.Add(3*time.Minute)))
	require.True(t, got.LastInteractionFailure.Equal(fixedTime.Add(4*time.Minute)))
	require.True(t, got.LastSeen.Equal(fixedTime.Add(5*time.Minute)))
}

func TestCentralizedPeerRegistry_Persistence_MultiplePeersRoundTrip(t *testing.T) {
	store := newTestBlobStore(t)
	ctx := context.Background()

	r := NewCentralizedPeerRegistry(DefaultBanConfig())
	r.Register(&PeerInfo{ID: "http-peer", TransportType: blockchain_api.TransportType_TRANSPORT_HTTP, Height: 100, Storage: "s3"})
	r.Register(&PeerInfo{ID: "wire-peer", TransportType: blockchain_api.TransportType_TRANSPORT_WIRE_PROTOCOL, Height: 200, Storage: "aerospike"})
	r.Register(&PeerInfo{ID: "unknown-peer", Height: 50})

	require.NoError(t, r.Save(ctx, store))

	r2 := NewCentralizedPeerRegistry(DefaultBanConfig())
	require.NoError(t, r2.Load(ctx, store, 24*time.Hour))
	require.Equal(t, 3, r2.Count())

	for _, id := range []string{"http-peer", "wire-peer", "unknown-peer"} {
		_, ok := r2.Get(id)
		require.True(t, ok, "peer %s should exist after round-trip", id)
	}

	got, _ := r2.Get("http-peer")
	require.Equal(t, "s3", got.Storage)

	got, _ = r2.Get("wire-peer")
	require.Equal(t, "aerospike", got.Storage)
}

func TestCentralizedPeerRegistry_Persistence_EmptyRegistrySave(t *testing.T) {
	store := newTestBlobStore(t)
	ctx := context.Background()

	r := NewCentralizedPeerRegistry(DefaultBanConfig())
	require.NoError(t, r.Save(ctx, store))

	r2 := NewCentralizedPeerRegistry(DefaultBanConfig())
	require.NoError(t, r2.Load(ctx, store, 24*time.Hour))
	require.Equal(t, 0, r2.Count())
}

func TestCentralizedPeerRegistry_Persistence_LoadReplacesExistingState(t *testing.T) {
	store := newTestBlobStore(t)
	ctx := context.Background()

	r := NewCentralizedPeerRegistry(DefaultBanConfig())
	r.Register(&PeerInfo{ID: "persisted-peer", Height: 10})
	require.NoError(t, r.Save(ctx, store))

	r2 := NewCentralizedPeerRegistry(DefaultBanConfig())
	r2.Register(&PeerInfo{ID: "in-memory-peer", Height: 20})
	require.Equal(t, 1, r2.Count())

	// Load replaces, not merges.
	require.NoError(t, r2.Load(ctx, store, 24*time.Hour))
	require.Equal(t, 1, r2.Count())
	_, ok := r2.Get("persisted-peer")
	require.True(t, ok)
	_, ok = r2.Get("in-memory-peer")
	require.False(t, ok)
}

func TestSavePeerRegistry_EnvelopeShape(t *testing.T) {
	store := newTestBlobStore(t)
	ctx := context.Background()

	peers := []*PeerInfo{
		{ID: "a", Height: 1},
		{ID: "b", Height: 2},
	}
	require.NoError(t, savePeerRegistry(ctx, store, peers, nil))

	data, err := store.Get(ctx, peerRegistryBlobKey, fileformat.FileTypePeerRegistry)
	require.NoError(t, err)

	var loaded persistedRegistry
	require.NoError(t, json.Unmarshal(data, &loaded))
	require.Len(t, loaded.Peers, 2)
	require.Equal(t, persistedRegistryVersion, loaded.Version)
}

func TestLoadPeerRegistry_AllExpired(t *testing.T) {
	store := newTestBlobStore(t)
	ctx := context.Background()

	stale := time.Now().Add(-72 * time.Hour)
	envelope := persistedRegistry{
		Version: persistedRegistryVersion,
		Peers: []*PeerInfo{
			{ID: "old-1", LastSeen: stale},
			{ID: "old-2", LastSeen: stale},
		},
	}
	data, err := json.Marshal(&envelope)
	require.NoError(t, err)
	require.NoError(t, store.Set(ctx, peerRegistryBlobKey, fileformat.FileTypePeerRegistry, data))

	loaded, _, _, err := loadPeerRegistry(ctx, ulogger.TestLogger{}, store, 24*time.Hour)
	require.NoError(t, err)
	require.Empty(t, loaded)
}

// TestPersistence_BansSurviveRestart verifies the major review-feedback fix:
// banScores are written and restored across Save/Load, so an in-flight ban
// keeps enforcing after a process restart.
func TestPersistence_BansSurviveRestart(t *testing.T) {
	store := newTestBlobStore(t)
	ctx := context.Background()

	r := NewCentralizedPeerRegistry(DefaultBanConfig())
	r.Register(&PeerInfo{ID: "p"})
	r.AddBanScore("p", "spam", 0)
	r.AddBanScore("p", "spam", 0)
	require.True(t, r.IsBannedPeer("p"))

	require.NoError(t, r.Save(ctx, store))

	r2 := NewCentralizedPeerRegistry(DefaultBanConfig())
	require.NoError(t, r2.Load(ctx, store, 24*time.Hour))

	require.True(t, r2.IsBannedPeer("p"), "ban must persist across Save/Load")
	require.Equal(t, []string{"p"}, r2.ListBannedPeers())
}

func TestPersistence_BannedPeersExemptFromTTL(t *testing.T) {
	store := newTestBlobStore(t)
	ctx := context.Background()

	r := NewCentralizedPeerRegistry(DefaultBanConfig())
	r.Register(&PeerInfo{ID: "p"})
	r.AddBanScore("p", "spam", 0)
	r.AddBanScore("p", "spam", 0)

	// Backdate LastSeen so it falls outside TTL — without the ban exemption it
	// would be evicted on Load and the ban entry would be orphaned.
	r.mu.Lock()
	r.peers["p"].LastSeen = time.Now().Add(-48 * time.Hour)
	r.mu.Unlock()

	require.NoError(t, r.Save(ctx, store))

	r2 := NewCentralizedPeerRegistry(DefaultBanConfig())
	require.NoError(t, r2.Load(ctx, store, 24*time.Hour))

	require.True(t, r2.IsBannedPeer("p"))
	_, ok := r2.Get("p")
	require.True(t, ok, "banned peer must not be evicted by TTL on Load")
}

func TestPersistence_CorruptBlobDroppedAndRegistryStartsEmpty(t *testing.T) {
	store := newTestBlobStore(t)
	ctx := context.Background()

	require.NoError(t, store.Set(ctx, peerRegistryBlobKey, fileformat.FileTypePeerRegistry, []byte("not valid json {{{")))

	r := NewCentralizedPeerRegistry(DefaultBanConfig())
	require.NoError(t, r.Load(ctx, store, 24*time.Hour))
	require.Equal(t, 0, r.Count())

	// Corrupt blob must have been deleted from the store.
	exists, err := store.Exists(ctx, peerRegistryBlobKey, fileformat.FileTypePeerRegistry)
	require.NoError(t, err)
	require.False(t, exists)
}

// TestPersistence_CorruptBlobArchivedToSidecar verifies that a corrupt registry
// blob isn't lost outright — Load copies the bytes to a sidecar key and the
// registry records that key in LastCorruptArchiveKey() so operators (and
// tests) can locate the archive without probing the wall-clock keyspace.
// Previously this test searched the keyspace at µs steps; that only worked
// on macOS where UnixNano() is µs-aligned and failed ~99.9% on Linux where
// the system clock returns true-ns timestamps.
func TestPersistence_CorruptBlobArchivedToSidecar(t *testing.T) {
	store := newTestBlobStore(t)
	ctx := context.Background()

	corruptPayload := []byte("not valid json {{{")
	require.NoError(t, store.Set(ctx, peerRegistryBlobKey, fileformat.FileTypePeerRegistry, corruptPayload))

	r := NewCentralizedPeerRegistry(DefaultBanConfig())
	require.NoError(t, r.Load(ctx, store, 24*time.Hour))

	// Primary key is gone.
	exists, err := store.Exists(ctx, peerRegistryBlobKey, fileformat.FileTypePeerRegistry)
	require.NoError(t, err)
	require.False(t, exists, "primary key must be deleted")

	// Registry exposes the exact sidecar key the load path wrote.
	archiveKey := r.LastCorruptArchiveKey()
	require.NotEmpty(t, archiveKey, "LastCorruptArchiveKey must be set after a corrupt load")
	require.True(t, strings.HasPrefix(archiveKey, "peer-registry.corrupt-"),
		"sidecar key prefix mismatch: %s", archiveKey)

	// Sidecar contents must equal the corrupt payload verbatim — operators
	// need byte-for-byte fidelity for post-mortem.
	archived, err := store.Get(ctx, []byte(archiveKey), fileformat.FileTypePeerRegistry)
	require.NoError(t, err)
	require.Equal(t, corruptPayload, archived)
}

// TestPersistence_RejectsFutureVersion confirms that a persisted envelope
// claiming a Version newer than this binary supports is rejected outright.
// Silently accepting unknown fields would let a downgrade lose data; the
// operator-visible error forces a deliberate choice between rolling forward
// or restoring a backup.
func TestPersistence_RejectsFutureVersion(t *testing.T) {
	store := newTestBlobStore(t)
	ctx := context.Background()

	envelope := persistedRegistry{
		Version: persistedRegistryVersion + 1,
		SavedAt: time.Now().UTC(),
		Peers:   []*PeerInfo{{ID: "p"}},
	}
	data, err := json.Marshal(&envelope)
	require.NoError(t, err)
	require.NoError(t, store.Set(ctx, peerRegistryBlobKey, fileformat.FileTypePeerRegistry, data))

	r := NewCentralizedPeerRegistry(DefaultBanConfig())
	err = r.Load(ctx, store, 24*time.Hour)
	require.Error(t, err)
	require.Contains(t, err.Error(), "version")
	require.Equal(t, 0, r.Count(), "no peers loaded on rejection")
}

// TestPersistence_FutureVersionDisablesPersistence is the end-to-end check
// for blocker #2 in PR #988: once Load detects a future-version blob, Save
// must NOT overwrite the operator's bytes — neither via the next periodic
// tick nor via the final shutdown save. Without this guarantee the version
// check is just documentation.
func TestPersistence_FutureVersionDisablesPersistence(t *testing.T) {
	store := newTestBlobStore(t)
	ctx := context.Background()

	envelope := persistedRegistry{
		Version: persistedRegistryVersion + 1,
		SavedAt: time.Now().UTC(),
		Peers:   []*PeerInfo{{ID: "future"}},
	}
	originalBytes, err := json.Marshal(&envelope)
	require.NoError(t, err)
	require.NoError(t, store.Set(ctx, peerRegistryBlobKey, fileformat.FileTypePeerRegistry, originalBytes))

	r := NewCentralizedPeerRegistry(DefaultBanConfig())

	// Load returns the version error and the registry latches saveDisabled.
	require.Error(t, r.Load(ctx, store, 24*time.Hour))
	require.True(t, r.SaveDisabled(), "saveDisabled must be set after future-version Load")

	// A manual Save must be a no-op — registry state is empty, but writing
	// it would clobber the operator's V=current+1 blob with a V=1 envelope.
	require.NoError(t, r.Save(ctx, store))

	// Bytes on disk must be unchanged.
	onDisk, err := store.Get(ctx, peerRegistryBlobKey, fileformat.FileTypePeerRegistry)
	require.NoError(t, err)
	require.Equal(t, originalBytes, onDisk, "Save with saveDisabled must NOT mutate the blob")

	// StartPeriodicSave must also refuse — no goroutine, no future tick.
	// Close() returns immediately because nothing was added to wg.
	r.StartPeriodicSave(ctx, time.Millisecond, store)
	done := make(chan struct{})
	go func() { r.Close(); close(done) }()
	select {
	case <-done:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("Close hung — StartPeriodicSave should have been a no-op")
	}

	// Final paranoid check: blob still unchanged after the would-be tick.
	onDisk, err = store.Get(ctx, peerRegistryBlobKey, fileformat.FileTypePeerRegistry)
	require.NoError(t, err)
	require.Equal(t, originalBytes, onDisk)
}

func TestPersistence_LoadAnchorsLastDecayWhenMissing(t *testing.T) {
	store := newTestBlobStore(t)
	ctx := context.Background()

	envelope := persistedRegistry{
		Version: persistedRegistryVersion,
		BanScores: map[string]persistedBanEntry{
			"p": {
				Score:    50,
				Banned:   false,
				BanUntil: time.Time{},
				// LastDecay deliberately zero — older serialised state may lack it;
				// Load anchors it to "now" so the next AddBanScore doesn't
				// retroactively decay across the restart gap.
			},
		},
	}
	data, err := json.Marshal(&envelope)
	require.NoError(t, err)
	require.NoError(t, store.Set(ctx, peerRegistryBlobKey, fileformat.FileTypePeerRegistry, data))

	r := NewCentralizedPeerRegistry(DefaultBanConfig())
	require.NoError(t, r.Load(ctx, store, 24*time.Hour))

	r.mu.RLock()
	defer r.mu.RUnlock()
	entry, ok := r.banScores["p"]
	require.True(t, ok)
	require.False(t, entry.LastDecay.IsZero(), "Load must anchor LastDecay")
}

func TestPersistence_ConcurrentSavesSerialize(t *testing.T) {
	store := newTestBlobStore(t)
	ctx := context.Background()

	r := NewCentralizedPeerRegistry(DefaultBanConfig())
	r.Register(&PeerInfo{ID: "p1"})

	const writers = 32
	errCh := make(chan error, writers)
	var wg sync.WaitGroup
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errCh <- r.Save(ctx, store)
		}()
	}
	wg.Wait()
	close(errCh)

	for err := range errCh {
		require.NoError(t, err)
	}

	data, err := store.Get(ctx, peerRegistryBlobKey, fileformat.FileTypePeerRegistry)
	require.NoError(t, err)
	var env persistedRegistry
	require.NoError(t, json.Unmarshal(data, &env))
	require.Equal(t, persistedRegistryVersion, env.Version)
	require.Len(t, env.Peers, 1)
}

func TestPersistence_LoadClearsBanFlagOnExpiredEntry(t *testing.T) {
	store := newTestBlobStore(t)
	ctx := context.Background()

	envelope := persistedRegistry{
		Version: persistedRegistryVersion,
		Peers: []*PeerInfo{
			{
				ID:       "p",
				IsBanned: true,
				BanScore: 200,
				LastSeen: time.Now(),
			},
		},
		BanScores: map[string]persistedBanEntry{
			"p": {
				Score:    200,
				Banned:   true,
				BanUntil: time.Now().Add(-1 * time.Hour),
			},
		},
	}
	data, err := json.Marshal(&envelope)
	require.NoError(t, err)
	require.NoError(t, store.Set(ctx, peerRegistryBlobKey, fileformat.FileTypePeerRegistry, data))

	r := NewCentralizedPeerRegistry(DefaultBanConfig())
	require.NoError(t, r.Load(ctx, store, 24*time.Hour))

	got, ok := r.Get("p")
	require.True(t, ok)
	require.False(t, got.IsBanned, "PeerInfo must be reconciled when ban entry expired")
	require.Equal(t, int32(0), got.BanScore)
	require.False(t, r.IsBannedPeer("p"))
}

func TestPersistence_ExpiredBanDroppedOnLoad(t *testing.T) {
	store := newTestBlobStore(t)
	ctx := context.Background()

	envelope := persistedRegistry{
		Version: persistedRegistryVersion,
		BanScores: map[string]persistedBanEntry{
			"already-expired": {
				Score:    150,
				Banned:   true,
				BanUntil: time.Now().Add(-1 * time.Hour),
			},
			"still-banned": {
				Score:    150,
				Banned:   true,
				BanUntil: time.Now().Add(1 * time.Hour),
			},
		},
	}
	data, err := json.Marshal(&envelope)
	require.NoError(t, err)
	require.NoError(t, store.Set(ctx, peerRegistryBlobKey, fileformat.FileTypePeerRegistry, data))

	r := NewCentralizedPeerRegistry(DefaultBanConfig())
	require.NoError(t, r.Load(ctx, store, 24*time.Hour))

	require.False(t, r.IsBannedPeer("already-expired"))
	require.True(t, r.IsBannedPeer("still-banned"))
}
