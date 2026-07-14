package blockchain

import (
	"context"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/services/blockchain/blockchain_api"
	"github.com/bsv-blockchain/teranode/stores/blob"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

func mustPeerRegistryHash(seed string) *chainhash.Hash {
	hash := chainhash.HashH([]byte(seed))
	return &hash
}

func TestCentralizedPeerRegistry_RegisterAndGet(t *testing.T) {
	r := NewCentralizedPeerRegistry(DefaultBanConfig())

	info := &PeerInfo{
		ID:              "peer-1",
		TransportType:   blockchain_api.TransportType_TRANSPORT_HTTP,
		ClientName:      "test-client",
		Height:          100,
		DataHubURL:      "http://peer1.example.com",
		ReputationScore: 75.0,
	}

	r.Register(info)

	got, ok := r.Get("peer-1")
	require.True(t, ok)
	require.Equal(t, "peer-1", got.ID)
	require.Equal(t, blockchain_api.TransportType_TRANSPORT_HTTP, got.TransportType)
	require.Equal(t, "test-client", got.ClientName)
	require.Equal(t, uint32(100), got.Height)
	// Reputation is reset to 50.0 for new entries.
	require.Equal(t, 50.0, got.ReputationScore)
	require.False(t, got.ConnectedAt.IsZero())
	require.False(t, got.LastSeen.IsZero())
}

func TestCentralizedPeerRegistry_RegisterUpdatesExisting(t *testing.T) {
	r := NewCentralizedPeerRegistry(DefaultBanConfig())

	r.Register(&PeerInfo{ID: "peer-1", Height: 10, DataHubURL: "http://old.example.com"})
	r.Register(&PeerInfo{ID: "peer-1", Height: 20, DataHubURL: "http://new.example.com"})

	got, ok := r.Get("peer-1")
	require.True(t, ok)
	require.Equal(t, uint32(20), got.Height)
	require.Equal(t, "http://new.example.com", got.DataHubURL)
}

func TestCentralizedPeerRegistry_Remove(t *testing.T) {
	r := NewCentralizedPeerRegistry(DefaultBanConfig())

	r.Register(&PeerInfo{ID: "peer-1"})
	require.Equal(t, 1, r.Count())

	r.Remove("peer-1")
	require.Equal(t, 0, r.Count())

	_, ok := r.Get("peer-1")
	require.False(t, ok)
}

func TestCentralizedPeerRegistry_ListFilters(t *testing.T) {
	r := NewCentralizedPeerRegistry(DefaultBanConfig())

	r.Register(&PeerInfo{ID: "http-peer", TransportType: blockchain_api.TransportType_TRANSPORT_HTTP, Height: 100})
	r.Register(&PeerInfo{ID: "wire-peer", TransportType: blockchain_api.TransportType_TRANSPORT_WIRE_PROTOCOL, Height: 50})
	r.Register(&PeerInfo{ID: "banned-peer", TransportType: blockchain_api.TransportType_TRANSPORT_WIRE_PROTOCOL, Height: 200})
	// Actually ban the peer via AddBanScore so isPeerBannedLocked finds the ban entry.
	r.AddBanScore("banned-peer", "spam", 100)
	r.AddBanScore("banned-peer", "spam", 100)

	// No filters — returns all.
	all := r.List(nil, 0, 0, false, false)
	require.Len(t, all, 3)

	// Filter by transport.
	httpFilter := blockchain_api.TransportType_TRANSPORT_HTTP
	httpOnly := r.List(&httpFilter, 0, 0, false, false)
	require.Len(t, httpOnly, 1)
	require.Equal(t, "http-peer", httpOnly[0].ID)

	// Exclude banned.
	noBanned := r.List(nil, 0, 0, true, false)
	require.Len(t, noBanned, 2)

	// Min height.
	highOnly := r.List(nil, 0, 100, false, false)
	require.Len(t, highOnly, 2) // http-peer (100) and banned-peer (200)
}

func TestCentralizedPeerRegistry_ListSortedByReputation(t *testing.T) {
	r := NewCentralizedPeerRegistry(DefaultBanConfig())

	r.Register(&PeerInfo{ID: "low", Height: 50})
	r.Register(&PeerInfo{ID: "high", Height: 50})

	// Manually set reputation via UpdateMetrics to simulate different scores.
	// Record a success for "high" to push it above 50.
	r.UpdateMetrics("high", 0, 0, 0, true, false, false, 100)

	peers := r.List(nil, 0, 0, false, false)
	require.Len(t, peers, 2)
	// Higher reputation should come first.
	require.Equal(t, "high", peers[0].ID)
}

func TestCentralizedPeerRegistry_UpdateMetrics(t *testing.T) {
	r := NewCentralizedPeerRegistry(DefaultBanConfig())

	r.Register(&PeerInfo{ID: "peer-1"})

	r.UpdateMetrics("peer-1", 200, 1024, 512, true, false, false, 150)

	got, ok := r.Get("peer-1")
	require.True(t, ok)
	require.Equal(t, uint32(200), got.Height)
	require.Equal(t, uint64(1024), got.BytesSent)
	require.Equal(t, uint64(512), got.BytesReceived)
	require.Equal(t, int64(1), got.InteractionSuccesses)
	require.Equal(t, int64(150), got.AvgResponseTimeMs)
}

func TestCentralizedPeerRegistry_UpdateMetrics_IgnoresUntimedSuccessForAverage(t *testing.T) {
	r := NewCentralizedPeerRegistry(DefaultBanConfig())

	r.Register(&PeerInfo{ID: "peer-1"})

	r.UpdateMetrics("peer-1", 100, 0, 0, true, false, false, 0)

	got, ok := r.Get("peer-1")
	require.True(t, ok)
	require.Equal(t, int64(1), got.InteractionSuccesses)
	require.Equal(t, int64(0), got.AvgResponseTimeMs)

	r.UpdateMetrics("peer-1", 100, 0, 0, true, false, false, 200)
	r.UpdateMetrics("peer-1", 100, 0, 0, true, false, false, 0)

	got, ok = r.Get("peer-1")
	require.True(t, ok)
	require.Equal(t, int64(3), got.InteractionSuccesses)
	require.Equal(t, int64(200), got.AvgResponseTimeMs)
}

func TestCentralizedPeerRegistry_UpdateMetrics_Malicious(t *testing.T) {
	r := NewCentralizedPeerRegistry(DefaultBanConfig())

	r.Register(&PeerInfo{ID: "bad-peer"})
	r.UpdateMetrics("bad-peer", 0, 0, 0, false, false, true, 0)

	got, ok := r.Get("bad-peer")
	require.True(t, ok)
	require.Equal(t, 5.0, got.ReputationScore)
	require.Equal(t, int64(1), got.MaliciousCount)
}

// newTestBlobStore returns an in-memory blob.Store for persistence tests.
func newTestBlobStore(t *testing.T) blob.Store {
	t.Helper()
	u, err := url.Parse("memory://")
	require.NoError(t, err)
	store, err := blob.NewStore(ulogger.TestLogger{}, u)
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close(context.Background()) })
	return store
}

func TestCentralizedPeerRegistry_Persistence(t *testing.T) {
	store := newTestBlobStore(t)
	ctx := context.Background()

	r := NewCentralizedPeerRegistry(DefaultBanConfig())
	r.Register(&PeerInfo{ID: "peer-1", Height: 42, TransportType: blockchain_api.TransportType_TRANSPORT_WIRE_PROTOCOL})
	r.Register(&PeerInfo{ID: "peer-2", Height: 99, TransportType: blockchain_api.TransportType_TRANSPORT_HTTP})

	require.NoError(t, r.Save(ctx, store))

	r2 := NewCentralizedPeerRegistry(DefaultBanConfig())
	require.NoError(t, r2.Load(ctx, store, 24*time.Hour))

	require.Equal(t, 2, r2.Count())

	p1, ok := r2.Get("peer-1")
	require.True(t, ok)
	require.Equal(t, uint32(42), p1.Height)
	require.Equal(t, blockchain_api.TransportType_TRANSPORT_WIRE_PROTOCOL, p1.TransportType)
}

func TestCentralizedPeerRegistry_Persistence_MissingKey(t *testing.T) {
	store := newTestBlobStore(t)
	r := NewCentralizedPeerRegistry(DefaultBanConfig())

	// Loading from a store with no peer registry blob should succeed and leave the registry empty.
	require.NoError(t, r.Load(context.Background(), store, 24*time.Hour))
	require.Equal(t, 0, r.Count())
}

func TestCentralizedPeerRegistry_Persistence_CorruptBlob(t *testing.T) {
	store := newTestBlobStore(t)
	ctx := context.Background()

	require.NoError(t, store.Set(ctx, peerRegistryBlobKey, fileformat.FileTypePeerRegistry, []byte("not valid json {{{{")))

	r := NewCentralizedPeerRegistry(DefaultBanConfig())
	// Should not return an error — corrupt blob is dropped and registry starts empty.
	require.NoError(t, r.Load(ctx, store, 24*time.Hour))
	require.Equal(t, 0, r.Count())

	// The corrupt blob should have been deleted.
	exists, err := store.Exists(ctx, peerRegistryBlobKey, fileformat.FileTypePeerRegistry)
	require.NoError(t, err)
	require.False(t, exists)
}

func TestCentralizedPeerRegistry_Persistence_TTLCleanup(t *testing.T) {
	store := newTestBlobStore(t)
	ctx := context.Background()

	r := NewCentralizedPeerRegistry(DefaultBanConfig())
	r.Register(&PeerInfo{ID: "fresh"})
	r.Register(&PeerInfo{ID: "stale"})

	// Backdate the stale peer's LastSeen so it falls outside TTL.
	r.mu.Lock()
	r.peers["stale"].LastSeen = time.Now().Add(-48 * time.Hour)
	r.mu.Unlock()

	require.NoError(t, r.Save(ctx, store))

	r2 := NewCentralizedPeerRegistry(DefaultBanConfig())
	require.NoError(t, r2.Load(ctx, store, 24*time.Hour))

	require.Equal(t, 1, r2.Count())
	_, ok := r2.Get("fresh")
	require.True(t, ok)
	_, ok = r2.Get("stale")
	require.False(t, ok)
}

// ---------------------------------------------------------------------------
// P2P-domain method coverage
// ---------------------------------------------------------------------------

func TestCentralizedPeerRegistry_UpdateConnectionState(t *testing.T) {
	r := NewCentralizedPeerRegistry(DefaultBanConfig())

	r.Register(&PeerInfo{ID: "p", IsConnected: false})
	r.UpdateConnectionState("p", true)

	got, _ := r.Get("p")
	require.True(t, got.IsConnected)

	r.UpdateConnectionState("p", false)
	got, _ = r.Get("p")
	require.False(t, got.IsConnected)

	// Unknown peer — no panic, no insert.
	r.UpdateConnectionState("ghost", true)
	require.Equal(t, 1, r.Count())
}

func TestCentralizedPeerRegistry_RegisterSanitizesClientName(t *testing.T) {
	r := NewCentralizedPeerRegistry(DefaultBanConfig())

	dirty := "<script>alert('xss')</script>" + string([]byte{0, 1, 2}) + "node/1.0"
	r.Register(&PeerInfo{ID: "peer-1", ClientName: dirty})

	got, _ := r.Get("peer-1")
	require.NotContains(t, got.ClientName, "<")
	require.NotContains(t, got.ClientName, ">")
	require.NotContains(t, got.ClientName, "'")
	require.NotContains(t, got.ClientName, "\x00")
	require.Contains(t, got.ClientName, "node/1.0", "safe segment must survive")

	// Update path also sanitizes.
	r.Register(&PeerInfo{ID: "peer-1", ClientName: "good name<bad>"})
	got, _ = r.Get("peer-1")
	require.NotContains(t, got.ClientName, "<")
	require.NotContains(t, got.ClientName, ">")
}

func TestCentralizedPeerRegistry_RegisterCapsLongClientName(t *testing.T) {
	r := NewCentralizedPeerRegistry(DefaultBanConfig())

	long := strings.Repeat("a", 500)
	r.Register(&PeerInfo{ID: "p", ClientName: long})

	got, _ := r.Get("p")
	require.LessOrEqual(t, len(got.ClientName), 128, "names capped at 128")
}

func TestCentralizedPeerRegistry_RegisterPreservesIsConnectedOnUpdate(t *testing.T) {
	r := NewCentralizedPeerRegistry(DefaultBanConfig())

	r.Register(&PeerInfo{ID: "p"})
	r.UpdateConnectionState("p", true)

	// A subsequent Register call (without IsConnected set) must not flip the flag.
	r.Register(&PeerInfo{ID: "p", Height: 100})
	got, _ := r.Get("p")
	require.True(t, got.IsConnected, "Register update path must not overwrite IsConnected")
	require.Equal(t, uint32(100), got.Height)
}

func TestCentralizedPeerRegistry_UpdateLastMessageTimeAndStorage(t *testing.T) {
	r := NewCentralizedPeerRegistry(DefaultBanConfig())

	r.Register(&PeerInfo{ID: "p"})
	first, _ := r.Get("p")

	time.Sleep(2 * time.Millisecond)
	r.UpdateLastMessageTime("p")
	r.UpdateStorage("p", "full")

	second, _ := r.Get("p")
	require.True(t, second.LastMessageTime.After(first.LastMessageTime))
	require.Equal(t, "full", second.Storage)
}

func TestCentralizedPeerRegistry_RecordSyncAttemptAndClear(t *testing.T) {
	r := NewCentralizedPeerRegistry(DefaultBanConfig())

	r.Register(&PeerInfo{ID: "a"})
	r.Register(&PeerInfo{ID: "b"})

	r.RecordSyncAttempt("a")
	r.RecordSyncAttempt("a")
	r.RecordSyncAttempt("b")

	a, _ := r.Get("a")
	require.Equal(t, int32(2), a.SyncAttemptCount)
	require.False(t, a.LastSyncAttempt.IsZero())

	cleared := r.ClearAllSyncAttempts()
	require.Equal(t, 2, cleared)

	a, _ = r.Get("a")
	require.True(t, a.LastSyncAttempt.IsZero())
	// Counter is intentionally NOT reset by ClearAll — only the timestamp.
}

func TestCentralizedPeerRegistry_RecordBlockSubtreeTransaction(t *testing.T) {
	r := NewCentralizedPeerRegistry(DefaultBanConfig())

	r.Register(&PeerInfo{ID: "p"})

	r.RecordBlockReceived("p", 150)
	r.RecordSubtreeReceived("p", 0)
	r.RecordTransactionReceived("p")

	got, _ := r.Get("p")
	require.Equal(t, int64(1), got.BlocksReceived)
	require.Equal(t, int64(1), got.SubtreesReceived)
	require.Equal(t, int64(1), got.TransactionsReceived)
	require.Equal(t, int64(3), got.InteractionSuccesses)
	require.Equal(t, int64(150), got.AvgResponseTimeMs)
	require.False(t, got.LastBlockTime.IsZero())
}

func TestCentralizedPeerRegistry_RecordCatchupError(t *testing.T) {
	r := NewCentralizedPeerRegistry(DefaultBanConfig())

	r.Register(&PeerInfo{ID: "p"})
	r.RecordCatchupError("p", "block 0xdead missing")

	got, _ := r.Get("p")
	require.Equal(t, "block 0xdead missing", got.LastCatchupError)
	require.False(t, got.LastCatchupErrorTime.IsZero())
}

func TestCentralizedPeerRegistry_RecordValidatedPeerProgress_MonotonicChainWork(t *testing.T) {
	r := NewCentralizedPeerRegistry(DefaultBanConfig())

	r.Register(&PeerInfo{ID: "p", Height: 500, BlockHash: mustPeerRegistryHash("advertised")})

	firstHash := mustPeerRegistryHash("validated-first")
	require.NoError(t, r.RecordValidatedPeerProgress("p", 100, firstHash, []byte{0x02}))

	got, ok := r.Get("p")
	require.True(t, ok)
	require.Equal(t, uint32(100), got.ValidatedHeight)
	require.Equal(t, firstHash.String(), got.ValidatedBlockHash.String())
	require.Equal(t, []byte{0x02}, got.ValidatedChainWork)
	require.False(t, got.LastValidatedAt.IsZero())
	require.Equal(t, uint32(500), got.Height, "advertised height remains separate")

	equalHash := mustPeerRegistryHash("validated-equal")
	require.NoError(t, r.RecordValidatedPeerProgress("p", 101, equalHash, []byte{0x02}))

	got, ok = r.Get("p")
	require.True(t, ok)
	require.Equal(t, uint32(100), got.ValidatedHeight)
	require.Equal(t, firstHash.String(), got.ValidatedBlockHash.String())

	lowerHash := mustPeerRegistryHash("validated-lower")
	require.NoError(t, r.RecordValidatedPeerProgress("p", 99, lowerHash, []byte{0x01}))

	got, ok = r.Get("p")
	require.True(t, ok)
	require.Equal(t, uint32(100), got.ValidatedHeight)
	require.Equal(t, firstHash.String(), got.ValidatedBlockHash.String())

	require.Error(t, r.RecordValidatedPeerProgress("p", 200, mustPeerRegistryHash("invalid-empty-work"), nil))
	require.Error(t, r.RecordValidatedPeerProgress("p", 200, nil, []byte{0x04}))

	got, ok = r.Get("p")
	require.True(t, ok)
	require.Equal(t, uint32(100), got.ValidatedHeight)
	require.Equal(t, firstHash.String(), got.ValidatedBlockHash.String())

	higherHash := mustPeerRegistryHash("validated-higher")
	require.NoError(t, r.RecordValidatedPeerProgress("p", 120, higherHash, []byte{0x03}))

	got, ok = r.Get("p")
	require.True(t, ok)
	require.Equal(t, uint32(120), got.ValidatedHeight)
	require.Equal(t, higherHash.String(), got.ValidatedBlockHash.String())
	require.Equal(t, []byte{0x03}, got.ValidatedChainWork)

	staleRegisterHash := mustPeerRegistryHash("validated-stale-register")
	r.Register(&PeerInfo{
		ID:                 "p",
		ValidatedHeight:    80,
		ValidatedBlockHash: staleRegisterHash,
		ValidatedChainWork: []byte{0x02},
		LastValidatedAt:    time.Now().Add(time.Minute),
	})

	got, ok = r.Get("p")
	require.True(t, ok)
	require.Equal(t, uint32(120), got.ValidatedHeight)
	require.Equal(t, higherHash.String(), got.ValidatedBlockHash.String())
	require.Equal(t, []byte{0x03}, got.ValidatedChainWork)
}

func TestCentralizedPeerRegistry_Register_ValidatedChainWorkSizeCap(t *testing.T) {
	r := NewCentralizedPeerRegistry(DefaultBanConfig())

	// The 128-byte cap is enforced on the merge path (shouldAdvanceValidatedWork),
	// NOT the new-peer path: Register clones a brand-new entry directly and returns
	// before the merge. Seed each peer with only its ID first so the subsequent
	// Register exercises the merge-path cap check.

	// Over the cap (MaxValidatedChainWorkBytes + 1 = 129 bytes): rejected by
	// shouldAdvanceValidatedWork, so no validated progress is recorded.
	r.Register(&PeerInfo{ID: "over"})

	overCap := make([]byte, MaxValidatedChainWorkBytes+1)
	overCap[len(overCap)-1] = 0x01
	oversizedHash := mustPeerRegistryHash("validated-oversized")
	r.Register(&PeerInfo{
		ID:                 "over",
		ValidatedHeight:    100,
		ValidatedBlockHash: oversizedHash,
		ValidatedChainWork: overCap,
	})

	got, ok := r.Get("over")
	require.True(t, ok)
	require.Empty(t, got.ValidatedChainWork, "over-128-byte ChainWork must be rejected")
	require.Nil(t, got.ValidatedBlockHash, "over-cap ChainWork must not advance validated progress")
	require.Equal(t, uint32(0), got.ValidatedHeight)

	// Exactly at the cap (128 bytes): accepted, so the test pins the boundary
	// rather than merely "large is rejected".
	r.Register(&PeerInfo{ID: "at"})

	atCap := make([]byte, MaxValidatedChainWorkBytes)
	atCap[len(atCap)-1] = 0x01
	atCapHash := mustPeerRegistryHash("validated-atcap")
	r.Register(&PeerInfo{
		ID:                 "at",
		ValidatedHeight:    100,
		ValidatedBlockHash: atCapHash,
		ValidatedChainWork: atCap,
	})

	got, ok = r.Get("at")
	require.True(t, ok)
	require.Len(t, got.ValidatedChainWork, MaxValidatedChainWorkBytes, "exactly-128-byte ChainWork must be accepted")
	require.Equal(t, atCap, got.ValidatedChainWork)
	require.Equal(t, atCapHash.String(), got.ValidatedBlockHash.String())
	require.Equal(t, uint32(100), got.ValidatedHeight)
}

// RecordValidatedPeerProgress must not credit validated work while the peer is inside an
// active delivery-failure penalty window; otherwise a header-only non-deliverer would
// re-accumulate work and regain top-tier eligibility the moment the window expires without
// ever delivering a block.
func TestCentralizedPeerRegistry_RecordValidatedPeerProgress_PenaltyWindowGatesCrediting(t *testing.T) {
	r := NewCentralizedPeerRegistry(DefaultBanConfig())

	// Peer inside an active penalty window: crediting must be refused (no error, advisory).
	r.Register(&PeerInfo{ID: "penalized", FullStoragePenaltyUntil: time.Now().Add(time.Hour)})
	require.NoError(t, r.RecordValidatedPeerProgress("penalized", 100, mustPeerRegistryHash("penalized-work"), []byte{0x05}))

	got, ok := r.Get("penalized")
	require.True(t, ok)
	require.Empty(t, got.ValidatedChainWork, "validated work must not be credited during an active penalty window")
	require.Nil(t, got.ValidatedBlockHash)
	require.Equal(t, uint32(0), got.ValidatedHeight)

	// Peer whose penalty window has just expired: crediting resumes.
	r.Register(&PeerInfo{ID: "expired", FullStoragePenaltyUntil: time.Now().Add(-time.Millisecond)})
	require.NoError(t, r.RecordValidatedPeerProgress("expired", 100, mustPeerRegistryHash("expired-work"), []byte{0x05}))

	got, ok = r.Get("expired")
	require.True(t, ok)
	require.Equal(t, []byte{0x05}, got.ValidatedChainWork, "crediting resumes once the penalty window has expired")
	require.Equal(t, uint32(100), got.ValidatedHeight)
}

// RecordValidatedPeerProgress rejects chainwork over the 128-byte cap on its own validation
// path (distinct from the Register/merge path covered above).
func TestCentralizedPeerRegistry_RecordValidatedPeerProgress_RejectsOverCapChainWork(t *testing.T) {
	r := NewCentralizedPeerRegistry(DefaultBanConfig())
	r.Register(&PeerInfo{ID: "p"})

	overCap := make([]byte, MaxValidatedChainWorkBytes+1)
	overCap[len(overCap)-1] = 0x01
	require.Error(t, r.RecordValidatedPeerProgress("p", 100, mustPeerRegistryHash("over"), overCap),
		"chainwork over the cap must be rejected")

	got, ok := r.Get("p")
	require.True(t, ok)
	require.Empty(t, got.ValidatedChainWork, "over-cap input must not advance validated progress")
	require.Nil(t, got.ValidatedBlockHash)
	require.Equal(t, uint32(0), got.ValidatedHeight)

	// Exactly at the cap is accepted, pinning the boundary.
	atCap := make([]byte, MaxValidatedChainWorkBytes)
	atCap[len(atCap)-1] = 0x01
	require.NoError(t, r.RecordValidatedPeerProgress("p", 100, mustPeerRegistryHash("at"), atCap))

	got, ok = r.Get("p")
	require.True(t, ok)
	require.Len(t, got.ValidatedChainWork, MaxValidatedChainWorkBytes, "exactly-128-byte chainwork must be accepted")
	require.Equal(t, uint32(100), got.ValidatedHeight)
}

func TestCentralizedPeerRegistry_Register_PenaltyAdvanceDecaysValidatedWork(t *testing.T) {
	r := NewCentralizedPeerRegistry(DefaultBanConfig())

	// Seed then merge so validated work is actually stored (the new-peer path
	// clones directly and would bypass the merge under test).
	r.Register(&PeerInfo{ID: "p"})
	workHash := mustPeerRegistryHash("validated-before-penalty")
	r.Register(&PeerInfo{
		ID:                 "p",
		ValidatedHeight:    100,
		ValidatedBlockHash: workHash,
		ValidatedChainWork: []byte{0x05},
	})

	got, ok := r.Get("p")
	require.True(t, ok)
	require.Equal(t, []byte{0x05}, got.ValidatedChainWork, "precondition: validated work is stored")

	// A penalty that advances the window decays the credited validated work.
	r.Register(&PeerInfo{ID: "p", FullStoragePenaltyUntil: time.Now().Add(time.Hour)})

	got, ok = r.Get("p")
	require.True(t, ok)
	require.True(t, got.FullStoragePenaltyUntil.After(time.Now()))
	require.Empty(t, got.ValidatedChainWork, "advancing penalty must decay validated work")
	require.Nil(t, got.ValidatedBlockHash)
	require.Equal(t, uint32(0), got.ValidatedHeight)

	// Idempotency: re-earn work, then re-send the SAME penalty timestamp — a
	// duplicate/late penalty must NOT re-wipe freshly re-earned work.
	penaltyTime := time.Now().Add(2 * time.Hour)
	r.Register(&PeerInfo{ID: "p", FullStoragePenaltyUntil: penaltyTime})

	reHash := mustPeerRegistryHash("validated-after-penalty")
	r.Register(&PeerInfo{
		ID:                 "p",
		ValidatedHeight:    120,
		ValidatedBlockHash: reHash,
		ValidatedChainWork: []byte{0x06},
	})

	got, ok = r.Get("p")
	require.True(t, ok)
	require.Equal(t, []byte{0x06}, got.ValidatedChainWork, "precondition: work re-earned")

	r.Register(&PeerInfo{ID: "p", FullStoragePenaltyUntil: penaltyTime})

	got, ok = r.Get("p")
	require.True(t, ok)
	require.Equal(t, []byte{0x06}, got.ValidatedChainWork, "same-timestamp penalty must not re-wipe re-earned work")
	require.Equal(t, reHash.String(), got.ValidatedBlockHash.String())
	require.Equal(t, uint32(120), got.ValidatedHeight)
}

func TestCentralizedPeerRegistry_Register_ConcurrentFullStorageContradictionsAreNotLost(t *testing.T) {
	r := NewCentralizedPeerRegistry(DefaultBanConfig())

	// Seed the peer so subsequent Registers hit the merge path (the new-peer
	// path clones directly and would not exercise the additive merge).
	r.Register(&PeerInfo{ID: "p"})

	const n = 200

	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			// Each caller sends a delta of 1; the registry adds it under r.mu.
			r.Register(&PeerInfo{ID: "p", FullStorageContradictions: 1})
		}()
	}
	wg.Wait()

	got, ok := r.Get("p")
	require.True(t, ok)
	require.Equal(t, int64(n), got.FullStorageContradictions, "concurrent increments must not be lost")
}

func TestCentralizedPeerRegistry_RecordValidatedPeerProgress_DeepCopiesInputs(t *testing.T) {
	r := NewCentralizedPeerRegistry(DefaultBanConfig())

	r.Register(&PeerInfo{ID: "p"})

	blockHash := mustPeerRegistryHash("validated-copy")
	chainWork := []byte{0x01, 0x02, 0x03}
	require.NoError(t, r.RecordValidatedPeerProgress("p", 10, blockHash, chainWork))

	blockHash[0] ^= 0xff
	chainWork[0] = 0xff

	got, ok := r.Get("p")
	require.True(t, ok)
	require.NotEqual(t, blockHash.String(), got.ValidatedBlockHash.String())
	require.Equal(t, []byte{0x01, 0x02, 0x03}, got.ValidatedChainWork)

	got.ValidatedBlockHash[1] ^= 0xff
	got.ValidatedChainWork[1] = 0xee

	again, ok := r.Get("p")
	require.True(t, ok)
	require.NotEqual(t, got.ValidatedBlockHash.String(), again.ValidatedBlockHash.String())
	require.Equal(t, []byte{0x01, 0x02, 0x03}, again.ValidatedChainWork)
}

func TestCentralizedPeerRegistry_StoragePenalty_BlocksFullRepromotion(t *testing.T) {
	r := NewCentralizedPeerRegistry(DefaultBanConfig())
	penaltyUntil := time.Now().Add(time.Hour)

	r.Register(&PeerInfo{
		ID:                        "p",
		Storage:                   "pruned",
		FullStorageContradictions: 1,
		FullStoragePenaltyUntil:   penaltyUntil,
	})

	r.UpdateStorage("p", "full")
	got, ok := r.Get("p")
	require.True(t, ok)
	require.Equal(t, "pruned", got.Storage)
	require.Equal(t, int64(1), got.FullStorageContradictions)
	require.True(t, got.FullStoragePenaltyUntil.Equal(penaltyUntil))

	earlierPenalty := penaltyUntil.Add(-30 * time.Minute)
	r.Register(&PeerInfo{ID: "p", FullStoragePenaltyUntil: earlierPenalty})
	got, ok = r.Get("p")
	require.True(t, ok)
	require.True(t, got.FullStoragePenaltyUntil.Equal(penaltyUntil), "stale re-registration must not shorten an active penalty")

	laterPenalty := penaltyUntil.Add(30 * time.Minute)
	r.Register(&PeerInfo{ID: "p", FullStoragePenaltyUntil: laterPenalty})
	got, ok = r.Get("p")
	require.True(t, ok)
	require.True(t, got.FullStoragePenaltyUntil.Equal(laterPenalty), "re-registration may extend a penalty")

	r.Register(&PeerInfo{ID: "p", Storage: "full"})
	got, ok = r.Get("p")
	require.True(t, ok)
	require.Equal(t, "pruned", got.Storage)

	r.Register(&PeerInfo{ID: "new-p", Storage: "full", FullStoragePenaltyUntil: penaltyUntil})
	newPeer, ok := r.Get("new-p")
	require.True(t, ok)
	require.NotEqual(t, "full", newPeer.Storage)

	r.mu.Lock()
	r.peers["p"].FullStoragePenaltyUntil = time.Now().Add(-time.Hour)
	r.mu.Unlock()

	r.UpdateStorage("p", "full")
	got, ok = r.Get("p")
	require.True(t, ok)
	require.Equal(t, "full", got.Storage)
}

func TestCentralizedPeerRegistry_UpdateStorage_FullClaimBlockedDuringPenalty(t *testing.T) {
	r := NewCentralizedPeerRegistry(DefaultBanConfig())
	penaltyUntil := time.Now().Add(time.Hour)

	r.Register(&PeerInfo{ID: "p", Storage: "pruned", FullStoragePenaltyUntil: penaltyUntil})
	r.UpdateStorage("p", "full")

	got, ok := r.Get("p")
	require.True(t, ok)
	require.Equal(t, "pruned", got.Storage)

	r.mu.Lock()
	r.peers["p"].FullStoragePenaltyUntil = time.Now().Add(-time.Minute)
	r.mu.Unlock()

	r.UpdateStorage("p", "full")
	got, ok = r.Get("p")
	require.True(t, ok)
	require.Equal(t, "full", got.Storage)
}

func TestCentralizedPeerRegistry_ResetReputation(t *testing.T) {
	r := NewCentralizedPeerRegistry(DefaultBanConfig())

	r.Register(&PeerInfo{ID: "a"})
	r.UpdateMetrics("a", 0, 0, 0, false, false, true, 0)
	r.RecordSyncAttempt("a")

	require.Equal(t, 1, r.ResetReputation("a"))
	got, _ := r.Get("a")
	require.Equal(t, 50.0, got.ReputationScore)
	require.Equal(t, int64(0), got.MaliciousCount)
	require.Equal(t, int32(0), got.SyncAttemptCount)

	// Reset all.
	r.Register(&PeerInfo{ID: "b"})
	r.UpdateMetrics("b", 0, 0, 0, false, false, true, 0)
	require.Equal(t, 2, r.ResetReputation(""))
}

func TestCentralizedPeerRegistry_Cleanup_TTL(t *testing.T) {
	r := NewCentralizedPeerRegistry(DefaultBanConfig())

	r.Register(&PeerInfo{ID: "fresh"})
	r.Register(&PeerInfo{ID: "stale"})

	// Backdate stale beyond TTL on both freshness fields (Cleanup looks at
	// LastSeen first, with LastMessageTime as fallback).
	r.mu.Lock()
	r.peers["stale"].LastSeen = time.Now().Add(-2 * time.Hour)
	r.peers["stale"].LastMessageTime = time.Now().Add(-2 * time.Hour)
	r.mu.Unlock()

	expired, lru := r.Cleanup(0, 1*time.Hour)
	require.Equal(t, 1, expired)
	require.Equal(t, 0, lru)
	require.Equal(t, 1, r.Count())
}

func TestCentralizedPeerRegistry_Cleanup_LRUExemptsConnectedAndBanned(t *testing.T) {
	r := NewCentralizedPeerRegistry(DefaultBanConfig())

	r.Register(&PeerInfo{ID: "connected"})
	r.UpdateConnectionState("connected", true)

	r.Register(&PeerInfo{ID: "banned"})
	r.AddBanScore("banned", "spam", 200)

	for _, id := range []string{"a", "b", "c", "d"} {
		r.Register(&PeerInfo{ID: id})
	}

	// maxSize=2 and 2 exempt → all 4 non-exempt evicted.
	expired, lru := r.Cleanup(2, 24*time.Hour)
	require.Equal(t, 0, expired)
	require.Equal(t, 4, lru)
	require.Equal(t, 2, r.Count())
}

func TestCentralizedPeerRegistry_List_ExcludeBannedAppliesExpiry(t *testing.T) {
	cfg := DefaultBanConfig()
	cfg.Duration = time.Millisecond
	r := NewCentralizedPeerRegistry(cfg)

	r.Register(&PeerInfo{ID: "p"})
	// spam = 50 in DefaultBanConfig; threshold = 100. Two strikes triggers a
	// ban that will expire almost immediately given the millisecond duration.
	r.AddBanScore("p", "spam", 0)
	r.AddBanScore("p", "spam", 0)

	got, _ := r.Get("p")
	require.True(t, got.IsBanned, "expected ban-on-threshold sync to PeerInfo")

	time.Sleep(5 * time.Millisecond)

	// List(excludeBanned=true) must run expiry inline so the now-expired peer
	// appears in the result and its PeerInfo flags are reset.
	peers := r.List(nil, 0, 0, true, false)
	require.Len(t, peers, 1)
	require.False(t, peers[0].IsBanned)
	require.Equal(t, int32(0), peers[0].BanScore)

	got, _ = r.Get("p")
	require.False(t, got.IsBanned)
}

func TestCentralizedPeerRegistry_RemovePreservesBanScore(t *testing.T) {
	r := NewCentralizedPeerRegistry(DefaultBanConfig())

	r.Register(&PeerInfo{ID: "p"})
	// spam = 50 in DefaultBanConfig; threshold = 100. Two strikes triggers a ban.
	r.AddBanScore("p", "spam", 0)
	r.AddBanScore("p", "spam", 0)
	require.True(t, r.IsBannedPeer("p"))

	// Remove should drop the peer entry but preserve the ban score, otherwise
	// a peer could clear its own ban by reconnecting.
	r.Remove("p")

	require.True(t, r.IsBannedPeer("p"), "ban must outlive Remove")
	require.Equal(t, []string{"p"}, r.ListBannedPeers())
}

func TestCentralizedPeerRegistry_Get_NormalisesExpiredBan(t *testing.T) {
	cfg := DefaultBanConfig()
	cfg.Duration = time.Millisecond
	r := NewCentralizedPeerRegistry(cfg)

	r.Register(&PeerInfo{ID: "p"})
	// Two strikes ban: 50 + 50 = 100 = threshold.
	r.AddBanScore("p", "spam", 0)
	r.AddBanScore("p", "spam", 0)
	got, _ := r.Get("p")
	require.True(t, got.IsBanned)

	time.Sleep(5 * time.Millisecond)

	// Get must run expireBansLocked so the snapshot it returns reflects
	// the elapsed ban window.
	got, _ = r.Get("p")
	require.False(t, got.IsBanned)
	require.Equal(t, int32(0), got.BanScore)
}

func TestCentralizedPeerRegistry_List_NormalisesExpiredBan(t *testing.T) {
	cfg := DefaultBanConfig()
	cfg.Duration = time.Millisecond
	r := NewCentralizedPeerRegistry(cfg)

	r.Register(&PeerInfo{ID: "p"})
	r.AddBanScore("p", "spam", 0)
	r.AddBanScore("p", "spam", 0)

	time.Sleep(5 * time.Millisecond)

	// excludeBanned=false used to skip expiry; the snapshot would still
	// carry IsBanned=true after the window elapsed.
	peers := r.List(nil, 0, 0, false, false)
	require.Len(t, peers, 1)
	require.False(t, peers[0].IsBanned)
}

func TestCentralizedPeerRegistry_ListBannedPeers_NormalisesExpiredBan(t *testing.T) {
	cfg := DefaultBanConfig()
	cfg.Duration = time.Millisecond
	r := NewCentralizedPeerRegistry(cfg)

	r.Register(&PeerInfo{ID: "p"})
	r.AddBanScore("p", "spam", 0)
	r.AddBanScore("p", "spam", 0)
	require.Equal(t, []string{"p"}, r.ListBannedPeers())

	time.Sleep(5 * time.Millisecond)
	require.Empty(t, r.ListBannedPeers(), "expired ban must be normalised before listing")
}

func TestCentralizedPeerRegistry_Cleanup_ExpiredBanIsNotExempt(t *testing.T) {
	cfg := DefaultBanConfig()
	cfg.Duration = time.Millisecond
	r := NewCentralizedPeerRegistry(cfg)

	r.Register(&PeerInfo{ID: "p"})
	r.AddBanScore("p", "spam", 0)
	r.AddBanScore("p", "spam", 0)
	require.True(t, r.IsBannedPeer("p"))

	// Backdate both freshness fields so TTL cleanup would normally kick in.
	r.mu.Lock()
	r.peers["p"].LastSeen = time.Now().Add(-2 * time.Hour)
	r.peers["p"].LastMessageTime = time.Now().Add(-2 * time.Hour)
	r.mu.Unlock()

	time.Sleep(5 * time.Millisecond)

	// Without expiry-on-cleanup, IsBanned would keep this peer exempt forever.
	expired, _ := r.Cleanup(0, time.Hour)
	require.Equal(t, 1, expired, "expired-ban peer must be evictable on cleanup")
	require.Equal(t, 0, r.Count())
}

func TestCentralizedPeerRegistry_Cleanup_LastSeenKeepsActivePeerAlive(t *testing.T) {
	r := NewCentralizedPeerRegistry(DefaultBanConfig())

	// Active peer: LastMessageTime stale (no recent gossip) but LastSeen
	// fresh (recent UpdateMetrics / RecordBlockReceived would do this).
	r.Register(&PeerInfo{ID: "active"})
	r.mu.Lock()
	r.peers["active"].LastMessageTime = time.Now().Add(-3 * time.Hour)
	r.peers["active"].LastSeen = time.Now()
	r.mu.Unlock()

	expired, _ := r.Cleanup(0, time.Hour)
	require.Equal(t, 0, expired, "active peer with fresh LastSeen must not be evicted")
	_, ok := r.Get("active")
	require.True(t, ok)
}

func TestCentralizedPeerRegistry_Cleanup_StaleLastSeenFreshLastMessageTime(t *testing.T) {
	// UpdateLastMessageTime refreshes only LastMessageTime. A peer that had a
	// non-zero LastSeen (from an earlier interaction) and then received fresh
	// last-message updates must still be considered active by Cleanup.
	r := NewCentralizedPeerRegistry(DefaultBanConfig())

	r.Register(&PeerInfo{ID: "p"})
	r.mu.Lock()
	r.peers["p"].LastSeen = time.Now().Add(-3 * time.Hour)
	r.peers["p"].LastMessageTime = time.Now()
	r.mu.Unlock()

	expired, _ := r.Cleanup(0, time.Hour)
	require.Equal(t, 0, expired,
		"newest activity timestamp wins; stale LastSeen must not evict a peer with fresh LastMessageTime")
	_, ok := r.Get("p")
	require.True(t, ok)
}

func TestCentralizedPeerRegistry_Cleanup_FallsBackToLastMessageTime(t *testing.T) {
	r := NewCentralizedPeerRegistry(DefaultBanConfig())

	// Pre-LastSeen-aware persisted record: LastSeen zero, LastMessageTime
	// recent. Cleanup must fall back to LastMessageTime so older state isn't
	// silently treated as stale.
	r.Register(&PeerInfo{ID: "old-record"})
	r.mu.Lock()
	r.peers["old-record"].LastSeen = time.Time{}
	r.peers["old-record"].LastMessageTime = time.Now()
	r.mu.Unlock()

	expired, _ := r.Cleanup(0, time.Hour)
	require.Equal(t, 0, expired, "fallback to LastMessageTime must keep recent older record")
}

func TestCentralizedPeerRegistry_StartCleanup_RunsAndStops(t *testing.T) {
	r := NewCentralizedPeerRegistry(DefaultBanConfig())

	// Register a peer with stale activity so the loop will evict it.
	// Cleanup now uses LastSeen as the canonical recency signal, so backdate
	// both fields to keep the test resilient against future tweaks.
	r.Register(&PeerInfo{ID: "stale"})
	r.mu.Lock()
	r.peers["stale"].LastSeen = time.Now().Add(-2 * time.Hour)
	r.peers["stale"].LastMessageTime = time.Now().Add(-2 * time.Hour)
	r.mu.Unlock()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	r.StartCleanup(ctx, 5*time.Millisecond, time.Hour, 0)

	require.Eventually(t, func() bool {
		return r.Count() == 0
	}, time.Second, 5*time.Millisecond, "cleanup loop must evict the stale peer")

	doneCh := make(chan struct{})
	go func() {
		r.Close()
		close(doneCh)
	}()

	select {
	case <-doneCh:
	case <-time.After(time.Second):
		t.Fatal("Close did not return — cleanup goroutine leaked")
	}
}

func TestCentralizedPeerRegistry_StartCleanup_ZeroIntervalIsNoOp(t *testing.T) {
	r := NewCentralizedPeerRegistry(DefaultBanConfig())
	require.NotPanics(t, func() {
		r.StartCleanup(context.Background(), 0, time.Hour, 0)
	})
	r.Close()
}

func TestCentralizedPeerRegistry_List_SortByStorage(t *testing.T) {
	r := NewCentralizedPeerRegistry(DefaultBanConfig())

	r.Register(&PeerInfo{ID: "pruned-high", Storage: "pruned"})
	r.Register(&PeerInfo{ID: "full-low", Storage: "full"})
	r.Register(&PeerInfo{ID: "unknown", Storage: ""})

	// Boost reputation of pruned-high to confirm storage takes precedence.
	r.UpdateMetrics("pruned-high", 0, 0, 0, true, false, false, 100)
	r.UpdateMetrics("pruned-high", 0, 0, 0, true, false, false, 100)

	peers := r.List(nil, 0, 0, false, true)
	require.Len(t, peers, 3)
	require.Equal(t, "full-low", peers[0].ID, "full storage must rank above pruned regardless of reputation")
	require.Equal(t, "pruned-high", peers[1].ID)
	require.Equal(t, "unknown", peers[2].ID)
}
