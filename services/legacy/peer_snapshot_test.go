package legacy

import (
	"context"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

// fakeLegacyPeer is a legacyPeerStats stand-in. It exists so the field mapping
// can be verified without a live legacy server; every value it returns is the
// value a real peer accessor would return.
type fakeLegacyPeer struct {
	id             int32
	addr           string
	userAgent      string
	lastBlock      int32
	bytesSent      uint64
	bytesReceived  uint64
	lastRecv       time.Time
	inbound        bool
	version        uint32
	services       wire.ServiceFlag
	pingMicros     int64
	timeOffset     int64
	startingHeight int32
	timeConnected  time.Time
	streamPeer     bool
}

func (f *fakeLegacyPeer) ID() int32                  { return f.id }
func (f *fakeLegacyPeer) Addr() string               { return f.addr }
func (f *fakeLegacyPeer) UserAgent() string          { return f.userAgent }
func (f *fakeLegacyPeer) LastBlock() int32           { return f.lastBlock }
func (f *fakeLegacyPeer) BytesSent() uint64          { return f.bytesSent }
func (f *fakeLegacyPeer) BytesReceived() uint64      { return f.bytesReceived }
func (f *fakeLegacyPeer) LastRecv() time.Time        { return f.lastRecv }
func (f *fakeLegacyPeer) Inbound() bool              { return f.inbound }
func (f *fakeLegacyPeer) ProtocolVersion() uint32    { return f.version }
func (f *fakeLegacyPeer) Services() wire.ServiceFlag { return f.services }
func (f *fakeLegacyPeer) LastPingMicros() int64      { return f.pingMicros }
func (f *fakeLegacyPeer) TimeOffset() int64          { return f.timeOffset }
func (f *fakeLegacyPeer) StartingHeight() int32      { return f.startingHeight }
func (f *fakeLegacyPeer) TimeConnected() time.Time   { return f.timeConnected }
func (f *fakeLegacyPeer) IsStreamPeer() bool         { return f.streamPeer }

func sampleFakePeer() *fakeLegacyPeer {
	return &fakeLegacyPeer{
		id:             7,
		addr:           "203.0.113.7:8333",
		userAgent:      "/Bitcoin SV:1.0.16/",
		lastBlock:      912345,
		bytesSent:      111,
		bytesReceived:  222,
		lastRecv:       time.Unix(1750000500, 0).UTC(),
		inbound:        true,
		version:        70016,
		services:       wire.SFNodeNetwork | wire.SFNodeBloom,
		pingMicros:     42000,
		timeOffset:     -3,
		startingHeight: 912000,
		timeConnected:  time.Unix(1750000000, 0).UTC(),
	}
}

// TestPeerSnapshotFrom_MapsEveryField pins the mapping from legacy peer
// accessors onto the registry view. This is the field-for-field contract the
// dashboard renders.
func TestPeerSnapshotFrom_MapsEveryField(t *testing.T) {
	got, ok := peerSnapshotFrom(sampleFakePeer(), 7)
	require.True(t, ok)

	require.Equal(t, "legacy:203.0.113.7:8333", got.id)
	require.Equal(t, "203.0.113.7:8333", got.addr)
	require.Equal(t, "/Bitcoin SV:1.0.16/", got.userAgent)
	require.Equal(t, uint32(912345), got.height)
	require.Equal(t, uint64(111), got.bytesSent)
	require.Equal(t, uint64(222), got.bytesReceived)
	require.Equal(t, time.Unix(1750000500, 0).UTC(), got.lastRecv)

	require.Equal(t, blockchain.LegacyPeerInfo{
		Inbound:         true,
		ProtocolVersion: 70016,
		ServiceFlags:    uint64(wire.SFNodeNetwork | wire.SFNodeBloom),
		PingMicros:      42000,
		TimeOffsetSecs:  -3,
		StartingHeight:  912000,
		IsSyncPeer:      true,
		TimeConnected:   time.Unix(1750000000, 0).UTC(),
	}, got.legacy)
}

// TestPeerSnapshotFrom_SyncPeerFlag checks the sync badge only lights for the
// peer the legacy sync manager actually selected, and never when there is none.
func TestPeerSnapshotFrom_SyncPeerFlag(t *testing.T) {
	peer := sampleFakePeer()

	got, ok := peerSnapshotFrom(peer, 9)
	require.True(t, ok)
	require.False(t, got.legacy.IsSyncPeer, "a different sync peer must not badge this one")

	// Legacy peer IDs come from a counter starting at 1, so 0 means "no sync
	// peer" and must never match.
	peer.id = 0
	got, ok = peerSnapshotFrom(peer, 0)
	require.True(t, ok)
	require.False(t, got.legacy.IsSyncPeer, "no sync peer must not badge id 0")
}

// TestPeerSnapshotFrom_SkipsStreamPeers checks a secondary multistream peer
// produces no entry, so one connection yields one registry entry.
func TestPeerSnapshotFrom_SkipsStreamPeers(t *testing.T) {
	peer := sampleFakePeer()
	peer.streamPeer = true

	_, ok := peerSnapshotFrom(peer, 7)
	require.False(t, ok)
}

// TestPeerSnapshotFrom_SkipsUnusableAddress checks an address that cannot be
// split into host and port is rejected rather than registered under a broken ID.
func TestPeerSnapshotFrom_SkipsUnusableAddress(t *testing.T) {
	for _, addr := range []string{"", "not-an-address", "203.0.113.7"} {
		peer := sampleFakePeer()
		peer.addr = addr

		_, ok := peerSnapshotFrom(peer, 7)
		require.False(t, ok, "address %q must be rejected", addr)
	}
}

// TestPeerSnapshotFrom_NegativeLastBlockClampsToZero checks the int32 to uint32
// conversion cannot wrap. A legacy peer reports -1 before it announces a tip.
func TestPeerSnapshotFrom_NegativeLastBlockClampsToZero(t *testing.T) {
	peer := sampleFakePeer()
	peer.lastBlock = -1

	got, ok := peerSnapshotFrom(peer, 7)
	require.True(t, ok)
	require.Zero(t, got.height, "a negative last block must not wrap into a huge height")
}

// TestRun_ReconcilesImmediatelyAndStopsOnContextCancel checks the loop does not
// wait a full interval before its first reconcile, and that cancelling the
// context ends it.
func TestRun_ReconcilesImmediatelyAndStopsOnContextCancel(t *testing.T) {
	reg := blockchain.NewCentralizedPeerRegistry(blockchain.DefaultBanConfig())
	client := blockchain.NewLocalPeerRegistryClient(reg)

	tSettings := testSyncSettings()
	tSettings.Legacy.PeerRegistrySyncInterval = time.Hour // never fires during the test

	snap := testSnapshot("203.0.113.7:8333", 500, time.Unix(1750000100, 0))
	sync := newPeerRegistrySync(ulogger.TestLogger{}, tSettings, client,
		func() []peerSnapshot { return []peerSnapshot{snap} })

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})

	go func() {
		sync.run(ctx)
		close(done)
	}()

	// The up-front reconcile must land without waiting for the one-hour tick.
	require.Eventually(t, func() bool {
		_, ok := reg.Get("legacy:203.0.113.7:8333")
		return ok
	}, 2*time.Second, 5*time.Millisecond, "run must reconcile before its first tick")

	cancel()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("run did not stop after the context was cancelled")
	}
}

// TestLegacyPeerSnapshots_NilServerReturnsNil pins the nil-means-no-data
// contract the reconcile loop depends on: a nil return must never be read as
// "no peers connected".
func TestLegacyPeerSnapshots_NilServerReturnsNil(t *testing.T) {
	s := &Server{logger: ulogger.TestLogger{}}

	require.Nil(t, s.legacyPeerSnapshots(),
		"no internal server means no data, which must stay distinguishable from an empty slice")
}
