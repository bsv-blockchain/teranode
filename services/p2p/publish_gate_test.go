package p2p

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/blockchain/blockchain_api"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func newGateTestServer(t *testing.T, blockchainClient blockchain.ClientI) (*Server, *MockServerP2PClient) {
	t.Helper()

	initPrometheusMetrics()

	p2pClient := &MockServerP2PClient{}
	p2pClient.On("Publish", mock.Anything, mock.Anything, mock.Anything).Return(nil)

	return &Server{
		logger:              ulogger.TestLogger{},
		settings:            &settings.Settings{P2P: settings.P2PSettings{ListenMode: settings.ListenModeFull}},
		blockchainClient:    blockchainClient,
		P2PClient:           p2pClient,
		blockTopicName:      "test-block",
		subtreeTopicName:    "test-subtree",
		rejectedTxTopicName: "test-rejected-tx",
		nodeStatusTopicName: "test-node_status",
	}, p2pClient
}

func mockBlockchainInState(state blockchain_api.FSMStateType) *blockchain.Mock {
	client := &blockchain.Mock{}
	client.On("GetFSMCurrentState", mock.Anything).Return(&state, nil)

	return client
}

// TestOutboundTopicsAllowedCoversAllFSMStates fails loudly when a new FSM
// state is added without declaring its outbound allow-list.
func TestOutboundTopicsAllowedCoversAllFSMStates(t *testing.T) {
	for value, name := range blockchain_api.FSMStateType_name {
		state := blockchain_api.FSMStateType(value)

		allowed, ok := outboundTopicsAllowed[state]
		require.True(t, ok, "FSM state %s missing from outboundTopicsAllowed", name)
		require.True(t, allowed[topicKindNodeStatus], "node_status must stay allowed in FSM state %s so peers can track our height", name)
	}
}

// TestPublishGateIsOnlyPublishCallSite enforces the choke-point invariant:
// every outbound publish must go through publishToNetwork so the FSM
// allow-list applies. Limitations: it scans only this package (non-recursive)
// and matches the literal ".P2PClient.Publish(" - a call through a local
// alias (c := s.P2PClient; c.Publish(...)) or from another package slips
// through. A broader ".Publish(" match would false-positive on the Kafka
// producers in this package.
func TestPublishGateIsOnlyPublishCallSite(t *testing.T) {
	files, err := filepath.Glob("*.go")
	require.NoError(t, err)
	require.NotEmpty(t, files)

	for _, f := range files {
		if strings.HasSuffix(f, "_test.go") || f == "publish_gate.go" {
			continue
		}

		src, err := os.ReadFile(f)
		require.NoError(t, err)
		require.NotContains(t, string(src), ".P2PClient.Publish(",
			"%s calls P2PClient.Publish directly; route it through publishToNetwork so the FSM allow-list applies", f)
	}
}

func TestPublishToNetworkPerState(t *testing.T) {
	tests := []struct {
		state     blockchain_api.FSMStateType
		topic     string
		published bool
	}{
		{blockchain_api.FSMStateType_RUNNING, "test-block", true},
		{blockchain_api.FSMStateType_RUNNING, "test-subtree", true},
		{blockchain_api.FSMStateType_RUNNING, "test-rejected-tx", true},
		{blockchain_api.FSMStateType_RUNNING, "test-node_status", true},
		{blockchain_api.FSMStateType_CATCHINGBLOCKS, "test-block", false},
		{blockchain_api.FSMStateType_CATCHINGBLOCKS, "test-subtree", false},
		{blockchain_api.FSMStateType_CATCHINGBLOCKS, "test-rejected-tx", false},
		{blockchain_api.FSMStateType_CATCHINGBLOCKS, "test-node_status", true},
		// IDLE means "stopped" or "state unknown" (the blockchain client's
		// safety fallback): only node_status may go out.
		{blockchain_api.FSMStateType_IDLE, "test-block", false},
		{blockchain_api.FSMStateType_IDLE, "test-subtree", false},
		{blockchain_api.FSMStateType_IDLE, "test-rejected-tx", false},
		{blockchain_api.FSMStateType_IDLE, "test-node_status", true},
	}

	for _, tc := range tests {
		t.Run(tc.state.String()+"/"+tc.topic, func(t *testing.T) {
			server, p2pClient := newGateTestServer(t, mockBlockchainInState(tc.state))

			err := server.publishToNetwork(context.Background(), tc.topic, []byte("msg"))
			require.NoError(t, err)

			if tc.published {
				p2pClient.AssertCalled(t, "Publish", mock.Anything, tc.topic, []byte("msg"))
			} else {
				require.Empty(t, p2pClient.Calls, "expected no publish for %s in state %s", tc.topic, tc.state)
			}
		})
	}
}

// TestPublishToNetworkIncrementsBlockedCounter guards the fail-loud
// property: a drop must be visible in the prometheus counter, with the stage
// label separating expected pre-check skips from choke-point leaks.
func TestPublishToNetworkIncrementsBlockedCounter(t *testing.T) {
	server, _ := newGateTestServer(t, mockBlockchainInState(blockchain_api.FSMStateType_CATCHINGBLOCKS))

	chokepoint := prometheusP2PPublishBlocked.WithLabelValues("block", "CATCHINGBLOCKS", "chokepoint")
	precheck := prometheusP2PPublishBlocked.WithLabelValues("block", "CATCHINGBLOCKS", "precheck")
	chokepointBefore := testutil.ToFloat64(chokepoint)
	precheckBefore := testutil.ToFloat64(precheck)

	require.NoError(t, server.publishToNetwork(context.Background(), "test-block", []byte("msg")))
	require.Equal(t, chokepointBefore+1, testutil.ToFloat64(chokepoint))

	require.False(t, server.canSendToNetwork(context.Background(), topicKindBlock))
	require.Equal(t, precheckBefore+1, testutil.ToFloat64(precheck))
	require.Equal(t, chokepointBefore+1, testutil.ToFloat64(chokepoint), "pre-check skip must not count as a choke-point leak")
}

// TestPublishToNetworkUnknownTopicFailsClosed: a publish to a topic that was
// never registered in the allow-list is dropped even when RUNNING.
func TestPublishToNetworkUnknownTopicFailsClosed(t *testing.T) {
	server, p2pClient := newGateTestServer(t, mockBlockchainInState(blockchain_api.FSMStateType_RUNNING))

	err := server.publishToNetwork(context.Background(), "test-bogus", []byte("msg"))
	require.NoError(t, err)
	require.Empty(t, p2pClient.Calls)
}

// TestPublishToNetworkUnknownFSMStateFallsBackToNodeStatusOnly: a state
// added by a newer blockchain service must not silence node_status, and
// must not leak gossip.
func TestPublishToNetworkUnknownFSMStateFallsBackToNodeStatusOnly(t *testing.T) {
	server, p2pClient := newGateTestServer(t, mockBlockchainInState(blockchain_api.FSMStateType(99)))

	require.NoError(t, server.publishToNetwork(context.Background(), "test-block", []byte("msg")))
	require.Empty(t, p2pClient.Calls)

	require.NoError(t, server.publishToNetwork(context.Background(), "test-node_status", []byte("msg")))
	p2pClient.AssertCalled(t, "Publish", mock.Anything, "test-node_status", []byte("msg"))
}

// TestPublishToNetworkListenMode: the choke point enforces listen-mode
// policy centrally - silent publishes nothing, listen-only publishes only
// node_status.
func TestPublishToNetworkListenMode(t *testing.T) {
	tests := []struct {
		mode      string
		topic     string
		published bool
	}{
		{settings.ListenModeSilent, "test-block", false},
		{settings.ListenModeSilent, "test-node_status", false},
		{settings.ListenModeListenOnly, "test-block", false},
		{settings.ListenModeListenOnly, "test-node_status", true},
		{settings.ListenModeFull, "test-block", true},
	}

	for _, tc := range tests {
		t.Run(tc.mode+"/"+tc.topic, func(t *testing.T) {
			server, p2pClient := newGateTestServer(t, mockBlockchainInState(blockchain_api.FSMStateType_RUNNING))
			server.settings = &settings.Settings{P2P: settings.P2PSettings{ListenMode: tc.mode}}

			require.NoError(t, server.publishToNetwork(context.Background(), tc.topic, []byte("msg")))

			if tc.published {
				p2pClient.AssertCalled(t, "Publish", mock.Anything, tc.topic, []byte("msg"))
			} else {
				require.Empty(t, p2pClient.Calls)
			}
		})
	}
}

// TestPublishToNetworkFailsOpenOnStateError: a briefly unreachable
// blockchain service must not silence the node.
func TestPublishToNetworkFailsOpenOnStateError(t *testing.T) {
	client := &blockchain.Mock{}
	client.On("GetFSMCurrentState", mock.Anything).Return(nil, errors.NewServiceError("blockchain unavailable"))

	server, p2pClient := newGateTestServer(t, client)

	err := server.publishToNetwork(context.Background(), "test-block", []byte("msg"))
	require.NoError(t, err)
	p2pClient.AssertCalled(t, "Publish", mock.Anything, "test-block", []byte("msg"))
}

// TestPublishToNetworkNilBlockchainClient: setups without a blockchain
// client stay permissive, matching isBlockchainSyncingOrCatchingUp.
func TestPublishToNetworkNilBlockchainClient(t *testing.T) {
	server, p2pClient := newGateTestServer(t, nil)

	err := server.publishToNetwork(context.Background(), "test-block", []byte("msg"))
	require.NoError(t, err)
	p2pClient.AssertCalled(t, "Publish", mock.Anything, "test-block", []byte("msg"))
}

// TestPublishToNetworkWithLocalBlockchainClient exercises the gate against
// the real LocalClient (sqlitememory-backed) rather than a mock.
func TestPublishToNetworkWithLocalBlockchainClient(t *testing.T) {
	setup := SetupTestBlockchain(t)
	defer setup.Cleanup()

	server, p2pClient := newGateTestServer(t, setup.Client)

	err := server.publishToNetwork(context.Background(), "test-block", []byte("msg"))
	require.NoError(t, err)
	p2pClient.AssertCalled(t, "Publish", mock.Anything, "test-block", []byte("msg"))
}

func TestShouldSkipNotification(t *testing.T) {
	tests := []struct {
		state            blockchain_api.FSMStateType
		notificationType model.NotificationType
		skip             bool
	}{
		{blockchain_api.FSMStateType_CATCHINGBLOCKS, model.NotificationType_Block, true},
		{blockchain_api.FSMStateType_CATCHINGBLOCKS, model.NotificationType_Subtree, true},
		{blockchain_api.FSMStateType_CATCHINGBLOCKS, model.NotificationType_PeerFailure, false},
		// Types with no outbound announcement follow block gossip, preserving
		// the previous blanket skip during catchup.
		{blockchain_api.FSMStateType_CATCHINGBLOCKS, model.NotificationType_BlockSubtreesSet, true},
		{blockchain_api.FSMStateType_RUNNING, model.NotificationType_Block, false},
		{blockchain_api.FSMStateType_RUNNING, model.NotificationType_Subtree, false},
		{blockchain_api.FSMStateType_RUNNING, model.NotificationType_PeerFailure, false},
		{blockchain_api.FSMStateType_RUNNING, model.NotificationType_BlockSubtreesSet, false},
		{blockchain_api.FSMStateType_IDLE, model.NotificationType_Block, true},
		{blockchain_api.FSMStateType_IDLE, model.NotificationType_PeerFailure, false},
	}

	for _, tc := range tests {
		t.Run(tc.state.String()+"/"+tc.notificationType.String(), func(t *testing.T) {
			server, _ := newGateTestServer(t, mockBlockchainInState(tc.state))

			require.Equal(t, tc.skip, server.shouldSkipNotification(context.Background(), tc.notificationType))
		})
	}
}
