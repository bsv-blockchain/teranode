package p2p

import (
	"context"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/blockchain/blockchain_api"
	"github.com/bsv-blockchain/teranode/ulogger"
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
				p2pClient.AssertNotCalled(t, "Publish", mock.Anything, mock.Anything, mock.Anything)
			}
		})
	}
}

// TestPublishToNetworkUnknownTopicFailsClosed guards the fail-loud property:
// a publish to a topic that was never registered in the allow-list is
// dropped even when the node is RUNNING.
func TestPublishToNetworkUnknownTopicFailsClosed(t *testing.T) {
	server, p2pClient := newGateTestServer(t, mockBlockchainInState(blockchain_api.FSMStateType_RUNNING))

	err := server.publishToNetwork(context.Background(), "test-bogus", []byte("msg"))
	require.NoError(t, err)
	p2pClient.AssertNotCalled(t, "Publish", mock.Anything, mock.Anything, mock.Anything)
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

func TestShouldSkipNotification(t *testing.T) {
	tests := []struct {
		state            blockchain_api.FSMStateType
		notificationType model.NotificationType
		skip             bool
	}{
		{blockchain_api.FSMStateType_CATCHINGBLOCKS, model.NotificationType_Block, true},
		{blockchain_api.FSMStateType_CATCHINGBLOCKS, model.NotificationType_Subtree, true},
		{blockchain_api.FSMStateType_CATCHINGBLOCKS, model.NotificationType_PeerFailure, false},
		{blockchain_api.FSMStateType_RUNNING, model.NotificationType_Block, false},
		{blockchain_api.FSMStateType_RUNNING, model.NotificationType_Subtree, false},
		{blockchain_api.FSMStateType_RUNNING, model.NotificationType_PeerFailure, false},
	}

	for _, tc := range tests {
		t.Run(tc.state.String()+"/"+tc.notificationType.String(), func(t *testing.T) {
			server, _ := newGateTestServer(t, mockBlockchainInState(tc.state))

			require.Equal(t, tc.skip, server.shouldSkipNotification(context.Background(), tc.notificationType))
		})
	}
}

// TestCurrentFSMStateCache: with a non-zero TTL, repeated publishes reuse
// the cached FSM state instead of calling the blockchain service each time.
func TestCurrentFSMStateCache(t *testing.T) {
	client := mockBlockchainInState(blockchain_api.FSMStateType_RUNNING)
	server, _ := newGateTestServer(t, client)
	server.fsmStateTTL = time.Minute

	for range 3 {
		require.NoError(t, server.publishToNetwork(context.Background(), "test-block", []byte("msg")))
	}

	client.AssertNumberOfCalls(t, "GetFSMCurrentState", 1)
}
