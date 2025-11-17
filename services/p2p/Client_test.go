package p2p

import (
	"context"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/services/p2p/p2p_api"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"
)

// MockGRPCClientConn is a mock gRPC connection for testing
type MockGRPCClientConn struct {
	grpc.ClientConnInterface
}

// MockPeerServiceClient is a mock implementation of p2p_api.PeerServiceClient
type MockPeerServiceClient struct {
	GetPeersFunc                func(ctx context.Context, in *emptypb.Empty, opts ...grpc.CallOption) (*p2p_api.GetPeersResponse, error)
	BanPeerFunc                 func(ctx context.Context, in *p2p_api.BanPeerRequest, opts ...grpc.CallOption) (*p2p_api.BanPeerResponse, error)
	UnbanPeerFunc               func(ctx context.Context, in *p2p_api.UnbanPeerRequest, opts ...grpc.CallOption) (*p2p_api.UnbanPeerResponse, error)
	IsBannedFunc                func(ctx context.Context, in *p2p_api.IsBannedRequest, opts ...grpc.CallOption) (*p2p_api.IsBannedResponse, error)
	ListBannedFunc              func(ctx context.Context, in *emptypb.Empty, opts ...grpc.CallOption) (*p2p_api.ListBannedResponse, error)
	ClearBannedFunc             func(ctx context.Context, in *emptypb.Empty, opts ...grpc.CallOption) (*p2p_api.ClearBannedResponse, error)
	AddBanScoreFunc             func(ctx context.Context, in *p2p_api.AddBanScoreRequest, opts ...grpc.CallOption) (*p2p_api.AddBanScoreResponse, error)
	ConnectPeerFunc             func(ctx context.Context, in *p2p_api.ConnectPeerRequest, opts ...grpc.CallOption) (*p2p_api.ConnectPeerResponse, error)
	DisconnectPeerFunc          func(ctx context.Context, in *p2p_api.DisconnectPeerRequest, opts ...grpc.CallOption) (*p2p_api.DisconnectPeerResponse, error)
	RecordCatchupAttemptFunc    func(ctx context.Context, in *p2p_api.RecordCatchupAttemptRequest, opts ...grpc.CallOption) (*p2p_api.RecordCatchupAttemptResponse, error)
	RecordCatchupSuccessFunc    func(ctx context.Context, in *p2p_api.RecordCatchupSuccessRequest, opts ...grpc.CallOption) (*p2p_api.RecordCatchupSuccessResponse, error)
	RecordCatchupFailureFunc    func(ctx context.Context, in *p2p_api.RecordCatchupFailureRequest, opts ...grpc.CallOption) (*p2p_api.RecordCatchupFailureResponse, error)
	RecordCatchupMaliciousFunc  func(ctx context.Context, in *p2p_api.RecordCatchupMaliciousRequest, opts ...grpc.CallOption) (*p2p_api.RecordCatchupMaliciousResponse, error)
	UpdateCatchupReputationFunc func(ctx context.Context, in *p2p_api.UpdateCatchupReputationRequest, opts ...grpc.CallOption) (*p2p_api.UpdateCatchupReputationResponse, error)
	UpdateCatchupErrorFunc      func(ctx context.Context, in *p2p_api.UpdateCatchupErrorRequest, opts ...grpc.CallOption) (*p2p_api.UpdateCatchupErrorResponse, error)
	GetPeersForCatchupFunc      func(ctx context.Context, in *p2p_api.GetPeersForCatchupRequest, opts ...grpc.CallOption) (*p2p_api.GetPeersForCatchupResponse, error)
	ReportValidSubtreeFunc      func(ctx context.Context, in *p2p_api.ReportValidSubtreeRequest, opts ...grpc.CallOption) (*p2p_api.ReportValidSubtreeResponse, error)
	ReportValidBlockFunc        func(ctx context.Context, in *p2p_api.ReportValidBlockRequest, opts ...grpc.CallOption) (*p2p_api.ReportValidBlockResponse, error)
	IsPeerMaliciousFunc         func(ctx context.Context, in *p2p_api.IsPeerMaliciousRequest, opts ...grpc.CallOption) (*p2p_api.IsPeerMaliciousResponse, error)
	IsPeerUnhealthyFunc         func(ctx context.Context, in *p2p_api.IsPeerUnhealthyRequest, opts ...grpc.CallOption) (*p2p_api.IsPeerUnhealthyResponse, error)
	GetPeerRegistryFunc         func(ctx context.Context, in *emptypb.Empty, opts ...grpc.CallOption) (*p2p_api.GetPeerRegistryResponse, error)
	GetPeerFunc                 func(ctx context.Context, in *p2p_api.GetPeerRequest, opts ...grpc.CallOption) (*p2p_api.GetPeerResponse, error)
}

func (m *MockPeerServiceClient) GetPeers(ctx context.Context, in *emptypb.Empty, opts ...grpc.CallOption) (*p2p_api.GetPeersResponse, error) {
	if m.GetPeersFunc != nil {
		return m.GetPeersFunc(ctx, in, opts...)
	}
	return nil, nil
}

func (m *MockPeerServiceClient) BanPeer(ctx context.Context, in *p2p_api.BanPeerRequest, opts ...grpc.CallOption) (*p2p_api.BanPeerResponse, error) {
	if m.BanPeerFunc != nil {
		return m.BanPeerFunc(ctx, in, opts...)
	}
	return nil, nil
}

func (m *MockPeerServiceClient) UnbanPeer(ctx context.Context, in *p2p_api.UnbanPeerRequest, opts ...grpc.CallOption) (*p2p_api.UnbanPeerResponse, error) {
	if m.UnbanPeerFunc != nil {
		return m.UnbanPeerFunc(ctx, in, opts...)
	}
	return nil, nil
}

func (m *MockPeerServiceClient) IsBanned(ctx context.Context, in *p2p_api.IsBannedRequest, opts ...grpc.CallOption) (*p2p_api.IsBannedResponse, error) {
	if m.IsBannedFunc != nil {
		return m.IsBannedFunc(ctx, in, opts...)
	}
	return nil, nil
}

func (m *MockPeerServiceClient) ListBanned(ctx context.Context, in *emptypb.Empty, opts ...grpc.CallOption) (*p2p_api.ListBannedResponse, error) {
	if m.ListBannedFunc != nil {
		return m.ListBannedFunc(ctx, in, opts...)
	}
	return nil, nil
}

func (m *MockPeerServiceClient) ClearBanned(ctx context.Context, in *emptypb.Empty, opts ...grpc.CallOption) (*p2p_api.ClearBannedResponse, error) {
	if m.ClearBannedFunc != nil {
		return m.ClearBannedFunc(ctx, in, opts...)
	}
	return nil, nil
}

func (m *MockPeerServiceClient) AddBanScore(ctx context.Context, in *p2p_api.AddBanScoreRequest, opts ...grpc.CallOption) (*p2p_api.AddBanScoreResponse, error) {
	if m.AddBanScoreFunc != nil {
		return m.AddBanScoreFunc(ctx, in, opts...)
	}
	return nil, nil
}

func (m *MockPeerServiceClient) ConnectPeer(ctx context.Context, in *p2p_api.ConnectPeerRequest, opts ...grpc.CallOption) (*p2p_api.ConnectPeerResponse, error) {
	if m.ConnectPeerFunc != nil {
		return m.ConnectPeerFunc(ctx, in, opts...)
	}
	return nil, nil
}

func (m *MockPeerServiceClient) DisconnectPeer(ctx context.Context, in *p2p_api.DisconnectPeerRequest, opts ...grpc.CallOption) (*p2p_api.DisconnectPeerResponse, error) {
	if m.DisconnectPeerFunc != nil {
		return m.DisconnectPeerFunc(ctx, in, opts...)
	}
	return nil, nil
}

func (m *MockPeerServiceClient) RecordCatchupAttempt(ctx context.Context, in *p2p_api.RecordCatchupAttemptRequest, opts ...grpc.CallOption) (*p2p_api.RecordCatchupAttemptResponse, error) {
	if m.RecordCatchupAttemptFunc != nil {
		return m.RecordCatchupAttemptFunc(ctx, in, opts...)
	}
	return &p2p_api.RecordCatchupAttemptResponse{Ok: true}, nil
}

func (m *MockPeerServiceClient) RecordCatchupSuccess(ctx context.Context, in *p2p_api.RecordCatchupSuccessRequest, opts ...grpc.CallOption) (*p2p_api.RecordCatchupSuccessResponse, error) {
	if m.RecordCatchupSuccessFunc != nil {
		return m.RecordCatchupSuccessFunc(ctx, in, opts...)
	}
	return &p2p_api.RecordCatchupSuccessResponse{Ok: true}, nil
}

func (m *MockPeerServiceClient) RecordCatchupFailure(ctx context.Context, in *p2p_api.RecordCatchupFailureRequest, opts ...grpc.CallOption) (*p2p_api.RecordCatchupFailureResponse, error) {
	if m.RecordCatchupFailureFunc != nil {
		return m.RecordCatchupFailureFunc(ctx, in, opts...)
	}
	return &p2p_api.RecordCatchupFailureResponse{Ok: true}, nil
}

func (m *MockPeerServiceClient) RecordCatchupMalicious(ctx context.Context, in *p2p_api.RecordCatchupMaliciousRequest, opts ...grpc.CallOption) (*p2p_api.RecordCatchupMaliciousResponse, error) {
	if m.RecordCatchupMaliciousFunc != nil {
		return m.RecordCatchupMaliciousFunc(ctx, in, opts...)
	}
	return &p2p_api.RecordCatchupMaliciousResponse{Ok: true}, nil
}

func (m *MockPeerServiceClient) UpdateCatchupReputation(ctx context.Context, in *p2p_api.UpdateCatchupReputationRequest, opts ...grpc.CallOption) (*p2p_api.UpdateCatchupReputationResponse, error) {
	if m.UpdateCatchupReputationFunc != nil {
		return m.UpdateCatchupReputationFunc(ctx, in, opts...)
	}
	return &p2p_api.UpdateCatchupReputationResponse{Ok: true}, nil
}

func (m *MockPeerServiceClient) UpdateCatchupError(ctx context.Context, in *p2p_api.UpdateCatchupErrorRequest, opts ...grpc.CallOption) (*p2p_api.UpdateCatchupErrorResponse, error) {
	if m.UpdateCatchupErrorFunc != nil {
		return m.UpdateCatchupErrorFunc(ctx, in, opts...)
	}
	return &p2p_api.UpdateCatchupErrorResponse{Ok: true}, nil
}

func (m *MockPeerServiceClient) GetPeersForCatchup(ctx context.Context, in *p2p_api.GetPeersForCatchupRequest, opts ...grpc.CallOption) (*p2p_api.GetPeersForCatchupResponse, error) {
	if m.GetPeersForCatchupFunc != nil {
		return m.GetPeersForCatchupFunc(ctx, in, opts...)
	}
	return &p2p_api.GetPeersForCatchupResponse{Peers: []*p2p_api.PeerInfoForCatchup{}}, nil
}

func (m *MockPeerServiceClient) ReportValidSubtree(ctx context.Context, in *p2p_api.ReportValidSubtreeRequest, opts ...grpc.CallOption) (*p2p_api.ReportValidSubtreeResponse, error) {
	if m.ReportValidSubtreeFunc != nil {
		return m.ReportValidSubtreeFunc(ctx, in, opts...)
	}
	return &p2p_api.ReportValidSubtreeResponse{Success: true}, nil
}

func (m *MockPeerServiceClient) ReportValidBlock(ctx context.Context, in *p2p_api.ReportValidBlockRequest, opts ...grpc.CallOption) (*p2p_api.ReportValidBlockResponse, error) {
	if m.ReportValidBlockFunc != nil {
		return m.ReportValidBlockFunc(ctx, in, opts...)
	}
	return &p2p_api.ReportValidBlockResponse{Success: true}, nil
}

func (m *MockPeerServiceClient) IsPeerMalicious(ctx context.Context, in *p2p_api.IsPeerMaliciousRequest, opts ...grpc.CallOption) (*p2p_api.IsPeerMaliciousResponse, error) {
	if m.IsPeerMaliciousFunc != nil {
		return m.IsPeerMaliciousFunc(ctx, in, opts...)
	}
	return &p2p_api.IsPeerMaliciousResponse{IsMalicious: false}, nil
}

func (m *MockPeerServiceClient) IsPeerUnhealthy(ctx context.Context, in *p2p_api.IsPeerUnhealthyRequest, opts ...grpc.CallOption) (*p2p_api.IsPeerUnhealthyResponse, error) {
	if m.IsPeerUnhealthyFunc != nil {
		return m.IsPeerUnhealthyFunc(ctx, in, opts...)
	}
	return &p2p_api.IsPeerUnhealthyResponse{IsUnhealthy: false, ReputationScore: 50.0}, nil
}

func (m *MockPeerServiceClient) GetPeerRegistry(ctx context.Context, in *emptypb.Empty, opts ...grpc.CallOption) (*p2p_api.GetPeerRegistryResponse, error) {
	if m.GetPeerRegistryFunc != nil {
		return m.GetPeerRegistryFunc(ctx, in, opts...)
	}
	return &p2p_api.GetPeerRegistryResponse{
		Peers: []*p2p_api.PeerRegistryInfo{},
	}, nil
}

func (m *MockPeerServiceClient) RecordBytesDownloaded(ctx context.Context, in *p2p_api.RecordBytesDownloadedRequest, opts ...grpc.CallOption) (*p2p_api.RecordBytesDownloadedResponse, error) {
	return &p2p_api.RecordBytesDownloadedResponse{Ok: true}, nil
}

func (m *MockPeerServiceClient) GetPeer(ctx context.Context, in *p2p_api.GetPeerRequest, opts ...grpc.CallOption) (*p2p_api.GetPeerResponse, error) {
	if m.GetPeerFunc != nil {
		return m.GetPeerFunc(ctx, in, opts...)
	}
	return &p2p_api.GetPeerResponse{
		Found: false,
	}, nil
}

func TestSimpleClientGetPeers(t *testing.T) {
	mockClient := &MockPeerServiceClient{
		GetPeersFunc: func(ctx context.Context, in *emptypb.Empty, opts ...grpc.CallOption) (*p2p_api.GetPeersResponse, error) {
			return &p2p_api.GetPeersResponse{
				Peers: []*p2p_api.Peer{
					{Id: "peer1", Addr: "/ip4/127.0.0.1/tcp/9905"},
					{Id: "peer2", Addr: "/ip4/127.0.0.2/tcp/9905"},
				},
			}, nil
		},
	}

	client := &Client{
		client: mockClient,
		logger: ulogger.New("test"),
	}

	ctx := context.Background()
	resp, err := client.GetPeers(ctx)
	assert.NoError(t, err)
	assert.NotNil(t, resp)
	// GetPeers now returns empty slice as it uses legacy format
	assert.Len(t, resp, 0)
}

func TestSimpleClientBanPeer(t *testing.T) {
	mockClient := &MockPeerServiceClient{
		BanPeerFunc: func(ctx context.Context, in *p2p_api.BanPeerRequest, opts ...grpc.CallOption) (*p2p_api.BanPeerResponse, error) {
			assert.Equal(t, "192.168.1.1", in.Addr)
			assert.Equal(t, int64(3600), in.Until)
			return &p2p_api.BanPeerResponse{Ok: true}, nil
		},
	}

	client := &Client{
		client: mockClient,
		logger: ulogger.New("test"),
	}

	ctx := context.Background()
	err := client.BanPeer(ctx, "192.168.1.1", 3600)
	assert.NoError(t, err)
}

func TestSimpleClientUnbanPeer(t *testing.T) {
	mockClient := &MockPeerServiceClient{
		UnbanPeerFunc: func(ctx context.Context, in *p2p_api.UnbanPeerRequest, opts ...grpc.CallOption) (*p2p_api.UnbanPeerResponse, error) {
			assert.Equal(t, "192.168.1.1", in.Addr)
			return &p2p_api.UnbanPeerResponse{Ok: true}, nil
		},
	}

	client := &Client{
		client: mockClient,
		logger: ulogger.New("test"),
	}

	ctx := context.Background()
	err := client.UnbanPeer(ctx, "192.168.1.1")
	assert.NoError(t, err)
}

func TestSimpleClientIsBanned(t *testing.T) {
	mockClient := &MockPeerServiceClient{
		IsBannedFunc: func(ctx context.Context, in *p2p_api.IsBannedRequest, opts ...grpc.CallOption) (*p2p_api.IsBannedResponse, error) {
			assert.Equal(t, "192.168.1.1", in.IpOrSubnet)
			return &p2p_api.IsBannedResponse{IsBanned: true}, nil
		},
	}

	client := &Client{
		client: mockClient,
		logger: ulogger.New("test"),
	}

	ctx := context.Background()
	isBanned, err := client.IsBanned(ctx, "192.168.1.1")
	assert.NoError(t, err)
	assert.True(t, isBanned)
}

func TestSimpleClientListBanned(t *testing.T) {
	mockClient := &MockPeerServiceClient{
		ListBannedFunc: func(ctx context.Context, in *emptypb.Empty, opts ...grpc.CallOption) (*p2p_api.ListBannedResponse, error) {
			return &p2p_api.ListBannedResponse{
				Banned: []string{"192.168.1.1", "192.168.1.2"},
			}, nil
		},
	}

	client := &Client{
		client: mockClient,
		logger: ulogger.New("test"),
	}

	ctx := context.Background()
	banned, err := client.ListBanned(ctx)
	assert.NoError(t, err)
	assert.NotNil(t, banned)
	assert.Len(t, banned, 2)
	assert.Contains(t, banned, "192.168.1.1")
}

func TestSimpleClientClearBanned(t *testing.T) {
	mockClient := &MockPeerServiceClient{
		ClearBannedFunc: func(ctx context.Context, in *emptypb.Empty, opts ...grpc.CallOption) (*p2p_api.ClearBannedResponse, error) {
			return &p2p_api.ClearBannedResponse{Ok: true}, nil
		},
	}

	client := &Client{
		client: mockClient,
		logger: ulogger.New("test"),
	}

	ctx := context.Background()
	err := client.ClearBanned(ctx)
	assert.NoError(t, err)
}

func TestSimpleClientAddBanScore(t *testing.T) {
	mockClient := &MockPeerServiceClient{
		AddBanScoreFunc: func(ctx context.Context, in *p2p_api.AddBanScoreRequest, opts ...grpc.CallOption) (*p2p_api.AddBanScoreResponse, error) {
			assert.Equal(t, "peer1", in.PeerId)
			assert.Equal(t, "spam", in.Reason)
			return &p2p_api.AddBanScoreResponse{Ok: true}, nil
		},
	}

	client := &Client{
		client: mockClient,
		logger: ulogger.New("test"),
	}

	ctx := context.Background()
	err := client.AddBanScore(ctx, "peer1", "spam")
	assert.NoError(t, err)
}

func TestSimpleClientConnectPeer(t *testing.T) {
	t.Run("Success", func(t *testing.T) {
		mockClient := &MockPeerServiceClient{
			ConnectPeerFunc: func(ctx context.Context, in *p2p_api.ConnectPeerRequest, opts ...grpc.CallOption) (*p2p_api.ConnectPeerResponse, error) {
				assert.Equal(t, "/ip4/127.0.0.1/tcp/9905", in.PeerAddress)
				return &p2p_api.ConnectPeerResponse{Success: true}, nil
			},
		}

		client := &Client{
			client: mockClient,
			logger: ulogger.New("test"),
		}

		ctx := context.Background()
		err := client.ConnectPeer(ctx, "/ip4/127.0.0.1/tcp/9905")
		assert.NoError(t, err)
	})

	t.Run("Failure", func(t *testing.T) {
		mockClient := &MockPeerServiceClient{
			ConnectPeerFunc: func(ctx context.Context, in *p2p_api.ConnectPeerRequest, opts ...grpc.CallOption) (*p2p_api.ConnectPeerResponse, error) {
				return &p2p_api.ConnectPeerResponse{Success: false, Error: "connection refused"}, nil
			},
		}

		client := &Client{
			client: mockClient,
			logger: ulogger.New("test"),
		}

		ctx := context.Background()
		err := client.ConnectPeer(ctx, "/ip4/127.0.0.1/tcp/9905")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "connection refused")
	})
}

func TestSimpleClientDisconnectPeer(t *testing.T) {
	t.Run("Success", func(t *testing.T) {
		mockClient := &MockPeerServiceClient{
			DisconnectPeerFunc: func(ctx context.Context, in *p2p_api.DisconnectPeerRequest, opts ...grpc.CallOption) (*p2p_api.DisconnectPeerResponse, error) {
				assert.Equal(t, "peer1", in.PeerId)
				return &p2p_api.DisconnectPeerResponse{Success: true}, nil
			},
		}

		client := &Client{
			client: mockClient,
			logger: ulogger.New("test"),
		}

		ctx := context.Background()
		err := client.DisconnectPeer(ctx, "peer1")
		assert.NoError(t, err)
	})

	t.Run("Failure", func(t *testing.T) {
		mockClient := &MockPeerServiceClient{
			DisconnectPeerFunc: func(ctx context.Context, in *p2p_api.DisconnectPeerRequest, opts ...grpc.CallOption) (*p2p_api.DisconnectPeerResponse, error) {
				return &p2p_api.DisconnectPeerResponse{Success: false, Error: "peer not found"}, nil
			},
		}

		client := &Client{
			client: mockClient,
			logger: ulogger.New("test"),
		}

		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		defer cancel()
		err := client.DisconnectPeer(ctx, "peer1")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "peer not found")
	})
}
