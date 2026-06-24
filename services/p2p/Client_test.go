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
	ResetReputationFunc         func(ctx context.Context, in *p2p_api.ResetReputationRequest, opts ...grpc.CallOption) (*p2p_api.ResetReputationResponse, error)
	GetPeersForCatchupFunc      func(ctx context.Context, in *p2p_api.GetPeersForCatchupRequest, opts ...grpc.CallOption) (*p2p_api.GetPeersForCatchupResponse, error)
	ReportValidSubtreeFunc      func(ctx context.Context, in *p2p_api.ReportValidSubtreeRequest, opts ...grpc.CallOption) (*p2p_api.ReportValidSubtreeResponse, error)
	ReportValidBlockFunc        func(ctx context.Context, in *p2p_api.ReportValidBlockRequest, opts ...grpc.CallOption) (*p2p_api.ReportValidBlockResponse, error)
	IsPeerMaliciousFunc         func(ctx context.Context, in *p2p_api.IsPeerMaliciousRequest, opts ...grpc.CallOption) (*p2p_api.IsPeerMaliciousResponse, error)
	IsPeerUnhealthyFunc         func(ctx context.Context, in *p2p_api.IsPeerUnhealthyRequest, opts ...grpc.CallOption) (*p2p_api.IsPeerUnhealthyResponse, error)
	GetPeerRegistryFunc         func(ctx context.Context, in *emptypb.Empty, opts ...grpc.CallOption) (*p2p_api.GetPeerRegistryResponse, error)
	GetPeerFunc                 func(ctx context.Context, in *p2p_api.GetPeerRequest, opts ...grpc.CallOption) (*p2p_api.GetPeerResponse, error)
	RecordBytesDownloadedFunc   func(ctx context.Context, in *p2p_api.RecordBytesDownloadedRequest, opts ...grpc.CallOption) (*p2p_api.RecordBytesDownloadedResponse, error)
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

func (m *MockPeerServiceClient) ResetReputation(ctx context.Context, in *p2p_api.ResetReputationRequest, opts ...grpc.CallOption) (*p2p_api.ResetReputationResponse, error) {
	if m.ResetReputationFunc != nil {
		return m.ResetReputationFunc(ctx, in, opts...)
	}
	return &p2p_api.ResetReputationResponse{Ok: true, PeersReset: 0}, nil
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
	if m.RecordBytesDownloadedFunc != nil {
		return m.RecordBytesDownloadedFunc(ctx, in, opts...)
	}
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

// --- Catchup-recording wrapper coverage ---
//
// Each Client.go wrapper has the same shape: build proto, call gRPC, propagate
// the gRPC error if any, return a service error when the response Ok flag is
// false, otherwise return nil. These tests cover the three exit paths per
// method, plus the data-shaping conversions in GetPeer/GetPeerRegistry/
// GetPeersForCatchup/ResetReputation.

func newClientWithMock(m *MockPeerServiceClient) *Client {
	return &Client{
		client: m,
		logger: ulogger.New("test"),
	}
}

func TestSimpleClientRecordCatchupAttempt(t *testing.T) {
	t.Run("ok", func(t *testing.T) {
		client := newClientWithMock(&MockPeerServiceClient{
			RecordCatchupAttemptFunc: func(ctx context.Context, in *p2p_api.RecordCatchupAttemptRequest, opts ...grpc.CallOption) (*p2p_api.RecordCatchupAttemptResponse, error) {
				assert.Equal(t, "peer1", in.PeerId)
				return &p2p_api.RecordCatchupAttemptResponse{Ok: true}, nil
			},
		})
		assert.NoError(t, client.RecordCatchupAttempt(context.Background(), "peer1"))
	})
	t.Run("grpc_error", func(t *testing.T) {
		client := newClientWithMock(&MockPeerServiceClient{
			RecordCatchupAttemptFunc: func(ctx context.Context, in *p2p_api.RecordCatchupAttemptRequest, opts ...grpc.CallOption) (*p2p_api.RecordCatchupAttemptResponse, error) {
				return nil, assert.AnError
			},
		})
		assert.ErrorIs(t, client.RecordCatchupAttempt(context.Background(), "peer1"), assert.AnError)
	})
	t.Run("not_ok", func(t *testing.T) {
		client := newClientWithMock(&MockPeerServiceClient{
			RecordCatchupAttemptFunc: func(ctx context.Context, in *p2p_api.RecordCatchupAttemptRequest, opts ...grpc.CallOption) (*p2p_api.RecordCatchupAttemptResponse, error) {
				return &p2p_api.RecordCatchupAttemptResponse{Ok: false}, nil
			},
		})
		err := client.RecordCatchupAttempt(context.Background(), "peer1")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to record catchup attempt")
	})
}

func TestSimpleClientRecordCatchupSuccess(t *testing.T) {
	t.Run("ok", func(t *testing.T) {
		client := newClientWithMock(&MockPeerServiceClient{
			RecordCatchupSuccessFunc: func(ctx context.Context, in *p2p_api.RecordCatchupSuccessRequest, opts ...grpc.CallOption) (*p2p_api.RecordCatchupSuccessResponse, error) {
				assert.Equal(t, "peer1", in.PeerId)
				assert.Equal(t, int64(1500), in.DurationMs)
				return &p2p_api.RecordCatchupSuccessResponse{Ok: true}, nil
			},
		})
		assert.NoError(t, client.RecordCatchupSuccess(context.Background(), "peer1", 1500))
	})
	t.Run("grpc_error", func(t *testing.T) {
		client := newClientWithMock(&MockPeerServiceClient{
			RecordCatchupSuccessFunc: func(ctx context.Context, in *p2p_api.RecordCatchupSuccessRequest, opts ...grpc.CallOption) (*p2p_api.RecordCatchupSuccessResponse, error) {
				return nil, assert.AnError
			},
		})
		assert.Error(t, client.RecordCatchupSuccess(context.Background(), "peer1", 0))
	})
	t.Run("not_ok", func(t *testing.T) {
		client := newClientWithMock(&MockPeerServiceClient{
			RecordCatchupSuccessFunc: func(ctx context.Context, in *p2p_api.RecordCatchupSuccessRequest, opts ...grpc.CallOption) (*p2p_api.RecordCatchupSuccessResponse, error) {
				return &p2p_api.RecordCatchupSuccessResponse{Ok: false}, nil
			},
		})
		err := client.RecordCatchupSuccess(context.Background(), "peer1", 0)
		assert.Contains(t, err.Error(), "failed to record catchup success")
	})
}

func TestSimpleClientRecordCatchupFailure(t *testing.T) {
	t.Run("ok", func(t *testing.T) {
		client := newClientWithMock(&MockPeerServiceClient{
			RecordCatchupFailureFunc: func(ctx context.Context, in *p2p_api.RecordCatchupFailureRequest, opts ...grpc.CallOption) (*p2p_api.RecordCatchupFailureResponse, error) {
				return &p2p_api.RecordCatchupFailureResponse{Ok: true}, nil
			},
		})
		assert.NoError(t, client.RecordCatchupFailure(context.Background(), "peer1"))
	})
	t.Run("grpc_error", func(t *testing.T) {
		client := newClientWithMock(&MockPeerServiceClient{
			RecordCatchupFailureFunc: func(ctx context.Context, in *p2p_api.RecordCatchupFailureRequest, opts ...grpc.CallOption) (*p2p_api.RecordCatchupFailureResponse, error) {
				return nil, assert.AnError
			},
		})
		assert.Error(t, client.RecordCatchupFailure(context.Background(), "peer1"))
	})
	t.Run("not_ok", func(t *testing.T) {
		client := newClientWithMock(&MockPeerServiceClient{
			RecordCatchupFailureFunc: func(ctx context.Context, in *p2p_api.RecordCatchupFailureRequest, opts ...grpc.CallOption) (*p2p_api.RecordCatchupFailureResponse, error) {
				return &p2p_api.RecordCatchupFailureResponse{Ok: false}, nil
			},
		})
		err := client.RecordCatchupFailure(context.Background(), "peer1")
		assert.Contains(t, err.Error(), "failed to record catchup failure")
	})
}

func TestSimpleClientRecordCatchupMalicious(t *testing.T) {
	t.Run("ok", func(t *testing.T) {
		client := newClientWithMock(&MockPeerServiceClient{
			RecordCatchupMaliciousFunc: func(ctx context.Context, in *p2p_api.RecordCatchupMaliciousRequest, opts ...grpc.CallOption) (*p2p_api.RecordCatchupMaliciousResponse, error) {
				return &p2p_api.RecordCatchupMaliciousResponse{Ok: true}, nil
			},
		})
		assert.NoError(t, client.RecordCatchupMalicious(context.Background(), "peer1"))
	})
	t.Run("grpc_error", func(t *testing.T) {
		client := newClientWithMock(&MockPeerServiceClient{
			RecordCatchupMaliciousFunc: func(ctx context.Context, in *p2p_api.RecordCatchupMaliciousRequest, opts ...grpc.CallOption) (*p2p_api.RecordCatchupMaliciousResponse, error) {
				return nil, assert.AnError
			},
		})
		assert.Error(t, client.RecordCatchupMalicious(context.Background(), "peer1"))
	})
	t.Run("not_ok", func(t *testing.T) {
		client := newClientWithMock(&MockPeerServiceClient{
			RecordCatchupMaliciousFunc: func(ctx context.Context, in *p2p_api.RecordCatchupMaliciousRequest, opts ...grpc.CallOption) (*p2p_api.RecordCatchupMaliciousResponse, error) {
				return &p2p_api.RecordCatchupMaliciousResponse{Ok: false}, nil
			},
		})
		err := client.RecordCatchupMalicious(context.Background(), "peer1")
		assert.Contains(t, err.Error(), "failed to record catchup malicious")
	})
}

func TestSimpleClientUpdateCatchupError(t *testing.T) {
	t.Run("ok", func(t *testing.T) {
		client := newClientWithMock(&MockPeerServiceClient{
			UpdateCatchupErrorFunc: func(ctx context.Context, in *p2p_api.UpdateCatchupErrorRequest, opts ...grpc.CallOption) (*p2p_api.UpdateCatchupErrorResponse, error) {
				assert.Equal(t, "peer1", in.PeerId)
				assert.Equal(t, "boom", in.ErrorMsg)
				return &p2p_api.UpdateCatchupErrorResponse{Ok: true}, nil
			},
		})
		assert.NoError(t, client.UpdateCatchupError(context.Background(), "peer1", "boom"))
	})
	t.Run("grpc_error", func(t *testing.T) {
		client := newClientWithMock(&MockPeerServiceClient{
			UpdateCatchupErrorFunc: func(ctx context.Context, in *p2p_api.UpdateCatchupErrorRequest, opts ...grpc.CallOption) (*p2p_api.UpdateCatchupErrorResponse, error) {
				return nil, assert.AnError
			},
		})
		assert.Error(t, client.UpdateCatchupError(context.Background(), "peer1", "boom"))
	})
	t.Run("not_ok", func(t *testing.T) {
		client := newClientWithMock(&MockPeerServiceClient{
			UpdateCatchupErrorFunc: func(ctx context.Context, in *p2p_api.UpdateCatchupErrorRequest, opts ...grpc.CallOption) (*p2p_api.UpdateCatchupErrorResponse, error) {
				return &p2p_api.UpdateCatchupErrorResponse{Ok: false}, nil
			},
		})
		err := client.UpdateCatchupError(context.Background(), "peer1", "boom")
		assert.Contains(t, err.Error(), "failed to update catchup error")
	})
}

func TestSimpleClientUpdateCatchupReputation(t *testing.T) {
	t.Run("ok", func(t *testing.T) {
		client := newClientWithMock(&MockPeerServiceClient{
			UpdateCatchupReputationFunc: func(ctx context.Context, in *p2p_api.UpdateCatchupReputationRequest, opts ...grpc.CallOption) (*p2p_api.UpdateCatchupReputationResponse, error) {
				assert.InDelta(t, 75.0, in.Score, 0.001)
				return &p2p_api.UpdateCatchupReputationResponse{Ok: true}, nil
			},
		})
		assert.NoError(t, client.UpdateCatchupReputation(context.Background(), "peer1", 75.0))
	})
	t.Run("grpc_error", func(t *testing.T) {
		client := newClientWithMock(&MockPeerServiceClient{
			UpdateCatchupReputationFunc: func(ctx context.Context, in *p2p_api.UpdateCatchupReputationRequest, opts ...grpc.CallOption) (*p2p_api.UpdateCatchupReputationResponse, error) {
				return nil, assert.AnError
			},
		})
		assert.Error(t, client.UpdateCatchupReputation(context.Background(), "peer1", 75.0))
	})
	t.Run("not_ok", func(t *testing.T) {
		client := newClientWithMock(&MockPeerServiceClient{
			UpdateCatchupReputationFunc: func(ctx context.Context, in *p2p_api.UpdateCatchupReputationRequest, opts ...grpc.CallOption) (*p2p_api.UpdateCatchupReputationResponse, error) {
				return &p2p_api.UpdateCatchupReputationResponse{Ok: false}, nil
			},
		})
		err := client.UpdateCatchupReputation(context.Background(), "peer1", 75.0)
		assert.Contains(t, err.Error(), "failed to update catchup reputation")
	})
}

func TestSimpleClientResetReputation(t *testing.T) {
	t.Run("ok_specific_peer", func(t *testing.T) {
		client := newClientWithMock(&MockPeerServiceClient{
			ResetReputationFunc: func(ctx context.Context, in *p2p_api.ResetReputationRequest, opts ...grpc.CallOption) (*p2p_api.ResetReputationResponse, error) {
				assert.Equal(t, "peer1", in.PeerId)
				return &p2p_api.ResetReputationResponse{Ok: true, PeersReset: 1}, nil
			},
		})
		n, err := client.ResetReputation(context.Background(), "peer1")
		assert.NoError(t, err)
		assert.Equal(t, 1, n)
	})
	t.Run("ok_all_peers", func(t *testing.T) {
		client := newClientWithMock(&MockPeerServiceClient{
			ResetReputationFunc: func(ctx context.Context, in *p2p_api.ResetReputationRequest, opts ...grpc.CallOption) (*p2p_api.ResetReputationResponse, error) {
				return &p2p_api.ResetReputationResponse{Ok: true, PeersReset: 7}, nil
			},
		})
		n, err := client.ResetReputation(context.Background(), "")
		assert.NoError(t, err)
		assert.Equal(t, 7, n)
	})
	t.Run("grpc_error", func(t *testing.T) {
		client := newClientWithMock(&MockPeerServiceClient{
			ResetReputationFunc: func(ctx context.Context, in *p2p_api.ResetReputationRequest, opts ...grpc.CallOption) (*p2p_api.ResetReputationResponse, error) {
				return nil, assert.AnError
			},
		})
		_, err := client.ResetReputation(context.Background(), "peer1")
		assert.Error(t, err)
	})
	t.Run("not_ok", func(t *testing.T) {
		client := newClientWithMock(&MockPeerServiceClient{
			ResetReputationFunc: func(ctx context.Context, in *p2p_api.ResetReputationRequest, opts ...grpc.CallOption) (*p2p_api.ResetReputationResponse, error) {
				return &p2p_api.ResetReputationResponse{Ok: false}, nil
			},
		})
		_, err := client.ResetReputation(context.Background(), "peer1")
		assert.Contains(t, err.Error(), "failed to reset reputation")
	})
}

func TestSimpleClientGetPeersForCatchup(t *testing.T) {
	t.Run("ok", func(t *testing.T) {
		client := newClientWithMock(&MockPeerServiceClient{
			GetPeersForCatchupFunc: func(ctx context.Context, in *p2p_api.GetPeersForCatchupRequest, opts ...grpc.CallOption) (*p2p_api.GetPeersForCatchupResponse, error) {
				return &p2p_api.GetPeersForCatchupResponse{
					Peers: []*p2p_api.PeerInfoForCatchup{
						{
							Id:                     "12D3KooWBhWMmHCXuyfM48dEPRsBzkemQQu71yC9rR2zHGmAjzQz",
							Height:                 42,
							CatchupReputationScore: 88.5,
							CatchupAttempts:        3,
							CatchupSuccesses:       2,
							CatchupFailures:        1,
						},
					},
				}, nil
			},
		})
		peers, err := client.GetPeersForCatchup(context.Background())
		assert.NoError(t, err)
		assert.Len(t, peers, 1)
		assert.Equal(t, uint32(42), peers[0].Height)
		assert.InDelta(t, 88.5, peers[0].ReputationScore, 0.001)
		assert.Equal(t, int64(3), peers[0].InteractionAttempts)
	})
	t.Run("grpc_error", func(t *testing.T) {
		client := newClientWithMock(&MockPeerServiceClient{
			GetPeersForCatchupFunc: func(ctx context.Context, in *p2p_api.GetPeersForCatchupRequest, opts ...grpc.CallOption) (*p2p_api.GetPeersForCatchupResponse, error) {
				return nil, assert.AnError
			},
		})
		_, err := client.GetPeersForCatchup(context.Background())
		assert.Error(t, err)
	})
}

func TestSimpleClientReportValidSubtree(t *testing.T) {
	t.Run("ok", func(t *testing.T) {
		client := newClientWithMock(&MockPeerServiceClient{
			ReportValidSubtreeFunc: func(ctx context.Context, in *p2p_api.ReportValidSubtreeRequest, opts ...grpc.CallOption) (*p2p_api.ReportValidSubtreeResponse, error) {
				assert.Equal(t, "peer1", in.PeerId)
				assert.Equal(t, "subtreehash", in.SubtreeHash)
				return &p2p_api.ReportValidSubtreeResponse{Success: true}, nil
			},
		})
		assert.NoError(t, client.ReportValidSubtree(context.Background(), "peer1", "subtreehash"))
	})
	t.Run("grpc_error", func(t *testing.T) {
		client := newClientWithMock(&MockPeerServiceClient{
			ReportValidSubtreeFunc: func(ctx context.Context, in *p2p_api.ReportValidSubtreeRequest, opts ...grpc.CallOption) (*p2p_api.ReportValidSubtreeResponse, error) {
				return nil, assert.AnError
			},
		})
		assert.Error(t, client.ReportValidSubtree(context.Background(), "peer1", "h"))
	})
	t.Run("not_success", func(t *testing.T) {
		client := newClientWithMock(&MockPeerServiceClient{
			ReportValidSubtreeFunc: func(ctx context.Context, in *p2p_api.ReportValidSubtreeRequest, opts ...grpc.CallOption) (*p2p_api.ReportValidSubtreeResponse, error) {
				return &p2p_api.ReportValidSubtreeResponse{Success: false, Message: "rejected"}, nil
			},
		})
		err := client.ReportValidSubtree(context.Background(), "peer1", "h")
		assert.Contains(t, err.Error(), "rejected")
	})
}

func TestSimpleClientReportValidBlock(t *testing.T) {
	t.Run("ok", func(t *testing.T) {
		client := newClientWithMock(&MockPeerServiceClient{
			ReportValidBlockFunc: func(ctx context.Context, in *p2p_api.ReportValidBlockRequest, opts ...grpc.CallOption) (*p2p_api.ReportValidBlockResponse, error) {
				return &p2p_api.ReportValidBlockResponse{Success: true}, nil
			},
		})
		assert.NoError(t, client.ReportValidBlock(context.Background(), "peer1", "blockhash"))
	})
	t.Run("grpc_error", func(t *testing.T) {
		client := newClientWithMock(&MockPeerServiceClient{
			ReportValidBlockFunc: func(ctx context.Context, in *p2p_api.ReportValidBlockRequest, opts ...grpc.CallOption) (*p2p_api.ReportValidBlockResponse, error) {
				return nil, assert.AnError
			},
		})
		assert.Error(t, client.ReportValidBlock(context.Background(), "peer1", "h"))
	})
	t.Run("not_success", func(t *testing.T) {
		client := newClientWithMock(&MockPeerServiceClient{
			ReportValidBlockFunc: func(ctx context.Context, in *p2p_api.ReportValidBlockRequest, opts ...grpc.CallOption) (*p2p_api.ReportValidBlockResponse, error) {
				return &p2p_api.ReportValidBlockResponse{Success: false, Message: "stale"}, nil
			},
		})
		err := client.ReportValidBlock(context.Background(), "peer1", "h")
		assert.Contains(t, err.Error(), "stale")
	})
}

func TestSimpleClientIsPeerMalicious(t *testing.T) {
	t.Run("ok", func(t *testing.T) {
		client := newClientWithMock(&MockPeerServiceClient{
			IsPeerMaliciousFunc: func(ctx context.Context, in *p2p_api.IsPeerMaliciousRequest, opts ...grpc.CallOption) (*p2p_api.IsPeerMaliciousResponse, error) {
				return &p2p_api.IsPeerMaliciousResponse{IsMalicious: true, Reason: "spam"}, nil
			},
		})
		mal, reason, err := client.IsPeerMalicious(context.Background(), "peer1")
		assert.NoError(t, err)
		assert.True(t, mal)
		assert.Equal(t, "spam", reason)
	})
	t.Run("grpc_error", func(t *testing.T) {
		client := newClientWithMock(&MockPeerServiceClient{
			IsPeerMaliciousFunc: func(ctx context.Context, in *p2p_api.IsPeerMaliciousRequest, opts ...grpc.CallOption) (*p2p_api.IsPeerMaliciousResponse, error) {
				return nil, assert.AnError
			},
		})
		_, _, err := client.IsPeerMalicious(context.Background(), "peer1")
		assert.Error(t, err)
	})
}

func TestSimpleClientIsPeerUnhealthy(t *testing.T) {
	t.Run("ok", func(t *testing.T) {
		client := newClientWithMock(&MockPeerServiceClient{
			IsPeerUnhealthyFunc: func(ctx context.Context, in *p2p_api.IsPeerUnhealthyRequest, opts ...grpc.CallOption) (*p2p_api.IsPeerUnhealthyResponse, error) {
				return &p2p_api.IsPeerUnhealthyResponse{IsUnhealthy: true, Reason: "low rep", ReputationScore: 12.5}, nil
			},
		})
		unhealthy, reason, score, err := client.IsPeerUnhealthy(context.Background(), "peer1")
		assert.NoError(t, err)
		assert.True(t, unhealthy)
		assert.Equal(t, "low rep", reason)
		assert.InDelta(t, 12.5, score, 0.001)
	})
	t.Run("grpc_error", func(t *testing.T) {
		client := newClientWithMock(&MockPeerServiceClient{
			IsPeerUnhealthyFunc: func(ctx context.Context, in *p2p_api.IsPeerUnhealthyRequest, opts ...grpc.CallOption) (*p2p_api.IsPeerUnhealthyResponse, error) {
				return nil, assert.AnError
			},
		})
		_, _, _, err := client.IsPeerUnhealthy(context.Background(), "peer1")
		assert.Error(t, err)
	})
}

func TestSimpleClientGetPeerRegistry(t *testing.T) {
	t.Run("ok", func(t *testing.T) {
		client := newClientWithMock(&MockPeerServiceClient{
			GetPeerRegistryFunc: func(ctx context.Context, in *emptypb.Empty, opts ...grpc.CallOption) (*p2p_api.GetPeerRegistryResponse, error) {
				return &p2p_api.GetPeerRegistryResponse{
					Peers: []*p2p_api.PeerRegistryInfo{
						{Id: "12D3KooWBhWMmHCXuyfM48dEPRsBzkemQQu71yC9rR2zHGmAjzQz", Height: 99, IsConnected: true},
					},
				}, nil
			},
		})
		peers, err := client.GetPeerRegistry(context.Background())
		assert.NoError(t, err)
		assert.Len(t, peers, 1)
		assert.Equal(t, uint32(99), peers[0].Height)
	})
	t.Run("grpc_error", func(t *testing.T) {
		client := newClientWithMock(&MockPeerServiceClient{
			GetPeerRegistryFunc: func(ctx context.Context, in *emptypb.Empty, opts ...grpc.CallOption) (*p2p_api.GetPeerRegistryResponse, error) {
				return nil, assert.AnError
			},
		})
		_, err := client.GetPeerRegistry(context.Background())
		assert.Error(t, err)
	})
}

func TestSimpleClientRecordBytesDownloaded(t *testing.T) {
	t.Run("ok", func(t *testing.T) {
		client := newClientWithMock(&MockPeerServiceClient{
			RecordBytesDownloadedFunc: func(ctx context.Context, in *p2p_api.RecordBytesDownloadedRequest, opts ...grpc.CallOption) (*p2p_api.RecordBytesDownloadedResponse, error) {
				assert.Equal(t, uint64(2048), in.BytesDownloaded)
				return &p2p_api.RecordBytesDownloadedResponse{Ok: true}, nil
			},
		})
		assert.NoError(t, client.RecordBytesDownloaded(context.Background(), "peer1", 2048))
	})
	t.Run("grpc_error", func(t *testing.T) {
		client := newClientWithMock(&MockPeerServiceClient{
			RecordBytesDownloadedFunc: func(ctx context.Context, in *p2p_api.RecordBytesDownloadedRequest, opts ...grpc.CallOption) (*p2p_api.RecordBytesDownloadedResponse, error) {
				return nil, assert.AnError
			},
		})
		assert.Error(t, client.RecordBytesDownloaded(context.Background(), "peer1", 0))
	})
	t.Run("not_ok", func(t *testing.T) {
		client := newClientWithMock(&MockPeerServiceClient{
			RecordBytesDownloadedFunc: func(ctx context.Context, in *p2p_api.RecordBytesDownloadedRequest, opts ...grpc.CallOption) (*p2p_api.RecordBytesDownloadedResponse, error) {
				return &p2p_api.RecordBytesDownloadedResponse{Ok: false}, nil
			},
		})
		err := client.RecordBytesDownloaded(context.Background(), "peer1", 0)
		assert.Contains(t, err.Error(), "failed to record bytes downloaded")
	})
}

func TestSimpleClientGetPeer(t *testing.T) {
	t.Run("found", func(t *testing.T) {
		client := newClientWithMock(&MockPeerServiceClient{
			GetPeerFunc: func(ctx context.Context, in *p2p_api.GetPeerRequest, opts ...grpc.CallOption) (*p2p_api.GetPeerResponse, error) {
				assert.Equal(t, "peer1", in.PeerId)
				return &p2p_api.GetPeerResponse{
					Found: true,
					Peer: &p2p_api.PeerRegistryInfo{
						Id:     "12D3KooWBhWMmHCXuyfM48dEPRsBzkemQQu71yC9rR2zHGmAjzQz",
						Height: 17,
					},
				}, nil
			},
		})
		info, err := client.GetPeer(context.Background(), "peer1")
		assert.NoError(t, err)
		assert.NotNil(t, info)
		assert.Equal(t, uint32(17), info.Height)
	})
	t.Run("not_found", func(t *testing.T) {
		client := newClientWithMock(&MockPeerServiceClient{
			GetPeerFunc: func(ctx context.Context, in *p2p_api.GetPeerRequest, opts ...grpc.CallOption) (*p2p_api.GetPeerResponse, error) {
				return &p2p_api.GetPeerResponse{Found: false}, nil
			},
		})
		info, err := client.GetPeer(context.Background(), "peer1")
		assert.NoError(t, err)
		assert.Nil(t, info, "not-found should return nil PeerInfo with no error")
	})
	t.Run("grpc_error", func(t *testing.T) {
		client := newClientWithMock(&MockPeerServiceClient{
			GetPeerFunc: func(ctx context.Context, in *p2p_api.GetPeerRequest, opts ...grpc.CallOption) (*p2p_api.GetPeerResponse, error) {
				return nil, assert.AnError
			},
		})
		_, err := client.GetPeer(context.Background(), "peer1")
		assert.Error(t, err)
	})
}
