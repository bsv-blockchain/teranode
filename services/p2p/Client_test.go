package p2p

import (
	"context"
	"testing"
	"time"

	"github.com/bitcoin-sv/teranode/services/p2p/p2p_api"
	"github.com/bitcoin-sv/teranode/ulogger"
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
	GetPeersFunc       func(ctx context.Context, in *emptypb.Empty, opts ...grpc.CallOption) (*p2p_api.GetPeersResponse, error)
	BanPeerFunc        func(ctx context.Context, in *p2p_api.BanPeerRequest, opts ...grpc.CallOption) (*p2p_api.BanPeerResponse, error)
	UnbanPeerFunc      func(ctx context.Context, in *p2p_api.UnbanPeerRequest, opts ...grpc.CallOption) (*p2p_api.UnbanPeerResponse, error)
	IsBannedFunc       func(ctx context.Context, in *p2p_api.IsBannedRequest, opts ...grpc.CallOption) (*p2p_api.IsBannedResponse, error)
	ListBannedFunc     func(ctx context.Context, in *emptypb.Empty, opts ...grpc.CallOption) (*p2p_api.ListBannedResponse, error)
	ClearBannedFunc    func(ctx context.Context, in *emptypb.Empty, opts ...grpc.CallOption) (*p2p_api.ClearBannedResponse, error)
	AddBanScoreFunc    func(ctx context.Context, in *p2p_api.AddBanScoreRequest, opts ...grpc.CallOption) (*p2p_api.AddBanScoreResponse, error)
	ConnectPeerFunc    func(ctx context.Context, in *p2p_api.ConnectPeerRequest, opts ...grpc.CallOption) (*p2p_api.ConnectPeerResponse, error)
	DisconnectPeerFunc func(ctx context.Context, in *p2p_api.DisconnectPeerRequest, opts ...grpc.CallOption) (*p2p_api.DisconnectPeerResponse, error)
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
	assert.Len(t, resp.Peers, 2)
	assert.Equal(t, "peer1", resp.Peers[0].Id)
	assert.Equal(t, "peer2", resp.Peers[1].Id)
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
	req := &p2p_api.BanPeerRequest{
		Addr:  "192.168.1.1",
		Until: 3600,
	}
	resp, err := client.BanPeer(ctx, req)
	assert.NoError(t, err)
	assert.NotNil(t, resp)
	assert.True(t, resp.Ok)
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
	req := &p2p_api.UnbanPeerRequest{
		Addr: "192.168.1.1",
	}
	resp, err := client.UnbanPeer(ctx, req)
	assert.NoError(t, err)
	assert.NotNil(t, resp)
	assert.True(t, resp.Ok)
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
	req := &p2p_api.IsBannedRequest{
		IpOrSubnet: "192.168.1.1",
	}
	resp, err := client.IsBanned(ctx, req)
	assert.NoError(t, err)
	assert.NotNil(t, resp)
	assert.True(t, resp.IsBanned)
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
	resp, err := client.ListBanned(ctx, &emptypb.Empty{})
	assert.NoError(t, err)
	assert.NotNil(t, resp)
	assert.Len(t, resp.Banned, 2)
	assert.Contains(t, resp.Banned, "192.168.1.1")
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
	resp, err := client.ClearBanned(ctx, &emptypb.Empty{})
	assert.NoError(t, err)
	assert.NotNil(t, resp)
	assert.True(t, resp.Ok)
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
	req := &p2p_api.AddBanScoreRequest{
		PeerId: "peer1",
		Reason: "spam",
	}
	resp, err := client.AddBanScore(ctx, req)
	assert.NoError(t, err)
	assert.NotNil(t, resp)
	assert.True(t, resp.Ok)
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
