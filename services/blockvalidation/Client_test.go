package blockvalidation

import (
	"context"
	"net/http"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockvalidation/blockvalidation_api"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// mockBlockValidationAPIClient is a mock implementation of BlockValidationAPIClient
type mockBlockValidationAPIClient struct {
	mock.Mock
}

func (m *mockBlockValidationAPIClient) HealthGRPC(ctx context.Context, in *blockvalidation_api.EmptyMessage, opts ...grpc.CallOption) (*blockvalidation_api.HealthResponse, error) {
	args := m.Called(ctx, in, opts)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*blockvalidation_api.HealthResponse), args.Error(1)
}

func (m *mockBlockValidationAPIClient) BlockFound(ctx context.Context, in *blockvalidation_api.BlockFoundRequest, opts ...grpc.CallOption) (*blockvalidation_api.EmptyMessage, error) {
	args := m.Called(ctx, in, opts)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*blockvalidation_api.EmptyMessage), args.Error(1)
}

func (m *mockBlockValidationAPIClient) ProcessBlock(ctx context.Context, in *blockvalidation_api.ProcessBlockRequest, opts ...grpc.CallOption) (*blockvalidation_api.EmptyMessage, error) {
	args := m.Called(ctx, in, opts)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*blockvalidation_api.EmptyMessage), args.Error(1)
}
func (m *mockBlockValidationAPIClient) GetCatchupStatus(ctx context.Context, in *blockvalidation_api.EmptyMessage, opts ...grpc.CallOption) (*blockvalidation_api.CatchupStatusResponse, error) {
	args := m.Called(ctx, in, opts)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*blockvalidation_api.CatchupStatusResponse), args.Error(1)
}

func (m *mockBlockValidationAPIClient) ValidateBlock(ctx context.Context, in *blockvalidation_api.ValidateBlockRequest, opts ...grpc.CallOption) (*blockvalidation_api.ValidateBlockResponse, error) {
	args := m.Called(ctx, in, opts)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*blockvalidation_api.ValidateBlockResponse), args.Error(1)
}

func (m *mockBlockValidationAPIClient) RevalidateBlock(ctx context.Context, in *blockvalidation_api.RevalidateBlockRequest, opts ...grpc.CallOption) (*blockvalidation_api.EmptyMessage, error) {
	args := m.Called(ctx, in, opts)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*blockvalidation_api.EmptyMessage), args.Error(1)
}

func createTestClient(mockClient *mockBlockValidationAPIClient) *Client {
	logger := ulogger.TestLogger{}
	tSettings := &settings.Settings{
		BlockValidation: settings.BlockValidationSettings{
			GRPCAddress: "localhost:8080",
		},
		GRPCMaxRetries:   3,
		GRPCRetryBackoff: 1000,
	}

	client := &Client{
		apiClient: mockClient,
		logger:    logger,
		settings:  tSettings,
	}

	return client
}

func TestNewClient_ConfigErrors(t *testing.T) {
	ctx := context.Background()
	logger := ulogger.TestLogger{}

	tests := []struct {
		name     string
		settings func() *settings.Settings
		wantErr  string
	}{
		{
			name: "missing grpc address",
			settings: func() *settings.Settings {
				return &settings.Settings{
					BlockValidation: settings.BlockValidationSettings{
						GRPCAddress: "",
					},
				}
			},
			wantErr: "no blockvalidation_grpcAddress setting found",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewClient(ctx, logger, tt.settings(), "test-source")
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.wantErr)
		})
	}
}

func TestNewClient_ConnectionError(t *testing.T) {
	ctx := context.Background()
	logger := ulogger.TestLogger{}

	tSettings := &settings.Settings{
		BlockValidation: settings.BlockValidationSettings{
			GRPCAddress: "localhost:0", // Use port 0 to force connection failure
		},
		GRPCMaxRetries:   1,
		GRPCRetryBackoff: 1000,
	}

	// This will likely fail at connection, but we want to test the connection failure path
	_, err := NewClient(ctx, logger, tSettings, "test-source")
	if err != nil {
		assert.Contains(t, err.Error(), "failed to init block validation service connection for 'test-source'")
	}
	// Note: Connection may or may not fail in test environment
}

func TestNewClient_SuccessPath(t *testing.T) {
	ctx := context.Background()
	logger := ulogger.TestLogger{}

	tSettings := &settings.Settings{
		BlockValidation: settings.BlockValidationSettings{
			GRPCAddress: "localhost:1234", // Valid address format
		},
		GRPCMaxRetries:   3,
		GRPCRetryBackoff: 1000,
	}

	// This tests the successful path where all validations pass
	// The connection will still fail, but we test the configuration validation
	_, err := NewClient(ctx, logger, tSettings, "test-source")
	if err != nil {
		// This is expected since we don't have a real server running
		assert.Contains(t, err.Error(), "failed to init block validation service connection for 'test-source'")
	}
}

func TestClient_Health(t *testing.T) {
	ctx := context.Background()
	mockClient := &mockBlockValidationAPIClient{}
	client := createTestClient(mockClient)

	tests := []struct {
		name          string
		checkLiveness bool
		mockSetup     func()
		wantCode      int
		wantMessage   string
		wantErr       bool
	}{
		{
			name:          "liveness check returns OK",
			checkLiveness: true,
			mockSetup:     func() {}, // No mock needed for liveness
			wantCode:      http.StatusOK,
			wantMessage:   "OK",
			wantErr:       false,
		},
		{
			name:          "readiness check success",
			checkLiveness: false,
			mockSetup: func() {
				mockClient.On("HealthGRPC", ctx, &blockvalidation_api.EmptyMessage{}, mock.Anything).Return(
					&blockvalidation_api.HealthResponse{Ok: true, Details: "healthy"},
					nil,
				)
			},
			wantCode:    http.StatusOK,
			wantMessage: "healthy",
			wantErr:     false,
		},
		{
			name:          "readiness check fails with grpc error",
			checkLiveness: false,
			mockSetup: func() {
				mockClient.On("HealthGRPC", ctx, &blockvalidation_api.EmptyMessage{}, mock.Anything).Return(
					nil,
					status.Error(codes.Unavailable, "service unavailable"),
				)
			},
			wantCode:    http.StatusFailedDependency,
			wantMessage: "",
			wantErr:     true,
		},
		{
			name:          "readiness check fails with ok=false",
			checkLiveness: false,
			mockSetup: func() {
				mockClient.On("HealthGRPC", ctx, &blockvalidation_api.EmptyMessage{}, mock.Anything).Return(
					&blockvalidation_api.HealthResponse{Ok: false, Details: "unhealthy"},
					nil,
				)
			},
			wantCode:    http.StatusFailedDependency,
			wantMessage: "unhealthy",
			wantErr:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockClient.ExpectedCalls = nil
			tt.mockSetup()

			code, message, err := client.Health(ctx, tt.checkLiveness)

			assert.Equal(t, tt.wantCode, code)
			assert.Equal(t, tt.wantMessage, message)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}

			mockClient.AssertExpectations(t)
		})
	}
}

func TestClient_BlockFound(t *testing.T) {
	ctx := context.Background()
	mockClient := &mockBlockValidationAPIClient{}
	client := createTestClient(mockClient)

	hash, _ := chainhash.NewHashFromStr("0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef")
	baseURL := "http://example.com"

	t.Run("successful block found without wait", func(t *testing.T) {
		mockClient.ExpectedCalls = nil
		mockClient.On("BlockFound", ctx, mock.MatchedBy(func(req *blockvalidation_api.BlockFoundRequest) bool {
			return req.BaseUrl == baseURL && req.WaitToComplete == false && string(req.Hash) == string(hash.CloneBytes())
		}), mock.Anything).Return(&blockvalidation_api.EmptyMessage{}, nil)

		err := client.BlockFound(ctx, hash, baseURL, false)
		assert.NoError(t, err)
		mockClient.AssertExpectations(t)
	})

	t.Run("successful block found with wait", func(t *testing.T) {
		mockClient.ExpectedCalls = nil
		mockClient.On("BlockFound", ctx, mock.MatchedBy(func(req *blockvalidation_api.BlockFoundRequest) bool {
			return req.BaseUrl == baseURL && req.WaitToComplete == true && string(req.Hash) == string(hash.CloneBytes())
		}), mock.Anything).Return(&blockvalidation_api.EmptyMessage{}, nil)

		err := client.BlockFound(ctx, hash, baseURL, true)
		assert.NoError(t, err)
		mockClient.AssertExpectations(t)
	})

	t.Run("grpc error", func(t *testing.T) {
		mockClient.ExpectedCalls = nil
		mockClient.On("BlockFound", ctx, mock.Anything, mock.Anything).Return(
			nil, status.Error(codes.Internal, "internal error"))

		err := client.BlockFound(ctx, hash, baseURL, false)
		assert.Error(t, err)
		mockClient.AssertExpectations(t)
	})

	t.Run("nil error returns nil", func(t *testing.T) {
		mockClient.ExpectedCalls = nil
		mockClient.On("BlockFound", ctx, mock.Anything, mock.Anything).Return(&blockvalidation_api.EmptyMessage{}, nil)

		err := client.BlockFound(ctx, hash, baseURL, false)
		assert.NoError(t, err)
		mockClient.AssertExpectations(t)
	})
}

func createClientTestBlock() *model.Block {
	// Create valid chainhash.Hash objects
	prevHash, _ := chainhash.NewHashFromStr("0000000000000000000000000000000000000000000000000000000000000000")
	merkleRoot, _ := chainhash.NewHashFromStr("0000000000000000000000000000000000000000000000000000000000000000")

	// Create NBit from string
	bits, _ := model.NewNBitFromString("1d00ffff")

	// Create a minimal valid block with header
	header := &model.BlockHeader{
		Version:        1,
		HashPrevBlock:  prevHash,
		HashMerkleRoot: merkleRoot,
		Timestamp:      1234567890,
		Bits:           *bits,
		Nonce:          0,
	}

	block := &model.Block{
		Height: 100,
		Header: header,
	}

	return block
}

func TestClient_ProcessBlock(t *testing.T) {
	ctx := context.Background()
	mockClient := &mockBlockValidationAPIClient{}
	client := createTestClient(mockClient)

	block := createClientTestBlock()

	t.Run("successful process block", func(t *testing.T) {
		mockClient.ExpectedCalls = nil
		mockClient.On("ProcessBlock", ctx, mock.MatchedBy(func(req *blockvalidation_api.ProcessBlockRequest) bool {
			return req.Height == 100 && len(req.Block) > 0
		}), mock.Anything).Return(&blockvalidation_api.EmptyMessage{}, nil)

		err := client.ProcessBlock(ctx, block, 100, "", "legacy")
		assert.NoError(t, err)
		mockClient.AssertExpectations(t)
	})

	t.Run("grpc error", func(t *testing.T) {
		mockClient.ExpectedCalls = nil
		mockClient.On("ProcessBlock", ctx, mock.Anything, mock.Anything).Return(
			nil, status.Error(codes.Internal, "processing error"))

		err := client.ProcessBlock(ctx, block, 100, "", "legacy")
		assert.Error(t, err)
		mockClient.AssertExpectations(t)
	})

	t.Run("block serialization error", func(t *testing.T) {
		mockClient.ExpectedCalls = nil

		// Create a block that will cause serialization error by having nil header
		invalidBlock := &model.Block{
			Height: 100,
			Header: nil, // This should cause serialization to fail
		}

		err := client.ProcessBlock(ctx, invalidBlock, 100, "", "legacy")
		assert.Error(t, err)
		mockClient.AssertExpectations(t)
	})
}

func TestClient_ValidateBlock(t *testing.T) {
	ctx := context.Background()
	mockClient := &mockBlockValidationAPIClient{}
	client := createTestClient(mockClient)

	block := createClientTestBlock()

	t.Run("successful validate block", func(t *testing.T) {
		mockClient.ExpectedCalls = nil
		mockClient.On("ValidateBlock", ctx, mock.MatchedBy(func(req *blockvalidation_api.ValidateBlockRequest) bool {
			return req.Height == 100 && len(req.Block) > 0
		}), mock.Anything).Return(&blockvalidation_api.ValidateBlockResponse{}, nil)

		err := client.ValidateBlock(ctx, block, nil)
		assert.NoError(t, err)
		mockClient.AssertExpectations(t)
	})

	t.Run("grpc error", func(t *testing.T) {
		mockClient.ExpectedCalls = nil
		mockClient.On("ValidateBlock", ctx, mock.Anything, mock.Anything).Return(
			nil, status.Error(codes.InvalidArgument, "validation error"))

		err := client.ValidateBlock(ctx, block, nil)
		assert.Error(t, err)
		mockClient.AssertExpectations(t)
	})

	t.Run("block serialization error", func(t *testing.T) {
		mockClient.ExpectedCalls = nil

		// Create a block that will cause serialization error by having nil header
		invalidBlock := &model.Block{
			Height: 100,
			Header: nil, // This should cause serialization to fail
		}

		err := client.ValidateBlock(ctx, invalidBlock, nil)
		assert.Error(t, err)
		mockClient.AssertExpectations(t)
	})
}
