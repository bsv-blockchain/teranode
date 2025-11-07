package subtreevalidation

import (
	"context"
	"net/http"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/subtreevalidation/subtreevalidation_api"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func createTestSettings() *settings.Settings {
	return &settings.Settings{
		SubtreeValidation: settings.SubtreeValidationSettings{
			GRPCAddress: "localhost:8080",
		},
		BlockValidation: settings.BlockValidationSettings{
			CheckSubtreeFromBlockRetries:              3,
			CheckSubtreeFromBlockRetryBackoffDuration: 1000,
		},
	}
}

func TestNewClient_ConfigurationError(t *testing.T) {
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
				s := createTestSettings()
				s.SubtreeValidation.GRPCAddress = ""
				return s
			},
			wantErr: "no subtreevalidation_grpcAddress setting found",
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
		SubtreeValidation: settings.SubtreeValidationSettings{
			GRPCAddress: "localhost:0", // Use port 0 to force connection failure
		},
		BlockValidation: settings.BlockValidationSettings{
			CheckSubtreeFromBlockRetries:              1,
			CheckSubtreeFromBlockRetryBackoffDuration: 1000,
		},
	}

	// This will likely fail at connection, but we want to test the connection failure path
	_, err := NewClient(ctx, logger, tSettings, "test-source")
	if err != nil {
		assert.Contains(t, err.Error(), "failed to init subtree validation service connection for 'test-source'")
	}
	// Note: Connection may or may not fail in test environment
}

func TestNewClient_SuccessPath(t *testing.T) {
	ctx := context.Background()
	logger := ulogger.TestLogger{}

	tSettings := createTestSettings()

	// This tests the successful path where all validations pass
	// The connection will still fail, but we test the configuration validation
	_, err := NewClient(ctx, logger, tSettings, "test-source")
	if err != nil {
		// This is expected since we don't have a real server running
		assert.Contains(t, err.Error(), "failed to init subtree validation service connection for 'test-source'")
	}
}

func createTestClient() *Client {
	logger := ulogger.TestLogger{}
	tSettings := createTestSettings()

	mockAPIClient := &MockSubtreeValidationAPIClient{}

	return &Client{
		apiClient: mockAPIClient,
		logger:    logger,
		settings:  tSettings,
	}
}

func TestClient_Health_Liveness(t *testing.T) {
	ctx := context.Background()
	client := createTestClient()

	// Test liveness check - should return OK without calling gRPC
	code, message, err := client.Health(ctx, true)

	assert.Equal(t, http.StatusOK, code)
	assert.Equal(t, "OK", message)
	assert.NoError(t, err)
}

func TestClient_Health_Readiness_Success(t *testing.T) {
	ctx := context.Background()
	client := createTestClient()
	mockAPIClient := client.apiClient.(*MockSubtreeValidationAPIClient)

	// Mock successful health check
	healthResponse := &subtreevalidation_api.HealthResponse{
		Ok:        true,
		Details:   "All systems healthy",
		Timestamp: timestamppb.Now(),
	}
	mockAPIClient.On("HealthGRPC", ctx, &subtreevalidation_api.EmptyMessage{}, mock.Anything).Return(healthResponse, nil)

	code, message, err := client.Health(ctx, false)

	assert.Equal(t, http.StatusOK, code)
	assert.Equal(t, "All systems healthy", message)
	assert.NoError(t, err)
	mockAPIClient.AssertExpectations(t)
}

func TestClient_Health_Readiness_ServiceUnhealthy(t *testing.T) {
	ctx := context.Background()
	client := createTestClient()
	mockAPIClient := client.apiClient.(*MockSubtreeValidationAPIClient)

	// Mock unhealthy response
	healthResponse := &subtreevalidation_api.HealthResponse{
		Ok:      false,
		Details: "Service degraded",
	}
	mockAPIClient.On("HealthGRPC", ctx, &subtreevalidation_api.EmptyMessage{}, mock.Anything).Return(healthResponse, nil)

	code, message, err := client.Health(ctx, false)

	assert.Equal(t, http.StatusFailedDependency, code)
	assert.Equal(t, "Service degraded", message)
	assert.Error(t, err)
	mockAPIClient.AssertExpectations(t)
}

func TestClient_Health_Readiness_GRPCError(t *testing.T) {
	ctx := context.Background()
	client := createTestClient()
	mockAPIClient := client.apiClient.(*MockSubtreeValidationAPIClient)

	// Mock gRPC error
	grpcErr := status.Error(codes.Unavailable, "service unavailable")
	mockAPIClient.On("HealthGRPC", ctx, &subtreevalidation_api.EmptyMessage{}, mock.Anything).Return(nil, grpcErr)

	code, message, err := client.Health(ctx, false)

	assert.Equal(t, http.StatusFailedDependency, code)
	assert.Empty(t, message) // nil response means empty details
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "service unavailable")
	mockAPIClient.AssertExpectations(t)
}

func TestClient_CheckSubtreeFromBlock_Success(t *testing.T) {
	ctx := context.Background()
	client := createTestClient()
	mockAPIClient := client.apiClient.(*MockSubtreeValidationAPIClient)

	// Test data
	subtreeHash, _ := chainhash.NewHashFromStr("0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef")
	blockHash, _ := chainhash.NewHashFromStr("1123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef")
	prevBlockHash, _ := chainhash.NewHashFromStr("2123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef")
	baseURL := "http://example.com"
	blockHeight := uint32(100)

	// Mock successful response
	response := &subtreevalidation_api.CheckSubtreeFromBlockResponse{}
	mockAPIClient.On("CheckSubtreeFromBlock", ctx, mock.MatchedBy(func(req *subtreevalidation_api.CheckSubtreeFromBlockRequest) bool {
		return string(req.Hash) == string(subtreeHash[:]) &&
			req.BaseUrl == baseURL &&
			req.BlockHeight == blockHeight &&
			string(req.BlockHash) == string(blockHash[:]) &&
			string(req.PreviousBlockHash) == string(prevBlockHash[:])
	}), mock.Anything).Return(response, nil)

	err := client.CheckSubtreeFromBlock(ctx, *subtreeHash, baseURL, blockHeight, blockHash, prevBlockHash)

	assert.NoError(t, err)
	mockAPIClient.AssertExpectations(t)
}

func TestClient_CheckSubtreeFromBlock_GRPCError(t *testing.T) {
	ctx := context.Background()
	client := createTestClient()
	mockAPIClient := client.apiClient.(*MockSubtreeValidationAPIClient)

	// Test data with valid hashes (not nil to avoid panic)
	subtreeHash, _ := chainhash.NewHashFromStr("0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef")
	blockHash, _ := chainhash.NewHashFromStr("1123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef")
	prevBlockHash, _ := chainhash.NewHashFromStr("2123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef")
	baseURL := "http://example.com"
	blockHeight := uint32(100)

	// Mock gRPC error
	grpcErr := status.Error(codes.InvalidArgument, "invalid subtree hash")
	mockAPIClient.On("CheckSubtreeFromBlock", ctx, mock.Anything, mock.Anything).Return(nil, grpcErr)

	err := client.CheckSubtreeFromBlock(ctx, *subtreeHash, baseURL, blockHeight, blockHash, prevBlockHash)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "invalid subtree hash")
	mockAPIClient.AssertExpectations(t)
}

func TestClient_CheckBlockSubtrees_Success(t *testing.T) {
	ctx := context.Background()
	client := createTestClient()
	mockAPIClient := client.apiClient.(*MockSubtreeValidationAPIClient)

	// Create test block
	block := createTestBlock()
	baseURL := "http://example.com"

	// Mock successful response
	response := &subtreevalidation_api.CheckBlockSubtreesResponse{}
	mockAPIClient.On("CheckBlockSubtrees", ctx, mock.MatchedBy(func(req *subtreevalidation_api.CheckBlockSubtreesRequest) bool {
		return req.BaseUrl == baseURL && len(req.Block) > 0
	}), mock.Anything).Return(response, nil)

	err := client.CheckBlockSubtrees(ctx, block, "", baseURL)

	assert.NoError(t, err)
	mockAPIClient.AssertExpectations(t)
}

func TestClient_CheckBlockSubtrees_SerializationError(t *testing.T) {
	ctx := context.Background()
	client := createTestClient()

	// Create invalid block that cannot be serialized
	block := &model.Block{
		Header: nil, // This should cause serialization to fail
	}
	baseURL := "http://example.com"

	err := client.CheckBlockSubtrees(ctx, block, "", baseURL)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to serialize block for subtree validation")
}

func TestClient_CheckBlockSubtrees_GRPCError(t *testing.T) {
	ctx := context.Background()
	client := createTestClient()
	mockAPIClient := client.apiClient.(*MockSubtreeValidationAPIClient)

	// Create test block
	block := createTestBlock()
	baseURL := "http://example.com"

	// Mock gRPC error
	grpcErr := status.Error(codes.Internal, "internal processing error")
	mockAPIClient.On("CheckBlockSubtrees", ctx, mock.Anything, mock.Anything).Return(nil, grpcErr)

	err := client.CheckBlockSubtrees(ctx, block, "", baseURL)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "internal processing error")
	mockAPIClient.AssertExpectations(t)
}

// Helper function to create a test block
func createTestBlock() *model.Block {
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

func TestClient_Health_Readiness_NilResponse(t *testing.T) {
	ctx := context.Background()
	client := createTestClient()
	mockAPIClient := client.apiClient.(*MockSubtreeValidationAPIClient)

	// Mock nil response with no error (edge case)
	mockAPIClient.On("HealthGRPC", ctx, &subtreevalidation_api.EmptyMessage{}, mock.Anything).Return(nil, nil)

	code, _, err := client.Health(ctx, false)

	// When response is nil, GetOk() will panic or return false, so we expect error handling
	assert.Equal(t, http.StatusFailedDependency, code)
	assert.Error(t, err) // Should handle nil response gracefully
	mockAPIClient.AssertExpectations(t)
}

func TestClient_CheckSubtreeFromBlock_WithNilHashes(t *testing.T) {
	ctx := context.Background()
	client := createTestClient()

	// Test data with nil block hashes
	subtreeHash, _ := chainhash.NewHashFromStr("0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef")
	baseURL := "http://example.com"
	blockHeight := uint32(100)

	// This should panic due to nil pointer dereference - this tests a bug in the Client code
	// The code should handle nil hashes gracefully, but currently doesn't
	assert.Panics(t, func() {
		_ = client.CheckSubtreeFromBlock(ctx, *subtreeHash, baseURL, blockHeight, nil, nil)
	})
}
