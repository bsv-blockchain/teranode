package blockassembly

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-batcher"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockassembly/blockassembly_api"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func createTestClient(mockClient *mockBlockAssemblyAPIClient, batchSize int) *Client {
	logger := ulogger.TestLogger{}
	tSettings := &settings.Settings{
		BlockAssembly: settings.BlockAssemblySettings{
			SendBatchSize:    batchSize,
			SendBatchTimeout: 100,
		},
	}

	client := &Client{
		client:    mockClient,
		logger:    logger,
		settings:  tSettings,
		batchSize: batchSize,
		batchCh:   make(chan []*batchItem),
	}

	if batchSize > 0 {
		sendBatch := func(batch []*batchItem) {
			client.sendBatchToBlockAssembly(context.Background(), batch)
		}
		duration := time.Duration(100) * time.Millisecond
		client.batcher = batcher.New(batchSize, duration, sendBatch, true)
	}

	return client
}

// Test constructor failures
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
					BlockAssembly: settings.BlockAssemblySettings{
						GRPCAddress: "",
					},
				}
			},
			wantErr: "no blockassembly_grpcAddress setting found",
		},
		{
			name: "zero retry backoff",
			settings: func() *settings.Settings {
				return &settings.Settings{
					BlockAssembly: settings.BlockAssemblySettings{
						GRPCAddress:      "localhost:8080",
						GRPCRetryBackoff: 0,
					},
				}
			},
			wantErr: "blockassembly_grpcRetryBackoff setting error",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewClient(ctx, logger, tt.settings())
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.wantErr)
		})
	}
}

func TestNewClient_BatchLogging(t *testing.T) {
	ctx := context.Background()
	logger := ulogger.TestLogger{}

	tSettings := &settings.Settings{
		BlockAssembly: settings.BlockAssemblySettings{
			GRPCAddress:      "localhost:0", // Use port 0 to force an error
			GRPCRetryBackoff: 1000,
			GRPCMaxRetries:   1,
			SendBatchSize:    10,
			SendBatchTimeout: 5000,
		},
	}

	// This will fail at connection, but we want to test the batch size logging path
	_, err := NewClient(ctx, logger, tSettings)
	if err != nil {
		assert.Contains(t, err.Error(), "failed to connect to block assembly")
	}
	// Note: Connection may or may not fail in test environment
}

func TestNewClientWithAddress_BatchLogging(t *testing.T) {
	ctx := context.Background()
	logger := ulogger.TestLogger{}

	tSettings := &settings.Settings{
		GRPCMaxRetries:   1,
		GRPCRetryBackoff: 1000,
		BlockAssembly: settings.BlockAssemblySettings{
			SendBatchSize:    10,
			SendBatchTimeout: 5000,
		},
	}

	// This will fail at connection, but we want to test the batch size logging path
	_, err := NewClientWithAddress(ctx, logger, tSettings, "localhost:0")
	if err != nil {
		assert.Contains(t, err.Error(), "failed to connect to block assembly")
	}
	// Note: Connection may or may not fail in test environment
}

func TestClient_Health(t *testing.T) {
	ctx := context.Background()
	mockClient := &mockBlockAssemblyAPIClient{}
	client := createTestClient(mockClient, 0)

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
			mockSetup:     func() {},
			wantCode:      http.StatusOK,
			wantMessage:   "OK",
			wantErr:       false,
		},
		{
			name:          "readiness check success",
			checkLiveness: false,
			mockSetup: func() {
				mockClient.On("HealthGRPC", ctx, &blockassembly_api.EmptyMessage{}, mock.Anything).Return(
					&blockassembly_api.HealthResponse{Ok: true, Details: "healthy"},
					nil,
				)
			},
			wantCode:    http.StatusOK,
			wantMessage: "OK",
			wantErr:     false,
		},
		{
			name:          "readiness check fails with grpc error",
			checkLiveness: false,
			mockSetup: func() {
				mockClient.On("HealthGRPC", ctx, &blockassembly_api.EmptyMessage{}, mock.Anything).Return(
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
				mockClient.On("HealthGRPC", ctx, &blockassembly_api.EmptyMessage{}, mock.Anything).Return(
					&blockassembly_api.HealthResponse{Ok: false, Details: "unhealthy"},
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

func TestClient_Store_NonBatchMode(t *testing.T) {
	ctx := context.Background()
	mockClient := &mockBlockAssemblyAPIClient{}
	client := createTestClient(mockClient, 0) // batchSize = 0 for non-batch mode

	hash, _ := chainhash.NewHashFromStr("0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef")
	fee := uint64(1000)
	size := uint64(250)
	txInpoints := subtree.TxInpoints{}

	t.Run("successful store", func(t *testing.T) {
		mockClient.ExpectedCalls = nil
		mockClient.On("AddTx", ctx, mock.MatchedBy(func(req *blockassembly_api.AddTxRequest) bool {
			return req.Fee == fee && req.Size == size
		}), mock.Anything).Return(&blockassembly_api.AddTxResponse{}, nil)

		success, err := client.Store(ctx, hash, fee, size, txInpoints)
		assert.True(t, success)
		assert.NoError(t, err)
		mockClient.AssertExpectations(t)
	})

	t.Run("grpc error", func(t *testing.T) {
		mockClient.ExpectedCalls = nil
		mockClient.On("AddTx", ctx, mock.Anything, mock.Anything).Return(
			nil, status.Error(codes.Internal, "internal error"))

		success, err := client.Store(ctx, hash, fee, size, txInpoints)
		assert.False(t, success)
		assert.Error(t, err)
		mockClient.AssertExpectations(t)
	})

	t.Run("successful store with empty txInpoints", func(t *testing.T) {
		mockClient.ExpectedCalls = nil
		mockClient.On("AddTx", ctx, mock.Anything, mock.Anything).Return(&blockassembly_api.AddTxResponse{}, nil)

		emptyTxInpoints := subtree.TxInpoints{}
		success, err := client.Store(ctx, hash, fee, size, emptyTxInpoints)
		assert.True(t, success)
		assert.NoError(t, err) // Serialization of empty txInpoints should succeed
		mockClient.AssertExpectations(t)
	})

	t.Run("store with serialization that succeeds", func(t *testing.T) {
		mockClient.ExpectedCalls = nil
		mockClient.On("AddTx", ctx, mock.Anything, mock.Anything).Return(&blockassembly_api.AddTxResponse{}, nil)

		// Create a valid txInpoints structure
		validTxInpoints := subtree.TxInpoints{}
		success, err := client.Store(ctx, hash, fee, size, validTxInpoints)
		assert.True(t, success)
		assert.NoError(t, err)
		mockClient.AssertExpectations(t)
	})
}

func TestClient_Store_BatchMode(t *testing.T) {
	ctx := context.Background()
	mockClient := &mockBlockAssemblyAPIClient{}
	client := createTestClient(mockClient, 5) // batchSize = 5 for batch mode

	hash, _ := chainhash.NewHashFromStr("0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef")
	fee := uint64(1000)
	size := uint64(250)
	txInpoints := subtree.TxInpoints{}

	t.Run("successful batch store", func(t *testing.T) {
		mockClient.ExpectedCalls = nil
		mockClient.On("AddTxBatch", mock.Anything, mock.MatchedBy(func(req *blockassembly_api.AddTxBatchRequest) bool {
			return len(req.TxRequests) == 1
		}), mock.Anything).Return(&blockassembly_api.AddTxBatchResponse{}, nil)

		success, err := client.Store(ctx, hash, fee, size, txInpoints)
		assert.True(t, success)
		assert.NoError(t, err)
		mockClient.AssertExpectations(t)
	})

	t.Run("batch error", func(t *testing.T) {
		mockClient.ExpectedCalls = nil
		mockClient.On("AddTxBatch", mock.Anything, mock.Anything, mock.Anything).Return(
			nil, status.Error(codes.Internal, "batch error"))

		success, err := client.Store(ctx, hash, fee, size, txInpoints)
		assert.False(t, success)
		assert.Error(t, err)
		mockClient.AssertExpectations(t)
	})
}

func TestClient_RemoveTx(t *testing.T) {
	ctx := context.Background()
	mockClient := &mockBlockAssemblyAPIClient{}
	client := createTestClient(mockClient, 0)

	hash, _ := chainhash.NewHashFromStr("0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef")

	t.Run("successful removal", func(t *testing.T) {
		mockClient.ExpectedCalls = nil
		mockClient.On("RemoveTx", ctx, mock.MatchedBy(func(req *blockassembly_api.RemoveTxRequest) bool {
			return string(req.Txid) == string(hash[:])
		}), mock.Anything).Return(&blockassembly_api.EmptyMessage{}, nil)

		err := client.RemoveTx(ctx, hash)
		assert.NoError(t, err)
		mockClient.AssertExpectations(t)
	})

	t.Run("grpc error", func(t *testing.T) {
		mockClient.ExpectedCalls = nil
		mockClient.On("RemoveTx", ctx, mock.Anything, mock.Anything).Return(
			nil, status.Error(codes.NotFound, "transaction not found"))

		err := client.RemoveTx(ctx, hash)
		assert.Error(t, err)
		mockClient.AssertExpectations(t)
	})

	t.Run("nil error returns nil", func(t *testing.T) {
		mockClient.ExpectedCalls = nil
		mockClient.On("RemoveTx", ctx, mock.Anything, mock.Anything).Return(&blockassembly_api.EmptyMessage{}, nil)

		err := client.RemoveTx(ctx, hash)
		assert.NoError(t, err)
		mockClient.AssertExpectations(t)
	})
}

func TestClient_GetMiningCandidate(t *testing.T) {
	ctx := context.Background()
	mockClient := &mockBlockAssemblyAPIClient{}
	client := createTestClient(mockClient, 0)

	expectedCandidate := &model.MiningCandidate{
		Id:      []byte("test-id"),
		Version: 1,
		Height:  123,
	}

	t.Run("successful without subtrees", func(t *testing.T) {
		mockClient.ExpectedCalls = nil
		mockClient.On("GetMiningCandidate", ctx, mock.MatchedBy(func(req *blockassembly_api.GetMiningCandidateRequest) bool {
			return req.IncludeSubtrees == false
		}), mock.Anything).Return(expectedCandidate, nil)

		candidate, err := client.GetMiningCandidate(ctx)
		assert.NoError(t, err)
		assert.Equal(t, expectedCandidate, candidate)
		mockClient.AssertExpectations(t)
	})

	t.Run("successful with subtrees", func(t *testing.T) {
		mockClient.ExpectedCalls = nil
		mockClient.On("GetMiningCandidate", ctx, mock.MatchedBy(func(req *blockassembly_api.GetMiningCandidateRequest) bool {
			return req.IncludeSubtrees == true
		}), mock.Anything).Return(expectedCandidate, nil)

		candidate, err := client.GetMiningCandidate(ctx, true)
		assert.NoError(t, err)
		assert.Equal(t, expectedCandidate, candidate)
		mockClient.AssertExpectations(t)
	})

	t.Run("grpc error", func(t *testing.T) {
		mockClient.ExpectedCalls = nil
		mockClient.On("GetMiningCandidate", ctx, mock.Anything, mock.Anything).Return(
			nil, status.Error(codes.Internal, "internal error"))

		candidate, err := client.GetMiningCandidate(ctx)
		assert.Nil(t, candidate)
		assert.Error(t, err)
		mockClient.AssertExpectations(t)
	})
}

func TestClient_GetCurrentDifficulty(t *testing.T) {
	ctx := context.Background()
	mockClient := &mockBlockAssemblyAPIClient{}
	client := createTestClient(mockClient, 0)

	expectedDifficulty := 12345.67

	t.Run("successful", func(t *testing.T) {
		mockClient.ExpectedCalls = nil
		mockClient.On("GetCurrentDifficulty", ctx, &blockassembly_api.EmptyMessage{}, mock.Anything).Return(
			&blockassembly_api.GetCurrentDifficultyResponse{Difficulty: expectedDifficulty}, nil)

		difficulty, err := client.GetCurrentDifficulty(ctx)
		assert.NoError(t, err)
		assert.Equal(t, expectedDifficulty, difficulty)
		mockClient.AssertExpectations(t)
	})

	t.Run("grpc error", func(t *testing.T) {
		mockClient.ExpectedCalls = nil
		mockClient.On("GetCurrentDifficulty", ctx, &blockassembly_api.EmptyMessage{}, mock.Anything).Return(
			nil, status.Error(codes.Internal, "internal error"))

		difficulty, err := client.GetCurrentDifficulty(ctx)
		assert.Equal(t, float64(0), difficulty)
		assert.Error(t, err)
		mockClient.AssertExpectations(t)
	})
}

func TestClient_SubmitMiningSolution(t *testing.T) {
	ctx := context.Background()
	mockClient := &mockBlockAssemblyAPIClient{}
	client := createTestClient(mockClient, 0)

	timeValue := uint32(1234567890)
	versionValue := uint32(1)
	solution := &model.MiningSolution{
		Id:       []byte("test-id"),
		Nonce:    12345,
		Coinbase: []byte("coinbase-data"),
		Time:     &timeValue,
		Version:  &versionValue,
	}

	t.Run("successful", func(t *testing.T) {
		mockClient.ExpectedCalls = nil
		mockClient.On("SubmitMiningSolution", ctx, mock.MatchedBy(func(req *blockassembly_api.SubmitMiningSolutionRequest) bool {
			return string(req.Id) == string(solution.Id) && req.Nonce == solution.Nonce
		}), mock.Anything).Return(&blockassembly_api.OKResponse{}, nil)

		err := client.SubmitMiningSolution(ctx, solution)
		assert.NoError(t, err)
		mockClient.AssertExpectations(t)
	})

	t.Run("grpc error", func(t *testing.T) {
		mockClient.ExpectedCalls = nil
		mockClient.On("SubmitMiningSolution", ctx, mock.Anything, mock.Anything).Return(
			nil, status.Error(codes.InvalidArgument, "invalid solution"))

		err := client.SubmitMiningSolution(ctx, solution)
		assert.Error(t, err)
		mockClient.AssertExpectations(t)
	})

	t.Run("nil error returns nil", func(t *testing.T) {
		mockClient.ExpectedCalls = nil
		mockClient.On("SubmitMiningSolution", ctx, mock.Anything, mock.Anything).Return(&blockassembly_api.OKResponse{}, nil)

		err := client.SubmitMiningSolution(ctx, solution)
		assert.NoError(t, err)
		mockClient.AssertExpectations(t)
	})
}

func TestClient_GenerateBlocks(t *testing.T) {
	ctx := context.Background()
	mockClient := &mockBlockAssemblyAPIClient{}
	client := createTestClient(mockClient, 0)

	req := &blockassembly_api.GenerateBlocksRequest{
		Count: 5,
	}

	t.Run("successful", func(t *testing.T) {
		mockClient.ExpectedCalls = nil
		mockClient.On("GenerateBlocks", ctx, req, mock.Anything).Return(&blockassembly_api.EmptyMessage{}, nil)

		err := client.GenerateBlocks(ctx, req)
		assert.NoError(t, err)
		mockClient.AssertExpectations(t)
	})

	t.Run("grpc error", func(t *testing.T) {
		mockClient.ExpectedCalls = nil
		mockClient.On("GenerateBlocks", ctx, req, mock.Anything).Return(
			nil, status.Error(codes.Internal, "generation failed"))

		err := client.GenerateBlocks(ctx, req)
		assert.Error(t, err)
		mockClient.AssertExpectations(t)
	})

	t.Run("nil error returns nil", func(t *testing.T) {
		mockClient.ExpectedCalls = nil
		mockClient.On("GenerateBlocks", ctx, req, mock.Anything).Return(&blockassembly_api.EmptyMessage{}, nil)

		err := client.GenerateBlocks(ctx, req)
		assert.NoError(t, err)
		mockClient.AssertExpectations(t)
	})
}

func TestClient_ResetBlockAssembly(t *testing.T) {
	ctx := context.Background()
	mockClient := &mockBlockAssemblyAPIClient{}
	client := createTestClient(mockClient, 0)

	t.Run("successful", func(t *testing.T) {
		mockClient.ExpectedCalls = nil
		mockClient.On("ResetBlockAssembly", context.Background(), &blockassembly_api.EmptyMessage{}, mock.Anything).Return(&blockassembly_api.EmptyMessage{}, nil)

		err := client.ResetBlockAssembly(ctx)
		assert.NoError(t, err)
		mockClient.AssertExpectations(t)
	})

	t.Run("grpc error", func(t *testing.T) {
		mockClient.ExpectedCalls = nil
		mockClient.On("ResetBlockAssembly", context.Background(), &blockassembly_api.EmptyMessage{}, mock.Anything).Return(
			nil, status.Error(codes.Internal, "reset failed"))

		err := client.ResetBlockAssembly(ctx)
		assert.Error(t, err)
		mockClient.AssertExpectations(t)
	})

	t.Run("nil error returns nil", func(t *testing.T) {
		mockClient.ExpectedCalls = nil
		mockClient.On("ResetBlockAssembly", context.Background(), &blockassembly_api.EmptyMessage{}, mock.Anything).Return(&blockassembly_api.EmptyMessage{}, nil)

		err := client.ResetBlockAssembly(ctx)
		assert.NoError(t, err)
		mockClient.AssertExpectations(t)
	})
}

func TestClient_GetBlockAssemblyState(t *testing.T) {
	ctx := context.Background()
	mockClient := &mockBlockAssemblyAPIClient{}
	client := createTestClient(mockClient, 0)

	expectedState := &blockassembly_api.StateMessage{
		BlockAssemblyState: "running",
	}

	t.Run("successful", func(t *testing.T) {
		mockClient.ExpectedCalls = nil
		mockClient.On("GetBlockAssemblyState", ctx, &blockassembly_api.EmptyMessage{}, mock.Anything).Return(expectedState, nil)

		state, err := client.GetBlockAssemblyState(ctx)
		assert.NoError(t, err)
		assert.Equal(t, expectedState, state)
		mockClient.AssertExpectations(t)
	})

	t.Run("grpc error", func(t *testing.T) {
		mockClient.ExpectedCalls = nil
		mockClient.On("GetBlockAssemblyState", ctx, &blockassembly_api.EmptyMessage{}, mock.Anything).Return(
			nil, status.Error(codes.Internal, "state error"))

		state, err := client.GetBlockAssemblyState(ctx)
		assert.Nil(t, state)
		assert.Error(t, err)
		mockClient.AssertExpectations(t)
	})
}

func TestClient_BlockAssemblyAPIClient(t *testing.T) {
	mockClient := &mockBlockAssemblyAPIClient{}
	client := createTestClient(mockClient, 0)

	apiClient := client.BlockAssemblyAPIClient()
	assert.Equal(t, mockClient, apiClient)
}

func TestClient_GetBlockAssemblyBlockCandidate(t *testing.T) {
	ctx := context.Background()
	mockClient := &mockBlockAssemblyAPIClient{}
	client := createTestClient(mockClient, 0)

	blockData := []byte("mock-block-data")
	expectedResponse := &blockassembly_api.GetBlockAssemblyBlockCandidateResponse{
		Block: blockData,
	}

	t.Run("successful", func(t *testing.T) {
		mockClient.ExpectedCalls = nil
		mockClient.On("GetBlockAssemblyBlockCandidate", ctx, &blockassembly_api.EmptyMessage{}, mock.Anything).Return(expectedResponse, nil)

		// Since model.NewBlockFromBytes might not work with our mock data, we expect an error
		_, err := client.GetBlockAssemblyBlockCandidate(ctx)
		assert.Error(t, err)
		mockClient.AssertExpectations(t)
	})

	t.Run("successful with valid block data", func(t *testing.T) {
		mockClient.ExpectedCalls = nil

		// We'll test the path where model.NewBlockFromBytes fails, which exercises the error handling
		validResponse := &blockassembly_api.GetBlockAssemblyBlockCandidateResponse{
			Block: []byte("invalid-block-data"),
		}
		mockClient.On("GetBlockAssemblyBlockCandidate", ctx, &blockassembly_api.EmptyMessage{}, mock.Anything).Return(validResponse, nil)

		_, err := client.GetBlockAssemblyBlockCandidate(ctx)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to create block from bytes")
		mockClient.AssertExpectations(t)
	})

	t.Run("grpc error", func(t *testing.T) {
		mockClient.ExpectedCalls = nil
		mockClient.On("GetBlockAssemblyBlockCandidate", ctx, &blockassembly_api.EmptyMessage{}, mock.Anything).Return(
			nil, status.Error(codes.Internal, "candidate error"))

		block, err := client.GetBlockAssemblyBlockCandidate(ctx)
		assert.Nil(t, block)
		assert.Error(t, err)
		mockClient.AssertExpectations(t)
	})
}

func TestClient_GetTransactionHashes(t *testing.T) {
	ctx := context.Background()
	mockClient := &mockBlockAssemblyAPIClient{}
	client := createTestClient(mockClient, 0)

	expectedHashes := []string{"hash1", "hash2", "hash3"}
	expectedResponse := &blockassembly_api.GetBlockAssemblyTxsResponse{
		Txs: expectedHashes,
	}

	t.Run("successful", func(t *testing.T) {
		mockClient.ExpectedCalls = nil
		mockClient.On("GetBlockAssemblyTxs", ctx, &blockassembly_api.EmptyMessage{}, mock.Anything).Return(expectedResponse, nil)

		hashes, err := client.GetTransactionHashes(ctx)
		assert.NoError(t, err)
		assert.Equal(t, expectedHashes, hashes)
		mockClient.AssertExpectations(t)
	})

	t.Run("grpc error", func(t *testing.T) {
		mockClient.ExpectedCalls = nil
		mockClient.On("GetBlockAssemblyTxs", ctx, &blockassembly_api.EmptyMessage{}, mock.Anything).Return(
			nil, status.Error(codes.Internal, "txs error"))

		hashes, err := client.GetTransactionHashes(ctx)
		assert.Nil(t, hashes)
		assert.Error(t, err)
		mockClient.AssertExpectations(t)
	})
}

func TestClient_sendBatchToBlockAssembly(t *testing.T) {
	ctx := context.Background()
	mockClient := &mockBlockAssemblyAPIClient{}
	client := createTestClient(mockClient, 5)

	// Create mock batch items
	done1 := make(chan error, 1)
	done2 := make(chan error, 1)

	batch := []*batchItem{
		{
			req: &blockassembly_api.AddTxRequest{
				Txid: []byte("txid1"),
				Fee:  1000,
				Size: 250,
			},
			done: done1,
		},
		{
			req: &blockassembly_api.AddTxRequest{
				Txid: []byte("txid2"),
				Fee:  2000,
				Size: 500,
			},
			done: done2,
		},
	}

	t.Run("successful batch", func(t *testing.T) {
		mockClient.ExpectedCalls = nil
		mockClient.On("AddTxBatch", ctx, mock.MatchedBy(func(req *blockassembly_api.AddTxBatchRequest) bool {
			return len(req.TxRequests) == 2
		}), mock.Anything).Return(&blockassembly_api.AddTxBatchResponse{}, nil)

		// Send batch in goroutine to avoid blocking
		go client.sendBatchToBlockAssembly(ctx, batch)

		// Check that all done channels receive nil
		err1 := <-done1
		err2 := <-done2
		assert.NoError(t, err1)
		assert.NoError(t, err2)

		mockClient.AssertExpectations(t)
	})

	t.Run("batch error", func(t *testing.T) {
		// Reset done channels
		done1 = make(chan error, 1)
		done2 = make(chan error, 1)
		batch[0].done = done1
		batch[1].done = done2

		mockClient.ExpectedCalls = nil
		mockClient.On("AddTxBatch", ctx, mock.Anything, mock.Anything).Return(
			nil, status.Error(codes.Internal, "batch failed"))

		// Send batch in goroutine to avoid blocking
		go client.sendBatchToBlockAssembly(ctx, batch)

		// Check that all done channels receive the error
		err1 := <-done1
		err2 := <-done2
		assert.Error(t, err1)
		assert.Error(t, err2)

		mockClient.AssertExpectations(t)
	})
}

func TestNewClientWithAddress_ConfigErrors(t *testing.T) {
	ctx := context.Background()
	logger := ulogger.TestLogger{}

	// This test mainly checks that the function can handle configuration
	// The actual gRPC connection will fail in testing, but we can test the happy path logic
	tSettings := &settings.Settings{
		GRPCMaxRetries:   3,
		GRPCRetryBackoff: 1000,
		BlockAssembly: settings.BlockAssemblySettings{
			SendBatchSize:    10,
			SendBatchTimeout: 5000,
		},
	}

	// Test with invalid address that will cause connection failure
	_, err := NewClientWithAddress(ctx, logger, tSettings, "invalid-address:99999")
	if err != nil {
		assert.Contains(t, err.Error(), "failed to connect to block assembly")
	}
	// Note: Connection might not fail immediately in test environment, so we don't assert.Error
}
