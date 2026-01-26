// Package blockassembly provides functionality for assembling Bitcoin blocks in Teranode.
//
// This file contains mock implementations of the block assembly interfaces for testing purposes.
// The mock implementations use the testify/mock framework to provide controllable behavior
// during unit tests, allowing developers to simulate various scenarios and verify
// interactions with the block assembly service without requiring actual service dependencies.
package blockassembly

import (
	"context"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockassembly/blockassembly_api"
	"github.com/stretchr/testify/mock"
	"google.golang.org/grpc"
)

// Mock implements a mock version of the ClientI interface for testing.
// This mock provides controllable implementations of all ClientI methods,
// allowing tests to define expected behavior, verify method calls, and
// simulate various success and failure scenarios. It uses the testify/mock
// framework to track method calls and return predefined values.
//
// The mock is particularly useful for:
//   - Unit testing components that depend on block assembly functionality
//   - Integration testing without requiring a full block assembly service
//   - Simulating error conditions and edge cases
//   - Verifying correct interaction patterns with the block assembly API
type Mock struct {
	mock.Mock
}

// NewMock creates a new mock client instance.
// This constructor initializes a fresh mock that can be configured
// with expected method calls and return values for testing scenarios.
//
// Returns:
//   - *Mock: New mock client implementation ready for configuration
func NewMock() *Mock {
	return &Mock{}
}

func (m *Mock) Health(ctx context.Context, checkLiveness bool) (int, string, error) {
	args := m.Called(ctx, checkLiveness)

	if args.Error(2) != nil {
		return 0, "", args.Error(2)
	}

	return args.Int(0), args.String(1), args.Error(2)
}

func (m *Mock) Store(ctx context.Context, hash *chainhash.Hash, fee, size uint64, txInpoints subtree.TxInpoints) (bool, error) {
	args := m.Called(ctx, hash, fee, size, txInpoints)

	if args.Error(1) != nil {
		return false, args.Error(1)
	}

	return args.Bool(0), nil
}

func (m *Mock) RemoveTx(ctx context.Context, hash *chainhash.Hash) error {
	args := m.Called(ctx, hash)

	if args.Error(0) != nil {
		return args.Error(0)
	}

	return nil
}

func (m *Mock) GetMiningCandidate(ctx context.Context, includeSubtreeHashes ...bool) (*model.MiningCandidate, error) {
	args := m.Called(ctx, includeSubtreeHashes)

	if args.Error(1) != nil {
		return nil, args.Error(1)
	}

	return args.Get(0).(*model.MiningCandidate), nil
}

func (m *Mock) GetCurrentDifficulty(ctx context.Context) (float64, error) {
	args := m.Called(ctx)

	if args.Error(1) != nil {
		return 0, args.Error(1)
	}

	return args.Get(0).(float64), nil
}

func (m *Mock) SubmitMiningSolution(ctx context.Context, solution *model.MiningSolution) error {
	args := m.Called(ctx, solution)

	if args.Error(0) != nil {
		return args.Error(0)
	}

	return nil
}

func (m *Mock) GenerateBlocks(ctx context.Context, req *blockassembly_api.GenerateBlocksRequest) error {
	args := m.Called(ctx, req)

	if args.Error(0) != nil {
		return args.Error(0)
	}

	return nil
}

func (m *Mock) ResetBlockAssembly(ctx context.Context) error {
	args := m.Called(ctx)

	if args.Error(0) != nil {
		return args.Error(0)
	}

	return nil
}

func (m *Mock) ResetBlockAssemblyFully(ctx context.Context) error {
	args := m.Called(ctx)

	if args.Error(0) != nil {
		return args.Error(0)
	}

	return nil
}

func (m *Mock) GetBlockAssemblyState(ctx context.Context) (*blockassembly_api.StateMessage, error) {
	args := m.Called(ctx)

	if args.Error(1) != nil {
		return nil, args.Error(1)
	}

	return args.Get(0).(*blockassembly_api.StateMessage), nil
}

func (m *Mock) GetBlockAssemblyBlockCandidate(ctx context.Context) (*model.Block, error) {
	args := m.Called(ctx)

	if args.Error(1) != nil {
		return nil, args.Error(1)
	}

	return args.Get(0).(*model.Block), nil
}

func (m *Mock) GetTransactionHashes(ctx context.Context) ([]string, error) {
	args := m.Called(ctx)

	if args.Error(1) != nil {
		return nil, args.Error(1)
	}

	if args.Get(0) == nil {
		return nil, nil
	}

	return args.Get(0).([]string), nil
}

// mockBlockAssemblyAPIClient is a mock implementation of BlockAssemblyAPIClient
type mockBlockAssemblyAPIClient struct {
	mock.Mock
}

func (m *mockBlockAssemblyAPIClient) HealthGRPC(ctx context.Context, in *blockassembly_api.EmptyMessage, opts ...grpc.CallOption) (*blockassembly_api.HealthResponse, error) {
	args := m.Called(ctx, in, opts)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*blockassembly_api.HealthResponse), args.Error(1)
}

func (m *mockBlockAssemblyAPIClient) AddTx(ctx context.Context, in *blockassembly_api.AddTxRequest, opts ...grpc.CallOption) (*blockassembly_api.AddTxResponse, error) {
	args := m.Called(ctx, in, opts)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*blockassembly_api.AddTxResponse), args.Error(1)
}

func (m *mockBlockAssemblyAPIClient) RemoveTx(ctx context.Context, in *blockassembly_api.RemoveTxRequest, opts ...grpc.CallOption) (*blockassembly_api.EmptyMessage, error) {
	args := m.Called(ctx, in, opts)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*blockassembly_api.EmptyMessage), args.Error(1)
}

func (m *mockBlockAssemblyAPIClient) AddTxBatch(ctx context.Context, in *blockassembly_api.AddTxBatchRequest, opts ...grpc.CallOption) (*blockassembly_api.AddTxBatchResponse, error) {
	args := m.Called(ctx, in, opts)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*blockassembly_api.AddTxBatchResponse), args.Error(1)
}

func (m *mockBlockAssemblyAPIClient) AddTxBatchColumnar(ctx context.Context, in *blockassembly_api.AddTxBatchColumnarRequest, opts ...grpc.CallOption) (*blockassembly_api.AddTxBatchResponse, error) {
	args := m.Called(ctx, in, opts)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*blockassembly_api.AddTxBatchResponse), args.Error(1)
}

func (m *mockBlockAssemblyAPIClient) GetMiningCandidate(ctx context.Context, in *blockassembly_api.GetMiningCandidateRequest, opts ...grpc.CallOption) (*model.MiningCandidate, error) {
	args := m.Called(ctx, in, opts)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*model.MiningCandidate), args.Error(1)
}

func (m *mockBlockAssemblyAPIClient) GetCurrentDifficulty(ctx context.Context, in *blockassembly_api.EmptyMessage, opts ...grpc.CallOption) (*blockassembly_api.GetCurrentDifficultyResponse, error) {
	args := m.Called(ctx, in, opts)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*blockassembly_api.GetCurrentDifficultyResponse), args.Error(1)
}

func (m *mockBlockAssemblyAPIClient) SubmitMiningSolution(ctx context.Context, in *blockassembly_api.SubmitMiningSolutionRequest, opts ...grpc.CallOption) (*blockassembly_api.OKResponse, error) {
	args := m.Called(ctx, in, opts)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*blockassembly_api.OKResponse), args.Error(1)
}

func (m *mockBlockAssemblyAPIClient) ResetBlockAssembly(ctx context.Context, in *blockassembly_api.EmptyMessage, opts ...grpc.CallOption) (*blockassembly_api.EmptyMessage, error) {
	args := m.Called(ctx, in, opts)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*blockassembly_api.EmptyMessage), args.Error(1)
}

func (m *mockBlockAssemblyAPIClient) ResetBlockAssemblyFully(ctx context.Context, in *blockassembly_api.EmptyMessage, opts ...grpc.CallOption) (*blockassembly_api.EmptyMessage, error) {
	args := m.Called(ctx, in, opts)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*blockassembly_api.EmptyMessage), args.Error(1)
}

func (m *mockBlockAssemblyAPIClient) GetBlockAssemblyState(ctx context.Context, in *blockassembly_api.EmptyMessage, opts ...grpc.CallOption) (*blockassembly_api.StateMessage, error) {
	args := m.Called(ctx, in, opts)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*blockassembly_api.StateMessage), args.Error(1)
}

func (m *mockBlockAssemblyAPIClient) GenerateBlocks(ctx context.Context, in *blockassembly_api.GenerateBlocksRequest, opts ...grpc.CallOption) (*blockassembly_api.EmptyMessage, error) {
	args := m.Called(ctx, in, opts)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*blockassembly_api.EmptyMessage), args.Error(1)
}

func (m *mockBlockAssemblyAPIClient) CheckBlockAssembly(ctx context.Context, in *blockassembly_api.EmptyMessage, opts ...grpc.CallOption) (*blockassembly_api.OKResponse, error) {
	args := m.Called(ctx, in, opts)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*blockassembly_api.OKResponse), args.Error(1)
}

func (m *mockBlockAssemblyAPIClient) GetBlockAssemblyBlockCandidate(ctx context.Context, in *blockassembly_api.EmptyMessage, opts ...grpc.CallOption) (*blockassembly_api.GetBlockAssemblyBlockCandidateResponse, error) {
	args := m.Called(ctx, in, opts)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*blockassembly_api.GetBlockAssemblyBlockCandidateResponse), args.Error(1)
}

func (m *mockBlockAssemblyAPIClient) GetBlockAssemblyTxs(ctx context.Context, in *blockassembly_api.EmptyMessage, opts ...grpc.CallOption) (*blockassembly_api.GetBlockAssemblyTxsResponse, error) {
	args := m.Called(ctx, in, opts)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*blockassembly_api.GetBlockAssemblyTxsResponse), args.Error(1)
}
