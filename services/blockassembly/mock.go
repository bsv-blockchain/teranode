package blockassembly

import (
	"context"

	"github.com/bitcoin-sv/teranode/model"
	"github.com/bitcoin-sv/teranode/services/blockassembly/blockassembly_api"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/stretchr/testify/mock"
)

// Mock implements a mock version of the ClientI interface for testing.
type Mock struct {
	mock.Mock
}

// NewMock creates a new mock client.
//
// Returns:
//   - ClientI: Mock client implementation
//   - error: Any error encountered during creation
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

func (m *Mock) Store(ctx context.Context, hash *chainhash.Hash, fee, size uint64, parentTxHashes []chainhash.Hash) (bool, error) {
	args := m.Called(ctx, hash, fee, size, parentTxHashes)

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

func (m *Mock) DeDuplicateBlockAssembly(ctx context.Context) error {
	args := m.Called(ctx)

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
