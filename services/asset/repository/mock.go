package repository

import (
	"context"
	"io"

	"github.com/bitcoin-sv/teranode/model"
	"github.com/bitcoin-sv/teranode/stores/utxo"
	"github.com/bitcoin-sv/teranode/stores/utxo/meta"
	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-subtree"
	"github.com/stretchr/testify/mock"
)

var _ Interface = (*Mock)(nil)

type Mock struct {
	mock.Mock
}

func (m *Mock) GetTxMeta(_ context.Context, hash *chainhash.Hash) (*meta.Data, error) {
	args := m.Called(hash)

	if args.Error(1) != nil {
		return nil, args.Error(1)
	}

	// return the mocked response
	return args.Get(0).(*meta.Data), args.Error(1)
}

func (m *Mock) GetLegacyBlockReader(_ context.Context, hash *chainhash.Hash, _ ...bool) (*io.PipeReader, error) {
	args := m.Called(hash)

	if args.Error(1) != nil {
		return nil, args.Error(1)
	}

	// return the mocked response
	return args.Get(0).(*io.PipeReader), args.Error(1)
}

func (m *Mock) Health(_ context.Context, _ bool) (int, string, error) {
	return 0, "", nil
}

func (m *Mock) GetTransaction(_ context.Context, hash *chainhash.Hash) ([]byte, error) {
	// use the mock to record the function call
	args := m.Called(hash)

	if args.Error(1) != nil {
		return nil, args.Error(1)
	}

	// return the mocked response
	return args.Get(0).([]byte), args.Error(1)
}

func (m *Mock) GetBlockStats(_ context.Context) (*model.BlockStats, error) {
	args := m.Called()

	if args.Error(1) != nil {
		return nil, args.Error(1)
	}

	return args.Get(0).(*model.BlockStats), args.Error(1)
}

func (m *Mock) GetBlockGraphData(_ context.Context, periodMillis uint64) (*model.BlockDataPoints, error) {
	args := m.Called(periodMillis)

	if args.Error(1) != nil {
		return nil, args.Error(1)
	}

	return args.Get(0).(*model.BlockDataPoints), args.Error(1)
}

func (m *Mock) GetTransactionMeta(_ context.Context, hash *chainhash.Hash) (*meta.Data, error) {
	args := m.Called(hash)

	if args.Error(1) != nil {
		return nil, args.Error(1)
	}

	// return the mocked response
	return args.Get(0).(*meta.Data), args.Error(1)
}

func (m *Mock) GetBlockByHash(_ context.Context, hash *chainhash.Hash) (*model.Block, error) {
	args := m.Called(hash)

	if args.Error(1) != nil {
		return nil, args.Error(1)
	}

	return args.Get(0).(*model.Block), args.Error(1)
}

func (m *Mock) GetBlockByHeight(_ context.Context, height uint32) (*model.Block, error) {
	args := m.Called(height)

	if args.Error(1) != nil {
		return nil, args.Error(1)
	}

	return args.Get(0).(*model.Block), args.Error(1)
}

func (m *Mock) GetLastNBlocks(_ context.Context, n int64, includeOrphans bool, fromHeight uint32) ([]*model.BlockInfo, error) {
	args := m.Called(n, includeOrphans, fromHeight)

	if args.Error(1) != nil {
		return nil, args.Error(1)
	}

	return args.Get(0).([]*model.BlockInfo), args.Error(1)
}

func (m *Mock) GetBlocks(_ context.Context, hash *chainhash.Hash, n uint32) ([]*model.Block, error) {
	args := m.Called(hash, n)

	if args.Error(1) != nil {
		return nil, args.Error(1)
	}

	return args.Get(0).([]*model.Block), args.Error(1)
}

func (m *Mock) GetBlockHeaders(_ context.Context, hash *chainhash.Hash, numberOfHeaders uint64) ([]*model.BlockHeader, []*model.BlockHeaderMeta, error) {
	args := m.Called(hash, numberOfHeaders)

	if args.Error(2) != nil {
		// return nil if there is an error, as code expects
		return nil, nil, args.Error(2)
	}

	return args.Get(0).([]*model.BlockHeader), args.Get(1).([]*model.BlockHeaderMeta), args.Error(2)
}

func (m *Mock) GetBlockHeadersToCommonAncestor(_ context.Context, hasTarget *chainhash.Hash, blockLocatorHashes []*chainhash.Hash, maxHeaders uint32) ([]*model.BlockHeader, []*model.BlockHeaderMeta, error) {
	args := m.Called(hasTarget, blockLocatorHashes, maxHeaders)

	if args.Error(2) != nil {
		return nil, nil, args.Error(2)
	}

	return args.Get(0).([]*model.BlockHeader), args.Get(1).([]*model.BlockHeaderMeta), args.Error(2)
}

func (m *Mock) GetBlockHeadersFromHeight(_ context.Context, height, limit uint32) ([]*model.BlockHeader, []*model.BlockHeaderMeta, error) {
	args := m.Called(height, limit)

	if args.Error(2) != nil {
		return nil, nil, args.Error(2)
	}

	return args.Get(0).([]*model.BlockHeader), args.Get(1).([]*model.BlockHeaderMeta), args.Error(2)
}

func (m *Mock) GetSubtreeBytes(_ context.Context, hash *chainhash.Hash) ([]byte, error) {
	args := m.Called(hash)

	if args.Error(1) != nil {
		return nil, args.Error(1)
	}

	return args.Get(0).([]byte), args.Error(1)
}

func (m *Mock) GetSubtreeTxIDsReader(_ context.Context, hash *chainhash.Hash) (io.ReadCloser, error) {
	args := m.Called(hash)

	if args.Error(1) != nil {
		return nil, args.Error(1)
	}

	return args.Get(0).(io.ReadCloser), args.Error(1)
}

func (m *Mock) GetSubtreeDataReaderFromBlockPersister(_ context.Context, hash *chainhash.Hash) (io.ReadCloser, error) {
	args := m.Called(hash)

	if args.Error(1) != nil {
		return nil, args.Error(1)
	}

	return args.Get(0).(io.ReadCloser), args.Error(1)
}

func (m *Mock) GetSubtreeDataReader(ctx context.Context, subtreeHash *chainhash.Hash) (*io.PipeReader, error) {
	args := m.Called(ctx, subtreeHash)

	if args.Error(1) != nil {
		return nil, args.Error(1)
	}

	return args.Get(0).(*io.PipeReader), args.Error(1)
}

func (m *Mock) GetSubtree(_ context.Context, hash *chainhash.Hash) (*subtree.Subtree, error) {
	args := m.Called(hash)

	if args.Error(1) != nil {
		return nil, args.Error(1)
	}

	return args.Get(0).(*subtree.Subtree), args.Error(1)
}

func (m *Mock) GetSubtreeData(ctx context.Context, hash *chainhash.Hash) (*subtree.SubtreeData, error) {
	args := m.Called(ctx, hash)

	if args.Error(1) != nil {
		return nil, args.Error(1)
	}

	return args.Get(0).(*subtree.SubtreeData), args.Error(1)
}

func (m *Mock) GetSubtreeTransactions(ctx context.Context, hash *chainhash.Hash) (map[chainhash.Hash]*bt.Tx, error) {
	args := m.Called(ctx, hash)

	if args.Error(1) != nil {
		return nil, args.Error(1)
	}

	// return the mocked response
	return args.Get(0).(map[chainhash.Hash]*bt.Tx), args.Error(1)
}

func (m *Mock) GetSubtreeExists(_ context.Context, hash *chainhash.Hash) (bool, error) {
	args := m.Called(hash)

	return args.Bool(0), args.Error(1)
}

func (m *Mock) GetSubtreeHead(_ context.Context, hash *chainhash.Hash) (*subtree.Subtree, int, error) {
	args := m.Called(hash)

	if args.Error(2) != nil {
		return nil, 0, args.Error(2)
	}

	return args.Get(0).(*subtree.Subtree), args.Int(1), args.Error(2)
}

func (m *Mock) GetUtxo(_ context.Context, spend *utxo.Spend) (*utxo.SpendResponse, error) {
	args := m.Called(spend)

	if args.Error(1) != nil {
		return nil, args.Error(1)
	}

	return args.Get(0).(*utxo.SpendResponse), args.Error(1)
}

func (m *Mock) GetBestBlockHeader(_ context.Context) (*model.BlockHeader, *model.BlockHeaderMeta, error) {
	args := m.Called()

	if args.Error(2) != nil {
		// return nil if there is an error, as code expects
		return nil, nil, args.Error(2)
	}

	return args.Get(0).(*model.BlockHeader), args.Get(1).(*model.BlockHeaderMeta), args.Error(2)
}

func (m *Mock) GetBlockHeader(_ context.Context, hash *chainhash.Hash) (*model.BlockHeader, *model.BlockHeaderMeta, error) {
	args := m.Called(hash)

	if args.Error(2) != nil {
		// return nil if there is an error, as code expects
		return nil, nil, args.Error(2)
	}

	return args.Get(0).(*model.BlockHeader), args.Get(1).(*model.BlockHeaderMeta), args.Error(2)
}

func (m *Mock) GetBlockLocator(_ context.Context, _ *chainhash.Hash, _ uint32) ([]*chainhash.Hash, error) {
	return nil, nil
}
