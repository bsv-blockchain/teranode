package blockchain

import (
	"context"
	"net/http"
	"time"

	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/model"
	"github.com/bitcoin-sv/teranode/stores/blob/file"
	"github.com/bitcoin-sv/teranode/stores/blockchain/options"
	"github.com/bitcoin-sv/teranode/util"
	"github.com/bitcoin-sv/teranode/util/usql"
	"github.com/libsv/go-bt/v2/chainhash"
)

type MockStore struct {
	Blocks        map[chainhash.Hash]*model.Block
	BlockExists   map[chainhash.Hash]bool
	BlockByHeight map[uint32]*model.Block
	BestBlock     *model.Block
	state         string
}

func NewMockStore() *MockStore {
	return &MockStore{
		Blocks:        map[chainhash.Hash]*model.Block{},
		BlockExists:   map[chainhash.Hash]bool{},
		BlockByHeight: map[uint32]*model.Block{},
		state:         "IDLE",
	}
}

const implementMe = "implement me"

func (m *MockStore) Health(ctx context.Context, checkLiveness bool) (int, string, error) {
	return http.StatusOK, "OK", nil
}

func (m *MockStore) GetDB() *usql.DB {
	return nil
}

func (m *MockStore) GetDBEngine() util.SQLEngine {
	// TODO implement me
	panic(implementMe)
}

func (m *MockStore) GetHeader(ctx context.Context, blockHash *chainhash.Hash) (*model.BlockHeader, error) {
	// TODO implement me
	panic(implementMe)
}

func (m *MockStore) GetBlock(ctx context.Context, blockHash *chainhash.Hash) (*model.Block, uint32, error) {
	block, ok := m.Blocks[*blockHash]
	if !ok {
		return nil, 0, errors.ErrBlockNotFound
	}

	return block, block.Height, nil
}

func (m *MockStore) GetBlocks(ctx context.Context, blockHash *chainhash.Hash, numberOfBlocks uint32) ([]*model.Block, error) {
	// TODO implement me
	panic(implementMe)
}

func (m *MockStore) GetBlockByHeight(ctx context.Context, height uint32) (*model.Block, error) {
	block, ok := m.BlockByHeight[height]
	if !ok {
		return nil, errors.ErrBlockNotFound
	}

	return block, nil
}

func (m *MockStore) GetBlockInChainByHeightHash(ctx context.Context, height uint32, hash *chainhash.Hash) (*model.Block, bool, error) {
	block, err := m.GetBlockByHeight(ctx, height)
	if err != nil {
		return nil, false, err
	}

	return block, false, nil
}

func (m *MockStore) GetBlockStats(ctx context.Context) (*model.BlockStats, error) {
	// TODO implement me
	panic(implementMe)
}

func (m *MockStore) GetBlockGraphData(ctx context.Context, periodMillis uint64) (*model.BlockDataPoints, error) {
	// TODO implement me
	panic(implementMe)
}

func (m *MockStore) GetLastNBlocks(ctx context.Context, n int64, includeOrphans bool, fromHeight uint32) ([]*model.BlockInfo, error) {
	// TODO implement me
	panic(implementMe)
}

func (m *MockStore) GetSuitableBlock(ctx context.Context, blockHash *chainhash.Hash) (*model.SuitableBlock, error) {
	// TODO implement me
	panic(implementMe)
}

func (m *MockStore) GetHashOfAncestorBlock(ctx context.Context, blockHash *chainhash.Hash, depth int) (*chainhash.Hash, error) {
	// TODO implement me
	panic(implementMe)
}

func (m *MockStore) GetBlockExists(_ context.Context, blockHash *chainhash.Hash) (bool, error) {
	exists, ok := m.BlockExists[*blockHash]

	if !ok {
		return false, nil
	}

	return exists, nil
}

func (m *MockStore) GetBlockHeight(ctx context.Context, blockHash *chainhash.Hash) (uint32, error) {
	// TODO implement me
	panic(implementMe)
}

func (m *MockStore) StoreBlock(ctx context.Context, block *model.Block, peerID string, opts ...options.StoreBlockOption) (uint64, uint32, error) {
	m.Blocks[*block.Hash()] = block
	m.BlockByHeight[block.Height] = block
	m.BlockExists[*block.Hash()] = true

	if m.BestBlock == nil || block.Height > m.BestBlock.Height {
		m.BestBlock = block
	}

	return uint64(block.Height), block.Height, nil
}

func (m *MockStore) GetBestBlockHeader(ctx context.Context) (*model.BlockHeader, *model.BlockHeaderMeta, error) {
	return m.BestBlock.Header, &model.BlockHeaderMeta{Height: m.BestBlock.Height}, nil
}

func (m *MockStore) GetBlockHeader(ctx context.Context, blockHash *chainhash.Hash) (*model.BlockHeader, *model.BlockHeaderMeta, error) {
	block, ok := m.Blocks[*blockHash]
	if !ok {
		return nil, nil, errors.NewBlockNotFoundError(blockHash.String())
	}

	return block.Header, &model.BlockHeaderMeta{Height: block.Height}, nil
}

func (m *MockStore) GetBlockHeaders(ctx context.Context, blockHash *chainhash.Hash, numberOfHeaders uint64) ([]*model.BlockHeader, []*model.BlockHeaderMeta, error) {
	headers := make([]*model.BlockHeader, 0, numberOfHeaders)
	metas := make([]*model.BlockHeaderMeta, 0, numberOfHeaders)

	currentHash := blockHash
	for i := uint64(0); i < numberOfHeaders; i++ {
		block, ok := m.Blocks[*currentHash]
		if !ok {
			break
		}

		headers = append(headers, block.Header)
		metas = append(metas, &model.BlockHeaderMeta{
			ID:        block.ID,
			Height:    block.Height,
			TxCount:   block.TransactionCount,
			BlockTime: block.Header.Timestamp,
		})

		currentHash = block.Header.HashPrevBlock
	}

	return headers, metas, nil
}

func (m *MockStore) GetBlockHeadersFromTill(ctx context.Context, blockHashFrom *chainhash.Hash, blockHashTill *chainhash.Hash) ([]*model.BlockHeader, []*model.BlockHeaderMeta, error) {
	return []*model.BlockHeader{}, []*model.BlockHeaderMeta{}, nil
}

func (m *MockStore) GetForkedBlockHeaders(ctx context.Context, blockHash *chainhash.Hash, numberOfHeaders uint64) ([]*model.BlockHeader, []*model.BlockHeaderMeta, error) {
	// TODO implement me
	panic(implementMe)
}

func (m *MockStore) GetBlockHeadersFromHeight(ctx context.Context, height, limit uint32) ([]*model.BlockHeader, []*model.BlockHeaderMeta, error) {
	// TODO implement me
	panic(implementMe)
}

func (m *MockStore) GetBlockHeadersByHeight(ctx context.Context, startHeight, endHeight uint32) ([]*model.BlockHeader, []*model.BlockHeaderMeta, error) {
	// TODO implement me
	panic(implementMe)
}

func (m *MockStore) InvalidateBlock(ctx context.Context, blockHash *chainhash.Hash) error {
	// TODO implement me
	panic(implementMe)
}

func (m *MockStore) RevalidateBlock(ctx context.Context, blockHash *chainhash.Hash) error {
	// TODO implement me
	panic(implementMe)
}

func (m *MockStore) GetBlockHeaderIDs(ctx context.Context, blockHash *chainhash.Hash, numberOfHeaders uint64) ([]uint32, error) {
	return []uint32{}, nil
}

func (m *MockStore) GetState(ctx context.Context, key string) ([]byte, error) {
	// TODO implement me
	panic(implementMe)
}

func (m *MockStore) SetState(ctx context.Context, key string, data []byte) error {
	// TODO implement me
	panic(implementMe)
}

func (m *MockStore) SetBlockMinedSet(ctx context.Context, blockHash *chainhash.Hash) error {
	// TODO implement me
	panic(implementMe)
}

func (m *MockStore) GetBlocksMinedNotSet(_ context.Context) ([]*model.Block, error) {
	return []*model.Block{}, nil
}

func (m *MockStore) SetBlockSubtreesSet(ctx context.Context, blockHash *chainhash.Hash) error {
	return nil
}

func (m *MockStore) GetBlocksSubtreesNotSet(ctx context.Context) ([]*model.Block, error) {
	return []*model.Block{}, nil
}

func (m *MockStore) GetBlocksByTime(ctx context.Context, fromTime, toTime time.Time) ([][]byte, error) {
	// TODO implement me
	panic(implementMe)
}

func (m *MockStore) LocateBlockHeaders(ctx context.Context, locator []*chainhash.Hash, hashStop *chainhash.Hash, maxHashes uint32) ([]*model.BlockHeader, error) {
	// TODO implement me
	panic(implementMe)
}

func (m *MockStore) ExportBlockDB(ctx context.Context, hash *chainhash.Hash) (*file.File, error) {
	// TODO implement me
	panic(implementMe)
}

func (m *MockStore) CheckBlockIsInCurrentChain(ctx context.Context, blockIDs []uint32) (bool, error) {
	// TODO implement me
	panic(implementMe)
}

func (m *MockStore) SetFSMState(ctx context.Context, fsmState string) error {
	m.state = fsmState
	return nil
}

func (m *MockStore) GetFSMState(ctx context.Context) (string, error) {
	return m.state, nil
}
