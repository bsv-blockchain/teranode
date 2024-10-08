package blockchain

import (
	"context"
	"net/http"
	"time"

	"github.com/bitcoin-sv/ubsv/model"
	"github.com/bitcoin-sv/ubsv/stores/blob/file"
	"github.com/bitcoin-sv/ubsv/stores/blockchain/options"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/bitcoin-sv/ubsv/util/usql"
	"github.com/libsv/go-bt/v2/chainhash"
)

type MockStore struct {
	Blocks      map[chainhash.Hash]*model.Block
	BlockExists map[chainhash.Hash]bool
}

func NewMockStore() *MockStore {
	return &MockStore{
		Blocks:      map[chainhash.Hash]*model.Block{},
		BlockExists: map[chainhash.Hash]bool{},
	}
}

func (m MockStore) Health(ctx context.Context, checkLiveness bool) (int, string, error) {
	return http.StatusOK, "OK", nil
}

func (m MockStore) GetDB() *usql.DB {
	return nil
}

func (m MockStore) GetDBEngine() util.SQLEngine {
	//TODO implement me
	panic("implement me")
}

func (m MockStore) GetHeader(ctx context.Context, blockHash *chainhash.Hash) (*model.BlockHeader, error) {
	//TODO implement me
	panic("implement me")
}

func (m MockStore) GetBlock(ctx context.Context, blockHash *chainhash.Hash) (*model.Block, uint32, error) {
	//TODO implement me
	panic("implement me")
}

func (m MockStore) GetBlocks(ctx context.Context, blockHash *chainhash.Hash, numberOfBlocks uint32) ([]*model.Block, error) {
	//TODO implement me
	panic("implement me")
}

func (m MockStore) GetBlockByHeight(ctx context.Context, height uint32) (*model.Block, error) {
	//TODO implement me
	panic("implement me")
}

func (m MockStore) GetBlockStats(ctx context.Context) (*model.BlockStats, error) {
	//TODO implement me
	panic("implement me")
}

func (m MockStore) GetBlockGraphData(ctx context.Context, periodMillis uint64) (*model.BlockDataPoints, error) {
	//TODO implement me
	panic("implement me")
}

func (m MockStore) GetLastNBlocks(ctx context.Context, n int64, includeOrphans bool, fromHeight uint32) ([]*model.BlockInfo, error) {
	//TODO implement me
	panic("implement me")
}

func (m MockStore) GetSuitableBlock(ctx context.Context, blockHash *chainhash.Hash) (*model.SuitableBlock, error) {
	//TODO implement me
	panic("implement me")
}

func (m MockStore) GetHashOfAncestorBlock(ctx context.Context, blockHash *chainhash.Hash, depth int) (*chainhash.Hash, error) {
	//TODO implement me
	panic("implement me")
}

func (m MockStore) GetBlockExists(_ context.Context, blockHash *chainhash.Hash) (bool, error) {
	exists, ok := m.BlockExists[*blockHash]

	if !ok {
		return false, nil
	}

	return exists, nil
}

func (m MockStore) GetBlockHeight(ctx context.Context, blockHash *chainhash.Hash) (uint32, error) {
	//TODO implement me
	panic("implement me")
}

func (m MockStore) StoreBlock(ctx context.Context, block *model.Block, peerID string, opts ...options.StoreBlockOption) (uint64, uint32, error) {
	m.Blocks[*block.Hash()] = block

	return 0, 0, nil
}

func (m MockStore) GetBestBlockHeader(ctx context.Context) (*model.BlockHeader, *model.BlockHeaderMeta, error) {
	//TODO implement me
	panic("implement me")
}

func (m MockStore) GetBlockHeader(ctx context.Context, blockHash *chainhash.Hash) (*model.BlockHeader, *model.BlockHeaderMeta, error) {
	//TODO implement me
	panic("implement me")
}

func (m MockStore) GetBlockHeaders(ctx context.Context, blockHash *chainhash.Hash, numberOfHeaders uint64) ([]*model.BlockHeader, []*model.BlockHeaderMeta, error) {
	return []*model.BlockHeader{}, []*model.BlockHeaderMeta{}, nil
}

func (m MockStore) GetForkedBlockHeaders(ctx context.Context, blockHash *chainhash.Hash, numberOfHeaders uint64) ([]*model.BlockHeader, []*model.BlockHeaderMeta, error) {
	//TODO implement me
	panic("implement me")
}

func (m MockStore) GetBlockHeadersFromHeight(ctx context.Context, height, limit uint32) ([]*model.BlockHeader, []*model.BlockHeaderMeta, error) {
	//TODO implement me
	panic("implement me")
}

func (m MockStore) GetBlockHeadersByHeight(ctx context.Context, startHeight, endHeight uint32) ([]*model.BlockHeader, []*model.BlockHeaderMeta, error) {
	//TODO implement me
	panic("implement me")
}

func (m MockStore) InvalidateBlock(ctx context.Context, blockHash *chainhash.Hash) error {
	//TODO implement me
	panic("implement me")
}

func (m MockStore) RevalidateBlock(ctx context.Context, blockHash *chainhash.Hash) error {
	//TODO implement me
	panic("implement me")
}

func (m MockStore) GetBlockHeaderIDs(ctx context.Context, blockHash *chainhash.Hash, numberOfHeaders uint64) ([]uint32, error) {
	return []uint32{}, nil
}

func (m MockStore) GetState(ctx context.Context, key string) ([]byte, error) {
	//TODO implement me
	panic("implement me")
}

func (m MockStore) SetState(ctx context.Context, key string, data []byte) error {
	//TODO implement me
	panic("implement me")
}

func (m MockStore) SetBlockMinedSet(ctx context.Context, blockHash *chainhash.Hash) error {
	//TODO implement me
	panic("implement me")
}

func (m MockStore) GetBlocksMinedNotSet(_ context.Context) ([]*model.Block, error) {
	return []*model.Block{}, nil
}

func (m MockStore) SetBlockSubtreesSet(ctx context.Context, blockHash *chainhash.Hash) error {
	return nil
}

func (m MockStore) GetBlocksSubtreesNotSet(ctx context.Context) ([]*model.Block, error) {
	return []*model.Block{}, nil
}

func (m MockStore) GetBlocksByTime(ctx context.Context, fromTime, toTime time.Time) ([][]byte, error) {
	// TODO implement me
	panic("implement me")
}

func (m MockStore) LocateBlockHeaders(ctx context.Context, locator []*chainhash.Hash, hashStop *chainhash.Hash, maxHashes uint32) ([]*model.BlockHeader, error) {
	// TODO implement me
	panic("implement me")
}

func (m MockStore) ExportBlockDB(ctx context.Context, hash *chainhash.Hash) (*file.File, error) {
	// TODO implement me
	panic("implement me")
}

func (m MockStore) CheckBlockIsInCurrentChain(ctx context.Context, blockIDs []uint32) (bool, error) {
	// TODO implement me
	panic("implement me")
}
