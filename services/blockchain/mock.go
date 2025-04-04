// Package blockchain provides mocking functionality for testing.
package blockchain

import (
	"context"
	"net/http"
	"time"

	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/model"
	"github.com/bitcoin-sv/teranode/stores/blob/file"
	blockchain_options "github.com/bitcoin-sv/teranode/stores/blockchain/options"
	"github.com/bitcoin-sv/teranode/util"
	"github.com/bitcoin-sv/teranode/util/usql"
	"github.com/libsv/go-bt/v2/chainhash"
)

// newMockStore creates a new mock store instance with optional initial block.
func newMockStore(block *model.Block) *mockStore {
	return &mockStore{
		block:            block,
		state:            "STOPPED",
		getBlockByHeight: make(map[uint32]*model.Block),
	}
}

// mockStore implements a mock blockchain store for testing.
type mockStore struct {
	block            *model.Block            // Current block
	state            string                  // Current state
	getBlockByHeight map[uint32]*model.Block // Map of blocks by height
}

func (s *mockStore) Health(ctx context.Context, checkLiveness bool) (int, string, error) {
	return http.StatusOK, "OK", nil
}

func (s *mockStore) GetDB() *usql.DB {
	panic("not implemented")
}

func (s *mockStore) GetDBEngine() util.SQLEngine {
	panic("not implemented")
}
func (s *mockStore) GetHeader(ctx context.Context, blockHash *chainhash.Hash) (*model.BlockHeader, error) {
	panic("not implemented")
}
func (s *mockStore) GetBlock(ctx context.Context, blockHash *chainhash.Hash) (*model.Block, uint32, error) {
	return s.block, 0, nil
}
func (s *mockStore) GetBlocks(ctx context.Context, blockHash *chainhash.Hash, numberOfBlockss uint32) ([]*model.Block, error) {
	panic("not implemented")
}
func (s *mockStore) GetBlockByHeight(_ context.Context, height uint32) (*model.Block, error) {
	if block, ok := s.getBlockByHeight[height]; ok {
		return block, nil
	}

	return nil, errors.ErrBlockNotFound
}
func (s *mockStore) GetBlockInChainByHeightHash(ctx context.Context, height uint32, startHash *chainhash.Hash) (*model.Block, bool, error) {
	block, err := s.GetBlockByHeight(ctx, height)
	if err != nil {
		return nil, false, err
	}

	return block, false, nil
}

func (s *mockStore) GetBlockStats(ctx context.Context) (*model.BlockStats, error) {
	panic("not implemented")
}
func (s *mockStore) GetBlockGraphData(ctx context.Context, periodMillis uint64) (*model.BlockDataPoints, error) {
	panic("not implemented")
}
func (s *mockStore) GetLastNBlocks(ctx context.Context, n int64, includeOrphans bool, fromHeight uint32) ([]*model.BlockInfo, error) {
	panic("not implemented")
}
func (s *mockStore) GetSuitableBlock(ctx context.Context, blockHash *chainhash.Hash) (*model.SuitableBlock, error) {
	panic("not implemented")
}
func (s *mockStore) GetHashOfAncestorBlock(ctx context.Context, blockHash *chainhash.Hash, depth int) (*chainhash.Hash, error) {
	panic("not implemented")
}
func (s *mockStore) GetBlockExists(ctx context.Context, blockHash *chainhash.Hash) (bool, error) {
	panic("not implemented")
}
func (s *mockStore) GetBlockHeight(ctx context.Context, blockHash *chainhash.Hash) (uint32, error) {
	panic("not implemented")
}
func (s *mockStore) StoreBlock(ctx context.Context, block *model.Block, peerID string, opts ...blockchain_options.StoreBlockOption) (uint64, uint32, error) {
	s.block = block
	return 0, 0, nil
}
func (s *mockStore) GetBestBlockHeader(ctx context.Context) (*model.BlockHeader, *model.BlockHeaderMeta, error) {
	panic("not implemented")
}
func (s *mockStore) GetBlockHeader(ctx context.Context, blockHash *chainhash.Hash) (*model.BlockHeader, *model.BlockHeaderMeta, error) {
	panic("not implemented")
}
func (s *mockStore) GetBlockHeaders(ctx context.Context, blockHash *chainhash.Hash, numberOfHeaders uint64) ([]*model.BlockHeader, []*model.BlockHeaderMeta, error) {
	panic("not implemented")
}
func (s *mockStore) GetBlockHeadersFromTill(ctx context.Context, blockHashFrom *chainhash.Hash, blockHashTill *chainhash.Hash) ([]*model.BlockHeader, []*model.BlockHeaderMeta, error) {
	panic("implement me")
}
func (s *mockStore) GetForkedBlockHeaders(ctx context.Context, blockHash *chainhash.Hash, numberOfHeaders uint64) ([]*model.BlockHeader, []*model.BlockHeaderMeta, error) {
	panic("not implemented")
}
func (s *mockStore) GetBlockHeadersFromHeight(ctx context.Context, height, limit uint32) ([]*model.BlockHeader, []*model.BlockHeaderMeta, error) {
	panic("not implemented")
}
func (s *mockStore) GetBlockHeadersByHeight(ctx context.Context, startHeight, endHeight uint32) ([]*model.BlockHeader, []*model.BlockHeaderMeta, error) {
	panic("not implemented")
}
func (s *mockStore) InvalidateBlock(ctx context.Context, blockHash *chainhash.Hash) error {
	panic("not implemented")
}
func (s *mockStore) RevalidateBlock(ctx context.Context, blockHash *chainhash.Hash) error {
	panic("not implemented")
}
func (s *mockStore) GetBlockHeaderIDs(ctx context.Context, blockHash *chainhash.Hash, numberOfHeaders uint64) ([]uint32, error) {
	panic("not implemented")
}
func (s *mockStore) GetState(ctx context.Context, key string) ([]byte, error) {
	panic("not implemented")
}
func (s *mockStore) SetState(ctx context.Context, key string, data []byte) error {
	panic("not implemented")
}

//nolint:unused
func (s *mockStore) GetBlockByID(ctx context.Context, id uint64) (*model.Block, error) {
	// TODO implement me
	panic("implement me")
}

//nolint:unused
func (s *mockStore) GetBlockIsMined(ctx context.Context, blockHash *chainhash.Hash) (bool, error) {
	// TODO implement me
	panic("implement me")
}
func (s *mockStore) SetBlockMinedSet(ctx context.Context, blockHash *chainhash.Hash) error {
	panic("not implemented")
}
func (s *mockStore) GetBlocksMinedNotSet(ctx context.Context) ([]*model.Block, error) {
	panic("not implemented")
}
func (s *mockStore) SetBlockSubtreesSet(ctx context.Context, blockHash *chainhash.Hash) error {
	panic("not implemented")
}
func (s *mockStore) GetBlocksSubtreesNotSet(ctx context.Context) ([]*model.Block, error) {
	panic("not implemented")
}
func (s *mockStore) GetBlocksByTime(ctx context.Context, fromTime, toTime time.Time) ([][]byte, error) {
	panic("not implemented")
}
func (s *mockStore) LocateBlockHeaders(ctx context.Context, locator []*chainhash.Hash, hashStop *chainhash.Hash, maxHashes uint32) ([]*model.BlockHeader, error) {
	// TODO implement me
	panic("implement me")
}

func (s *mockStore) ExportBlockDB(ctx context.Context, hash *chainhash.Hash) (*file.File, error) {
	panic("not implemented")
}

func (s *mockStore) CheckBlockIsInCurrentChain(ctx context.Context, blockIDs []uint32) (bool, error) {
	panic("not implemented")
}
func (s *mockStore) SetFSMState(ctx context.Context, fsmState string) error {
	s.state = fsmState
	return nil
}
func (s *mockStore) GetFSMState(ctx context.Context) (string, error) {
	return s.state, nil
}
