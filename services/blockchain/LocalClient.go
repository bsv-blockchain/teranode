package blockchain

import (
	"context"
	"github.com/bitcoin-sv/ubsv/errors"
	"time"

	"github.com/bitcoin-sv/ubsv/model"
	"github.com/bitcoin-sv/ubsv/services/blockchain/blockchain_api"
	"github.com/bitcoin-sv/ubsv/stores/blob"
	"github.com/bitcoin-sv/ubsv/stores/blockchain"
	"github.com/bitcoin-sv/ubsv/stores/utxo"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/libsv/go-bt/v2/chainhash"
)

// LocalClient is an abstraction for a client that has a stored embedded directly
type LocalClient struct {
	store        blockchain.Store
	subtreeStore blob.Store
	utxoStore    utxo.Store
	logger       ulogger.Logger
}

func NewLocalClient(logger ulogger.Logger, store blockchain.Store, subtreeStore blob.Store, utxoStore utxo.Store) (ClientI, error) {
	return &LocalClient{
		logger:       logger,
		store:        store,
		subtreeStore: subtreeStore,
		utxoStore:    utxoStore,
	}, nil
}

func (c LocalClient) Health(_ context.Context) (*blockchain_api.HealthResponse, error) {
	return &blockchain_api.HealthResponse{
		Ok: true,
	}, nil
}

func (c LocalClient) AddBlock(ctx context.Context, block *model.Block, peerID string) error {
	_, err := c.store.StoreBlock(ctx, block, peerID)
	return err
}

func (c LocalClient) GetBlock(ctx context.Context, blockHash *chainhash.Hash) (*model.Block, error) {
	block, _, err := c.store.GetBlock(ctx, blockHash)
	if err != nil {
		return nil, err
	}

	return block, nil
}

func (c LocalClient) GetBlocks(ctx context.Context, blockHash *chainhash.Hash, numberOfBlocks uint32) ([]*model.Block, error) {
	blocks, err := c.store.GetBlocks(ctx, blockHash, numberOfBlocks)
	if err != nil {
		return nil, err
	}

	return blocks, nil
}

func (c LocalClient) GetBlockByHeight(ctx context.Context, height uint32) (*model.Block, error) {
	block, err := c.store.GetBlockByHeight(ctx, height)
	if err != nil {
		return nil, err
	}

	return block, nil
}

func (c LocalClient) GetBlockStats(ctx context.Context) (*model.BlockStats, error) {
	return c.store.GetBlockStats(ctx)
}

func (c LocalClient) GetBlockGraphData(ctx context.Context, periodMillis uint64) (*model.BlockDataPoints, error) {
	return c.store.GetBlockGraphData(ctx, periodMillis)
}

func (c LocalClient) GetLastNBlocks(ctx context.Context, n int64, includeOrphans bool, fromHeight uint32) ([]*model.BlockInfo, error) {
	return c.store.GetLastNBlocks(ctx, n, includeOrphans, fromHeight)
}
func (c LocalClient) GetSuitableBlock(ctx context.Context, blockHash *chainhash.Hash) (*model.SuitableBlock, error) {
	return c.store.GetSuitableBlock(ctx, blockHash)
}
func (c LocalClient) GetHashOfAncestorBlock(ctx context.Context, blockHash *chainhash.Hash, num int) (*chainhash.Hash, error) {
	return c.store.GetHashOfAncestorBlock(ctx, blockHash, num)
}
func (c LocalClient) GetNextWorkRequired(ctx context.Context, blockHash *chainhash.Hash) (*model.NBit, error) {
	return nil, nil
}

func (c LocalClient) GetBlockExists(ctx context.Context, blockHash *chainhash.Hash) (bool, error) {
	exists, err := c.store.GetBlockExists(ctx, blockHash)
	if err != nil {
		return false, err
	}

	return exists, nil
}

func (c LocalClient) GetBestBlockHeader(ctx context.Context) (*model.BlockHeader, *model.BlockHeaderMeta, error) {
	return c.store.GetBestBlockHeader(ctx)
}

func (c LocalClient) GetBlockHeader(ctx context.Context, blockHash *chainhash.Hash) (*model.BlockHeader, *model.BlockHeaderMeta, error) {
	return c.store.GetBlockHeader(ctx, blockHash)
}

func (c LocalClient) GetBlockHeaders(ctx context.Context, blockHash *chainhash.Hash, numberOfHeaders uint64) ([]*model.BlockHeader, []*model.BlockHeaderMeta, error) {
	return c.store.GetBlockHeaders(ctx, blockHash, numberOfHeaders)
}

func (c LocalClient) GetBlockHeadersFromHeight(ctx context.Context, height, limit uint32) ([]*model.BlockHeader, []*model.BlockHeaderMeta, error) {
	return c.store.GetBlockHeadersFromHeight(ctx, height, limit)
}

func (c LocalClient) InvalidateBlock(ctx context.Context, blockHash *chainhash.Hash) error {
	return c.store.InvalidateBlock(ctx, blockHash)
}

func (c LocalClient) RevalidateBlock(ctx context.Context, blockHash *chainhash.Hash) error {
	return c.store.RevalidateBlock(ctx, blockHash)
}

func (c LocalClient) GetBlockHeaderIDs(ctx context.Context, blockHash *chainhash.Hash, numberOfHeaders uint64) ([]uint32, error) {
	return c.store.GetBlockHeaderIDs(ctx, blockHash, numberOfHeaders)
}

func (c LocalClient) SendNotification(ctx context.Context, notification *model.Notification) error {
	return nil
}

func (c LocalClient) Subscribe(ctx context.Context, source string) (chan *model.Notification, error) {
	return nil, nil
}

func (c LocalClient) GetState(ctx context.Context, key string) ([]byte, error) {
	return c.store.GetState(ctx, key)
}

func (c LocalClient) SetState(ctx context.Context, key string, data []byte) error {
	return c.store.SetState(ctx, key, data)
}

func (c LocalClient) SetBlockMinedSet(ctx context.Context, blockHash *chainhash.Hash) error {
	return c.store.SetBlockMinedSet(ctx, blockHash)
}

func (c LocalClient) GetBlocksMinedNotSet(ctx context.Context) ([]*model.Block, error) {
	return c.store.GetBlocksMinedNotSet(ctx)
}

func (c LocalClient) SetBlockSubtreesSet(ctx context.Context, blockHash *chainhash.Hash) error {
	return c.store.SetBlockSubtreesSet(ctx, blockHash)
}

func (c LocalClient) GetBlocksSubtreesNotSet(ctx context.Context) ([]*model.Block, error) {
	return c.store.GetBlocksSubtreesNotSet(ctx)
}

func (c LocalClient) GetFSMCurrentState(ctx context.Context) (*blockchain_api.FSMStateType, error) {
	// TODO: check what this should be?
	state := blockchain_api.FSMStateType_MINING
	return &state, nil
}

func (c LocalClient) SendFSMEvent(ctx context.Context, state blockchain_api.FSMEventType) error {
	// TODO: "implement me"
	return nil
}

// GetBlockLocator returns a block locator for the latest block.
// This function will be much faster, when moved to the server side.
func (c LocalClient) GetBlockLocator(ctx context.Context, blockHeaderHash *chainhash.Hash, blockHeaderHeight uint32) ([]*chainhash.Hash, error) {
	return getBlockLocator(ctx, c.store, blockHeaderHash, blockHeaderHeight)
}
func (c LocalClient) LocateBlockHeaders(ctx context.Context, locator []*chainhash.Hash, hashStop *chainhash.Hash, maxHashes uint32) ([]*model.BlockHeader, error) {
	return nil, nil
}
func (c LocalClient) GetBestHeightAndTime(ctx context.Context) (uint32, uint32, error) {
	blockHeader, meta, err := c.store.GetBestBlockHeader(ctx)
	if err != nil {
		return 0, 0, err
	}

	// get the median block time for the last 11 blocks
	headers, _, err := c.store.GetBlockHeaders(ctx, blockHeader.Hash(), 11)
	if err != nil {
		return 0, 0, err
	}

	prevTimeStamps := make([]time.Time, 0, 11)
	for _, header := range headers {
		prevTimeStamps = append(prevTimeStamps, time.Unix(int64(header.Timestamp), 0))
	}

	medianTimestamp, err := model.CalculateMedianTimestamp(prevTimeStamps)
	if err != nil {
		return 0, 0, errors.NewProcessingError("[Blockchain] could not calculate median block time", err)
	}

	return meta.Height, uint32(medianTimestamp.Unix()), nil
}
