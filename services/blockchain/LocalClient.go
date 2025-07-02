// Package blockchain provides functionality for managing the Bitcoin blockchain.
package blockchain

import (
	"context"
	"net/http"
	"time"

	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/model"
	"github.com/bitcoin-sv/teranode/services/blockchain/blockchain_api"
	"github.com/bitcoin-sv/teranode/stores/blob"
	"github.com/bitcoin-sv/teranode/stores/blockchain"
	"github.com/bitcoin-sv/teranode/stores/utxo"
	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/bitcoin-sv/teranode/util/health"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	safeconversion "github.com/bsv-blockchain/go-safe-conversion"
)

// LocalClient implements a blockchain client with direct store access.
// It is an abstraction for a client that has a stored embedded directly
type LocalClient struct {
	store        blockchain.Store // Blockchain store
	subtreeStore blob.Store       // Subtree store
	utxoStore    utxo.Store       // UTXO store
	logger       ulogger.Logger   // Logger instance
}

// NewLocalClient creates a new LocalClient instance with the provided dependencies.
func NewLocalClient(logger ulogger.Logger, store blockchain.Store, subtreeStore blob.Store, utxoStore utxo.Store) (ClientI, error) {
	return &LocalClient{
		logger:       logger,
		store:        store,
		subtreeStore: subtreeStore,
		utxoStore:    utxoStore,
	}, nil
}

// Health performs health checks for the LocalClient and its dependencies.
// This method implements the ClientI interface health check functionality by
// examining the status of the blockchain store, subtree store, and UTXO store.
//
// The method supports two types of health checks based on the checkLiveness parameter:
// - Liveness checks: Verify that the service itself is running and responsive
// - Readiness checks: Verify that the service and all its dependencies are ready to serve requests
//
// For liveness checks, the method performs minimal validation to ensure the service
// is not stuck or deadlocked. For readiness checks, it validates all configured
// store dependencies to ensure they are healthy and accessible.
//
// The health check process:
// - If checkLiveness is true, returns OK immediately (liveness check)
// - If checkLiveness is false, checks all configured stores (readiness check)
// - Aggregates results from all dependency health checks
// - Returns appropriate HTTP status codes and messages
//
// Parameters:
//   - ctx: Context for the operation with timeout and cancellation support
//   - checkLiveness: If true, performs liveness check; if false, performs readiness check
//
// Returns:
//   - int: HTTP status code (200 for healthy, 503 for unhealthy)
//   - string: Human-readable status message describing the health state
//   - error: Any error encountered during health checking
func (c LocalClient) Health(ctx context.Context, checkLiveness bool) (int, string, error) {
	if checkLiveness {
		// Add liveness checks here. Don't include dependency checks.
		// If the service is stuck return http.StatusServiceUnavailable
		// to indicate a restart is needed
		return http.StatusOK, "OK", nil
	}

	// Add readiness checks here. Include dependency checks.
	// If any dependency is not ready, return http.StatusServiceUnavailable
	// If all dependencies are ready, return http.StatusOK
	// A failed dependency check does not imply the service needs restarting
	checks := make([]health.Check, 0, 3)

	if c.store != nil {
		checks = append(checks, health.Check{Name: "BlockchainStore", Check: c.store.Health})
	}

	if c.subtreeStore != nil {
		checks = append(checks, health.Check{Name: "SubtreeStore", Check: c.subtreeStore.Health})
	}

	if c.utxoStore != nil {
		checks = append(checks, health.Check{Name: "UTXOStore", Check: c.utxoStore.Health})
	}

	return health.CheckAll(ctx, checkLiveness, checks)
}

func (c LocalClient) AddBlock(ctx context.Context, block *model.Block, peerID string) error {
	ID, height, err := c.store.StoreBlock(ctx, block, peerID)
	if err != nil {
		return err
	}

	c.logger.Infof("[Blockchain LocalClient] stored block %s (ID: %d, height: %d)", block.Hash(), ID, height)

	return nil
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
	return c.store.GetBlockByHeight(ctx, height)
}

func (c LocalClient) GetBlockByID(ctx context.Context, id uint64) (*model.Block, error) {
	return c.store.GetBlockByID(ctx, id)
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

func (c LocalClient) GetLastNInvalidBlocks(ctx context.Context, n int64) ([]*model.BlockInfo, error) {
	return c.store.GetLastNInvalidBlocks(ctx, n)
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

func (c LocalClient) GetBlockHeadersToCommonAncestor(ctx context.Context, hashTarget *chainhash.Hash, blockLocatorHashes []*chainhash.Hash, maxHeaders uint32) ([]*model.BlockHeader, []*model.BlockHeaderMeta, error) {
	return getBlockHeadersToCommonAncestor(ctx, c.store, hashTarget, blockLocatorHashes, maxHeaders)
}

func (c LocalClient) GetBlockHeadersFromTill(ctx context.Context, blockHashFrom *chainhash.Hash, blockHashTill *chainhash.Hash) ([]*model.BlockHeader, []*model.BlockHeaderMeta, error) {
	return c.store.GetBlockHeadersFromTill(ctx, blockHashFrom, blockHashTill)
}

func (c LocalClient) CheckBlockIsInCurrentChain(ctx context.Context, blockIDs []uint32) (bool, error) {
	return c.store.CheckBlockIsInCurrentChain(ctx, blockIDs)
}

func (c LocalClient) GetBlockHeadersFromHeight(ctx context.Context, height, limit uint32) ([]*model.BlockHeader, []*model.BlockHeaderMeta, error) {
	return c.store.GetBlockHeadersFromHeight(ctx, height, limit)
}

func (c LocalClient) GetBlockHeadersByHeight(ctx context.Context, startHeight, endHeight uint32) ([]*model.BlockHeader, []*model.BlockHeaderMeta, error) {
	return c.store.GetBlockHeadersByHeight(ctx, startHeight, endHeight)
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

func (c LocalClient) SendNotification(ctx context.Context, notification *blockchain_api.Notification) error {
	return nil
}

func (c LocalClient) Subscribe(ctx context.Context, source string) (chan *blockchain_api.Notification, error) {
	return nil, nil
}

func (c LocalClient) GetState(ctx context.Context, key string) ([]byte, error) {
	return c.store.GetState(ctx, key)
}

func (c LocalClient) SetState(ctx context.Context, key string, data []byte) error {
	return c.store.SetState(ctx, key, data)
}

func (c LocalClient) GetBlockIsMined(ctx context.Context, blockHash *chainhash.Hash) (bool, error) {
	return c.store.GetBlockIsMined(ctx, blockHash)
}

func (c LocalClient) SetBlockMinedSet(ctx context.Context, blockHash *chainhash.Hash) error {
	return c.store.SetBlockMinedSet(ctx, blockHash)
}

func (c *LocalClient) SetBlockProcessedAt(ctx context.Context, blockHash *chainhash.Hash, clear ...bool) error {
	return c.store.SetBlockProcessedAt(ctx, blockHash, clear...)
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

func (c LocalClient) GetFSMCurrentState(_ context.Context) (*FSMStateType, error) {
	// TODO: Placeholder for now
	state := FSMStateRUNNING
	return &state, nil
}

func (c LocalClient) IsFSMCurrentState(_ context.Context, state FSMStateType) (bool, error) {
	return state == FSMStateRUNNING, nil
}

func (c LocalClient) WaitForFSMtoTransitionToGivenState(_ context.Context, _ FSMStateType) error {
	return nil
}

func (c LocalClient) WaitUntilFSMTransitionFromIdleState(_ context.Context) error {
	return nil
}

func (c LocalClient) GetFSMCurrentStateForE2ETestMode() FSMStateType {
	// TODO: Fix me, this is a temporary solution
	return FSMStateRUNNING
}

func (c LocalClient) SendFSMEvent(_ context.Context, _ blockchain_api.FSMEventType) error {
	// TODO: "implement me"
	return nil
}

func (c LocalClient) Run(ctx context.Context, source string) error {
	return nil
}

func (c LocalClient) Idle(ctx context.Context) error {
	return nil
}

func (c LocalClient) CatchUpBlocks(ctx context.Context) error {
	return nil
}

func (c LocalClient) LegacySync(ctx context.Context) error {
	return nil
}

// GetBlockLocator returns a block locator for the latest block.
// This function will be much faster, when moved to the server side.
func (c LocalClient) GetBlockLocator(ctx context.Context, blockHeaderHash *chainhash.Hash, blockHeaderHeight uint32) ([]*chainhash.Hash, error) {
	return getBlockLocator(ctx, c.store, blockHeaderHash, blockHeaderHeight)
}

func (c LocalClient) GetChainTips(ctx context.Context) ([]*model.ChainTip, error) {
	return c.store.GetChainTips(ctx)
}

func (c LocalClient) LocateBlockHeaders(ctx context.Context, locator []*chainhash.Hash, hashStop *chainhash.Hash, maxHashes uint32) ([]*model.BlockHeader, error) {
	return nil, nil
}

// GetBestHeightAndTime retrieves the height and median timestamp of the best block.
// This method provides essential blockchain state information by returning both the
// current blockchain height and the median timestamp calculated from recent blocks.
//
// The method performs the following operations:
// - Retrieves the current best block header and its metadata from the store
// - Fetches the last 11 block headers to calculate the median timestamp
// - Calculates the median timestamp using Bitcoin's median time past algorithm
// - Converts the timestamp to a safe uint32 representation
//
// The median timestamp calculation follows Bitcoin protocol rules where the median
// time past is calculated from the timestamps of the previous 11 blocks. This
// provides a more stable time reference that prevents timestamp manipulation attacks
// and ensures consistent time-based validation across the network.
//
// This information is commonly used for:
// - Time-based transaction validation (nLockTime, CSV)
// - Network synchronization and block validation
// - Mining operations that require current blockchain state
//
// Parameters:
//   - ctx: Context for the operation with timeout and cancellation support
//
// Returns:
//   - uint32: The height of the best block in the main chain
//   - uint32: The median timestamp of recent blocks as a Unix timestamp
//   - error: Any error encountered during block retrieval or timestamp calculation
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

	medianTimestampUint32, err := safeconversion.TimeToUint32(*medianTimestamp)
	if err != nil {
		return 0, 0, err
	}

	return meta.Height, medianTimestampUint32, nil
}
