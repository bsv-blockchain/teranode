// Package blockchain provides functionality for managing the Bitcoin blockchain.
package blockchain

import (
	"context"
	"net/http"
	"sync"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	safeconversion "github.com/bsv-blockchain/go-safe-conversion"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockchain/blockchain_api"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/blob"
	"github.com/bsv-blockchain/teranode/stores/blockchain"
	"github.com/bsv-blockchain/teranode/stores/blockchain/options"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/health"
)

// LocalClient implements a blockchain client with direct store access.
type LocalClient struct {
	logger       ulogger.Logger     // Logger instance
	settings     *settings.Settings // Configuration settings
	store        blockchain.Store   // Blockchain store
	subtreeStore blob.Store         // Subtree store
	utxoStore    utxo.Store         // UTXO store

	// Subscription management
	subscribersMu sync.RWMutex
	subscribers   map[string]chan *blockchain_api.Notification
}

// NewLocalClient creates a new LocalClient instance with the provided dependencies.
func NewLocalClient(logger ulogger.Logger, tSettings *settings.Settings, store blockchain.Store, subtreeStore blob.Store, utxoStore utxo.Store) (ClientI, error) {
	return &LocalClient{
		logger:       logger,
		settings:     tSettings,
		store:        store,
		subtreeStore: subtreeStore,
		utxoStore:    utxoStore,
		subscribers:  make(map[string]chan *blockchain_api.Notification),
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
func (c *LocalClient) Health(ctx context.Context, checkLiveness bool) (int, string, error) {
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

func (c *LocalClient) AddBlock(ctx context.Context, block *model.Block, peerID string, opts ...options.StoreBlockOption) error {
	ID, height, err := c.store.StoreBlock(ctx, block, peerID, opts...)
	if err != nil {
		return err
	}

	c.logger.Infof("[Blockchain LocalClient] stored block %s (ID: %d, height: %d)", block.Hash(), ID, height)

	// Send notification to all subscribers about the new block
	notification := &blockchain_api.Notification{
		Type: model.NotificationType_Block,
		Hash: block.Hash().CloneBytes(),
	}

	c.subscribersMu.RLock()
	defer c.subscribersMu.RUnlock()

	// Skip if no subscribers
	if c.subscribers == nil {
		return nil
	}

	for source, ch := range c.subscribers {
		select {
		case ch <- notification:
			c.logger.Debugf("[Blockchain LocalClient] sent block notification to subscriber %s", source)
		default:
			c.logger.Warnf("[Blockchain LocalClient] failed to send notification to subscriber %s (channel full)", source)
		}
	}

	return nil
}

func (c *LocalClient) GetBlock(ctx context.Context, blockHash *chainhash.Hash) (*model.Block, error) {
	if c.store == nil {
		return nil, errors.NewBlockNotFoundError("store not configured")
	}

	block, _, err := c.store.GetBlock(ctx, blockHash)
	if err != nil {
		return nil, err
	}

	return block, nil
}

func (c *LocalClient) GetBlocks(ctx context.Context, blockHash *chainhash.Hash, numberOfBlocks uint32) ([]*model.Block, error) {
	blocks, err := c.store.GetBlocks(ctx, blockHash, numberOfBlocks)
	if err != nil {
		return nil, err
	}

	return blocks, nil
}

func (c *LocalClient) GetBlockByHeight(ctx context.Context, height uint32) (*model.Block, error) {
	return c.store.GetBlockByHeight(ctx, height)
}

func (c *LocalClient) GetBlockByID(ctx context.Context, id uint64) (*model.Block, error) {
	return c.store.GetBlockByID(ctx, id)
}

func (c *LocalClient) GetNextBlockID(ctx context.Context) (uint64, error) {
	return c.store.GetNextBlockID(ctx)
}

func (c *LocalClient) GetBlockStats(ctx context.Context) (*model.BlockStats, error) {
	return c.store.GetBlockStats(ctx)
}

func (c *LocalClient) GetBlockGraphData(ctx context.Context, periodMillis uint64) (*model.BlockDataPoints, error) {
	return c.store.GetBlockGraphData(ctx, periodMillis)
}

func (c *LocalClient) GetLastNBlocks(ctx context.Context, n int64, includeOrphans bool, fromHeight uint32) ([]*model.BlockInfo, error) {
	return c.store.GetLastNBlocks(ctx, n, includeOrphans, fromHeight)
}

func (c *LocalClient) GetLastNInvalidBlocks(ctx context.Context, n int64) ([]*model.BlockInfo, error) {
	return c.store.GetLastNInvalidBlocks(ctx, n)
}

func (c *LocalClient) GetSuitableBlock(ctx context.Context, blockHash *chainhash.Hash) (*model.SuitableBlock, error) {
	return c.store.GetSuitableBlock(ctx, blockHash)
}
func (c *LocalClient) GetHashOfAncestorBlock(ctx context.Context, blockHash *chainhash.Hash, num int) (*chainhash.Hash, error) {
	return c.store.GetHashOfAncestorBlock(ctx, blockHash, num)
}
func (c *LocalClient) GetNextWorkRequired(ctx context.Context, blockHash *chainhash.Hash, currentBlockTime int64) (*model.NBit, error) {
	difficulty, err := NewDifficulty(c.store, c.logger, c.settings)
	if err != nil {
		c.logger.Errorf("[BlockAssembler] Couldn't create difficulty: %v", err)
	}

	blockHeader, meta, err := c.store.GetBlockHeader(ctx, blockHash)
	if err != nil {
		return nil, err
	}

	return difficulty.CalcNextWorkRequired(ctx, blockHeader, meta.Height, currentBlockTime)
}

func (c *LocalClient) GetBlockExists(ctx context.Context, blockHash *chainhash.Hash) (bool, error) {
	exists, err := c.store.GetBlockExists(ctx, blockHash)
	if err != nil {
		return false, err
	}

	return exists, nil
}

func (c *LocalClient) GetBestBlockHeader(ctx context.Context) (*model.BlockHeader, *model.BlockHeaderMeta, error) {
	return c.store.GetBestBlockHeader(ctx)
}

func (c *LocalClient) GetBlockHeader(ctx context.Context, blockHash *chainhash.Hash) (*model.BlockHeader, *model.BlockHeaderMeta, error) {
	return c.store.GetBlockHeader(ctx, blockHash)
}

func (c *LocalClient) GetBlockHeaders(ctx context.Context, blockHash *chainhash.Hash, numberOfHeaders uint64) ([]*model.BlockHeader, []*model.BlockHeaderMeta, error) {
	return c.store.GetBlockHeaders(ctx, blockHash, numberOfHeaders)
}

func (c *LocalClient) GetBlockHeadersToCommonAncestor(ctx context.Context, hashTarget *chainhash.Hash, blockLocatorHashes []*chainhash.Hash, maxHeaders uint32) ([]*model.BlockHeader, []*model.BlockHeaderMeta, error) {
	return getBlockHeadersToCommonAncestor(ctx, c.store, hashTarget, blockLocatorHashes, maxHeaders)
}

func (c *LocalClient) GetBlockHeadersFromCommonAncestor(ctx context.Context, chainTipHash *chainhash.Hash, blockLocatorHashes []chainhash.Hash, maxHeaders uint32) ([]*model.BlockHeader, []*model.BlockHeaderMeta, error) {
	// This function is similar to GetBlockHeadersToCommonAncestor but retrieves headers
	// starting from the common ancestor towards the target hash.
	return getBlockHeadersFromCommonAncestor(ctx, c.store, chainTipHash, blockLocatorHashes, maxHeaders)
}

// GetLatestBlockHeaderFromBlockLocator retrieves the latest block header from a block locator.
func (c *LocalClient) GetLatestBlockHeaderFromBlockLocator(ctx context.Context, bestBlockHash *chainhash.Hash, blockLocator []chainhash.Hash) (*model.BlockHeader, *model.BlockHeaderMeta, error) {
	return c.store.GetLatestBlockHeaderFromBlockLocator(ctx, bestBlockHash, blockLocator)
}

// GetBlockHeadersFromOldest retrieves block headers starting from the oldest block.
func (c *LocalClient) GetBlockHeadersFromOldest(ctx context.Context, chainTipHash, targetHash *chainhash.Hash, numberOfHeaders uint64) ([]*model.BlockHeader, []*model.BlockHeaderMeta, error) {
	return c.store.GetBlockHeadersFromOldest(ctx, chainTipHash, targetHash, numberOfHeaders)
}

func (c *LocalClient) GetBlockHeadersFromTill(ctx context.Context, blockHashFrom *chainhash.Hash, blockHashTill *chainhash.Hash) ([]*model.BlockHeader, []*model.BlockHeaderMeta, error) {
	return c.store.GetBlockHeadersFromTill(ctx, blockHashFrom, blockHashTill)
}

func (c *LocalClient) CheckBlockIsInCurrentChain(ctx context.Context, blockIDs []uint32) (bool, error) {
	return c.store.CheckBlockIsInCurrentChain(ctx, blockIDs)
}

func (c *LocalClient) CheckBlockIsAncestorOfBlock(ctx context.Context, blockIDs []uint32, blockHash *chainhash.Hash) (bool, error) {
	return c.store.CheckBlockIsAncestorOfBlock(ctx, blockIDs, blockHash)
}

func (c *LocalClient) GetBlockHeadersFromHeight(ctx context.Context, height, limit uint32) ([]*model.BlockHeader, []*model.BlockHeaderMeta, error) {
	return c.store.GetBlockHeadersFromHeight(ctx, height, limit)
}

func (c *LocalClient) GetBlockHeadersByHeight(ctx context.Context, startHeight, endHeight uint32) ([]*model.BlockHeader, []*model.BlockHeaderMeta, error) {
	return c.store.GetBlockHeadersByHeight(ctx, startHeight, endHeight)
}

func (c *LocalClient) GetBlocksByHeight(ctx context.Context, startHeight, endHeight uint32) ([]*model.Block, error) {
	return c.store.GetBlocksByHeight(ctx, startHeight, endHeight)
}

func (c *LocalClient) FindBlocksContainingSubtree(ctx context.Context, subtreeHash *chainhash.Hash, maxBlocks uint32) ([]*model.Block, error) {
	return c.store.FindBlocksContainingSubtree(ctx, subtreeHash, maxBlocks)
}

func (c *LocalClient) InvalidateBlock(ctx context.Context, blockHash *chainhash.Hash) ([]chainhash.Hash, error) {
	return c.store.InvalidateBlock(ctx, blockHash)
}

func (c *LocalClient) RevalidateBlock(ctx context.Context, blockHash *chainhash.Hash) error {
	return c.store.RevalidateBlock(ctx, blockHash)
}

func (c *LocalClient) GetBlockHeaderIDs(ctx context.Context, blockHash *chainhash.Hash, numberOfHeaders uint64) ([]uint32, error) {
	return c.store.GetBlockHeaderIDs(ctx, blockHash, numberOfHeaders)
}

func (c *LocalClient) SendNotification(ctx context.Context, notification *blockchain_api.Notification) error {
	// Send notification to all subscribers
	c.subscribersMu.RLock()
	defer c.subscribersMu.RUnlock()

	// Skip if no subscribers
	if c.subscribers == nil {
		return nil
	}

	for source, ch := range c.subscribers {
		select {
		case ch <- notification:
			c.logger.Debugf("[LocalClient] sent notification to subscriber %s", source)
		default:
			c.logger.Warnf("[LocalClient] failed to send notification to subscriber %s (channel full)", source)
		}
	}

	return nil
}

func (c *LocalClient) Subscribe(ctx context.Context, source string) (chan *blockchain_api.Notification, error) {
	// Return a buffered channel to prevent blocking
	ch := make(chan *blockchain_api.Notification, 10)

	// Register the subscriber
	c.subscribersMu.Lock()
	if c.subscribers == nil {
		c.subscribers = make(map[string]chan *blockchain_api.Notification)
	}
	c.subscribers[source] = ch
	c.subscribersMu.Unlock()

	c.logger.Infof("[LocalClient] Registered subscriber %s", source)

	// initial notification to let subscribers know the current state
	initialNotification := &blockchain_api.Notification{
		Type: model.NotificationType_Block,
		Hash: (&chainhash.Hash{}).CloneBytes(), // Empty hash for genesis/no blocks
	}

	if c.store != nil {
		chainTip, _, err := c.store.GetBestBlockHeader(ctx)
		if err != nil {
			return ch, errors.NewServiceError("[Subscribe] failed to get best block header", err)
		}

		initialNotification = &blockchain_api.Notification{
			Type: model.NotificationType_Block,
			Hash: chainTip.Hash().CloneBytes(),
		}
	}

	// Send the initial notification asynchronously to avoid race condition
	// where the goroutine listening on the channel hasn't started yet
	go func() {
		// Put the notification in the channel
		select {
		case ch <- initialNotification:
			c.logger.Infof("[LocalClient] Sent initial block notification to %s", source)
		case <-ctx.Done():
			c.logger.Debugf("[LocalClient] Context cancelled, skipping initial notification to %s", source)
		}
	}()

	// We don't close the channel here as it should be managed by the receiver
	// to avoid race conditions with concurrent send operations
	return ch, nil
}

func (c *LocalClient) GetState(ctx context.Context, key string) ([]byte, error) {
	return c.store.GetState(ctx, key)
}

func (c *LocalClient) SetState(ctx context.Context, key string, data []byte) error {
	return c.store.SetState(ctx, key, data)
}

func (c *LocalClient) GetBlockIsMined(ctx context.Context, blockHash *chainhash.Hash) (bool, error) {
	return c.store.GetBlockIsMined(ctx, blockHash)
}

func (c *LocalClient) SetBlockMinedSet(ctx context.Context, blockHash *chainhash.Hash) error {
	return c.store.SetBlockMinedSet(ctx, blockHash)
}

func (c *LocalClient) ClearBlockMinedSet(ctx context.Context, blockHash *chainhash.Hash) error {
	return c.store.ClearBlockMinedSet(ctx, blockHash)
}

func (c *LocalClient) SetBlockProcessedAt(ctx context.Context, blockHash *chainhash.Hash, clear ...bool) error {
	return c.store.SetBlockProcessedAt(ctx, blockHash, clear...)
}

func (c *LocalClient) SetBlockPersistedAt(ctx context.Context, blockHash *chainhash.Hash) error {
	return c.store.SetBlockPersistedAt(ctx, blockHash)
}

func (c *LocalClient) GetBlocksNotPersisted(ctx context.Context, limit int) ([]*model.Block, error) {
	return c.store.GetBlocksNotPersisted(ctx, limit)
}

func (c *LocalClient) GetBlocksMinedNotSet(ctx context.Context) ([]*model.Block, error) {
	return c.store.GetBlocksMinedNotSet(ctx)
}

func (c *LocalClient) SetBlockSubtreesSet(ctx context.Context, blockHash *chainhash.Hash) error {
	return c.store.SetBlockSubtreesSet(ctx, blockHash)
}

func (c *LocalClient) GetBlocksSubtreesNotSet(ctx context.Context) ([]*model.Block, error) {
	return c.store.GetBlocksSubtreesNotSet(ctx)
}

func (c *LocalClient) GetFSMCurrentState(_ context.Context) (*FSMStateType, error) {
	// TODO: Placeholder for now
	state := FSMStateRUNNING
	return &state, nil
}

func (c *LocalClient) IsFSMCurrentState(_ context.Context, state FSMStateType) (bool, error) {
	return state == FSMStateRUNNING, nil
}

func (c *LocalClient) WaitForFSMtoTransitionToGivenState(ctx context.Context, _ FSMStateType) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return nil
}

func (c *LocalClient) WaitUntilFSMTransitionFromIdleState(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return nil
}

func (c *LocalClient) IsFullyReady(_ context.Context) (bool, error) {
	// LocalClient is always ready since it doesn't depend on remote services
	// and has direct store access without subscription infrastructure
	c.logger.Debugf("[LocalClient] IsFullyReady check - Always ready (direct store access)")
	return true, nil
}

func (c *LocalClient) GetFSMCurrentStateForE2ETestMode() FSMStateType {
	// TODO: Fix me, this is a temporary solution
	return FSMStateRUNNING
}

func (c *LocalClient) SendFSMEvent(_ context.Context, _ blockchain_api.FSMEventType) error {
	// TODO: "implement me"
	return nil
}

func (c *LocalClient) Run(ctx context.Context, source string) error {
	return nil
}

func (c *LocalClient) Idle(ctx context.Context) error {
	return nil
}

func (c *LocalClient) CatchUpBlocks(ctx context.Context) error {
	return nil
}

func (c *LocalClient) ReportPeerFailure(ctx context.Context, hash *chainhash.Hash, peerID string, failureType string, reason string) error {
	return nil
}

func (c *LocalClient) LegacySync(ctx context.Context) error {
	return nil
}

// GetBlockLocator returns a block locator for the latest block.
// This function will be much faster, when moved to the server side.
func (c *LocalClient) GetBlockLocator(ctx context.Context, blockHeaderHash *chainhash.Hash, blockHeaderHeight uint32) ([]*chainhash.Hash, error) {
	return getBlockLocator(ctx, c.store, blockHeaderHash, blockHeaderHeight)
}

func (c *LocalClient) GetChainTips(ctx context.Context) ([]*model.ChainTip, error) {
	return c.store.GetChainTips(ctx)
}

func (c *LocalClient) LocateBlockHeaders(ctx context.Context, locator []*chainhash.Hash, hashStop *chainhash.Hash, maxHashes uint32) ([]*model.BlockHeader, error) {
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
func (c *LocalClient) GetBestHeightAndTime(ctx context.Context) (uint32, uint32, error) {
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

	medianTimestampUint32, err := safeconversion.TimeToUint32(*medianTimestamp) // cspell:ignore safeconversion
	if err != nil {
		return 0, 0, err
	}

	return meta.Height, medianTimestampUint32, nil
}
