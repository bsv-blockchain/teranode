// Package blockvalidation implements block validation for Bitcoin SV nodes in Teranode.
//
// This package provides the core functionality for validating Bitcoin blocks, managing block subtrees,
// and processing transaction metadata. It is designed for high-performance operation at scale,
// supporting features like:
//
// - Concurrent block validation with optimistic mining support
// - Subtree-based block organization and validation
// - Transaction metadata caching and management
// - Automatic chain catchup when falling behind
// - Integration with Kafka for distributed operation
//
// The package exposes gRPC interfaces for block validation operations,
// making it suitable for use in distributed Teranode deployments.
package blockvalidation

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/model"
	"github.com/bitcoin-sv/teranode/services/blockchain"
	"github.com/bitcoin-sv/teranode/services/subtreevalidation"
	"github.com/bitcoin-sv/teranode/services/validator"
	"github.com/bitcoin-sv/teranode/settings"
	"github.com/bitcoin-sv/teranode/stores/blob"
	"github.com/bitcoin-sv/teranode/stores/blob/options"
	"github.com/bitcoin-sv/teranode/stores/txmetacache"
	"github.com/bitcoin-sv/teranode/stores/utxo"
	"github.com/bitcoin-sv/teranode/stores/utxo/meta"
	"github.com/bitcoin-sv/teranode/tracing"
	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/bitcoin-sv/teranode/util"
	"github.com/bitcoin-sv/teranode/util/retry"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/ordishs/go-utils/expiringmap"
	"github.com/ordishs/gocore"
	"golang.org/x/sync/errgroup"
)

// revalidateBlockData contains information needed to revalidate a block
// that previously failed validation or requires additional verification.
type revalidateBlockData struct {
	// block is the full block data to be revalidated
	block *model.Block

	// blockHeaders contains historical block headers needed for validation context
	blockHeaders []*model.BlockHeader

	// blockHeaderIDs contains the sequential IDs of the block headers
	blockHeaderIDs []uint32

	// baseURL is the source URL from which the block was originally retrieved
	baseURL string

	// retries tracks the number of revalidation attempts
	retries int
}

// BlockValidation handles the core validation logic for blocks in Teranode.
// It manages block validation, subtree processing, and bloom filter creation.
type BlockValidation struct {
	// logger provides structured logging capabilities
	logger ulogger.Logger

	// settings contains operational parameters and feature flags
	settings *settings.Settings

	// blockchainClient interfaces with the blockchain for operations
	blockchainClient blockchain.ClientI

	// subtreeStore provides persistent storage for block subtrees
	subtreeStore blob.Store

	// subtreeBlockHeightRetention specifies how long subtrees should be retained
	subtreeBlockHeightRetention uint32

	// txStore handles permanent storage of transactions
	txStore blob.Store

	// utxoStore manages the UTXO set for transaction validation
	utxoStore utxo.Store

	// recentBlocksBloomFilters maintains bloom filters for recent blocks
	recentBlocksBloomFilters map[chainhash.Hash]*model.BlockBloomFilter

	// recentBlocksBloomFiltersMu protects concurrent access to bloom filters
	recentBlocksBloomFiltersMu sync.Mutex

	// bloomFilterRetentionSize defines bloom filter defines the number of blocks to keep the bloom filter for
	bloomFilterRetentionSize uint32

	// validatorClient handles transaction validation operations
	validatorClient validator.Interface

	// subtreeValidationClient manages subtree validation processes
	subtreeValidationClient subtreevalidation.Interface

	// subtreeDeDuplicator prevents duplicate processing of subtrees
	subtreeDeDuplicator *DeDuplicator

	// lastValidatedBlocks caches recently validated blocks for 2 minutes
	lastValidatedBlocks *expiringmap.ExpiringMap[chainhash.Hash, *model.Block]

	// blockExists tracks validated block hashes for 2 hours
	blockExists *expiringmap.ExpiringMap[chainhash.Hash, bool]

	// subtreeExists tracks validated subtree hashes for 10 minutes
	subtreeExists *expiringmap.ExpiringMap[chainhash.Hash, bool]

	// subtreeCount tracks the number of subtrees being processed
	subtreeCount atomic.Int32

	// blockHashesCurrentlyValidated tracks blocks in validation process
	blockHashesCurrentlyValidated *util.SwissMap

	// blockBloomFiltersBeingCreated tracks bloom filters being generated
	blockBloomFiltersBeingCreated *util.SwissMap

	// bloomFilterStats collects statistics about bloom filter operations
	bloomFilterStats *model.BloomStats

	// setMinedChan receives block hashes that need to be marked as mined
	setMinedChan chan *chainhash.Hash

	// revalidateBlockChan receives blocks that need revalidation
	revalidateBlockChan chan revalidateBlockData

	// stats tracks operational metrics for monitoring
	stats *gocore.Stat

	// lastUsedBaseURL stores the most recent source URL used for retrievals
	lastUsedBaseURL string
}

// NewBlockValidation creates a new block validation instance with the provided dependencies.
// It initializes all required components and starts background processing goroutines for
// handling block validation tasks.
//
// Parameters:
//   - ctx: Context for lifecycle management
//   - logger: Structured logging interface
//   - tSettings: Validation configuration parameters
//   - blockchainClient: Interface to blockchain operations
//   - subtreeStore: Storage for block subtrees
//   - txStore: Storage for transactions
//   - txMetaStore: Storage for transaction metadata
//   - validatorClient: Transaction validation interface
//   - subtreeValidationClient: Subtree validation interface
//   - bloomRetentionSize: Length of last X blocks to retain bloom filters
//
// Returns a configured BlockValidation instance ready for use.
func NewBlockValidation(ctx context.Context, logger ulogger.Logger, tSettings *settings.Settings, blockchainClient blockchain.ClientI, subtreeStore blob.Store,
	txStore blob.Store, txMetaStore utxo.Store, subtreeValidationClient subtreevalidation.Interface) *BlockValidation {
	logger.Infof("optimisticMining = %v", tSettings.BlockValidation.OptimisticMining)
	bv := &BlockValidation{
		logger:                        logger,
		settings:                      tSettings,
		blockchainClient:              blockchainClient,
		subtreeStore:                  subtreeStore,
		subtreeBlockHeightRetention:   tSettings.BlockValidation.SubtreeBlockHeightRetention,
		txStore:                       txStore,
		utxoStore:                     txMetaStore,
		recentBlocksBloomFilters:      make(map[chainhash.Hash]*model.BlockBloomFilter),
		bloomFilterRetentionSize:      tSettings.BlockValidation.BloomFilterRetentionSize,
		subtreeValidationClient:       subtreeValidationClient,
		subtreeDeDuplicator:           NewDeDuplicator(tSettings.BlockValidation.SubtreeBlockHeightRetention),
		lastValidatedBlocks:           expiringmap.New[chainhash.Hash, *model.Block](2 * time.Minute),
		blockExists:                   expiringmap.New[chainhash.Hash, bool](120 * time.Minute), // we keep this for 2 hours
		subtreeExists:                 expiringmap.New[chainhash.Hash, bool](10 * time.Minute),  // we keep this for 10 minutes
		subtreeCount:                  atomic.Int32{},
		blockHashesCurrentlyValidated: util.NewSwissMap(0),
		blockBloomFiltersBeingCreated: util.NewSwissMap(0),
		bloomFilterStats:              model.NewBloomStats(),
		setMinedChan:                  make(chan *chainhash.Hash, 1000),
		revalidateBlockChan:           make(chan revalidateBlockData, 2),
		stats:                         gocore.NewStat("blockvalidation"),
		lastUsedBaseURL:               "",
	}

	go func() {
		// update stats for the expiring maps every 5 seconds
		for {
			time.Sleep(5 * time.Second)
			prometheusBlockValidationLastValidatedBlocksCache.Set(float64(bv.lastValidatedBlocks.Len()))
			prometheusBlockValidationBlockExistsCache.Set(float64(bv.blockExists.Len()))
			prometheusBlockValidationSubtreeExistsCache.Set(float64(bv.subtreeExists.Len()))
		}
	}()

	var (
		subscribeCtx    context.Context
		subscribeCancel context.CancelFunc
	)

	if bv.blockchainClient != nil {
		go func() {
			for {
				select {
				case <-ctx.Done():
					if subscribeCancel != nil {
						bv.logger.Infof("[BlockValidation:setMined] cancelling blockchain subscription")

						subscribeCancel()
					}

					bv.logger.Warnf("[BlockValidation:setMined] exiting setMined goroutine: %s", ctx.Err())

					return
				default:
					bv.logger.Infof("[BlockValidation:setMined] subscribing to blockchain for setTxMined signal")
					subscribeCtx, subscribeCancel = context.WithCancel(ctx)

					blockchainSubscription, err := bv.blockchainClient.Subscribe(subscribeCtx, "blockvalidation")
					if err != nil {
						bv.logger.Errorf("[BlockValidation:setMined] failed to subscribe to blockchain: %s", err)

						// backoff for 5 seconds and try again
						time.Sleep(5 * time.Second)

						continue
					}

					for notification := range blockchainSubscription {
						if notification == nil {
							continue
						}

						if notification.Type == model.NotificationType_Block {
							cHash := chainhash.Hash(notification.Hash)
							bv.logger.Infof("[BlockValidation:setMined] received BlockSubtreesSet notification: %s", cHash.String())

							// push block hash to the setMinedChan
							bv.setMinedChan <- &cHash
						}
					}

					subscribeCancel()
				}
			}
		}()
	}

	go func() {
		if err := bv.start(ctx); err != nil {
			logger.Errorf("[BlockValidation:start] failed to start: %s", err)
		}
	}()

	return bv
}

// start initializes the block validation system and begins processing.
// It handles the recovery of unprocessed blocks and starts background workers
// for block validation tasks.
func (u *BlockValidation) start(ctx context.Context) error {
	go u.bloomFilterStats.BloomFilterStatsProcessor(ctx)

	if u.blockchainClient != nil {
		// first check whether all old blocks have been processed properly
		blocksMinedNotSet, err := u.blockchainClient.GetBlocksMinedNotSet(ctx)
		if err != nil {
			u.logger.Errorf("[BlockValidation:start] failed to get blocks mined not set: %s", err)
		}

		g, gCtx := errgroup.WithContext(ctx)

		if len(blocksMinedNotSet) > 0 {
			u.logger.Infof("[BlockValidation:start] found %d blocks mined not set", len(blocksMinedNotSet))

			for _, block := range blocksMinedNotSet {
				blockHash := block.Hash()

				_ = u.blockHashesCurrentlyValidated.Put(*blockHash)

				g.Go(func() error {
					u.logger.Debugf("[BlockValidation:start] processing block mined not set: %s", blockHash.String())

					if err := u.setTxMined(gCtx, blockHash); err != nil {
						u.logger.Errorf("[BlockValidation:start] failed to set block mined: %s", err)
						u.setMinedChan <- blockHash
					} else {
						_ = u.blockHashesCurrentlyValidated.Delete(*blockHash)
					}

					u.logger.Infof("[BlockValidation:start] processed block mined and set mined_set: %s", blockHash.String())

					return nil
				})
			}
		}

		// get all blocks that have subtrees not set
		blocksSubtreesNotSet, err := u.blockchainClient.GetBlocksSubtreesNotSet(ctx)
		if err != nil {
			u.logger.Errorf("[BlockValidation:start] failed to get blocks subtrees not set: %s", err)
		}

		if len(blocksSubtreesNotSet) > 0 {
			u.logger.Infof("[BlockValidation:start] found %d blocks subtrees not set", len(blocksSubtreesNotSet))

			for _, block := range blocksSubtreesNotSet {
				block := block

				g.Go(func() error {
					u.logger.Infof("[BlockValidation:start] processing block subtrees DAH not set: %s", block.Hash().String())

					if err := u.updateSubtreesDAH(gCtx, block); err != nil {
						u.logger.Errorf("[BlockValidation:start] failed to update subtrees DAH: %s", err)
					}

					return nil
				})
			}
		}

		// wait for all blocks to be processed
		if err = g.Wait(); err != nil {
			// we cannot start the block validation, we are in a bad state
			return errors.NewServiceError("[BlockValidation:start] failed to start, process old block mined/subtrees sets", err)
		}
	}

	// start a worker to process the setMinedChan
	u.logger.Infof("[BlockValidation:start] starting setMined goroutine")

	go func() {
		defer u.logger.Infof("[BlockValidation:start] setMinedChan worker stopped")

		for {
			select {
			case <-ctx.Done():
				u.logger.Warnf("[BlockValidation:start] exiting setMined goroutine: %s", ctx.Err())

				return
			case blockHash := <-u.setMinedChan:
				u.logger.Infof("[BlockValidation:start][%s] setMinedChan size: %d", blockHash.String(), len(u.setMinedChan))

				startTime := time.Now()

				u.logger.Debugf("[BlockValidation:start][%s] block setTxMined", blockHash.String())

				_ = u.blockHashesCurrentlyValidated.Put(*blockHash)

				if err := u.setTxMined(ctx, blockHash); err != nil {
					u.logger.Errorf("[BlockValidation:start][%s] failed setTxMined: %s", blockHash.String(), err)
					// put the block back in the setMinedChan
					u.setMinedChan <- blockHash
				} else {
					_ = u.blockHashesCurrentlyValidated.Delete(*blockHash)
				}

				u.logger.Debugf("[BlockValidation:start][%s] block setTxMined DONE in %s", blockHash.String(), time.Since(startTime))
			}
		}
	}()

	// start a worker to revalidate blocks
	u.logger.Infof("[BlockValidation:start] starting reValidation goroutine")

	go func() {
		defer u.logger.Infof("[BlockValidation:start] revalidateBlockChan worker stopped")

		for {
			select {
			case <-ctx.Done():
				u.logger.Warnf("[BlockValidation:start] exiting reValidation goroutine: %s", ctx.Err())

				return
			case blockData := <-u.revalidateBlockChan:
				startTime := time.Now()

				u.logger.Infof("[BlockValidation:start][%s] block revalidation Chan", blockData.block.String())

				err := u.reValidateBlock(blockData)
				if err != nil {
					prometheusBlockValidationReValidateBlockErr.Observe(float64(time.Since(startTime).Microseconds() / 1_000_000))
					u.logger.Errorf("[BlockValidation:start][%s] failed block revalidation, retrying: %s", blockData.block.String(), err)
					// put the block back in the revalidateBlockChan
					if blockData.retries < 3 {
						blockData.retries++
						go func() {
							u.revalidateBlockChan <- blockData
						}()
					} else {
						u.logger.Errorf("[BlockValidation:start][%s] failed block revalidation, retries exhausted: %s", blockData.block.String(), err)
					}
				} else {
					prometheusBlockValidationReValidateBlock.Observe(float64(time.Since(startTime).Microseconds() / 1_000_000))
				}

				u.logger.Infof("[BlockValidation:start][%s] block revalidation Chan DONE in %s", blockData.block.String(), time.Since(startTime))
			}
		}
	}()

	return nil
}

// SetBlockExists marks a block as existing in the validation system.
// This updates the internal cache to track validated blocks.
//
// Parameters:
//   - hash: Hash of the block to mark as existing
//
// Returns an error if the operation fails.
func (u *BlockValidation) SetBlockExists(hash *chainhash.Hash) error {
	u.blockExists.Set(*hash, true)
	return nil
}

// GetBlockExists checks whether a block exists in the validation system.
// It first checks the internal cache, then falls back to the blockchain client
// if necessary.
//
// Parameters:
//   - ctx: Context for the operation
//   - hash: Hash of the block to check
//
// Returns:
//   - bool: Whether the block exists
//   - error: Any error encountered during the check
func (u *BlockValidation) GetBlockExists(ctx context.Context, hash *chainhash.Hash) (bool, error) {
	start := time.Now()
	stat := gocore.NewStat("GetBlockExists")

	defer func() {
		stat.AddTime(start)
	}()

	_, ok := u.blockExists.Get(*hash)
	if ok {
		return true, nil
	}

	exists, err := u.blockchainClient.GetBlockExists(ctx, hash)
	if err != nil {
		return false, err
	}

	if exists {
		u.blockExists.Set(*hash, true)
	}

	return exists, nil
}

// SetSubtreeExists marks a subtree as existing in the validation system.
// This updates the internal cache to track validated subtrees.
//
// Parameters:
//   - hash: Hash of the subtree to mark as existing
//
// Returns an error if the operation fails.
func (u *BlockValidation) SetSubtreeExists(hash *chainhash.Hash) error {
	u.subtreeExists.Set(*hash, true)
	return nil
}

// GetSubtreeExists checks whether a subtree exists in the validation system.
// It first checks the internal cache, then falls back to the subtree store
// if necessary.
//
// Parameters:
//   - ctx: Context for the operation
//   - hash: Hash of the subtree to check
//
// Returns:
//   - bool: Whether the subtree exists
//   - error: Any error encountered during the check
func (u *BlockValidation) GetSubtreeExists(ctx context.Context, hash *chainhash.Hash) (bool, error) {
	ctx, _, deferFn := tracing.StartTracing(ctx, "GetSubtreeExists")
	defer deferFn()

	_, ok := u.subtreeExists.Get(*hash)
	if ok {
		return true, nil
	}

	exists, err := u.subtreeStore.Exists(ctx, hash[:], options.WithFileExtension("subtree"))
	if err != nil {
		return false, err
	}

	if exists {
		u.subtreeExists.Set(*hash, true)
	}

	return exists, nil
}

// setTxMined marks a block's transactions as mined in the system.
// It updates transaction metadata and manages block state transitions.
//
// Parameters:
//   - ctx: Context for the operation
//   - blockHash: Hash of the block containing mined transactions
//
// Returns an error if the mining status update fails.
func (u *BlockValidation) setTxMined(ctx context.Context, blockHash *chainhash.Hash) (err error) {
	ctx, _, deferFn := tracing.StartTracing(ctx, "setTxMined",
		tracing.WithParentStat(u.stats),
		tracing.WithLogMessage(u.logger, "[setTxMined][%s] setting tx mined", blockHash.String()),
	)
	defer deferFn()

	var (
		block *model.Block
		ids   []uint32
	)

	cachedBlock, blockWasAlreadyCached := u.lastValidatedBlocks.Get(*blockHash)
	if blockWasAlreadyCached && cachedBlock != nil {
		// we have just validated this block, so we can use the cached block
		// this should have all the subtrees already loaded
		block = cachedBlock
	} else {
		// get the block from the blockchain
		if block, err = u.blockchainClient.GetBlock(ctx, blockHash); err != nil {
			return errors.NewServiceError("[setTxMined][%s] failed to get block from blockchain", blockHash.String(), err)
		}
	}

	// make sure all the subtrees are loaded in the block
	fallbackGetFunc := func(subtreeHash chainhash.Hash) error {
		return u.subtreeValidationClient.CheckSubtreeFromBlock(ctx, subtreeHash, u.lastUsedBaseURL, block.Height, block.Hash(), block.Header.HashPrevBlock)
	}

	_, err = block.GetSubtrees(ctx, u.logger, u.subtreeStore, fallbackGetFunc)
	if err != nil {
		return errors.NewProcessingError("[setTxMined][%s] failed to get subtrees from block", block.Hash().String(), err)
	}

	if ids, err = u.blockchainClient.GetBlockHeaderIDs(ctx, blockHash, 1); err != nil || len(ids) != 1 {
		return errors.NewServiceError("[setTxMined][%s] failed to get block header ids", blockHash.String(), err)
	}

	blockID := ids[0]

	// add the transactions in this block to the txMeta block IDs in the txMeta store
	if err = model.UpdateTxMinedStatus(
		ctx,
		u.logger,
		u.settings,
		u.utxoStore,
		block,
		blockID,
	); err != nil {
		return errors.NewProcessingError("[setTxMined][%s] error updating tx mined status", block.Hash().String(), err)
	}

	// delete the block from the cache, if it was there
	if blockWasAlreadyCached {
		u.lastValidatedBlocks.Delete(*blockHash)
	}

	// update block mined_set to true
	if err = u.blockchainClient.SetBlockMinedSet(ctx, blockHash); err != nil {
		return errors.NewServiceError("[setTxMined][%s] failed to set block mined", block.Hash().String(), err)
	}

	return nil
}

// isParentMined verifies if a block's parent has been marked as mined.
// It checks against the database of unmined blocks to determine status.
//
// Parameters:
//   - ctx: Context for the operation
//   - blockHeader: Header of the block whose parent needs checking
//
// Returns:
//   - bool: Whether the parent block is mined
//   - error: Any error encountered during verification
func (u *BlockValidation) isParentMined(ctx context.Context, blockHeader *model.BlockHeader) (bool, error) {
	blockNotMined, err := u.blockchainClient.GetBlocksMinedNotSet(ctx)
	if err != nil {
		return false, errors.NewServiceError("[setTxMined][%s] failed to get blocks mined not set", blockHeader.Hash().String(), err)
	}

	// check whether our parent block is in the list of not mined blocks
	parentBlockMined := true

	for _, b := range blockNotMined {
		if b.Header.Hash().IsEqual(blockHeader.HashPrevBlock) {
			parentBlockMined = false
			break
		}
	}

	return parentBlockMined, nil
}

// SetTxMetaCache stores transaction metadata in the cache.
// This method provides fast access to transaction metadata for validation purposes.
//
// Parameters:
//   - ctx: Context for the operation
//   - hash: Transaction hash
//   - txMeta: Transaction metadata to store
//
// Returns an error if the operation fails.
func (u *BlockValidation) SetTxMetaCache(ctx context.Context, hash *chainhash.Hash, txMeta *meta.Data) error {
	if cache, ok := u.utxoStore.(*txmetacache.TxMetaCache); ok {
		_, _, deferFn := tracing.StartTracing(ctx, "SetTxMetaCache")
		defer deferFn()

		return cache.SetCache(hash, txMeta)
	}

	return nil
}

// SetTxMetaCacheFromBytes stores transaction metadata from raw bytes.
// This method provides efficient bulk loading of transaction metadata.
//
// Parameters:
//   - ctx: Context for the operation
//   - key: Transaction identifier bytes
//   - txMetaBytes: Raw metadata bytes to store
//
// Returns an error if storage fails.
func (u *BlockValidation) SetTxMetaCacheFromBytes(_ context.Context, key, txMetaBytes []byte) error {
	if cache, ok := u.utxoStore.(*txmetacache.TxMetaCache); ok {
		return cache.SetCacheFromBytes(key, txMetaBytes)
	}

	return nil
}

// SetTxMetaCacheMinedMulti marks multiple transactions as mined.
// This method efficiently updates mining status for transaction batches.
//
// Parameters:
//   - ctx: Context for the operation
//   - hashes: Transaction hashes to mark as mined
//   - blockID: ID of the block containing these transactions
//
// Returns an error if the update fails.
func (u *BlockValidation) SetTxMetaCacheMinedMulti(ctx context.Context, hashes []*chainhash.Hash, minedBlockInfo utxo.MinedBlockInfo) error {
	if cache, ok := u.utxoStore.(*txmetacache.TxMetaCache); ok {
		_, _, deferFn := tracing.StartTracing(ctx, "BlockValidation:SetTxMetaCacheMinedMulti")
		defer deferFn()

		return cache.SetMinedMulti(ctx, hashes, minedBlockInfo)
	}

	return nil
}

// SetTxMetaCacheMulti stores multiple transaction metadata entries.
// This method enables efficient batch updates to the metadata cache.
//
// Parameters:
//   - ctx: Context for the operation
//   - keys: Transaction identifier bytes
//   - values: Corresponding metadata bytes
//
// Returns an error if the batch update fails.
func (u *BlockValidation) SetTxMetaCacheMulti(ctx context.Context, keys [][]byte, values [][]byte) error {
	if cache, ok := u.utxoStore.(*txmetacache.TxMetaCache); ok {
		_, _, deferFn := tracing.StartTracing(ctx, "BlockValidation:SetTxMetaCacheMulti")
		defer deferFn()

		return cache.SetCacheMulti(keys, values)
	}

	return nil
}

// DelTxMetaCacheMulti removes transaction metadata from the cache.
// This method cleans up metadata for invalidated or reorganized transactions.
//
// Parameters:
//   - ctx: Context for the operation
//   - hash: Transaction hash to remove
//
// Returns an error if deletion fails.
func (u *BlockValidation) DelTxMetaCacheMulti(ctx context.Context, hash *chainhash.Hash) error {
	if cache, ok := u.utxoStore.(*txmetacache.TxMetaCache); ok {
		_, _, deferFn := tracing.StartTracing(ctx, "BlockValidation:DelTxMetaCacheMulti")
		defer deferFn()

		return cache.Delete(ctx, hash)
	}

	return nil
}

// ValidateBlock performs comprehensive validation of a Bitcoin block.
// It verifies block size, parent block status, subtrees, and transactions while
// supporting optimistic mining for improved performance.
//
// The method can operate in two modes:
//   - Standard validation: Complete verification before accepting the block
//   - Optimistic mining: Preliminary acceptance with background validation
//
// Parameters:
//   - ctx: Context for the validation operation
//   - block: Block to validate
//   - baseURL: Source URL for additional data retrieval
//   - bloomStats: Statistics collector for bloom filter operations
//   - disableOptimisticMining: Optional flag to force standard validation
//
// Returns an error if validation fails or nil on success.
func (u *BlockValidation) ValidateBlock(ctx context.Context, block *model.Block, baseURL string, bloomStats *model.BloomStats, disableOptimisticMining ...bool) error {
	ctx, _, deferFn := tracing.StartTracing(ctx, "ValidateBlock",
		tracing.WithParentStat(u.stats),
		tracing.WithHistogram(prometheusBlockValidationValidateBlock),
		tracing.WithLogMessage(u.logger, "[ValidateBlock][%s] validating block from %s", block.Hash().String(), baseURL),
	)
	defer deferFn()

	// TODO: Check if this is a safe thing to do
	u.lastUsedBaseURL = baseURL

	// first check if the block already exists in the blockchain
	blockExists, err := u.GetBlockExists(ctx, block.Header.Hash())
	if err == nil && blockExists {
		u.logger.Warnf("[ValidateBlock][%s] tried to validate existing block", block.Header.Hash().String())
		return nil
	}

	u.logger.Infof("[ValidateBlock][%s] called", block.Header.Hash().String())

	// check the size of the block
	// 0 is unlimited so don't check the size
	if u.settings.Policy.ExcessiveBlockSize > 0 {
		excessiveBlockSizeUint64, err := util.SafeIntToUint64(u.settings.Policy.ExcessiveBlockSize)
		if err != nil {
			return err
		}

		if block.SizeInBytes > excessiveBlockSizeUint64 {
			return errors.NewBlockInvalidError("[ValidateBlock][%s] block size %d exceeds excessiveblocksize %d", block.Header.Hash().String(), block.SizeInBytes, u.settings.Policy.ExcessiveBlockSize)
		}
	}

	if block.CoinbaseTx == nil || block.CoinbaseTx.Inputs == nil || len(block.CoinbaseTx.Inputs) == 0 {
		return errors.NewBlockInvalidError("[ValidateBlock][%s] coinbase tx is nil or empty", block.Header.Hash().String())
	}

	// check the coinbase length
	if len(block.CoinbaseTx.Inputs[0].UnlockingScript.Bytes()) < 2 || len(block.CoinbaseTx.Inputs[0].UnlockingScript.Bytes()) > int(u.settings.ChainCfgParams.MaxCoinbaseScriptSigSize) {
		return errors.NewBlockInvalidError("[ValidateBlock][%s] bad coinbase length", block.Header.Hash().String())
	}

	u.logger.Infof("[ValidateBlock][%s] GetBlockHeaders", block.Header.Hash().String())

	// get all X previous block headers, 100 is the default
	previousBlockHeaderCount := u.settings.BlockValidation.PreviousBlockHeaderCount

	blockHeaders, _, err := u.blockchainClient.GetBlockHeaders(ctx, block.Header.HashPrevBlock, previousBlockHeaderCount)
	if err != nil {
		u.logger.Errorf("[ValidateBlock][%s] failed to get block headers: %s", block.String(), err)
		u.ReValidateBlock(block, baseURL)

		return errors.NewServiceError("[ValidateBlock][%s] failed to get block headers", block.String(), err)
	}

	// Wait for reValidationBlock to do its thing
	// When waitForPreviousBlocksToBeProcessed is done, all the previous blocks will be processed, and all previous blocks' bloom filters should be created
	if err = u.waitForPreviousBlocksToBeProcessed(ctx, block, blockHeaders); err != nil {
		// re-trigger the setMinedChan for the parent block
		u.setMinedChan <- block.Header.HashPrevBlock

		if err = u.waitForPreviousBlocksToBeProcessed(ctx, block, blockHeaders); err != nil {
			// Give up, the parent block isn't being fully validated
			return errors.NewBlockError("[ValidateBlock][%s] given up waiting on previous blocks to be ready %s", block.Hash().String(), block.Header.HashPrevBlock.String())
		}
	}

	// validate all the subtrees in the block
	u.logger.Infof("[ValidateBlock][%s] validating %d subtrees", block.Hash().String(), len(block.Subtrees))

	if err = u.validateBlockSubtrees(ctx, block, baseURL); err != nil {
		return err
	}

	u.logger.Infof("[ValidateBlock][%s] validating %d subtrees DONE", block.Hash().String(), len(block.Subtrees))

	useOptimisticMining := u.settings.BlockValidation.OptimisticMining
	if len(disableOptimisticMining) > 0 {
		// if the disableOptimisticMining is set to true, then we don't use optimistic mining, even if it is enabled
		useOptimisticMining = useOptimisticMining && !disableOptimisticMining[0]
		u.logger.Infof("[ValidateBlock][%s] useOptimisticMining override: %v", block.Header.Hash().String(), useOptimisticMining)
	}

	var optimisticMiningWg sync.WaitGroup

	oldBlockIDsMap := &sync.Map{}

	if useOptimisticMining {
		// make sure the proof of work is enough
		headerValid, _, err := block.Header.HasMetTargetDifficulty()
		if !headerValid {
			return errors.NewBlockInvalidError("invalid block header: %s", block.Header.Hash().String(), err)
		}

		// set the block in the temporary block cache for 2 minutes, could then be used for SetMined
		// must be set before AddBlock is called
		u.lastValidatedBlocks.Set(*block.Hash(), block)

		u.logger.Infof("[ValidateBlock][%s] adding block optimistically to blockchain", block.Hash().String())

		if err = u.blockchainClient.AddBlock(ctx, block, baseURL); err != nil {
			return errors.NewServiceError("[ValidateBlock][%s] failed to store block", block.Hash().String(), err)
		}

		u.logger.Infof("[ValidateBlock][%s] adding block optimistically to blockchain DONE", block.Hash().String())

		if err = u.SetBlockExists(block.Header.Hash()); err != nil {
			u.logger.Errorf("[ValidateBlock][%s] failed to set block exists cache: %s", block.Header.Hash().String(), err)
		}

		// decouple the tracing context to not cancel the context when finalize the block processing in the background
		callerSpan := tracing.DecoupleTracingSpan(ctx, "decoupleValidateBlock")
		defer callerSpan.Finish()

		optimisticMiningWg.Add(1)

		go func() {
			defer optimisticMiningWg.Done()

			blockHeaderIDs, err := u.blockchainClient.GetBlockHeaderIDs(callerSpan.Ctx, block.Header.HashPrevBlock, u.settings.BlockValidation.MaxPreviousBlockHeadersToCheck)
			if err != nil {
				u.logger.Errorf("[ValidateBlock][%s] failed to get block header ids: %s", block.String(), err)

				u.ReValidateBlock(block, baseURL)

				return
			}

			u.logger.Infof("[ValidateBlock][%s] GetBlockHeaders DONE", block.Header.Hash().String())

			u.logger.Infof("[ValidateBlock][%s] validating block in background", block.Hash().String())

			// only get the bloom filters for the current chain.
			bloomFilters := u.collectNecessaryBloomFilters(blockHeaders)

			if ok, err := block.Valid(callerSpan.Ctx, u.logger, u.subtreeStore, u.utxoStore, oldBlockIDsMap, bloomFilters, blockHeaders, blockHeaderIDs, bloomStats); !ok {
				u.logger.Errorf("[ValidateBlock][%s] InvalidateBlock block is not valid in background: %v", block.String(), err)

				if errors.Is(err, errors.ErrStorageError) || errors.Is(err, errors.ErrProcessing) {
					// storage or processing error, block is not really invalid, but we need to re-validate
					u.ReValidateBlock(block, baseURL)
				} else {
					// TODO TEMP disable invalidation in the scaling test
					//      Since the invalidation is disabled, here we are not invalidating the block
					// 		Consider enabling the invalidation in the future
					// if err = u.blockchainClient.InvalidateBlock(validateCtx, block.Header.Hash()); err != nil {
					//	u.logger.Errorf("[ValidateBlock][%s][InvalidateBlock] failed to invalidate block: %v", block.String(), err)
					// }
					u.logger.Errorf("[ValidateBlock][%s][InvalidateBlock] block is invalid: %v", block.String(), err)
				}
			}

			// TODO: Should we return error here?
			// range over the oldBlockIDsMap to get txID - oldBlockID pairs
			oldBlockIDsMap.Range(func(txID, blockIDs interface{}) bool {
				txHash, ok := txID.(chainhash.Hash)
				if !ok {
					u.logger.Errorf("[Block Validation][Range] failed to assert txID to chainhash.Hash for txID: %v", txID)
				}

				parentTransactionsBlockIDs, ok := blockIDs.([]uint32)
				if !ok {
					u.logger.Errorf("[Block Validation][Range] failed to assert blockIDs to []uint32 for txID: %v", txID)
				}

				// Flag to check if the old blocks are part of the current chain
				var blocksPartOfCurrentChain bool
				// TODO: what to do with the error here other than logging?
				blocksPartOfCurrentChain, err = u.blockchainClient.CheckBlockIsInCurrentChain(ctx, parentTransactionsBlockIDs)
				if err != nil {
					u.logger.Errorf("[ValidateBlock][%s] failed to check if old blocks are part of the current chain: %v", block.String(), err)
				}

				if !blocksPartOfCurrentChain {
					// TODO TEMP disable invalidation in the scaling test
					//      Since the invalidation is disabled, here we are not invalidating the block
					// 		Consider enabling the invalidation in the future
					u.logger.Errorf("[ValidateBlock][%s][InvalidateBlock] block is invalid. Transaction's (%v) parent blocks (%v) are not from current chain", txHash, block.String())
				}

				return true
			})
		}()
	} else {
		blockHeaderIDs, err := u.blockchainClient.GetBlockHeaderIDs(ctx, block.Header.HashPrevBlock, 100)
		if err != nil {
			u.logger.Errorf("[ValidateBlock][%s] failed to get block header ids: %s", block.String(), err)
			u.ReValidateBlock(block, baseURL)

			return errors.NewServiceError("[ValidateBlock][%s] failed to get block header ids", block.String(), err)
		}

		u.logger.Infof("[ValidateBlock][%s] GetBlockHeaderIDs DONE", block.Header.Hash().String())

		// validate the block
		u.logger.Infof("[ValidateBlock][%s] validating block", block.Hash().String())

		// only get the bloom filters for the current chain
		bloomFilters := u.collectNecessaryBloomFilters(blockHeaders)

		if ok, err := block.Valid(ctx, u.logger, u.subtreeStore, u.utxoStore, oldBlockIDsMap, bloomFilters, blockHeaders, blockHeaderIDs, bloomStats); !ok {
			return errors.NewBlockInvalidError("[ValidateBlock][%s] block is not valid", block.String(), err)
		}

		iterationError := u.checkOldBlockIDs(ctx, oldBlockIDsMap, block.String())

		// if iterationError is not nil, return the iterationError
		if iterationError != nil {
			return iterationError
		}

		u.logger.Infof("[ValidateBlock][%s] validating block DONE", block.Hash().String())

		// set the block in the temporary block cache for 2 minutes, could then be used for SetMined
		// must be set before AddBlock is called
		u.lastValidatedBlocks.Set(*block.Hash(), block)

		// if valid, store the block
		u.logger.Infof("[ValidateBlock][%s] adding block to blockchain", block.Hash().String())

		if err = u.blockchainClient.AddBlock(ctx, block, baseURL); err != nil {
			return errors.NewServiceError("[ValidateBlock][%s] failed to store block", block.Hash().String(), err)
		}

		if err = u.SetBlockExists(block.Header.Hash()); err != nil {
			u.logger.Errorf("[ValidateBlock][%s] failed to set block exists cache: %s", block.Header.Hash().String(), err)
		}

		u.logger.Infof("[ValidateBlock][%s] adding block to blockchain DONE", block.Hash().String())
	}

	u.logger.Infof("[ValidateBlock][%s] storing coinbase in tx store: %s", block.Hash().String(), block.CoinbaseTx.TxIDChainHash().String())

	if u.txStore != nil {
		if err = u.txStore.Set(ctx, block.CoinbaseTx.TxIDChainHash()[:], block.CoinbaseTx.Bytes()); err != nil {
			u.logger.Errorf("[ValidateBlock][%s] failed to store coinbase transaction [%s]", block.Hash().String(), err)
		}
	}

	u.logger.Infof("[ValidateBlock][%s] storing coinbase in tx store: %s DONE", block.Hash().String(), block.CoinbaseTx.TxIDChainHash().String())

	// decouple the tracing context to not cancel the context when finalize the block processing in the background
	callerSpan := tracing.DecoupleTracingSpan(ctx, "decoupleValidateBlock")

	go func() {
		if err := u.updateSubtreesDAH(callerSpan.Ctx, block); err != nil {
			// TODO: what to do here? We have already added the block to the blockchain
			u.logger.Errorf("[ValidateBlock][%s] failed to update subtrees DAH [%s]", block.Hash().String(), err)
		}
	}()

	if useOptimisticMining {
		// run the bloom filter creation in the background
		go func() {
			// / create bloom filter for the block and store
			u.logger.Infof("[ValidateBlock][%s] creating bloom filter for the validated block in background", block.Hash().String())
			// TODO: for the next phase, consider re-building the bloom filter in the background when node restarts.
			// currently, if the node restarts, the bloom filter is not re-built and the node will not be able to validate transactions properly,
			// since there are no bloom filters. This is a temporary solution to get the node up and running.
			//
			// Options are:
			//   1. Re-build the bloom filter in the background when the node restarts
			//   2. after creating the bloom filter, record it in the storage, and delete it after it expires.
			u.createAppendBloomFilter(callerSpan.Ctx, block)

			u.logger.Infof("[ValidateBlock][%s] creating bloom filter is DONE", block.Hash().String())
		}()
	} else {
		// create bloom filter for the block and wait for it
		u.logger.Infof("[ValidateBlock][%s] creating bloom filter for the validated block", block.Hash().String())
		u.createAppendBloomFilter(callerSpan.Ctx, block)
		u.logger.Infof("[ValidateBlock][%s] creating bloom filter is DONE", block.Hash().String())
	}

	return nil
}

func (u *BlockValidation) collectNecessaryBloomFilters(currentChainBlockHeaders []*model.BlockHeader) []*model.BlockBloomFilter {
	// Collect only the bloom-filters whose BlockHash is in that set
	bloomFilters := make([]*model.BlockBloomFilter, 0, len(currentChainBlockHeaders))

	u.recentBlocksBloomFiltersMu.Lock()

	for _, h := range currentChainBlockHeaders {
		if bf, ok := u.recentBlocksBloomFilters[*h.Hash()]; ok {
			bloomFilters = append(bloomFilters, bf)
		}
	}

	u.recentBlocksBloomFiltersMu.Unlock()

	return bloomFilters
}

// waitForPreviousBlocksToBeProcessed ensures:
// 1. a block's parents are mined before validation.
// 2. bloom filters for the parent blocks that are part of the current chain are ready.
// It implements retry logic with configurable backoff for parent and bloom filter verification.
// Parameters:
//   - ctx: Context for the operation
//   - block: Block whose parent needs verification
//
// Returns an error if parent mining verification fails.
// Returns an error if parent mining verification and collecting bloom filters fails.
func (u *BlockValidation) waitForPreviousBlocksToBeProcessed(ctx context.Context, block *model.Block, currentChainBlockHeaders []*model.BlockHeader) error {
	u.recentBlocksBloomFiltersMu.Lock()

	// iterate over the bloom filters and check if they are ready
	missingBlockBloomFilters := make([]*chainhash.Hash, 0)

	for _, header := range currentChainBlockHeaders {
		// check if recentBlocksBloomFilters contains the block hash
		if _, exists := u.recentBlocksBloomFilters[*header.Hash()]; !exists {
			missingBlockBloomFilters = append(missingBlockBloomFilters, header.Hash())
		}
	}
	u.recentBlocksBloomFiltersMu.Unlock()

	for _, hash := range missingBlockBloomFilters {
		// try to get from the subtree store
		bloomFilterBytes, err := u.subtreeStore.Get(ctx, hash[:], options.WithFileExtension("bloomfilter"))

		// we have the bloom filter bytes in subtree store, so we are forming the bloom filter
		if err == nil && len(bloomFilterBytes) > 0 {
			createdBbf := &model.BlockBloomFilter{
				CreationTime: time.Now(),
				BlockHash:    block.Hash(),
			}

			err = createdBbf.Deserialize(bloomFilterBytes)
			if err != nil {
				return err
			}

			u.recentBlocksBloomFiltersMu.Lock()
			u.recentBlocksBloomFilters[*hash] = createdBbf
			u.recentBlocksBloomFiltersMu.Unlock()
		} else if len(bloomFilterBytes) == 0 { // bloom filter not found in subtree store
			u.logger.Infof("[waitForPreviousBlocksToBeProcessed][%s] bloom filter not found in subtree store, creating", hash.String(), "block height: ", block.Height)
			// we need to create the bloom filter
			// get the block of hash
			blockToCreateBloomFilter, err := u.blockchainClient.GetBlock(ctx, hash)
			if err != nil {
				u.logger.Errorf("[waitForPreviousBlocksToBeProcessed][%s] failed to get block: %s", hash.String(), err)
				return err
			}

			u.createAppendBloomFilter(ctx, blockToCreateBloomFilter)

			u.logger.Infof("[waitForPreviousBlocksToBeProcessed][%s] creating bloom filter is DONE", block.Hash().String())
		}
	}

	// Caution, in regtest, when mining initial blocks, this logic wants to retry over and over as fast as possible to ensure it keeps up
	checkParentBlock := func() (bool, error) {
		parentBlockMined, err := u.isParentMined(ctx, block.Header)
		if err != nil {
			return false, err
		}

		if !parentBlockMined {
			return false, errors.NewBlockParentNotMinedError("[BlockValidation:isParentMined][%s] parent block %s not mined yet", block.Hash().String(), block.Header.HashPrevBlock.String())
		}

		return true, nil
	}

	_, err := retry.Retry(
		ctx,
		u.logger,
		checkParentBlock,
		retry.WithBackoffDurationType(time.Millisecond),
		retry.WithBackoffMultiplier(u.settings.BlockValidation.ArePreviousBlocksProcessedRetryBackoffMultiplier),
		retry.WithRetryCount(u.settings.BlockValidation.ArePreviousBlocksProcessedMaxRetry),
	)

	return err
}

func (u *BlockValidation) ReValidateBlock(block *model.Block, baseURL string) {
	u.logger.Errorf("[ValidateBlock][%s] re-validating block", block.String())
	u.revalidateBlockChan <- revalidateBlockData{
		block:   block,
		baseURL: baseURL,
	}
}

// reValidateBlock performs a full block revalidation.
// This method handles blocks that failed initial validation or need reverification.
//
// Parameters:
//   - blockData: Contains the block and context for revalidation
//
// Returns an error if revalidation fails.
func (u *BlockValidation) reValidateBlock(blockData revalidateBlockData) error {
	ctx, _, deferFn := tracing.StartTracing(context.Background(), "reValidateBlock",
		tracing.WithParentStat(u.stats),
		tracing.WithLogMessage(u.logger, "[reValidateBlock][%s] validating block from %s", blockData.block.Hash().String(), blockData.baseURL),
	)
	defer deferFn()

	u.lastUsedBaseURL = blockData.baseURL

	// get all X previous block headers, 100 is the default
	previousBlockHeaderCount := u.settings.BlockValidation.PreviousBlockHeaderCount

	blockHeaders, _, err := u.blockchainClient.GetBlockHeaders(ctx, blockData.block.Header.HashPrevBlock, previousBlockHeaderCount)
	if err != nil {
		u.logger.Errorf("[reValidateBlock][%s] failed to get block headers: %s", blockData.block.String(), err)

		return errors.NewServiceError("[reValidateBlock][%s] failed to get block headers", blockData.block.String(), err)
	}

	// only get the bloom filters for the current chain
	bloomFilters := u.collectNecessaryBloomFilters(blockHeaders)

	// validate all the subtrees in the block
	u.logger.Infof("[ReValidateBlock][%s] validating %d subtrees", blockData.block.Hash().String(), len(blockData.block.Subtrees))

	if err := u.validateBlockSubtrees(ctx, blockData.block, blockData.baseURL); err != nil {
		return err
	}

	u.logger.Infof("[ReValidateBlock][%s] validating %d subtrees DONE", blockData.block.Hash().String(), len(blockData.block.Subtrees))

	oldBlockIDsMap := &sync.Map{}

	if ok, err := blockData.block.Valid(ctx, u.logger, u.subtreeStore, u.utxoStore, oldBlockIDsMap, bloomFilters, blockData.blockHeaders, blockData.blockHeaderIDs, u.bloomFilterStats); !ok {
		u.logger.Errorf("[ReValidateBlock][%s] InvalidateBlock block is not valid in background: %v", blockData.block.String(), err)

		if errors.Is(err, errors.ErrStorageError) || errors.Is(err, errors.ErrServiceError) {
			// storage or service error, block is not really invalid, but we need to re-validate
			return err
		} else {
			if err = u.blockchainClient.InvalidateBlock(ctx, blockData.block.Header.Hash()); err != nil {
				u.logger.Errorf("[ReValidateBlock][%s][InvalidateBlock] failed to invalidate block: %s", blockData.block.String(), err)
			}
		}
	}

	return u.checkOldBlockIDs(ctx, oldBlockIDsMap, blockData.block.String())
}

// createAppendBloomFilter generates and manages bloom filters for blocks.
// It handles filter creation, pruning, and concurrent access management.
//
// Parameters:
//   - ctx: Context for the operation
//   - block: Block to create filter for
func (u *BlockValidation) createAppendBloomFilter(ctx context.Context, block *model.Block) {
	ctx, _, deferFn := tracing.StartTracing(ctx, "createAppendBloomFilter",
		tracing.WithParentStat(u.stats),
		tracing.WithLogMessage(u.logger, "[createAppendBloomFilter][%s] creating bloom filter", block.Hash().String()),
	)
	defer deferFn()

	if u.blockBloomFiltersBeingCreated.Exists(*block.Hash()) {
		return
	}

	// check whether the bloom filter for this block already exists
	u.recentBlocksBloomFiltersMu.Lock()

	if _, exists := u.recentBlocksBloomFilters[*block.Hash()]; exists {
		u.recentBlocksBloomFiltersMu.Unlock()
		return
	}
	u.recentBlocksBloomFiltersMu.Unlock()

	_ = u.blockBloomFiltersBeingCreated.Put(*block.Hash())
	defer func() {
		_ = u.blockBloomFiltersBeingCreated.Delete(*block.Hash())
	}()

	startTime := time.Now()

	u.logger.Infof("[createAppendBloomFilter][%s] creating bloom filter", block.Hash().String())

	var err error

	// create a bloom filter for the block
	bbf := &model.BlockBloomFilter{
		CreationTime: time.Now(),
		BlockHash:    block.Hash(),
		BlockHeight:  block.Height,
	}

	bbf.Filter, err = block.NewOptimizedBloomFilter(ctx, u.logger, u.subtreeStore)
	if err != nil {
		u.logger.Errorf("[createAppendBloomFilter][%s] failed to create bloom filter: %s", block.Hash().String(), err)
		return
	}

	// Serialize the filter
	filterBytes, err := bbf.Serialize()
	if err != nil {
		u.logger.Errorf("[createAppendBloomFilter][%s] failed to serialize bloom filter: %s", block.Hash().String(), err)
		return
	}

	// record the bloom filter in the subtreestore
	err = u.subtreeStore.Set(ctx, block.Hash()[:], filterBytes, options.WithDeleteAt(u.bloomFilterRetentionSize), options.WithFileExtension("bloomfilter"))
	if err != nil {
		u.logger.Errorf("[createAppendBloomFilter][%s] failed to record bloom filter in subtree store: %s", block.Hash().String(), err)
		return
	}

	u.pruneBloomFilters(ctx, block, bbf)

	u.logger.Infof("[createAppendBloomFilter][%s] creating bloom filter DONE in %s (%d filters)", block.Hash().String(), time.Since(startTime), len(u.recentBlocksBloomFilters))
}

func (u *BlockValidation) pruneBloomFilters(ctx context.Context, block *model.Block, bbf *model.BlockBloomFilter) {
	// get best block height
	_, bestBlockHeaderMeta, err := u.blockchainClient.GetBestBlockHeader(ctx)
	if err != nil {
		u.logger.Errorf("[createAppendBloomFilter][%s] failed to get best block height: %s", block.Hash().String(), err)
		return
	}

	// calculate lowest height to keep
	var lowestBlockHeightToKeep uint32
	if bestBlockHeaderMeta.Height > u.bloomFilterRetentionSize {
		lowestBlockHeightToKeep = bestBlockHeaderMeta.Height - u.bloomFilterRetentionSize
	}

	filtersToPrune := make([]chainhash.Hash, 0)

	u.recentBlocksBloomFiltersMu.Lock()

	// Delete old filters directly from map
	for hash, bf := range u.recentBlocksBloomFilters {
		if bf.BlockHeight < lowestBlockHeightToKeep {
			delete(u.recentBlocksBloomFilters, hash)
			filtersToPrune = append(filtersToPrune, hash)
		}
	}

	// Add the new bloom filter
	u.recentBlocksBloomFilters[*block.Hash()] = bbf

	remainingCount := len(u.recentBlocksBloomFilters)

	u.recentBlocksBloomFiltersMu.Unlock()

	u.logger.Debugf("[pruneBloomFilters][%s] pruned %d filters, %d remaining",
		block.Hash().String(), len(filtersToPrune), remainingCount)

	if len(filtersToPrune) > 0 {
		// background pruning of disk storage
		go func(pruneList []chainhash.Hash) {
			for _, hash := range pruneList {
				if err := u.subtreeStore.Del(ctx, hash[:], options.WithFileExtension("bloomfilter")); err != nil {
					u.logger.Errorf("[pruneBloomFilters][%s] failed to prune filter %s from store: %s",
						block.Hash().String(), hash.String(), err)
				}
			}
		}(filtersToPrune)
	}
}

// updateSubtreesDAH manages retention periods for block subtrees.
// It updates the DAH values and marks subtrees as properly set in the blockchain.
//
// Parameters:
//   - ctx: Context for the operation
//   - block: Block containing subtrees to update
//
// Returns an error if DAH updates fail.
func (u *BlockValidation) updateSubtreesDAH(ctx context.Context, block *model.Block) (err error) {
	ctx, _, deferFn := tracing.StartTracing(ctx, "BlockValidation:updateSubtreesDAH",
		tracing.WithLogMessage(u.logger, "[updateSubtreesDAH][%s] updating subtrees DAH", block.Hash().String()),
	)

	defer deferFn()

	// update the subtree DAHs
	g, gCtx := errgroup.WithContext(ctx)
	util.SafeSetLimit(g, u.settings.SubtreeValidation.SubtreeDAHConcurrency)

	for _, subtreeHash := range block.Subtrees {
		subtreeHash := subtreeHash

		g.Go(func() error {
			u.logger.Debugf("[updateSubtreesDAH][%s] updating subtree DAH for %s", block.Hash().String(), subtreeHash.String())

			if err := u.subtreeStore.SetDAH(gCtx, subtreeHash[:], 0, options.WithFileExtension("subtree")); err != nil {
				return errors.NewStorageError("[updateSubtreesDAH][%s] failed to update subtree DAH for %s", block.Hash().String(), subtreeHash.String(), err)
			}

			return nil
		})
	}

	if err = g.Wait(); err != nil {
		return errors.NewServiceError("[updateSubtreesDAH][%s] failed to update subtree DAH", block.Hash().String(), err)
	}

	// update block subtrees_set to true
	u.logger.Debugf("[updateSubtreesDAH][%s] setting block subtrees_set to true", block.Hash().String())

	if err = u.blockchainClient.SetBlockSubtreesSet(ctx, block.Hash()); err != nil {
		return errors.NewServiceError("[updateSubtreesDAH][%s] failed to set block subtrees_set", block.Hash().String(), err)
	}

	u.logger.Infof("[ValidateBlock][%s] updated subtree DAHs and set block subtrees_set", block.Hash().String())

	return nil
}

// validateBlockSubtrees ensures all subtrees in a block are valid.
// It manages concurrent validation and retrieval of missing subtrees.
//
// Parameters:
//   - ctx: Context for the operation
//   - block: Block containing subtrees to validate
//   - baseURL: Source URL for missing subtree retrieval
//
// Returns an error if subtree validation fails.
func (u *BlockValidation) validateBlockSubtrees(ctx context.Context, block *model.Block, baseURL string) error {
	ctx, stat, deferFn := tracing.StartTracing(ctx, "ValidateBlockSubtrees")
	defer deferFn()

	u.lastUsedBaseURL = baseURL

	start1 := gocore.CurrentTime()
	g, gCtx := errgroup.WithContext(ctx)
	util.SafeSetLimit(g, u.settings.BlockValidation.ValidateBlockSubtreesConcurrency) // keep 32 cores free for other tasks

	blockHeight := block.Height
	if blockHeight == 0 && block.Header.Version > 1 {
		var err error

		blockHeight, err = block.ExtractCoinbaseHeight()
		if err != nil {
			return errors.NewProcessingError("[validateBlockSubtrees][%s] failed to extract coinbase height", block.Hash().String(), err)
		}
	}

	foundError := atomic.Bool{}

	for _, subtreeHash := range block.Subtrees {
		subtreeHash := subtreeHash // Create a new variable for each iteration to avoid data race

		g.Go(func() error {
			select {
			case <-gCtx.Done():
				return gCtx.Err()
			default:
				subtreeExists, err := u.GetSubtreeExists(gCtx, subtreeHash)
				if err != nil {
					// this error will stop all further processing
					return errors.NewStorageError("[validateBlockSubtrees][%s] failed to check if subtree exists in store", subtreeHash.String(), err)
				}

				if !subtreeExists {
					u.logger.Debugf("[validateBlockSubtrees][%s] checking missing subtree [%s] in parallel", block.Hash().String(), subtreeHash.String())

					checkCtx, cancel := context.WithTimeout(gCtx, u.settings.BlockValidation.CheckSubtreeFromBlockTimeout)
					defer cancel()

					// note: the subtree validation client will handle retries internally
					if err = u.subtreeValidationClient.CheckSubtreeFromBlock(checkCtx, *subtreeHash, baseURL, blockHeight, block.Hash(), block.Header.HashPrevBlock); err != nil {
						// mark an error as found, which will trigger a re-validation of all subtrees in order
						foundError.Store(true)
						u.logger.Errorf("[validateBlockSubtrees][%s] failed to check missing subtree [%s], will retry in series: %s", block.Hash().String(), subtreeHash.String(), err)
					}
				}

				return nil
			}
		})
	}

	// an error is only thrown if the existence of a subtree cannot be checked, which implies a service error
	if err := g.Wait(); err != nil {
		return err
	}

	// if we did find an error, we must run through all the subtrees again, but this time in order
	// returning an error if one of them is not validated properly
	if foundError.Load() {
		for _, subtreeHash := range block.Subtrees {
			subtreeExists, err := u.GetSubtreeExists(ctx, subtreeHash)
			if err != nil {
				return errors.NewStorageError("[validateBlockSubtrees][%s] failed to check if subtree exists in store", subtreeHash.String(), err)
			}

			if !subtreeExists {
				u.logger.Debugf("[validateBlockSubtrees][%s] checking missing subtree [%s] in series", block.Hash().String(), subtreeHash.String())

				checkCtx, cancel := context.WithTimeout(ctx, u.settings.BlockValidation.CheckSubtreeFromBlockTimeout)

				// note: the subtree validation client will handle retries internally
				if err = u.subtreeValidationClient.CheckSubtreeFromBlock(checkCtx, *subtreeHash, baseURL, blockHeight, block.Hash(), block.Header.HashPrevBlock); err != nil {
					cancel()

					return errors.NewStorageError("[validateBlockSubtrees][%s] failed to check missing subtree", block.Hash().String(), err)
				}

				cancel()
			}
		}
	}

	stat.NewStat("1. validateBlockSubtrees").AddTime(start1)

	return nil
}

// checkOldBlockIDs verifies that referenced blocks are in the current chain.
// It prevents invalid chain reorganizations and maintains chain consistency.
//
// Parameters:
//   - ctx: Context for the operation
//   - oldBlockIDsMap: Map of transaction IDs to their parent block IDs
//   - blockStr: String representation of the block for logging
//
// Returns an error if block verification fails.
func (u *BlockValidation) checkOldBlockIDs(ctx context.Context, oldBlockIDsMap *sync.Map, blockStr string) (iterationError error) {
	// range over the oldBlockIDsMap to get txID - oldBlockID pairs
	oldBlockIDsMap.Range(func(txID, blockIDs interface{}) bool {
		txHash, ok := txID.(chainhash.Hash)
		if !ok {
			iterationError = errors.NewProcessingError("[Block Validation][checkOldBlockIDs] failed to assert txID to chainhash.Hash for txID: %v", txID)
			return false
		}

		parentTransactionsBlockIDs, ok := blockIDs.([]uint32)
		if !ok {
			iterationError = errors.NewProcessingError("[Block Validation][checkOldBlockIDs] failed to assert blockIDs to []uint32 for txID: %v", txID)
			return false
		}

		// Flag to check if the old blocks are part of the current chain
		blocksPartOfCurrentChain, err := u.blockchainClient.CheckBlockIsInCurrentChain(ctx, parentTransactionsBlockIDs)
		// if err is not nil, log the error and continue iterating for the next transaction
		if err != nil {
			iterationError = errors.NewProcessingError("[Block Validation][checkOldBlockIDs][%s] failed to check if old blocks are part of the current chain", blockStr, err)
			return false
		}

		// if the blocks are not part of the current chain, stop iteration, set the iterationError and return false
		if !blocksPartOfCurrentChain {
			iterationError = errors.NewBlockInvalidError("[Block Validation][checkOldBlockIDs][%s] block is not invalid. Transaction's (%v) parent blocks (%v) are not from current chain", blockStr, txHash, blockIDs)
			return false
		}

		return true
	})

	return
}
