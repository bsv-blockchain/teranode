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
	"fmt"
	"math"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	safeconversion "github.com/bsv-blockchain/go-safe-conversion"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	txmap "github.com/bsv-blockchain/go-tx-map"
	"github.com/bsv-blockchain/teranode/errors"
	p2pconstants "github.com/bsv-blockchain/teranode/interfaces/p2p"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/subtreevalidation"
	"github.com/bsv-blockchain/teranode/services/validator"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/blob"
	"github.com/bsv-blockchain/teranode/stores/blob/options"
	blockchainoptions "github.com/bsv-blockchain/teranode/stores/blockchain/options"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/kafka"
	kafkamessage "github.com/bsv-blockchain/teranode/util/kafka/kafka_message"
	"github.com/bsv-blockchain/teranode/util/retry"
	"github.com/bsv-blockchain/teranode/util/tracing"
	"github.com/ordishs/go-utils/expiringmap"
	"github.com/ordishs/gocore"
	"golang.org/x/sync/errgroup"
	"google.golang.org/protobuf/proto"
)

// ValidateBlockOptions provides optional parameters for block validation.
// This is primarily used during catchup to optimize performance by reusing
// cached data rather than fetching it repeatedly.
type ValidateBlockOptions struct {
	// CachedHeaders provides pre-fetched headers to avoid redundant database queries.
	// When provided, ValidateBlock will use these headers instead of fetching them.
	CachedHeaders []*model.BlockHeader

	// IsCatchupMode indicates the block is being validated during catchup.
	// This enables optimizations like reduced logging and header reuse.
	IsCatchupMode bool

	// DisableOptimisticMining overrides the global optimistic mining setting.
	// This is typically set to true during catchup for better performance.
	DisableOptimisticMining bool

	// IsRevalidation indicates this is a revalidation of an invalid block.
	// When true, skips existence check and clears invalid flag after successful validation.
	IsRevalidation bool

	// PeerID is the P2P peer identifier used for reputation tracking.
	// This is used to track peer behavior during subtree validation.
	PeerID string
}

// validationResult holds the result of a block validation for sharing between goroutines
type validationResult struct {
	done chan struct{} // Closed when validation completes
	err  error         // The validation result
	mu   sync.RWMutex  // Protects err
}

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

	// validatorClient handles transaction validation operations
	validatorClient validator.Interface

	// recentBlocksBloomFilters maintains bloom filters for recent blocks
	recentBlocksBloomFilters *txmap.SyncedMap[chainhash.Hash, *model.BlockBloomFilter]

	// bloomFilterRetentionSize defines the number of blocks to keep bloom filters for
	bloomFilterRetentionSize uint32

	// subtreeValidationClient manages subtree validation processes
	subtreeValidationClient subtreevalidation.Interface

	// subtreeDeDuplicator prevents duplicate processing of subtrees
	subtreeDeDuplicator *DeDuplicator

	// lastValidatedBlocks caches recently validated blocks for 2 minutes
	lastValidatedBlocks *expiringmap.ExpiringMap[chainhash.Hash, *model.Block]

	// blockExistsCache tracks validated block hashes for 2 hours
	blockExistsCache *expiringmap.ExpiringMap[chainhash.Hash, bool]

	// subtreeExistsCache tracks validated subtree hashes for 10 minutes
	subtreeExistsCache *expiringmap.ExpiringMap[chainhash.Hash, bool]

	// subtreeCount tracks the number of subtrees being processed
	subtreeCount atomic.Int32

	// blockHashesCurrentlyValidated tracks blocks in validation process (for setTxMined)
	blockHashesCurrentlyValidated *txmap.SwissMap

	// blocksCurrentlyValidating tracks blocks being validated to prevent concurrent validation
	blocksCurrentlyValidating *txmap.SyncedMap[chainhash.Hash, *validationResult]

	// blockBloomFiltersBeingCreated tracks bloom filters being generated
	blockBloomFiltersBeingCreated *txmap.SwissMap

	// bloomFilterStats collects statistics about bloom filter operations
	bloomFilterStats *model.BloomStats

	// setMinedChan receives block hashes that need to be marked as mined
	setMinedChan chan *chainhash.Hash

	// revalidateBlockChan receives blocks that need revalidation
	revalidateBlockChan chan revalidateBlockData

	// stats tracks operational metrics for monitoring
	stats *gocore.Stat

	// invalidBlockKafkaProducer publishes invalid block events to Kafka
	invalidBlockKafkaProducer kafka.KafkaAsyncProducerI

	// backgroundTasks tracks background goroutines to ensure proper shutdown
	backgroundTasks sync.WaitGroup
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
//   - utxoStore: Storage for utxos and transaction metadata
//   - validatorClient: Transaction validation interface
//   - subtreeValidationClient: Subtree validation interface
//   - opts: Optional parameters:
//   - bloomRetentionSize: Length of last X blocks to retain bloom filters
//
// Returns a configured BlockValidation instance ready for use.
func NewBlockValidation(ctx context.Context, logger ulogger.Logger, tSettings *settings.Settings, blockchainClient blockchain.ClientI, subtreeStore blob.Store,
	txStore blob.Store, utxoStore utxo.Store, validatorClient validator.Interface, subtreeValidationClient subtreevalidation.Interface, opts ...interface{},
) *BlockValidation {
	logger.Infof("optimisticMining = %v", tSettings.BlockValidation.OptimisticMining)
	// Initialize Kafka producer for invalid blocks if configured
	var invalidBlockKafkaProducer kafka.KafkaAsyncProducerI

	if tSettings.Kafka.InvalidBlocks != "" {
		logger.Infof("Initializing Kafka producer for invalid blocks topic: %s", tSettings.Kafka.InvalidBlocks)

		var err error

		invalidBlockKafkaProducer, err = initialiseInvalidBlockKafkaProducer(ctx, logger, tSettings)
		if err != nil {
			logger.Errorf("Failed to create Kafka producer for invalid blocks: %v", err)
		} else {
			// Start the producer with a message channel
			go invalidBlockKafkaProducer.Start(ctx, make(chan *kafka.Message, 100))
		}
	} else {
		logger.Infof("No Kafka topic configured for invalid blocks, using interface handler only")
	}

	bv := &BlockValidation{
		logger:                        logger,
		settings:                      tSettings,
		blockchainClient:              blockchainClient,
		subtreeStore:                  subtreeStore,
		subtreeBlockHeightRetention:   tSettings.GetSubtreeValidationBlockHeightRetention(),
		txStore:                       txStore,
		utxoStore:                     utxoStore,
		validatorClient:               validatorClient,
		recentBlocksBloomFilters:      txmap.NewSyncedMap[chainhash.Hash, *model.BlockBloomFilter](),
		bloomFilterRetentionSize:      tSettings.GetSubtreeValidationBlockHeightRetention() + 2, // Needs to be larger than global value but not orders of magnitude larger
		subtreeValidationClient:       subtreeValidationClient,
		subtreeDeDuplicator:           NewDeDuplicator(tSettings.GetSubtreeValidationBlockHeightRetention()),
		lastValidatedBlocks:           expiringmap.New[chainhash.Hash, *model.Block](2 * time.Minute),
		blockExistsCache:              expiringmap.New[chainhash.Hash, bool](120 * time.Minute), // we keep this for 2 hours
		invalidBlockKafkaProducer:     invalidBlockKafkaProducer,
		subtreeExistsCache:            expiringmap.New[chainhash.Hash, bool](10 * time.Minute), // we keep this for 10 minutes
		subtreeCount:                  atomic.Int32{},
		blockHashesCurrentlyValidated: txmap.NewSwissMap(0),
		blocksCurrentlyValidating:     txmap.NewSyncedMap[chainhash.Hash, *validationResult](),
		blockBloomFiltersBeingCreated: txmap.NewSwissMap(0),
		bloomFilterStats:              model.NewBloomStats(),
		setMinedChan:                  make(chan *chainhash.Hash, 1000),
		revalidateBlockChan:           make(chan revalidateBlockData, 2),
		stats:                         gocore.NewStat("blockvalidation"),
	}

	go func() {
		// update stats for the expiring maps every 5 seconds
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				prometheusBlockValidationLastValidatedBlocksCache.Set(float64(bv.lastValidatedBlocks.Len()))
				prometheusBlockValidationBlockExistsCache.Set(float64(bv.blockExistsCache.Len()))
				prometheusBlockValidationSubtreeExistsCache.Set(float64(bv.subtreeExistsCache.Len()))
			}
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
					bv.logger.Warnf("[BlockValidation:setMined] exiting setMined goroutine: %s", ctx.Err())
					return
				default:
					bv.logger.Infof("[BlockValidation:setMined] subscribing to blockchain for setTxMined signal")

					subscribeCtx, subscribeCancel = context.WithCancel(ctx)

					blockchainSubscription, err := bv.blockchainClient.Subscribe(subscribeCtx, "blockvalidation")
					if err != nil {
						// Check if context is done before logging
						select {
						case <-ctx.Done():
							return
						default:
						}

						bv.logger.Errorf("[BlockValidation:setMined] failed to subscribe to blockchain: %s", err)

						// Cancel context before retrying to prevent leak
						subscribeCancel()

						// backoff for 5 seconds and try again
						time.Sleep(5 * time.Second)

						continue
					}

				subscriptionLoop:
					for {
						select {
						case <-ctx.Done():
							subscribeCancel()
							return

						case notification, ok := <-blockchainSubscription:
							if !ok {
								// Channel closed, reconnect
								bv.logger.Warnf("[BlockValidation:setMined] subscription channel closed, reconnecting")
								subscribeCancel()
								time.Sleep(1 * time.Second)
								break subscriptionLoop
							}

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
					}
				}
			}
		}()
	}

	go func() {
		if err := bv.start(ctx); err != nil {
			// Check if context is done before logging
			select {
			case <-ctx.Done():
				// Context canceled, don't log
			default:
				logger.Errorf("[BlockValidation:start] failed to start: %s", err)
			}
		}
	}()

	return bv
}

func initialiseInvalidBlockKafkaProducer(ctx context.Context, logger ulogger.Logger, tSettings *settings.Settings) (*kafka.KafkaAsyncProducer, error) {
	logger.Infof("Initializing Kafka producer for invalid blocks topic: %s", tSettings.Kafka.InvalidBlocks)

	invalidBlockKafkaProducer, err := kafka.NewKafkaAsyncProducerFromURL(ctx, logger, tSettings.Kafka.InvalidBlocksConfig, &tSettings.Kafka)
	if err != nil {
		return nil, err
	}

	return invalidBlockKafkaProducer, nil
}

// start initializes the block validation system and begins processing.
// It handles the recovery of unprocessed blocks and starts background workers
// for block validation tasks.
func (u *BlockValidation) start(ctx context.Context) error {
	go u.bloomFilterStats.BloomFilterStatsProcessor(ctx)

	g, gCtx := errgroup.WithContext(ctx)

	if u.blockchainClient != nil {
		// check whether all old blocks have their subtrees_set set
		u.processSubtreesNotSet(gCtx, g)

		// check whether all old blocks have their mined_set set
		u.processBlockMinedNotSet(gCtx, g)

		// wait for all blocks to be processed
		if err := g.Wait(); err != nil {
			// we cannot start the block validation, we are in a bad state
			return errors.NewServiceError("[BlockValidation:start] failed to start, process old block mined/subtrees sets", err)
		}
	}

	// start a ticker that checks every minute whether there are subtrees/mined that need to be set
	// this is a light routine for periodic cleanup and handling of invalidated blocks
	go func() {
		u.logger.Infof("[BlockValidation:start] starting periodic block processing goroutine")
		ticker := time.NewTicker(1 * time.Minute)

		for {
			select {
			case <-ctx.Done():
				u.logger.Warnf("[BlockValidation:start] exiting periodic block processing goroutine: %s", ctx.Err())
				return
			case <-ticker.C:
				u.processSubtreesNotSet(ctx, g)
				u.processBlockMinedNotSet(ctx, g)
			}
		}
	}()

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

				// check whether the block needs the tx mined, or it has already been done
				_, blockHeaderMeta, err := u.blockchainClient.GetBlockHeader(ctx, blockHash)
				if err != nil {
					// Don't log errors if context was cancelled, sometimes causes panic in tests
					if !errors.Is(err, context.Canceled) {
						u.logger.Errorf("[BlockValidation:start][%s] failed to get block header: %s", blockHash.String(), err)
					}
					continue
				}

				if blockHeaderMeta == nil {
					u.logger.Errorf("[BlockValidation:start][%s] blockHeaderMeta is nil", blockHash.String())
					continue
				}

				if blockHeaderMeta.MinedSet {
					u.logger.Infof("[BlockValidation:start][%s] block already has mined_set true, skipping setTxMined", blockHash.String())
					continue
				}

				_ = u.blockHashesCurrentlyValidated.Put(*blockHash)

				if err = u.setTxMinedStatus(ctx, blockHash, blockHeaderMeta.Invalid); err != nil {
					// Check if context is done before logging
					select {
					case <-ctx.Done():
						return
					default:
					}

					u.logger.Errorf("[BlockValidation:start][%s] failed setTxMined: %s", blockHash.String(), err)

					// Always remove from map on failure to prevent blocking child blocks
					if deleteErr := u.blockHashesCurrentlyValidated.Delete(*blockHash); deleteErr != nil {
						u.logger.Errorf("[BlockValidation:start][%s] failed to delete blockHash from blockHashesCurrentlyValidated: %s", blockHash.String(), deleteErr)
					}

					if !errors.Is(err, errors.ErrBlockNotFound) {
						time.Sleep(1 * time.Second)
						// put the block back in the setMinedChan for retry
						u.setMinedChan <- blockHash
					}
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
					// Check if context is done before logging
					select {
					case <-ctx.Done():
						return
					default:
					}

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

func (u *BlockValidation) processBlockMinedNotSet(ctx context.Context, g *errgroup.Group) {
	if u.blockchainClient == nil {
		return
	}

	// first check whether all old blocks have been processed properly
	blocksMinedNotSet, err := u.blockchainClient.GetBlocksMinedNotSet(ctx)
	if err != nil {
		u.logger.Errorf("[BlockValidation:start] failed to get blocks mined not set: %s", err)
	}

	if len(blocksMinedNotSet) > 0 {
		u.logger.Infof("[BlockValidation:start] found %d blocks mined not set", len(blocksMinedNotSet))

		for _, block := range blocksMinedNotSet {
			blockHash := block.Hash()

			_ = u.blockHashesCurrentlyValidated.Put(*blockHash)

			g.Go(func() error {
				u.logger.Debugf("[BlockValidation:start] processing block mined not set: %s", blockHash.String())

				select {
				case <-ctx.Done():
					return nil
				default:
					// get the block metadata to check if the block is invalid
					_, blockHeaderMeta, err := u.blockchainClient.GetBlockHeader(ctx, blockHash)
					if err != nil {
						u.logger.Errorf("[BlockValidation:start] failed to get block header: %s", err)

						u.setMinedChan <- blockHash

						return nil
					}

					if err = u.setTxMinedStatus(ctx, blockHash, blockHeaderMeta.Invalid); err != nil {
						if errors.Is(err, context.Canceled) {
							u.logger.Infof("[BlockValidation:start] failed to set block mined: %s", err)
						} else {
							u.logger.Errorf("[BlockValidation:start] failed to set block mined: %s", err)
						}
						u.setMinedChan <- blockHash
					}

					if err = u.blockHashesCurrentlyValidated.Delete(*blockHash); err != nil {
						u.logger.Errorf("[BlockValidation:start] failed to delete block from currently validated: %s", err)
					}

					u.logger.Infof("[BlockValidation:start] processed block mined and set mined_set: %s", blockHash.String())

					return nil
				}
			})
		}
	}
}

func (u *BlockValidation) processSubtreesNotSet(ctx context.Context, g *errgroup.Group) {
	if u.blockchainClient == nil {
		return
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

				if err := u.updateSubtreesDAH(ctx, block); err != nil {
					u.logger.Errorf("[BlockValidation:start] failed to update subtrees DAH: %s", err)
				}

				return nil
			})
		}
	}
}

// SetBlockExists marks a block as existing in the validation system's cache.
//
// This function updates the internal block existence cache to indicate that a block
// with the specified hash is known to exist. This is typically called after a block
// has been successfully validated or when its existence has been confirmed through
// other means, helping to optimize future existence checks.
//
// The function provides a simple caching mechanism that avoids repeated expensive
// lookups to the blockchain client for blocks that are known to exist.
//
// Parameters:
//   - hash: Hash of the block to mark as existing
//
// Returns:
//   - error: Always returns nil in the current implementation
func (u *BlockValidation) SetBlockExists(hash *chainhash.Hash) error {
	u.blockExistsCache.Set(*hash, true)
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

	_, ok := u.blockExistsCache.Get(*hash)
	if ok {
		return true, nil
	}

	exists, err := u.blockchainClient.GetBlockExists(ctx, hash)
	if err != nil {
		return false, err
	}

	if exists {
		u.blockExistsCache.Set(*hash, true)
	}

	return exists, nil
}

// SetSubtreeExists marks a subtree as existing in the validation system's cache.
//
// This function updates the internal subtree existence cache to indicate that a subtree
// with the specified hash is known to exist. This is typically called after a subtree
// has been successfully validated or when its existence has been confirmed through
// other means, helping to optimize future existence checks.
//
// The function provides a simple caching mechanism that avoids repeated expensive
// lookups to the blockchain client for subtrees that are known to exist. The cache
// is implemented as an expiring map, where entries are automatically removed after
// a specified retention period.
//
// Parameters:
//   - hash: Hash of the subtree to mark as existing
//
// Returns:
//   - error: Any error encountered during the cache update
func (u *BlockValidation) SetSubtreeExists(hash *chainhash.Hash) error {
	u.subtreeExistsCache.Set(*hash, true)
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
	ctx, _, deferFn := tracing.Tracer("blockvalidation").Start(ctx, "GetSubtreeExists")
	defer deferFn()

	_, ok := u.subtreeExistsCache.Get(*hash)
	if ok {
		return true, nil
	}

	exists, err := u.subtreeStore.Exists(ctx, hash[:], fileformat.FileTypeSubtree)
	if err != nil {
		exists, err = u.subtreeStore.Exists(ctx, hash[:], fileformat.FileTypeSubtreeToCheck)
		if err != nil {
			return false, err
		}
	}

	if exists {
		u.subtreeExistsCache.Set(*hash, true)
	}

	return exists, nil
}

// hasValidSubtrees checks if a block has all its subtrees properly loaded.
// A block is considered to have valid subtrees when:
// - The number of SubtreeSlices equals the number of Subtrees
// - There is at least one subtree
// - None of the SubtreeSlices are nil
//
// Parameters:
//   - block: The block to check
//
// Returns:
//   - bool: true if all subtrees are valid and loaded, false otherwise
func (u *BlockValidation) hasValidSubtrees(block *model.Block) bool {
	if block == nil {
		return false
	}

	// Check if subtrees are loaded and match expected count
	if len(block.SubtreeSlices) != len(block.Subtrees) || len(block.SubtreeSlices) == 0 {
		return false
	}

	// Verify all subtrees are non-nil
	for _, subtree := range block.SubtreeSlices {
		if subtree == nil {
			return false
		}
	}

	return true
}

// setTxMinedStatus marks all transactions within a block as mined in the blockchain system.
//
// This function updates the mining status of all transactions contained within the specified
// block, transitioning them from validated to mined state. This is a critical operation
// that occurs after a block has been successfully validated and accepted into the blockchain.
//
// The function performs several key operations:
// 1. Retrieves transaction metadata for all transactions in the block
// 2. Updates the mining status in the transaction store
// 3. Manages state transitions for transaction lifecycle tracking
// 4. Ensures consistency between block validation and transaction mining states
//
// Parameters:
//   - ctx: Context for the operation, enabling cancellation and timeout handling
//   - blockHash: Hash of the block containing transactions to mark as mined
//
// Returns:
//   - error: Any error encountered during the mining status update process
func (u *BlockValidation) setTxMinedStatus(ctx context.Context, blockHash *chainhash.Hash, unsetMined ...bool) (err error) {
	ctx, _, deferFn := tracing.Tracer("blockvalidation").Start(ctx, "setTxMined",
		tracing.WithParentStat(u.stats),
		tracing.WithLogMessage(u.logger, "[setTxMined][%s] setting tx mined", blockHash.String()),
	)
	defer deferFn()

	var (
		block           *model.Block
		blockHeaderMeta *model.BlockHeaderMeta
		onLongestChain  bool
		ids             []uint32
	)

	cachedBlock, blockWasAlreadyCached := u.lastValidatedBlocks.Get(*blockHash)

	if blockWasAlreadyCached && cachedBlock != nil {
		// Verify the cached block has subtrees loaded
		if u.hasValidSubtrees(cachedBlock) {
			u.logger.Debugf("[setTxMined][%s] using cached block with subtrees", blockHash.String())
			block = cachedBlock

			// Remove from cache immediately - we're about to use it and don't need it cached anymore
			// This frees memory sooner than waiting for cache expiry or the delete at line 894
			u.lastValidatedBlocks.Delete(*blockHash)
		} else {
			u.logger.Warnf("[setTxMined][%s] cached block has invalid subtrees, fetching from blockchain", blockHash.String())
			blockWasAlreadyCached = false
		}
	}

	if !blockWasAlreadyCached || block == nil {
		// get the block from the blockchain
		if block, err = u.blockchainClient.GetBlock(ctx, blockHash); err != nil {
			return errors.NewServiceError("[setTxMined][%s] failed to get block from blockchain", blockHash.String(), err)
		}
	}

	_, blockHeaderMeta, err = u.blockchainClient.GetBlockHeader(ctx, blockHash)
	if err != nil {
		return errors.NewServiceError("[setTxMined][%s] failed to get block header from blockchain", blockHash.String(), err)
	}

	onLongestChain, err = u.blockchainClient.CheckBlockIsInCurrentChain(ctx, []uint32{blockHeaderMeta.ID})
	if err != nil {
		return errors.NewServiceError("[setTxMined][%s] failed to check if block is on longest chain", blockHash.String(), err)
	}

	if len(unsetMined) > 0 && unsetMined[0] {
		u.logger.Warnf("[setTxMined][%s] block is marked as invalid, will attempt to unset tx mined", block.Hash().String())

		block.SubtreeSlices = make([]*subtreepkg.Subtree, len(block.Subtrees))

		// when the block is invalid, we might not have all the subtrees
		for subtreeIdx, subtreeHash := range block.Subtrees {
			subtreeBytes, err := u.subtreeStore.Get(ctx, subtreeHash[:], fileformat.FileTypeSubtree)
			if err != nil {
				subtreeBytes, err = u.subtreeStore.Get(ctx, subtreeHash[:], fileformat.FileTypeSubtreeToCheck)
				if err != nil {
					u.logger.Warnf("[setTxMined][%s] failed to get subtree %d/%s from store: %s", block.Hash().String(), subtreeIdx, subtreeHash.String(), err)
					continue
				}
			}

			subtree, err := subtreepkg.NewSubtreeFromBytes(subtreeBytes)
			if err != nil {
				u.logger.Warnf("[setTxMined][%s] failed to parse subtree %d/%s: %s", block.Hash().String(), subtreeIdx, subtreeHash.String(), err)
				continue
			}

			block.SubtreeSlices[subtreeIdx] = subtree

			u.logger.Debugf("[setTxMined][%s] loaded subtree %d/%s from store", block.Hash().String(), subtreeIdx, subtreeHash.String())
		}
	} else {
		// All subtrees should already be available for fully processed blocks
		_, err = block.GetSubtrees(ctx, u.logger, u.subtreeStore, u.settings.Block.GetAndValidateSubtreesConcurrency)
		if err != nil {
			return errors.NewProcessingError("[setTxMined][%s] failed to get subtrees from block", block.Hash().String(), err)
		}
	}

	// get all ancestor block ids without depth limit to ensure duplicate transaction detection
	// use math.MaxInt32 (2.1 billion blocks) which is effectively unlimited for blockchain purposes
	if ids, err = u.blockchainClient.GetBlockHeaderIDs(ctx, blockHash, math.MaxInt32); err != nil || len(ids) == 0 {
		if err != nil {
			return errors.NewServiceError("[setTxMined][%s] failed to get block header ids", blockHash.String(), err)
		}
		return errors.NewServiceError("[setTxMined][%s] failed to get block header ids", blockHash.String())
	}

	// add the transactions in this block to the block IDs in the utxo store
	if err = model.UpdateTxMinedStatus(
		ctx,
		u.logger,
		u.settings,
		u.utxoStore,
		block,
		ids[0],
		ids[0:], // all ancestor block IDs on this chain - used to detect if transactions were already mined on this fork
		onLongestChain,
		unsetMined...,
	); err != nil {
		// check whether we got already mined errors and mark the block as invalid
		if errors.Is(err, errors.ErrBlockInvalid) {
			// mark the block as invalid in the blockchain
			return u.markBlockAsInvalid(ctx, block, "contains transactions already on our chain: "+err.Error())
		}

		return errors.NewProcessingError("[setTxMined][%s] error updating tx mined status", block.Hash().String(), err)
	}

	// Clear subtrees to free memory - they're no longer needed after UpdateTxMinedStatus
	// This prevents memory retention in the blockchain store cache if block came from there and was mutated
	// Note: lastValidatedBlocks cache was already cleared at line 799 when we retrieved the block
	block.SubtreeSlices = nil

	// update block mined_set to true
	if err = u.blockchainClient.SetBlockMinedSet(ctx, blockHash); err != nil {
		return errors.NewServiceError("[setTxMined][%s] failed to set block mined", block.Hash().String(), err)
	}

	return nil
}

// isParentMined verifies if a block's parent has been successfully mined and committed to the blockchain.
//
// This function performs a critical validation step in the block processing pipeline by
// ensuring that a block's parent has completed the mining process before allowing the
// current block to proceed with validation. This maintains proper blockchain ordering
// and prevents validation of orphaned or premature blocks.
//
// The function queries the blockchain client to check the mining status of the parent
// block, using the HashPrevBlock field from the provided block header. This check is
// essential for maintaining chain consistency and proper block sequencing.
//
// Parameters:
//   - ctx: Context for the operation, enabling cancellation and timeout handling
//   - blockHeader: Header of the block whose parent mining status needs verification
//
// Returns:
//   - bool: True if the parent block has been mined, false otherwise
//   - error: Any error encountered during the mining status verification
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

// runOncePerBlock ensures validation runs only once per block.
// If another goroutine is already validating, it waits and returns that result.
func (u *BlockValidation) runOncePerBlock(blockHash *chainhash.Hash, opts *ValidateBlockOptions, validate func(opts *ValidateBlockOptions) error) error {
	result := &validationResult{
		done: make(chan struct{}),
	}

	existingResult, wasFirst := u.blocksCurrentlyValidating.SetIfNotExists(*blockHash, result)

	if !wasFirst {
		// Another thread is validating, wait for result
		u.logger.Debugf("[ValidateBlock][%s] waiting for concurrent validation", blockHash.String())
		<-existingResult.done
		existingResult.mu.RLock()
		defer existingResult.mu.RUnlock()
		return existingResult.err
	}

	// We're first - run validation
	err := validate(opts)

	// Store and broadcast result
	result.mu.Lock()
	result.err = err
	result.mu.Unlock()
	close(result.done)

	// Cleanup after delay
	go func() {
		time.Sleep(100 * time.Millisecond)
		u.blocksCurrentlyValidating.Delete(*blockHash)
	}()

	return err
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
	// Convert legacy parameters to options
	opts := &ValidateBlockOptions{}
	if len(disableOptimisticMining) > 0 {
		opts.DisableOptimisticMining = disableOptimisticMining[0]
	}
	return u.ValidateBlockWithOptions(ctx, block, baseURL, bloomStats, opts)
}

// ValidateBlockWithOptions performs comprehensive validation of a Bitcoin block with additional options.
// This method provides the same functionality as ValidateBlock but allows for performance optimizations
// during catchup operations by accepting cached data.
//
// Parameters:
//   - ctx: Context for the validation operation
//   - block: Block to validate
//   - baseURL: Source URL for additional data retrieval
//   - bloomStats: Statistics collector for bloom filter operations
//   - opts: Optional parameters for validation optimization
//
// Returns an error if validation fails or nil on success.
func (u *BlockValidation) ValidateBlockWithOptions(ctx context.Context, block *model.Block, baseURL string, bloomStats *model.BloomStats, opts *ValidateBlockOptions) error {
	if opts == nil {
		opts = &ValidateBlockOptions{}
	}
	ctx, _, deferFn := tracing.Tracer("blockvalidation").Start(ctx, "ValidateBlock",
		tracing.WithParentStat(u.stats),
		tracing.WithHistogram(prometheusBlockValidationValidateBlock),
		tracing.WithLogMessage(u.logger, "[ValidateBlock][%s] validating block from %s", block.Hash().String(), baseURL),
	)
	defer deferFn()

	// Use helper to ensure block is validated only once
	blockHash := block.Hash()
	return u.runOncePerBlock(blockHash, opts, func(opts *ValidateBlockOptions) error {
		var err error

		// Check if block already exists to prevent duplicate validation (unless revalidating)
		if !opts.IsRevalidation {
			blockExists, err := u.GetBlockExists(ctx, block.Header.Hash())
			if err != nil {
				// If there's an error checking existence, proceed with validation
				u.logger.Warnf("[ValidateBlock][%s] error checking block existence: %v, proceeding with validation", block.Header.Hash().String(), err)
			} else if blockExists {
				// Block exists - check if it's invalid
				_, blockMeta, err := u.blockchainClient.GetBlockHeader(ctx, block.Header.Hash())
				if err != nil {
					u.logger.Warnf("[ValidateBlock][%s] failed to get block metadata for existing block: %v, assuming valid", block.Header.Hash().String(), err)
					return nil
				}

				if blockMeta != nil && blockMeta.Invalid {
					u.logger.Warnf("[ValidateBlock][%s] block already exists and is marked as invalid", block.Header.Hash().String())
					return errors.NewBlockInvalidError("[ValidateBlock][%s] block already exists as invalid", block.Header.Hash().String())
				}

				u.logger.Warnf("[ValidateBlock][%s] tried to validate existing valid block", block.Header.Hash().String())
				return nil
			}
		} else {
			// If this is a revalidation, verify the block is actually marked as invalid
			_, blockMeta, err := u.blockchainClient.GetBlockHeader(ctx, block.Header.Hash())
			if err != nil {
				u.logger.Warnf("[ValidateBlock][%s] revalidation requested but couldn't get block metadata: %v", block.Header.Hash().String(), err)
				return errors.NewServiceError("[ValidateBlock][%s] failed to get block metadata for revalidation", block.Header.Hash().String(), err)
			}

			if !blockMeta.Invalid {
				u.logger.Warnf("[ValidateBlock][%s] revalidation requested but block is not marked as invalid", block.Header.Hash().String())
				return errors.NewProcessingError("[ValidateBlock][%s] cannot revalidate block that is not marked as invalid", block.Header.Hash().String())
			}

			u.logger.Infof("[ValidateBlock][%s] revalidating invalid block", block.Header.Hash().String())
		}

		// check the size of the block
		// 0 is unlimited so don't check the size
		if u.settings.Policy.ExcessiveBlockSize > 0 {
			excessiveBlockSizeUint64, err := safeconversion.IntToUint64(u.settings.Policy.ExcessiveBlockSize)
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

		// Use cached headers if available (during catchup), otherwise fetch from blockchain
		var blockHeaders []*model.BlockHeader
		if opts.CachedHeaders != nil && len(opts.CachedHeaders) > 0 {
			// Use provided cached headers
			blockHeaders = opts.CachedHeaders
			if opts.IsCatchupMode {
				u.logger.Debugf("[ValidateBlock][%s] using %d cached headers", block.Header.Hash().String(), len(blockHeaders))
			} else {
				u.logger.Infof("[ValidateBlock][%s] using %d cached headers", block.Header.Hash().String(), len(blockHeaders))
			}

			// Check if parent block is invalid - if so, child is automatically invalid
			// This optimization skips expensive validation when parent is already invalid
			// For catchup mode with cached headers, we need to query parent metadata
			_, parentMeta, err := u.blockchainClient.GetBlockHeader(ctx, block.Header.HashPrevBlock)
			if err != nil {
				u.logger.Warnf("[ValidateBlock][%s] failed to get parent block metadata: %v, continuing with validation", block.Hash().String(), err)
				// Continue with validation - this is defensive programming
			}
			if err := u.checkParentInvalidAndStore(ctx, block, baseURL, parentMeta); err != nil {
				return err
			}
		} else {
			// Fetch headers from blockchain service
			if opts.IsCatchupMode {
				u.logger.Debugf("[ValidateBlock][%s] GetBlockHeaders", block.Header.Hash().String())
			} else {
				u.logger.Infof("[ValidateBlock][%s] GetBlockHeaders", block.Header.Hash().String())
			}

			// get all X previous block headers, 100 is the default
			previousBlockHeaderCount := u.settings.BlockValidation.PreviousBlockHeaderCount

			var parentBlockHeadersMeta []*model.BlockHeaderMeta
			blockHeaders, parentBlockHeadersMeta, err = u.blockchainClient.GetBlockHeaders(ctx, block.Header.HashPrevBlock, previousBlockHeaderCount)
			if err != nil {
				u.logger.Errorf("[ValidateBlock][%s] failed to get block headers: %s", block.String(), err)
				u.ReValidateBlock(block, baseURL)

				return errors.NewServiceError("[ValidateBlock][%s] failed to get block headers", block.String(), err)
			}

			// Check if parent block is invalid using the metadata we just got
			var parentMeta *model.BlockHeaderMeta
			if len(parentBlockHeadersMeta) > 0 {
				parentMeta = parentBlockHeadersMeta[0]
			}
			if err := u.checkParentInvalidAndStore(ctx, block, baseURL, parentMeta); err != nil {
				return err
			}
		}

		// Wait for reValidationBlock to do its thing
		// When waitForPreviousBlocksToBeProcessed is done, all the previous blocks will be processed, and all previous blocks' bloom filters should be created
		if err = u.waitForPreviousBlocksToBeProcessed(ctx, block, blockHeaders); err != nil {
			// Check if parent block actually needs setTxMined before re-triggering
			blocksMinedNotSet, getErr := u.blockchainClient.GetBlocksMinedNotSet(ctx)
			if getErr == nil {
				parentNeedsMining := false
				for _, b := range blocksMinedNotSet {
					if b.Header.Hash().IsEqual(block.Header.HashPrevBlock) {
						parentNeedsMining = true
						break
					}
				}
				if parentNeedsMining {
					// re-trigger the setMinedChan for the parent block
					u.setMinedChan <- block.Header.HashPrevBlock
				}
			}

			if err = u.waitForPreviousBlocksToBeProcessed(ctx, block, blockHeaders); err != nil {
				// Give up, the parent block isn't being fully validated
				return errors.NewBlockError("[ValidateBlock][%s] given up waiting on previous blocks to be ready %s", block.Hash().String(), block.Header.HashPrevBlock.String())
			}
		}

		// validate all the subtrees in the block
		u.logger.Infof("[ValidateBlock][%s] validating %d subtrees", block.Hash().String(), len(block.Subtrees))

		if err = u.validateBlockSubtrees(ctx, block, opts.PeerID, baseURL); err != nil {
			if errors.Is(err, errors.ErrTxInvalid) || errors.Is(err, errors.ErrTxMissingParent) || errors.Is(err, errors.ErrTxNotFound) {
				u.logger.Warnf("[ValidateBlock][%s] block contains invalid transactions, marking as invalid: %s", block.Hash().String(), err)
				reason := fmt.Sprintf("block contains invalid transactions: %s", err.Error())
				u.storeInvalidBlock(ctx, block, baseURL, reason)
				return errors.NewBlockInvalidError("[ValidateBlock][%s] block contains invalid transactions: %s", block.Hash().String(), err)
			}

			return err
		}

		u.logger.Infof("[ValidateBlock][%s] validating %d subtrees DONE", block.Hash().String(), len(block.Subtrees))

		useOptimisticMining := u.settings.BlockValidation.OptimisticMining
		if opts.DisableOptimisticMining {
			// if the disableOptimisticMining is set to true, then we don't use optimistic mining, even if it is enabled
			useOptimisticMining = false
			if !opts.IsCatchupMode {
				u.logger.Infof("[ValidateBlock][%s] useOptimisticMining override: %v", block.Header.Hash().String(), useOptimisticMining)
			}
		}

		// Skip difficulty validation for blocks at or below the highest checkpoint
		// These blocks are already verified by checkpoints, so we don't need to validate difficulty
		highestCheckpointHeight := getHighestCheckpointHeight(u.settings.ChainCfgParams.Checkpoints)
		skipDifficultyCheck := block.Height <= highestCheckpointHeight

		if skipDifficultyCheck {
			u.logger.Debugf("[ValidateBlock][%s] skipping difficulty validation for block at height %d (at or below checkpoint height %d)",
				block.Header.Hash().String(), block.Height, highestCheckpointHeight)
		} else {
			// First check that the nBits (difficulty target) is correct for this block
			expectedNBits, err := u.blockchainClient.GetNextWorkRequired(ctx, block.Header.HashPrevBlock, int64(block.Header.Timestamp))
			if err != nil {
				return errors.NewServiceError("[ValidateBlock][%s] failed to get expected work required", block.Header.Hash().String(), err)
			}

			// Compare the block's nBits with the expected nBits
			if expectedNBits != nil && block.Header.Bits != *expectedNBits {
				reason := fmt.Sprintf("incorrect difficulty bits: got %v, expected %v", block.Header.Bits, *expectedNBits)
				u.storeInvalidBlock(ctx, block, baseURL, reason)

				return errors.NewBlockInvalidError("[ValidateBlock][%s] block has incorrect difficulty bits: got %v, expected %v",
					block.Header.Hash().String(), block.Header.Bits, expectedNBits)
			}

			// Then check that the block hash meets the difficulty target
			headerValid, _, err := block.Header.HasMetTargetDifficulty()
			if !headerValid {
				reason := "block does not meet target difficulty"
				if err != nil {
					reason = fmt.Sprintf("block does not meet target difficulty: %s", err.Error())
				}
				u.storeInvalidBlock(ctx, block, baseURL, reason)

				return errors.NewBlockInvalidError("[ValidateBlock][%s] block does not meet target difficulty: %s", block.Header.Hash().String(), err)
			}
		}

		var optimisticMiningWg sync.WaitGroup

		oldBlockIDsMap := txmap.NewSyncedMap[chainhash.Hash, []uint32]()

		if useOptimisticMining {
			// NOTE: We do NOT cache the block here as subtrees are not yet loaded.
			// The block will be cached after subtrees are validated in the background goroutine.

			u.logger.Infof("[ValidateBlock][%s] adding block optimistically to blockchain", block.Hash().String())

			if err = u.blockchainClient.AddBlock(ctx, block, baseURL); err != nil {
				return errors.NewServiceError("[ValidateBlock][%s] failed to store block", block.Hash().String(), err)
			}

			u.logger.Infof("[ValidateBlock][%s] adding block optimistically to blockchain DONE", block.Hash().String())

			if err = u.SetBlockExists(block.Header.Hash()); err != nil {
				u.logger.Errorf("[ValidateBlock][%s] failed to set block exists cache: %s", block.Header.Hash().String(), err)
			}

			// decouple the tracing context to not cancel the context when finalize the block processing in the background
			decoupledCtx, _, endSpanFn := tracing.DecoupleTracingSpan(ctx, "ValidateBlock", "decoupled")
			defer endSpanFn()

			optimisticMiningWg.Add(1)

			go func() {
				defer optimisticMiningWg.Done()

				blockHeaderIDs, err := u.blockchainClient.GetBlockHeaderIDs(decoupledCtx, block.Header.HashPrevBlock, u.settings.BlockValidation.MaxPreviousBlockHeadersToCheck)
				if err != nil {
					u.logger.Errorf("[ValidateBlock][%s] failed to get block header ids: %v", block.String(), err)

					u.ReValidateBlock(block, baseURL)

					return
				}

				u.logger.Infof("[ValidateBlock][%s] GetBlockHeaders DONE", block.Header.Hash().String())

				u.logger.Infof("[ValidateBlock][%s] validating block in background", block.Hash().String())

				// only get the bloom filters for the current chain.
				bloomFilters, err := u.collectNecessaryBloomFilters(decoupledCtx, block, blockHeaders)
				if err != nil {
					u.logger.Errorf("[ValidateBlock][%s] failed to collect necessary bloom filters: %s", block.String(), err)

					u.ReValidateBlock(block, baseURL)

					return
				}

				if ok, err := block.Valid(decoupledCtx, u.logger, u.subtreeStore, u.utxoStore, oldBlockIDsMap, bloomFilters, blockHeaders, blockHeaderIDs, bloomStats, u.settings); !ok {
					u.logger.Errorf("[ValidateBlock][%s] InvalidateBlock block is not valid in background: %v", block.String(), err)

					if errors.Is(err, errors.ErrBlockInvalid) {
						reason := p2pconstants.ReasonInvalidBlock.String()
						if err = u.markBlockAsInvalid(decoupledCtx, block, reason); err != nil {
							u.logger.Errorf("[ValidateBlock][%s][InvalidateBlock] failed to invalidate block: %v", block.String(), err)
							// we should try again to re-validate the block, as we failed to mark it as invalid
							u.ReValidateBlock(block, baseURL)
						}
					} else {
						// storage or processing error, block is not really invalid, but we need to re-validate
						u.ReValidateBlock(block, baseURL)
					}

					return
				}

				// check the old block IDs and invalidate the block if needed
				if err = u.checkOldBlockIDs(decoupledCtx, oldBlockIDsMap, block); err != nil {
					u.logger.Errorf("[ValidateBlock][%s] failed to check old block IDs: %s", block.String(), err)

					if errors.Is(err, errors.ErrBlockInvalid) {
						if _, invalidateBlockErr := u.blockchainClient.InvalidateBlock(decoupledCtx, block.Header.Hash()); invalidateBlockErr != nil {
							u.logger.Errorf("[ValidateBlock][%s][InvalidateBlock] failed to invalidate block: %v", block.String(), invalidateBlockErr)
						}
					} else {
						// some other error, re-validate the block
						u.ReValidateBlock(block, baseURL)
					}

					return
				}

				// Update subtrees DAH now that we know the block is valid
				if err := u.updateSubtreesDAH(decoupledCtx, block); err != nil {
					u.logger.Errorf("[ValidateBlock][%s] failed to update subtrees DAH [%s]", block.Hash().String(), err)
				}

				// Block validation succeeded - now cache it with subtrees loaded
				u.logger.Debugf("[ValidateBlock][%s] background validation complete, caching block", block.Hash().String())
				u.lastValidatedBlocks.Set(*block.Hash(), block)
			}()
		} else {
			// get all 100 previous block headers on the main chain
			u.logger.Infof("[ValidateBlock][%s] GetBlockHeaders", block.Header.Hash().String())

			blockHeaders, blockHeadersMeta, err := u.blockchainClient.GetBlockHeaders(ctx, block.Header.HashPrevBlock, 100)
			if err != nil {
				u.logger.Errorf("[ValidateBlock][%s] failed to get block headers: %s", block.String(), err)
				u.ReValidateBlock(block, baseURL)

				return errors.NewServiceError("[ValidateBlock][%s] failed to get block headers", block.String(), err)
			}

			blockHeaderIDs := make([]uint32, len(blockHeadersMeta))
			for i, blockHeaderMeta := range blockHeadersMeta {
				blockHeaderIDs[i] = blockHeaderMeta.ID
			}

			u.logger.Infof("[ValidateBlock][%s] GetBlockHeaderIDs DONE", block.Header.Hash().String())

			// validate the block
			u.logger.Infof("[ValidateBlock][%s] validating block", block.Hash().String())

			// only get the bloom filters for the current chain
			bloomFilters, err := u.collectNecessaryBloomFilters(ctx, block, blockHeaders)
			if err != nil {
				return errors.NewServiceError("[ValidateBlock][%s] failed to collect necessary bloom filters", block.String(), err)
			}

			if ok, err := block.Valid(ctx, u.logger, u.subtreeStore, u.utxoStore, oldBlockIDsMap, bloomFilters, blockHeaders, blockHeaderIDs, bloomStats, u.settings); !ok {
				reason := "unknown"
				if err != nil {
					reason = err.Error()
				}

				// Check if we had a storage error; if so do not mark the block as invalid
				if errors.Is(err, errors.ErrStorageError) {
					return err
				}

				u.storeInvalidBlock(ctx, block, baseURL, reason)

				return errors.NewBlockInvalidError("[ValidateBlock][%s] block is not valid", block.String(), err)
			}

			if iterationError := u.checkOldBlockIDs(ctx, oldBlockIDsMap, block); iterationError != nil {
				if errors.Is(iterationError, errors.ErrBlockInvalid) {
					reason := iterationError.Error()
					u.storeInvalidBlock(ctx, block, baseURL, reason)
				}

				return iterationError
			}

			u.logger.Infof("[ValidateBlock][%s] validating block DONE", block.Hash().String())

			// if valid, store the block (or update it if revalidating)
			u.logger.Infof("[ValidateBlock][%s] adding block to blockchain", block.Hash().String())

			if opts.IsRevalidation {
				// For reconsidered blocks, we need to clear the invalid flag
				// The block data already exists, so we just update its status
				u.logger.Infof("[ValidateBlock][%s] clearing invalid flag for successfully revalidated block", block.Hash().String())

				// Use background context for critical database operation
				// Once we've validated the block, we MUST complete the storage operation
				// even if the parent context (e.g., catchup) is canceled
				storeCtx, storeCancel := context.WithTimeout(context.Background(), 30*time.Second)
				defer storeCancel()

				if err = u.blockchainClient.RevalidateBlock(storeCtx, block.Header.Hash()); err != nil {
					return errors.NewServiceError("[ValidateBlock][%s] failed to clear invalid flag after successful revalidation", block.Hash().String(), err)
				}
			} else {
				// Normal case - add new block
				// Use background context for critical database operation
				// This prevents cascading cancellation from parent operations (e.g., fetch timeouts)
				// ensuring data consistency by completing the write even if catchup is canceled
				storeCtx, storeCancel := context.WithTimeout(context.Background(), 30*time.Second)
				defer storeCancel()

				if err = u.blockchainClient.AddBlock(storeCtx, block, baseURL); err != nil {
					return errors.NewServiceError("[ValidateBlock][%s] failed to store block", block.Hash().String(), err)
				}
			}

			if err = u.SetBlockExists(block.Header.Hash()); err != nil {
				u.logger.Errorf("[ValidateBlock][%s] failed to set block exists cache: %s", block.Header.Hash().String(), err)
			}

			u.logger.Infof("[ValidateBlock][%s] adding block to blockchain DONE", block.Hash().String())
		}

		u.logger.Infof("[ValidateBlock][%s] storing coinbase in tx store: %s", block.Hash().String(), block.CoinbaseTx.TxIDChainHash().String())

		if u.txStore != nil {
			if err = u.txStore.Set(ctx, block.CoinbaseTx.TxIDChainHash()[:], fileformat.FileTypeTx, block.CoinbaseTx.Bytes()); err != nil {
				u.logger.Errorf("[ValidateBlock][%s] failed to store coinbase transaction [%s]", block.Hash().String(), err)
			}
		}

		u.logger.Infof("[ValidateBlock][%s] storing coinbase in tx store: %s DONE", block.Hash().String(), block.CoinbaseTx.TxIDChainHash().String())

		// decouple the tracing context to not cancel the context when finalize the block processing in the background
		decoupledCtx, _, _ := tracing.DecoupleTracingSpan(ctx, "ValidateBlock", "decoupled")

		// Only update subtrees DAH for non-optimistic mining
		// (optimistic mining handles this in its background validation goroutine)
		if !useOptimisticMining {
			// it's critical that we call updateSubtreesDAH() only when we know the block is valid
			if err := u.updateSubtreesDAH(decoupledCtx, block); err != nil {
				u.logger.Errorf("[ValidateBlock][%s] failed to update subtrees DAH [%s]", block.Hash().String(), err)
			}
		}

		// create bloom filter for the block and wait for it
		if err = u.createAppendBloomFilter(decoupledCtx, block); err != nil {
			u.logger.Errorf("[ValidateBlock][%s] failed to create bloom filter: %s", block.Hash().String(), err)
		}

		// Cache the block only if subtrees are loaded (they should be from Valid() call)
		if u.hasValidSubtrees(block) {
			u.logger.Debugf("[ValidateBlock][%s] caching block with %d subtrees loaded", block.Hash().String(), len(block.SubtreeSlices))
			u.lastValidatedBlocks.Set(*block.Hash(), block)
		} else {
			if len(block.SubtreeSlices) != len(block.Subtrees) || len(block.SubtreeSlices) == 0 {
				u.logger.Warnf("[ValidateBlock][%s] not caching block - subtrees not loaded (%d slices, %d hashes)", block.Hash().String(), len(block.SubtreeSlices), len(block.Subtrees))
			} else {
				u.logger.Warnf("[ValidateBlock][%s] not caching block - some subtrees are nil", block.Hash().String())
			}
		}

		return nil
	})
}

func (u *BlockValidation) markBlockAsInvalid(ctx context.Context, block *model.Block, reason string) error {
	// Log the invalidation event - this is the key entry point for automatic invalidation
	u.logger.Warnf("[ValidateBlock] Marking block %s as invalid - Reason: %s", block.Hash().String(), reason)

	// Only use Kafka for reporting invalid blocks
	u.kafkaNotifyBlockInvalid(block, reason)

	if _, invalidateBlockErr := u.blockchainClient.InvalidateBlock(ctx, block.Header.Hash()); invalidateBlockErr != nil {
		return errors.NewProcessingError("[ValidateBlock][%s] Failed to invalidate block: %v", block.String(), invalidateBlockErr)
	}

	return nil
}

// storeInvalidBlock stores a block marked as invalid in the blockchain database.
// This helper function centralizes the logic for persisting invalid blocks and updating caches.
func (u *BlockValidation) storeInvalidBlock(ctx context.Context, block *model.Block, baseURL string, reason string) {
	u.logger.Warnf("[ValidateBlock][%s] storing block as invalid: %s", block.Hash().String(), reason)

	// Store the block marked as invalid so we have a record of it
	if storeErr := u.blockchainClient.AddBlock(ctx, block, baseURL, blockchainoptions.WithInvalid(true)); storeErr != nil {
		u.logger.Errorf("[ValidateBlock][%s] failed to store invalid block: %v", block.Hash().String(), storeErr)
	} else {
		// Update cache to reflect that block exists
		if cacheErr := u.SetBlockExists(block.Header.Hash()); cacheErr != nil {
			u.logger.Errorf("[ValidateBlock][%s] failed to set block exists cache: %s", block.Header.Hash().String(), cacheErr)
		}
	}

	u.kafkaNotifyBlockInvalid(block, reason)
}

// checkParentInvalidAndStore checks if the parent block is invalid and stores
// the child block as invalid if so. This is an optimization to skip expensive
// validation when the parent is already invalid.
//
// Parameters:
//   - ctx: Context for the operation
//   - block: Child block being validated
//   - baseURL: Source URL for the block
//   - parentMeta: Metadata of the parent block (can be nil)
//
// Returns:
//   - nil if parent is valid or metadata is nil
//   - error if parent is invalid (child is stored as invalid)
func (u *BlockValidation) checkParentInvalidAndStore(ctx context.Context, block *model.Block, baseURL string, parentMeta *model.BlockHeaderMeta) error {
	if parentMeta != nil && parentMeta.Invalid {
		reason := fmt.Sprintf("parent block %s is invalid", block.Header.HashPrevBlock.String())
		u.storeInvalidBlock(ctx, block, baseURL, reason)
		return errors.NewBlockInvalidError("[ValidateBlock][%s] parent block is invalid", block.Hash().String())
	}
	return nil
}

func (u *BlockValidation) kafkaNotifyBlockInvalid(block *model.Block, reason string) {
	if u.invalidBlockKafkaProducer != nil {
		u.logger.Infof("[ValidateBlock][%s] publishing invalid block to Kafka in background", block.Hash().String())
		msg := &kafkamessage.KafkaInvalidBlockTopicMessage{
			BlockHash: block.Hash().String(),
			Reason:    reason,
		}

		msgBytes, err := proto.Marshal(msg)
		if err != nil {
			u.logger.Errorf("[ValidateBlock][%s] failed to marshal invalid block message: %v", block.Hash().String(), err)
		} else {
			kafkaMsg := &kafka.Message{
				Key:   []byte(block.Hash().String()),
				Value: msgBytes,
			}
			u.invalidBlockKafkaProducer.Publish(kafkaMsg)
		}
	}
}

func (u *BlockValidation) collectNecessaryBloomFilters(ctx context.Context, block *model.Block, currentChainBlockHeaders []*model.BlockHeader) ([]*model.BlockBloomFilter, error) {
	ctx, _, deferFn := tracing.Tracer("blockvalidation").Start(ctx, "collectNecessaryBloomFilters",
		tracing.WithParentStat(u.stats),
		tracing.WithLogMessage(u.logger, "[collectNecessaryBloomFilters][%s] collecting bloom filters for %d headers", block.String(), len(currentChainBlockHeaders)),
	)
	defer deferFn()

	// Collect only the bloom-filters whose BlockHash is in that set
	bloomFilters := make([]*model.BlockBloomFilter, 0, len(currentChainBlockHeaders))

	for _, h := range currentChainBlockHeaders {
		if bf, ok := u.recentBlocksBloomFilters.Get(*h.Hash()); ok {
			bloomFilters = append(bloomFilters, bf)
		} else {
			// We should not be here, because we should have the bloom filter in the recentBlocksBloomFilters map.
			// Try to get the bloom filter from the subtree store
			bloomFilterFromSubtreeStore := u.getBloomFilterFromSubtreeStore(ctx, h.Hash())

			// if we found the bloom filter in the subtree store, we can use it
			if bloomFilterFromSubtreeStore != nil {
				bloomFilters = append(bloomFilters, bloomFilterFromSubtreeStore)
			} else {
				// bloom filter not found in subtree store
				// we need to create the bloom filter
				u.logger.Debugf("[collectNecessaryBloomFilters][%s] bloom filter for %s not found in subtree store, creating", block.String(), h.Hash().String())

				// we need to create the bloom filter, get the block
				blockToCreateBloomFilter, err := u.blockchainClient.GetBlock(ctx, h.Hash())
				if err != nil {
					return nil, errors.NewProcessingError("[collectNecessaryBloomFilters][%s] failed to get block %s from store", block.String(), h.Hash().String(), err)
				}

				if err = u.createAppendBloomFilter(ctx, blockToCreateBloomFilter); err != nil {
					return nil, errors.NewProcessingError("[collectNecessaryBloomFilters][%s] failed to create bloom filter %s from store", block.String(), h.Hash().String(), err)
				}

				// After creating the bloom filter, retrieve it from the map and append to the return slice
				if createdBf, ok := u.recentBlocksBloomFilters.Get(*h.Hash()); ok {
					bloomFilters = append(bloomFilters, createdBf)
				} else {
					return nil, errors.NewProcessingError("[collectNecessaryBloomFilters][%s] bloom filter was created but not found in recentBlocksBloomFilters map for %s", block.String(), h.Hash().String())
				}
			}
		}
	}

	return bloomFilters, nil
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
	// iterate over the bloom filters and check if they are ready
	missingBlockBloomFilters := make([]*chainhash.Hash, 0)

	for _, header := range currentChainBlockHeaders {
		// check if recentBlocksBloomFilters contains the block hash
		if _, exists := u.recentBlocksBloomFilters.Get(*header.Hash()); !exists {
			missingBlockBloomFilters = append(missingBlockBloomFilters, header.Hash())
		}
	}

	for _, hash := range missingBlockBloomFilters {
		// try to get the bloom filter from the subtree store
		bloomFilterFromSubtreeStore := u.getBloomFilterFromSubtreeStore(ctx, hash)

		// if we found the bloom filter in the subtree store, we can use it
		if bloomFilterFromSubtreeStore != nil {
			u.recentBlocksBloomFilters.Set(*hash, bloomFilterFromSubtreeStore)
		} else {
			// bloom filter not found in subtree store
			// we need to create the bloom filter
			blockToCreateBloomFilter, err := u.blockchainClient.GetBlock(ctx, hash)
			if err != nil {
				return errors.NewProcessingError("[waitForPreviousBlocksToBeProcessed][%s] failed to get block %s from store", block.String(), hash.String(), err)
			}

			if err = u.createAppendBloomFilter(ctx, blockToCreateBloomFilter); err != nil {
				return errors.NewProcessingError("[waitForPreviousBlocksToBeProcessed][%s] failed to create bloom filter %s", block.String(), hash.String(), err)
			}
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
		retry.WithBackoffDurationType(u.settings.BlockValidation.IsParentMinedRetryBackoffDuration),
		retry.WithBackoffMultiplier(u.settings.BlockValidation.IsParentMinedRetryBackoffMultiplier),
		retry.WithRetryCount(u.settings.BlockValidation.IsParentMinedRetryMaxRetry),
	)

	return err
}

func (u *BlockValidation) getBloomFilterFromSubtreeStore(ctx context.Context, hash *chainhash.Hash) *model.BlockBloomFilter {
	// try to get from the subtree store
	bloomFilterBytes, err := u.subtreeStore.Get(ctx, hash[:], fileformat.FileTypeBloomFilter)
	if err != nil || len(bloomFilterBytes) == 0 {
		return nil
	}

	// we have the bloom filter bytes in subtree store, so we are forming the bloom filter
	createdBbf := &model.BlockBloomFilter{
		CreationTime: time.Now(),
		BlockHash:    hash,
	}

	err = createdBbf.Deserialize(bloomFilterBytes)
	if err != nil {
		return nil
	}

	return createdBbf
}

// ReValidateBlock queues a block for revalidation after a previous validation failure.
//
// This function is called when a block that previously failed validation needs to be
// reprocessed, typically after resolving dependency issues or when new information
// becomes available that might allow the block to pass validation. The function
// operates asynchronously by queuing the block for revalidation rather than
// performing the validation immediately.
//
// The revalidation process is handled by a separate goroutine that processes the
// revalidation queue, allowing this function to return quickly without blocking
// the caller. This design prevents cascading delays when multiple blocks need
// revalidation.
//
// Parameters:
//   - block: The block that needs to be revalidated
//   - baseURL: Source URL for retrieving additional block data if needed during revalidation
//
// The function logs the revalidation attempt and queues the block for processing.
// No return value is provided as the operation is asynchronous.
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
	ctx, _, deferFn := tracing.Tracer("blockvalidation").Start(context.Background(), "reValidateBlock",
		tracing.WithParentStat(u.stats),
		tracing.WithLogMessage(u.logger, "[reValidateBlock][%s] validating block from %s", blockData.block.Hash().String(), blockData.baseURL),
	)
	defer deferFn()

	// Skip difficulty validation for blocks at or below the highest checkpoint
	// These blocks are already verified by checkpoints, so we don't need to validate difficulty
	highestCheckpointHeight := getHighestCheckpointHeight(u.settings.ChainCfgParams.Checkpoints)
	skipDifficultyCheck := blockData.block.Height <= highestCheckpointHeight

	if skipDifficultyCheck {
		u.logger.Debugf("[reValidateBlock][%s] skipping difficulty validation for block at height %d (at or below checkpoint height %d)",
			blockData.block.Header.Hash().String(), blockData.block.Height, highestCheckpointHeight)
	} else {
		// First check that the nBits (difficulty target) is correct for this block
		expectedNBits, err := u.blockchainClient.GetNextWorkRequired(ctx, blockData.block.Header.HashPrevBlock, int64(blockData.block.Header.Timestamp))
		if err != nil {
			return errors.NewServiceError("[reValidateBlock][%s] failed to get expected work required", blockData.block.Header.Hash().String(), err)
		}

		// Compare the block's nBits with the expected nBits
		if expectedNBits != nil && blockData.block.Header.Bits != *expectedNBits {
			return errors.NewBlockInvalidError("[reValidateBlock][%s] block has incorrect difficulty bits: got %v, expected %v",
				blockData.block.Header.Hash().String(), blockData.block.Header.Bits, expectedNBits)
		}

		// Then check that the block hash meets the difficulty target
		headerValid, _, err := blockData.block.Header.HasMetTargetDifficulty()
		if !headerValid {
			return errors.NewBlockInvalidError("[reValidateBlock][%s] block does not meet target difficulty: %s", blockData.block.Header.Hash().String(), err)
		}
	}

	// get all X previous block headers, 100 is the default
	previousBlockHeaderCount := u.settings.BlockValidation.PreviousBlockHeaderCount

	blockHeaders, blockHeadersMeta, err := u.blockchainClient.GetBlockHeaders(ctx, blockData.block.Header.HashPrevBlock, previousBlockHeaderCount)
	if err != nil {
		u.logger.Errorf("[reValidateBlock][%s] failed to get block headers: %s", blockData.block.String(), err)

		return errors.NewServiceError("[reValidateBlock][%s] failed to get block headers", blockData.block.String(), err)
	}

	// Extract block header IDs from the fresh block headers metadata
	blockHeaderIDs := make([]uint32, len(blockHeadersMeta))
	for i, blockHeaderMeta := range blockHeadersMeta {
		blockHeaderIDs[i] = blockHeaderMeta.ID
	}

	// Check if parent block is invalid during revalidation
	// If parent is invalid, no point revalidating the child
	if len(blockHeadersMeta) > 0 && blockHeadersMeta[0].Invalid {
		u.logger.Warnf("[reValidateBlock][%s] parent block %s is invalid, child cannot be revalidated as valid", blockData.block.Hash().String(), blockData.block.Header.HashPrevBlock.String())
		return errors.NewBlockInvalidError("[reValidateBlock][%s] parent block is invalid", blockData.block.Hash().String())
	}

	// only get the bloom filters for the current chain
	bloomFilters, err := u.collectNecessaryBloomFilters(ctx, blockData.block, blockHeaders)
	if err != nil {
		return errors.NewServiceError("[reValidateBlock][%s] failed to collect necessary bloom filters", blockData.block.String(), err)
	}

	// validate all the subtrees in the block
	u.logger.Infof("[ReValidateBlock][%s] validating %d subtrees", blockData.block.Hash().String(), len(blockData.block.Subtrees))

	if err = u.validateBlockSubtrees(ctx, blockData.block, "", blockData.baseURL); err != nil {
		return err
	}

	u.logger.Infof("[ReValidateBlock][%s] validating %d subtrees DONE", blockData.block.Hash().String(), len(blockData.block.Subtrees))

	oldBlockIDsMap := txmap.NewSyncedMap[chainhash.Hash, []uint32]()

	if ok, err := blockData.block.Valid(ctx, u.logger, u.subtreeStore, u.utxoStore, oldBlockIDsMap, bloomFilters, blockHeaders, blockHeaderIDs, u.bloomFilterStats, u.settings); !ok {
		u.logger.Errorf("[ReValidateBlock][%s] InvalidateBlock block is not valid in background: %v", blockData.block.String(), err)

		if errors.Is(err, errors.ErrBlockInvalid) {
			if _, invalidateBlockErr := u.blockchainClient.InvalidateBlock(ctx, blockData.block.Header.Hash()); invalidateBlockErr != nil {
				u.logger.Errorf("[ReValidateBlock][%s][InvalidateBlock] failed to invalidate block: %s", blockData.block.String(), invalidateBlockErr)
			}
		}

		return err
	}

	return u.checkOldBlockIDs(ctx, oldBlockIDsMap, blockData.block)
}

// createAppendBloomFilter generates and manages bloom filters for blocks.
// It handles filter creation, pruning, and concurrent access management.
//
// Parameters:
//   - ctx: Context for the operation
//   - block: Block to create filter for
func (u *BlockValidation) createAppendBloomFilter(ctx context.Context, block *model.Block) error {
	ctx, _, deferFn := tracing.Tracer("blockvalidation").Start(ctx, "createAppendBloomFilter",
		tracing.WithParentStat(u.stats),
		tracing.WithLogMessage(u.logger, "[createAppendBloomFilter][%s] creating bloom filter", block.Hash().String()),
	)
	defer deferFn()

	if u.blockBloomFiltersBeingCreated.Exists(*block.Hash()) {
		return nil
	}

	// check whether the bloom filter for this block already exists
	if _, exists := u.recentBlocksBloomFilters.Get(*block.Hash()); exists {
		return nil
	}

	_ = u.blockBloomFiltersBeingCreated.Put(*block.Hash())
	defer func() {
		_ = u.blockBloomFiltersBeingCreated.Delete(*block.Hash())
	}()

	var err error

	// create a bloom filter for the block
	bbf := &model.BlockBloomFilter{
		CreationTime: time.Now(),
		BlockHash:    block.Hash(),
		BlockHeight:  block.Height,
	}

	bbf.Filter, err = block.NewOptimizedBloomFilter(ctx, u.logger, u.subtreeStore, u.settings.Block.GetAndValidateSubtreesConcurrency)
	if err != nil {
		return errors.NewProcessingError("[createAppendBloomFilter][%s] failed to create bloom filter", block.Hash().String(), err)
	}

	// Serialize the filter
	filterBytes, err := bbf.Serialize()
	if err != nil {
		return errors.NewProcessingError("[createAppendBloomFilter][%s] failed to serialize bloom filter", block.Hash().String(), err)
	}

	// record the bloom filter in the subtreestore
	err = u.subtreeStore.Set(ctx, block.Hash()[:], fileformat.FileTypeBloomFilter, filterBytes, options.WithDeleteAt(block.Height+u.bloomFilterRetentionSize))
	if err != nil {
		return errors.NewProcessingError("[createAppendBloomFilter][%s] failed to record bloom filter in subtree store", block.Hash().String(), err)
	}

	u.pruneBloomFilters(ctx, block, bbf)

	return nil
}

func (u *BlockValidation) pruneBloomFilters(ctx context.Context, block *model.Block, bbf *model.BlockBloomFilter) {
	// get best block height
	_, bestBlockHeaderMeta, err := u.blockchainClient.GetBestBlockHeader(ctx)
	if err != nil {
		u.logger.Errorf("[createAppendBloomFilter][%s] failed to get best block height", block.Hash().String(), err)
		return
	}

	// calculate lowest height to keep
	var lowestBlockHeightToKeep uint32
	if bestBlockHeaderMeta.Height > u.bloomFilterRetentionSize {
		lowestBlockHeightToKeep = bestBlockHeaderMeta.Height - u.bloomFilterRetentionSize
	}

	filtersToPrune := make([]chainhash.Hash, 0)

	// Delete old filters directly from map
	for hash, bf := range u.recentBlocksBloomFilters.Range() {
		if bf.BlockHeight < lowestBlockHeightToKeep {
			u.recentBlocksBloomFilters.Delete(hash)
			filtersToPrune = append(filtersToPrune, hash)
		}
	}

	// NOTE: the subtree store will handle pruning of the bloom filter files

	// Add the new bloom filter
	u.recentBlocksBloomFilters.Set(*block.Hash(), bbf)

	remainingCount := u.recentBlocksBloomFilters.Length()

	u.logger.Debugf("[pruneBloomFilters][%s] pruned %d filters, %d remaining",
		block.Hash().String(), len(filtersToPrune), remainingCount)
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
	ctx, _, deferFn := tracing.Tracer("blockvalidation").Start(ctx, "BlockValidation:updateSubtreesDAH",
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

			if err := u.subtreeStore.SetDAH(gCtx, subtreeHash[:], fileformat.FileTypeSubtree, 0); err != nil {
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
//   - peerID: P2P peer identifier for reputation tracking
//   - baseURL: Source URL for missing subtree retrieval
//
// Returns an error if subtree validation fails.
func (u *BlockValidation) validateBlockSubtrees(ctx context.Context, block *model.Block, peerID, baseURL string) error {
	if len(block.Subtrees) == 0 {
		return nil
	}

	return u.subtreeValidationClient.CheckBlockSubtrees(ctx, block, peerID, baseURL)
}

// checkOldBlockIDs verifies that referenced blocks are in the current chain.
// It prevents invalid chain reorganizations and maintains chain consistency.
//
// Parameters:
//   - ctx: Context for the operation
//   - oldBlockIDsMap: Map of transaction IDs to their parent block IDs
//   - block: Block to check IDs for
//
// Returns an error if block verification fails.
func (u *BlockValidation) checkOldBlockIDs(ctx context.Context, oldBlockIDsMap *txmap.SyncedMap[chainhash.Hash, []uint32],
	block *model.Block,
) (iterationError error) {
	ctx, _, deferFn := tracing.Tracer("blockvalidation").Start(ctx, "BlockValidation:checkOldBlockIDs",
		tracing.WithDebugLogMessage(u.logger, "[checkOldBlockIDs][%s] checking %d old block IDs", oldBlockIDsMap.Length(), block.Hash().String()),
	)

	defer deferFn()

	currentChainBlockIDs, err := u.blockchainClient.GetBlockHeaderIDs(ctx, block.Hash(), 10_000)
	if err != nil {
		return errors.NewServiceError("[Block Validation][checkOldBlockIDs][%s] failed to get block header ids", block.String(), err)
	}

	currentChainBlockIDsMap := make(map[uint32]struct{}, len(currentChainBlockIDs))
	for _, blockID := range currentChainBlockIDs {
		currentChainBlockIDsMap[blockID] = struct{}{}
	}

	currentChainLookupCache := make(map[string]bool, len(currentChainBlockIDs))

	var builder strings.Builder

	// range over the oldBlockIDsMap to get txID - oldBlockID pairs
	oldBlockIDsMap.Iterate(func(txID chainhash.Hash, blockIDs []uint32) bool {
		if len(blockIDs) == 0 {
			iterationError = errors.NewProcessingError("[Block Validation][checkOldBlockIDs][%s] blockIDs is empty for txID: %v", block.String(), txID)
			return false
		}

		// check whether the blockIDs are in the current chain we just fetched
		for _, blockID := range blockIDs {
			if _, ok := currentChainBlockIDsMap[blockID]; ok {
				// all good, continue
				return true
			}
		}

		slices.Sort(blockIDs)

		builder.Reset()

		for i, id := range blockIDs {
			if i > 0 {
				builder.WriteString(",") // Add a separator
			}

			builder.WriteString(strconv.Itoa(int(id)))
		}

		blockIDsString := builder.String()

		// check whether we already checked exactly the same blockIDs and can use a cache
		if blocksPartOfCurrentChain, ok := currentChainLookupCache[blockIDsString]; ok {
			if !blocksPartOfCurrentChain {
				iterationError = errors.NewBlockInvalidError("[Block Validation][checkOldBlockIDs][%s] block is not valid. Transaction's (%v) parent blocks (%v) are not from current chain using cache", block.String(), txID, blockIDs)
				return false
			}

			return true
		}

		// Flag to check if the old blocks are part of the current chain
		blocksPartOfCurrentChain, err := u.blockchainClient.CheckBlockIsInCurrentChain(ctx, blockIDs)
		// if err is not nil, log the error and continue iterating for the next transaction
		if err != nil {
			iterationError = errors.NewProcessingError("[Block Validation][checkOldBlockIDs][%s] failed to check if old blocks are part of the current chain", block.String(), err)
			return false
		}

		// set the cache for the blockIDs
		currentChainLookupCache[blockIDsString] = blocksPartOfCurrentChain

		// if the blocks are not part of the current chain, stop iteration, set the iterationError and return false
		if !blocksPartOfCurrentChain {
			iterationError = errors.NewBlockInvalidError("[Block Validation][checkOldBlockIDs][%s] block is not valid. Transaction's (%v) parent blocks (%v) are not from current chain", block.String(), txID, blockIDs)
			return false
		}

		return true
	})

	return
}

// Wait waits for all background tasks to complete.
// This should be called during shutdown to ensure graceful termination.
func (u *BlockValidation) Wait() {
	u.backgroundTasks.Wait()
}
