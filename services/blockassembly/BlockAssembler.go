// Package blockassembly provides functionality for assembling Bitcoin blocks in Teranode.
package blockassembly

import (
	"context"
	"database/sql"
	"encoding/binary"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/model"
	"github.com/bitcoin-sv/teranode/services/blockassembly/subtreeprocessor"
	"github.com/bitcoin-sv/teranode/services/blockchain"
	"github.com/bitcoin-sv/teranode/settings"
	"github.com/bitcoin-sv/teranode/stores/blob"
	"github.com/bitcoin-sv/teranode/stores/cleanup"
	"github.com/bitcoin-sv/teranode/stores/utxo"
	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/bitcoin-sv/teranode/util"
	"github.com/bitcoin-sv/teranode/util/retry"
	"github.com/bitcoin-sv/teranode/util/tracing"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	safeconversion "github.com/bsv-blockchain/go-safe-conversion"
	"github.com/bsv-blockchain/go-subtree"
	"github.com/ordishs/go-utils"
	"github.com/ordishs/gocore"
)

// miningCandidateResponse encapsulates all data needed for a mining candidate response.
type miningCandidateResponse struct {
	// miningCandidate contains the block template for mining
	miningCandidate *model.MiningCandidate

	// subtrees contains all transaction subtrees included in the candidate
	subtrees []*subtree.Subtree

	// err contains any error encountered during candidate creation
	err error
}

type State uint32

const (
	// pendingBlocksPollInterval is the interval at which the block assembler
	// polls for pending blocks during startup
	pendingBlocksPollInterval = 1 * time.Second
)

// create state strings for the processor
var (
	// StateStarting indicates the processor is starting up
	StateStarting State = 0

	// StateRunning indicates the processor is actively processing
	StateRunning State = 1

	// StateResetting indicates the processor is resetting
	StateResetting State = 2

	// StateGetMiningCandidate indicates the processor is getting a mining candidate
	StateGetMiningCandidate State = 3

	// StateBlockchainSubscription indicates the processor is receiving blockchain notifications
	StateBlockchainSubscription State = 4

	// StateReorging indicates the processor is reorging the blockchain
	StateReorging State = 5

	// StateMovingUp indicates the processor is moving up the blockchain
	StateMovingUp State = 6
)

var StateStrings = map[State]string{
	StateStarting:               "starting",
	StateRunning:                "running",
	StateResetting:              "resetting",
	StateGetMiningCandidate:     "getMiningCandidate",
	StateBlockchainSubscription: "blockchainSubscription",
	StateReorging:               "reorging",
	StateMovingUp:               "movingUp",
}

// BlockAssembler manages the assembly of new blocks and coordinates mining operations.
// It is the central component responsible for transaction selection, block candidate
// generation, and interaction with the mining system.
//
// BlockAssembler maintains the current blockchain state, manages transaction queues,
// and coordinates with the subtree processor to organize transactions efficiently.
// It handles blockchain reorganizations and ensures that block templates remain valid
// as the chain state changes.
type BlockAssembler struct {
	// logger provides logging functionality for the assembler
	logger ulogger.Logger

	// stats tracks operational statistics for monitoring and debugging
	stats *gocore.Stat

	// settings contains configuration parameters for block assembly
	settings *settings.Settings

	// utxoStore manages the UTXO set storage and retrieval
	utxoStore utxo.Store

	// subtreeStore manages persistent storage of transaction subtrees
	subtreeStore blob.Store

	// blockchainClient interfaces with the blockchain for network operations
	blockchainClient blockchain.ClientI

	// subtreeProcessor handles the processing and organization of transaction subtrees
	subtreeProcessor subtreeprocessor.Interface

	// miningCandidateCh coordinates requests for mining candidates
	miningCandidateCh chan chan *miningCandidateResponse

	// bestBlockHeader atomically stores the current best block header
	bestBlockHeader atomic.Pointer[model.BlockHeader]

	// bestBlockHeight atomically stores the current best block height
	bestBlockHeight atomic.Uint32

	// currentChainMap maps block hashes to their heights
	currentChainMap map[chainhash.Hash]uint32

	// currentChainMapIDs tracks block IDs in the current chain
	currentChainMapIDs map[uint32]struct{}

	// currentChainMapMu protects access to chain maps
	currentChainMapMu sync.RWMutex

	// blockchainSubscriptionCh receives blockchain notifications
	blockchainSubscriptionCh chan *blockchain.Notification

	// defaultMiningNBits stores the default mining difficulty
	defaultMiningNBits *model.NBit

	// resetCh handles reset requests for the assembler
	resetCh chan struct{}

	// resetWaitCount tracks the number of blocks to wait after reset
	resetWaitCount atomic.Int32

	// resetWaitDuration tracks the time to wait after reset
	resetWaitDuration atomic.Int32

	// currentRunningState tracks the current operational state
	currentRunningState atomic.Value

	// cleanupService manages background cleanup tasks
	cleanupService cleanup.Service

	// cleanupServiceLoaded indicates if the cleanup service has been loaded
	cleanupServiceLoaded atomic.Bool

	// unminedCleanupTicker manages periodic cleanup of old unmined transactions
	unminedCleanupTicker *time.Ticker
	// cachedCandidate stores the cached mining candidate
	cachedCandidate *CachedMiningCandidate

	// skipWaitForPendingBlocks allows tests to skip waiting for pending blocks during startup
	skipWaitForPendingBlocks bool

	// unminedTransactionsLoading indicates if unmined transactions are currently being loaded
	unminedTransactionsLoading atomic.Bool
}

type blockHeaderWithMeta struct {
	header *model.BlockHeader
	meta   *model.BlockHeaderMeta
}

type blockWithMeta struct {
	block *model.Block
	meta  *model.BlockHeaderMeta
}

// CachedMiningCandidate holds a cached mining candidate with expiration
type CachedMiningCandidate struct {
	mu             sync.RWMutex
	candidate      *model.MiningCandidate
	subtrees       []*subtree.Subtree
	lastHeight     uint32
	lastUpdate     time.Time
	generating     bool
	generationChan chan struct{}
}

// NewBlockAssembler creates and initializes a new BlockAssembler instance.
//
// Parameters:
//   - ctx: Context for cancellation
//   - logger: Logger for recording operations
//   - tSettings: Teranode settings configuration
//   - stats: Statistics tracking instance
//   - utxoStore: UTXO set storage
//   - subtreeStore: Subtree storage
//   - blockchainClient: Interface to blockchain operations
//   - newSubtreeChan: Channel for new subtree notifications
//
// Returns:
//   - *BlockAssembler: New block assembler instance
func NewBlockAssembler(ctx context.Context, logger ulogger.Logger, tSettings *settings.Settings, stats *gocore.Stat, utxoStore utxo.Store,
	subtreeStore blob.Store, blockchainClient blockchain.ClientI, newSubtreeChan chan subtreeprocessor.NewSubtreeRequest) (*BlockAssembler, error) {
	bytesLittleEndian := make([]byte, 4)

	if tSettings.ChainCfgParams == nil {
		return nil, errors.NewError("chain cfg params are nil")
	}

	binary.LittleEndian.PutUint32(bytesLittleEndian, tSettings.ChainCfgParams.PowLimitBits)

	defaultMiningBits, err := model.NewNBitFromSlice(bytesLittleEndian)
	if err != nil {
		return nil, err
	}

	subtreeProcessor, err := subtreeprocessor.NewSubtreeProcessor(ctx, logger, tSettings, subtreeStore, blockchainClient, utxoStore, newSubtreeChan)
	if err != nil {
		return nil, err
	}

	b := &BlockAssembler{
		logger:              logger,
		stats:               stats.NewStat("BlockAssembler"),
		settings:            tSettings,
		utxoStore:           utxoStore,
		subtreeStore:        subtreeStore,
		blockchainClient:    blockchainClient,
		subtreeProcessor:    subtreeProcessor,
		miningCandidateCh:   make(chan chan *miningCandidateResponse),
		currentChainMap:     make(map[chainhash.Hash]uint32, tSettings.BlockAssembly.MaxBlockReorgCatchup),
		currentChainMapIDs:  make(map[uint32]struct{}, tSettings.BlockAssembly.MaxBlockReorgCatchup),
		defaultMiningNBits:  defaultMiningBits,
		resetCh:             make(chan struct{}, 2),
		resetWaitCount:      atomic.Int32{},
		resetWaitDuration:   atomic.Int32{},
		currentRunningState: atomic.Value{},
		cachedCandidate:     &CachedMiningCandidate{},
	}

	b.setCurrentRunningState(StateStarting)

	return b, nil
}

// TxCount returns the total number of transactions in the assembler.
//
// Returns:
//   - uint64: Total transaction count
func (b *BlockAssembler) TxCount() uint64 {
	return b.subtreeProcessor.TxCount()
}

// QueueLength returns the current length of the transaction queue.
//
// Returns:
//   - int64: Current queue length
func (b *BlockAssembler) QueueLength() int64 {
	return b.subtreeProcessor.QueueLength()
}

// SubtreeCount returns the total number of subtrees.
//
// Returns:
//   - int: Total number of subtrees
func (b *BlockAssembler) SubtreeCount() int {
	return b.subtreeProcessor.SubtreeCount()
}

// startChannelListeners initializes and starts all channel listeners for block assembly operations.
// It handles blockchain notifications, mining candidate requests, and reset operations.
//
// Parameters:
//   - ctx: Context for cancellation
func (b *BlockAssembler) startChannelListeners(ctx context.Context) error {
	var (
		err       error
		readyOnce sync.Once
	)

	// start a subscription for the best block header and the FSM state
	// this will be used to reset the subtree processor when a new block is mined
	b.blockchainSubscriptionCh, err = b.blockchainClient.Subscribe(ctx, "BlockAssembler")
	if err != nil {
		return errors.NewProcessingError("[BlockAssembler] error subscribing to blockchain notifications: %v", err)
	}

	readyCh := make(chan struct{})

	go func() {
		// variables are defined here to prevent unnecessary allocations
		var (
			bestBlockchainBlockHeader *model.BlockHeader
			meta                      *model.BlockHeaderMeta
		)

		b.setCurrentRunningState(StateRunning)

		for {
			select {
			case <-ctx.Done():
				b.logger.Infof("Stopping blockassembler as ctx is done")
				close(b.miningCandidateCh)
				// Note: We don't close blockchainSubscriptionCh here because we don't own it -
				// it's created by the blockchain client's Subscribe method
				return

			case <-b.resetCh:
				b.setCurrentRunningState(StateResetting)

				bestBlockchainBlockHeader, meta, err = b.blockchainClient.GetBestBlockHeader(ctx)
				if err != nil {
					b.logger.Errorf("[BlockAssembler][Reset] error getting best block header: %v", err)
					b.setCurrentRunningState(StateRunning)

					continue
				}

				// reset the block assembly
				b.logger.Warnf("[BlockAssembler][Reset] resetting: %d: %s -> %d: %s", b.bestBlockHeight.Load(), b.bestBlockHeader.Load().Hash(), meta.Height, bestBlockchainBlockHeader.String())

				moveBackBlocksWithMeta, moveForwardBlocksWithMeta, err := b.getReorgBlocks(ctx, bestBlockchainBlockHeader, meta.Height)
				if err != nil {
					b.logger.Errorf("[BlockAssembler][Reset] error getting reorg blocks: %w", err)
					b.setCurrentRunningState(StateRunning)

					continue
				}

				if len(moveBackBlocksWithMeta) == 0 && len(moveForwardBlocksWithMeta) == 0 {
					b.logger.Errorf("[BlockAssembler][Reset] no reorg blocks found, invalid reset")
					b.setCurrentRunningState(StateRunning)

					continue
				}

				isLegacySync, err := b.blockchainClient.IsFSMCurrentState(ctx, blockchain.FSMStateLEGACYSYNCING)
				if err != nil {
					b.logger.Errorf("[BlockAssembler][Reset] error getting FSM state: %v", err)

					// if we can't get the FSM state, we assume we are not in legacy sync, which is the default, but less optimized
					isLegacySync = false
				}

				currentHeight := meta.Height

				moveBackBlocks := make([]*model.Block, len(moveBackBlocksWithMeta))
				for i, withMeta := range moveBackBlocksWithMeta {
					moveBackBlocks[i] = withMeta.block
				}

				moveForwardBlocks := make([]*model.Block, len(moveForwardBlocksWithMeta))
				for i, withMeta := range moveForwardBlocksWithMeta {
					moveForwardBlocks[i] = withMeta.block
				}

				b.logger.Warnf("[BlockAssembler][Reset] resetting to new best block header: %d", meta.Height)

				if response := b.subtreeProcessor.Reset(b.bestBlockHeader.Load(), moveBackBlocks, moveForwardBlocks, isLegacySync); response.Err != nil {
					b.logger.Errorf("[BlockAssembler][Reset] resetting error resetting subtree processor: %v", response.Err)
					// something went wrong, we need to set the best block header in the block assembly to be the
					// same as the subtree processor's best block header
					bestBlockchainBlockHeader = b.subtreeProcessor.GetCurrentBlockHeader()

					_, bestBlockchainBlockHeaderMeta, err := b.blockchainClient.GetBlockHeader(ctx, bestBlockchainBlockHeader.Hash())
					if err != nil {
						b.logger.Errorf("[BlockAssembler][Reset] error getting best block header meta: %v", err)
						b.setCurrentRunningState(StateRunning)

						continue
					}

					// set the new height based on the best block header from the subtree processor
					currentHeight = bestBlockchainBlockHeaderMeta.Height
				}

				// make sure we have processed all pending blocks before loading unmined transactions
				if err = b.waitForPendingBlocks(ctx); err != nil {
					b.logger.Errorf("[BlockAssembler][Reset] error waiting for pending blocks: %v", err)
				}

				// reload the unmined transactions
				if err = b.loadUnminedTransactions(ctx); err != nil {
					b.logger.Errorf("[BlockAssembler][Reset] error loading unmined transactions: %v", err)
				}

				b.setBestBlockHeader(bestBlockchainBlockHeader, currentHeight)

				if err = b.SetState(ctx); err != nil {
					b.logger.Errorf("[BlockAssembler][Reset] error setting state: %v", err)
				}

				prometheusBlockAssemblyCurrentBlockHeight.Set(float64(b.bestBlockHeight.Load()))

				b.logger.Warnf("[BlockAssembler] setting wait count to %d for getMiningCandidate", b.settings.BlockAssembly.ResetWaitCount)
				b.resetWaitCount.Store(b.settings.BlockAssembly.ResetWaitCount) // wait 2 blocks before starting to mine again

				resetWaitTimeInt32, err := safeconversion.Int64ToInt32(time.Now().Add(b.settings.BlockAssembly.ResetWaitDuration).Unix())
				if err != nil {
					b.logger.Errorf("[BlockAssembler][Reset] error converting reset wait time: %v", err)
				}

				b.resetWaitDuration.Store(resetWaitTimeInt32)

				b.logger.Warnf("[BlockAssembler][Reset] resetting block assembler DONE")

				// empty out the reset channel
				for len(b.resetCh) > 0 {
					<-b.resetCh
				}

				b.setCurrentRunningState(StateRunning)

			case responseCh := <-b.miningCandidateCh:
				b.setCurrentRunningState(StateGetMiningCandidate)
				// start, stat, _ := util.NewStatFromContext(context, "miningCandidateCh", channelStats)
				// wait for the reset to complete before getting a new mining candidate
				// 2 blocks && at least 20 minutes

				timeNowInt32, err := safeconversion.Int64ToInt32(time.Now().Unix())
				if err != nil {
					b.logger.Errorf("[BlockAssembler][MiningCandidate] error converting time now: %v", err)
				}

				if b.resetWaitCount.Load() > 0 || timeNowInt32 < b.resetWaitDuration.Load() {
					b.logger.Warnf("[BlockAssembler] skipping mining candidate, waiting for reset to complete: %d blocks or until %s", b.resetWaitCount.Load(), time.Unix(int64(b.resetWaitDuration.Load()), 0).String())
					utils.SafeSend(responseCh, &miningCandidateResponse{
						err: errors.NewProcessingError("waiting for reset to complete"),
					})
				} else {
					currentState, err := b.blockchainClient.GetFSMCurrentState(ctx)
					if err != nil {
						// TODO: how to handle it gracefully?
						b.logger.Errorf("[BlockAssembly] Failed to get current state: %s", err)
					}

					// if the current state is not running, we don't give a mining candidate
					if *currentState == blockchain.FSMStateRUNNING {
						miningCandidate, subtrees, err := b.getMiningCandidate()
						utils.SafeSend(responseCh, &miningCandidateResponse{
							miningCandidate: miningCandidate,
							subtrees:        subtrees,
							err:             err,
						})
					} else {
						utils.SafeSend(responseCh, &miningCandidateResponse{
							err: errors.NewProcessingError("blockchain is not in running state, current state: " + currentState.String()),
						})
					}
				}
				// stat.AddTime(start)
				b.setCurrentRunningState(StateRunning)

			case notification := <-b.blockchainSubscriptionCh:
				b.setCurrentRunningState(StateBlockchainSubscription)

				if notification.Type == model.NotificationType_Block {
					b.processNewBlockAnnouncement(ctx)
				}

				b.setCurrentRunningState(StateRunning)

				readyOnce.Do(func() {
					readyCh <- struct{}{}
				})
			} // select
		} // for
	}()

	select {
	case <-time.After(time.Second * 30):
		return errors.NewProcessingError("[BlockAssembler] timeout waiting for blockchain subscription to be ready")
	case <-readyCh:
	case <-ctx.Done():
		return nil
	}

	return nil
}

// processNewBlockAnnouncement updates the best block information.
//
// Parameters:
//   - ctx: Context for cancellation
func (b *BlockAssembler) processNewBlockAnnouncement(ctx context.Context) {
	_, _, deferFn := tracing.Tracer("blockassembly").Start(ctx, "processNewBlockAnnouncement",
		tracing.WithParentStat(b.stats),
		tracing.WithHistogram(prometheusBlockAssemblerUpdateBestBlock),
		tracing.WithLogMessage(b.logger, "[processNewBlockAnnouncement] called"),
	)
	defer func() {
		deferFn()
	}()

	bestBlockchainBlockHeader, bestBlockchainBlockHeaderMeta, err := b.blockchainClient.GetBestBlockHeader(ctx)
	if err != nil {
		b.logger.Errorf("[BlockAssembler] error getting best block header: %v", err)
		return
	}

	b.logger.Infof("[BlockAssembler][%s] new best block header: %d", bestBlockchainBlockHeader.Hash(), bestBlockchainBlockHeaderMeta.Height)

	defer b.logger.Infof("[BlockAssembler][%s] new best block header: %d DONE", bestBlockchainBlockHeader.Hash(), bestBlockchainBlockHeaderMeta.Height)

	prometheusBlockAssemblyBestBlockHeight.Set(float64(bestBlockchainBlockHeaderMeta.Height))

	switch {
	case bestBlockchainBlockHeader.Hash().IsEqual(b.bestBlockHeader.Load().Hash()):
		b.logger.Infof("[BlockAssembler][%s] best block header is the same as the current best block header: %s", bestBlockchainBlockHeader.Hash(), b.bestBlockHeader.Load().Hash())
		return
	case !bestBlockchainBlockHeader.HashPrevBlock.IsEqual(b.bestBlockHeader.Load().Hash()):
		b.logger.Infof("[BlockAssembler][%s] best block header is not the same as the previous best block header, reorging: %s", bestBlockchainBlockHeader.Hash(), b.bestBlockHeader.Load().Hash())
		b.setCurrentRunningState(StateReorging)

		err = b.handleReorg(ctx, bestBlockchainBlockHeader, bestBlockchainBlockHeaderMeta.Height)
		if err != nil {
			if errors.Is(err, errors.ErrBlockAssemblyReset) {
				// only warn about the reset
				b.logger.Warnf("[BlockAssembler][%s] error handling reorg: %v", bestBlockchainBlockHeader.Hash(), err)
			} else {
				b.logger.Errorf("[BlockAssembler][%s] error handling reorg: %v", bestBlockchainBlockHeader.Hash(), err)
			}

			return
		}
	default:
		b.logger.Infof("[BlockAssembler][%s] best block header is the same as the previous best block header, moving up: %s", bestBlockchainBlockHeader.Hash(), b.bestBlockHeader.Load().Hash())

		var block *model.Block

		if block, err = b.blockchainClient.GetBlock(ctx, bestBlockchainBlockHeader.Hash()); err != nil {
			b.logger.Errorf("[BlockAssembler][%s] error getting block from blockchain: %v", bestBlockchainBlockHeader.Hash(), err)
			return
		}

		b.setCurrentRunningState(StateMovingUp)

		if err = b.subtreeProcessor.MoveForwardBlock(block); err != nil {
			b.logger.Errorf("[BlockAssembler][%s] error moveForwardBlock in subtree processor: %v", bestBlockchainBlockHeader.Hash(), err)
			return
		}
	}

	b.setBestBlockHeader(bestBlockchainBlockHeader, bestBlockchainBlockHeaderMeta.Height)

	prometheusBlockAssemblyCurrentBlockHeight.Set(float64(b.bestBlockHeight.Load()))

	if b.resetWaitCount.Load() > 0 {
		// decrement the reset wait count, we just found and processed a block
		b.resetWaitCount.Add(-1)
		b.logger.Warnf("[BlockAssembler] decremented getMiningCandidate wait count: %d", b.resetWaitCount.Load())
	}

	if err = b.SetState(ctx); err != nil && !errors.Is(err, context.Canceled) {
		b.logger.Errorf("[BlockAssembler][%s] error setting state: %v", bestBlockchainBlockHeader.Hash(), err)
	}
}

func (b *BlockAssembler) setBestBlockHeader(bestBlockchainBlockHeader *model.BlockHeader, height uint32) {
	// setBestBlockHeader updates the internal best block header and height atomically.
	// This method is used internally to maintain the current blockchain state within
	// the block assembler. It updates both the best block header and height in a
	// thread-safe manner using atomic operations.
	//
	// The function performs the following operations:
	// - Logs the update operation with block hash and height
	// - Atomically stores the new best block header
	// - Atomically stores the new block height
	//
	// This method is critical for maintaining consistency between the block assembler's
	// view of the blockchain state and the actual blockchain tip.
	//
	// Parameters:
	//   - bestBlockchainBlockHeader: The new best block header to set
	//   - height: The height of the new best block
	b.logger.Infof("[BlockAssembler][%s] setting best block header to height %d", bestBlockchainBlockHeader.Hash(), height)

	b.bestBlockHeader.Store(bestBlockchainBlockHeader)
	b.bestBlockHeight.Store(height)

	// Invalidate cache when block height changes
	b.invalidateMiningCandidateCache()

	if b.cleanupServiceLoaded.Load() && b.cleanupService != nil && height > 0 {
		if err := b.cleanupService.UpdateBlockHeight(height); err != nil {
			b.logger.Errorf("[BlockAssembler] cleanup service error updating block height: %v", err)
		}
	}
}

// setCurrentRunningState sets the current operational state.
//
// Parameters:
//   - state: New state to set
func (b *BlockAssembler) setCurrentRunningState(state State) {
	b.currentRunningState.Store(state)
	prometheusBlockAssemblerCurrentState.Set(float64(state))
}

// GetCurrentRunningState returns the current operational state.
//
// Returns:
//   - string: Current state description
func (b *BlockAssembler) GetCurrentRunningState() State {
	return b.currentRunningState.Load().(State)
}

// Start initializes and begins the block assembler operations.
//
// Parameters:
//   - ctx: Context for cancellation
//
// Returns:
//   - error: Any error encountered during startup
func (b *BlockAssembler) Start(ctx context.Context) (err error) {
	if err = b.initState(ctx); err != nil {
		return errors.NewProcessingError("[BlockAssembler] failed to initialize state: %v", err)
	}

	// Wait for any pending blocks to be processed before loading unmined transactions
	if !b.skipWaitForPendingBlocks {
		if err = b.waitForPendingBlocks(ctx); err != nil {
			// we cannot start block assembly if we have not processed all pending blocks
			return errors.NewProcessingError("[BlockAssembler] failed to wait for pending blocks: %v", err)
		}
	}

	// Load unmined transactions (this includes cleanup of old unmined transactions first)
	if err = b.loadUnminedTransactions(ctx); err != nil {
		// we cannot start block assembly if we have not loaded unmined transactions successfully
		return errors.NewStorageError("[BlockAssembler] failed to load un-mined transactions: %v", err)
	}

	if err = b.startChannelListeners(ctx); err != nil {
		return errors.NewProcessingError("[BlockAssembler] failed to start channel listeners: %v", err)
	}

	// Check if the UTXO store supports cleanup operations
	if !b.settings.UtxoStore.DisableDAHCleaner {
		if cleanupServiceProvider, ok := b.utxoStore.(cleanup.CleanupServiceProvider); ok {
			b.logger.Infof("[BlockAssembler] initialising cleanup service")

			b.cleanupService, err = cleanupServiceProvider.GetCleanupService()
			if err != nil {
				return err
			}

			if b.cleanupService != nil {
				b.logger.Infof("[BlockAssembler] starting cleanup service")
				b.cleanupService.Start(ctx)
			}

			b.cleanupServiceLoaded.Store(true)
		}
	}

	prometheusBlockAssemblyCurrentBlockHeight.Set(float64(b.bestBlockHeight.Load()))

	// Start background cleanup of unmined transactions every 10 minutes
	// This is started after initial cleanup and loading is complete
	b.startUnminedTransactionCleanup(ctx)

	return nil
}

func (b *BlockAssembler) initState(ctx context.Context) error {
	bestBlockHeader, bestBlockHeight, err := b.GetState(ctx)
	if err != nil {
		if errors.Is(err, errors.ErrNotFound) || strings.Contains(err.Error(), sql.ErrNoRows.Error()) {
			b.logger.Warnf("[BlockAssembler] no state found in blockchain db")
		} else {
			b.logger.Errorf("[BlockAssembler] error getting state from blockchain db: %v", err)
		}
	} else {
		b.logger.Infof("[BlockAssembler] setting best block header from state: %d: %s", bestBlockHeight, bestBlockHeader.Hash())
		b.setBestBlockHeader(bestBlockHeader, bestBlockHeight)
		b.subtreeProcessor.InitCurrentBlockHeader(bestBlockHeader)
	}

	// we did not get any state back from the blockchain db, so we get the current best block header
	if b.bestBlockHeader.Load() == nil || b.bestBlockHeight.Load() == 0 {
		header, meta, err := b.blockchainClient.GetBestBlockHeader(ctx)
		if err != nil {
			// we must return an error here since we cannot continue without a best block header
			return errors.NewProcessingError("[BlockAssembler] error getting best block header: %v", err)
		} else {
			b.logger.Infof("[BlockAssembler] setting best block header from GetBestBlockHeader: %s", b.bestBlockHeader.Load().Hash())
			b.setBestBlockHeader(header, meta.Height)
			b.subtreeProcessor.InitCurrentBlockHeader(header)
		}
	}

	if err = b.SetState(ctx); err != nil {
		b.logger.Errorf("[BlockAssembler] error setting state: %v", err)
	}

	return nil
}

// GetState retrieves the current state of the block assembler from the blockchain.
//
// Parameters:
//   - ctx: Context for cancellation
//
// Returns:
//   - *model.BlockHeader: Current best block header
//   - uint32: Current block height
//   - error: Any error encountered during state retrieval
func (b *BlockAssembler) GetState(ctx context.Context) (*model.BlockHeader, uint32, error) {
	state, err := b.blockchainClient.GetState(ctx, "BlockAssembler")
	if err != nil {
		return nil, 0, err
	}

	bestBlockHeight := binary.LittleEndian.Uint32(state[:4])

	bestBlockHeader, err := model.NewBlockHeaderFromBytes(state[4:])
	if err != nil {
		return nil, 0, err
	}

	return bestBlockHeader, bestBlockHeight, nil
}

// SetState persists the current state of the block assembler to the blockchain.
//
// Parameters:
//   - ctx: Context for cancellation
//
// Returns:
//   - error: Any error encountered during state persistence
func (b *BlockAssembler) SetState(ctx context.Context) error {
	blockHeader := b.bestBlockHeader.Load()
	if blockHeader == nil {
		return errors.NewError("bestBlockHeader is nil")
	}

	blockHeaderBytes := blockHeader.Bytes()

	blockHeight := b.bestBlockHeight.Load()

	state := make([]byte, 4+len(blockHeaderBytes))
	binary.LittleEndian.PutUint32(state[:4], blockHeight)
	state = append(state[:4], blockHeaderBytes...)

	b.logger.Debugf("[BlockAssembler] setting state: %d: %s", blockHeight, blockHeader.Hash())

	return b.blockchainClient.SetState(ctx, "BlockAssembler", state)
}

// CurrentBlock returns the current best block header and height.
//
// Returns:
//   - *model.BlockHeader: Current best block header
//   - uint32: Current block height
func (b *BlockAssembler) CurrentBlock() (*model.BlockHeader, uint32) {
	return b.bestBlockHeader.Load(), b.bestBlockHeight.Load()
}

// AddTx adds a transaction to the block assembler.
//
// Parameters:
//   - node: Transaction node to add
func (b *BlockAssembler) AddTx(node subtree.SubtreeNode, txInpoints subtree.TxInpoints) {
	b.subtreeProcessor.Add(node, txInpoints)
}

// RemoveTx removes a transaction from the block assembler.
//
// Parameters:
//   - hash: Hash of the transaction to remove
//
// Returns:
//   - error: Any error encountered during removal
func (b *BlockAssembler) RemoveTx(hash chainhash.Hash) error {
	return b.subtreeProcessor.Remove(hash)
}

// Reset triggers a reset of the block assembler state.
// This operation runs asynchronously to prevent blocking.
func (b *BlockAssembler) Reset() {
	// run in a go routine to prevent blocking
	go func() {
		b.resetCh <- struct{}{}
	}()
}

// GetMiningCandidate retrieves a candidate block for mining.
//
// Parameters:
//   - ctx: Context for cancellation
//
// Returns:
//   - *model.MiningCandidate: Mining candidate block
//   - []*util.Subtree: Associated subtrees
//   - error: Any error encountered during retrieval
func (b *BlockAssembler) GetMiningCandidate(ctx context.Context) (*model.MiningCandidate, []*subtree.Subtree, error) {
	// make sure we call this on the select, so we don't get a candidate when we found a new block
	ctx, _, deferFn := tracing.Tracer("blockassembly").Start(ctx, "GetMiningCandidate",
		tracing.WithParentStat(b.stats),
		tracing.WithHistogram(prometheusBlockAssemblyGetMiningCandidateDuration),
		tracing.WithDebugLogMessage(b.logger, "[GetMiningCandidate] called"),
	)
	defer deferFn()

	// Try to get from cache first
	b.cachedCandidate.mu.RLock()

	currentHeight := b.bestBlockHeight.Load()

	// Return cached if still valid (same height and within timeout)
	if !b.settings.ChainCfgParams.ReduceMinDifficulty && b.cachedCandidate.candidate != nil &&
		b.cachedCandidate.lastHeight == currentHeight &&
		time.Since(b.cachedCandidate.lastUpdate) < b.settings.BlockAssembly.MiningCandidateCacheTimeout {
		candidate := b.cachedCandidate.candidate
		subtrees := b.cachedCandidate.subtrees
		b.cachedCandidate.mu.RUnlock()

		// Record cache hit metrics
		prometheusBlockAssemblerCacheHits.Inc()

		b.logger.Debugf("[BlockAssembler] Returning cached mining candidate %s", candidate.Id)

		return candidate, subtrees, nil
	}

	// Check if already generating
	if b.cachedCandidate.generating {
		ch := b.cachedCandidate.generationChan
		b.cachedCandidate.mu.RUnlock()

		// Wait for ongoing generation
		<-ch

		return b.GetMiningCandidate(ctx)
	}

	// Mark as generating - upgrade to write lock
	b.cachedCandidate.mu.RUnlock()
	b.cachedCandidate.mu.Lock()

	// Double check generating flag in case another goroutine set it while we upgraded locks
	if b.cachedCandidate.generating {
		ch := b.cachedCandidate.generationChan
		b.cachedCandidate.mu.Unlock()

		// Wait for ongoing generation
		<-ch

		return b.GetMiningCandidate(ctx)
	}

	b.cachedCandidate.generating = true
	b.cachedCandidate.generationChan = make(chan struct{})
	b.cachedCandidate.mu.Unlock()

	// Generate new candidate
	responseCh := make(chan *miningCandidateResponse)

	select {
	case <-ctx.Done():
		// context cancelled, do not send
	case <-time.After(1 * time.Second):
		return nil, nil, errors.NewServiceError("timeout sending mining candidate request")
	case b.miningCandidateCh <- responseCh:
		// sent successfully
	}

	// wait for 10 seconds for the response
	var candidate *model.MiningCandidate

	var subtrees []*subtree.Subtree

	var err error

	select {
	case <-ctx.Done():
		// context cancelled, do not send
	case <-time.After(10 * time.Second):
		// make sure to close the channel, otherwise the for select will hang, because no one is reading from it
		close(responseCh)

		err = errors.NewServiceError("timeout getting mining candidate")
	case response := <-responseCh:
		candidate = response.miningCandidate
		subtrees = response.subtrees
		err = response.err
	}

	// Update cache
	b.cachedCandidate.mu.Lock()
	if err == nil {
		b.cachedCandidate.candidate = candidate
		b.cachedCandidate.subtrees = subtrees
		b.cachedCandidate.lastHeight = currentHeight
		b.cachedCandidate.lastUpdate = time.Now()

		// Record cache miss metrics
		prometheusBlockAssemblerCacheMisses.Inc()
	}

	b.cachedCandidate.generating = false

	// Only close the channel if it's not nil
	if b.cachedCandidate.generationChan != nil {
		close(b.cachedCandidate.generationChan)
		b.cachedCandidate.generationChan = nil
	}
	b.cachedCandidate.mu.Unlock()

	return candidate, subtrees, err
}

// getMiningCandidate creates a new mining candidate from the current block state.
// This is an internal method called by GetMiningCandidate.
//
// Returns:
//   - *model.MiningCandidate: Created mining candidate
//   - []*util.Subtree: Associated subtrees
//   - error: Any error encountered during candidate creation
func (b *BlockAssembler) getMiningCandidate() (*model.MiningCandidate, []*subtree.Subtree, error) {
	prometheusBlockAssemblerGetMiningCandidate.Inc()

	if b.bestBlockHeader.Load() == nil {
		return nil, nil, errors.NewError("best block header is not available")
	}

	b.logger.Debugf("[BlockAssembler] getting mining candidate for header: %s", b.bestBlockHeader.Load().Hash())

	// Get the list of completed containers for the current chaintip and height...
	subtrees := b.subtreeProcessor.GetCompletedSubtreesForMiningCandidate()

	blockMaxSizeUint64, err := safeconversion.IntToUint64(b.settings.Policy.BlockMaxSize)
	if err != nil {
		return nil, nil, errors.NewProcessingError("error converting block max size", err)
	}

	if b.settings.Policy.BlockMaxSize > 0 && len(subtrees) > 0 && blockMaxSizeUint64 < subtrees[0].SizeInBytes {
		b.logger.Warnf("[BlockAssembler] max block size is less than the size of the subtree: %d < %d", b.settings.Policy.BlockMaxSize, subtrees[0].SizeInBytes)

		return nil, nil, errors.NewProcessingError("max block size is less than the size of the subtree")
	}

	var coinbaseValue uint64

	currentHeight := b.bestBlockHeight.Load() + 1

	// Log initial state for debugging
	b.logger.Debugf("Starting coinbase calculation for height %d", currentHeight)

	// Get the hash of the last subtree in the list...
	// We do this by using the same subtree processor logic to get the top tree hash.
	id := &chainhash.Hash{}

	var txCount uint32

	var sizeWithoutCoinbase uint64

	var subtreesToInclude []*subtree.Subtree

	var subtreeBytesToInclude [][]byte

	var coinbaseMerkleProofBytes [][]byte

	// set the size without the coinbase to the size of the block header
	// This should probably have the size of the varint for the tx count as well
	// but bitcoin-sv node doesn't do that.
	sizeWithoutCoinbase = 80

	if len(subtrees) == 0 {
		txCount = 1

		b.logger.Debugf("No subtrees to include, creating empty block with coinbase only")
	} else {
		currentBlockSize := uint64(0)
		totalFees := uint64(0)
		subtreeCount := 0

		topTree, err := subtree.NewIncompleteTreeByLeafCount(len(subtrees))
		if err != nil {
			return nil, nil, errors.NewProcessingError("error creating top tree", err)
		}

		b.logger.Debugf("Processing %d subtrees for inclusion", len(subtrees))

		for _, subtree := range subtrees {
			if b.settings.Policy.BlockMaxSize == 0 || currentBlockSize+subtree.SizeInBytes <= blockMaxSizeUint64 {
				subtreesToInclude = append(subtreesToInclude, subtree)
				subtreeBytesToInclude = append(subtreeBytesToInclude, subtree.RootHash().CloneBytes())
				coinbaseValue += subtree.Fees
				totalFees += subtree.Fees
				subtreeCount++
				currentBlockSize += subtree.SizeInBytes
				_ = topTree.AddNode(*subtree.RootHash(), subtree.Fees, subtree.SizeInBytes)

				b.logger.Debugf("Included subtree %d: fees=%d, size=%d, total_fees=%d", subtreeCount, subtree.Fees, subtree.SizeInBytes, totalFees)

				lenSubtreeNodesUint32, err := safeconversion.IntToUint32(len(subtree.Nodes))
				if err != nil {
					return nil, nil, errors.NewProcessingError("error converting subtree nodes length", err)
				}

				txCount += lenSubtreeNodesUint32

				sizeWithoutCoinbase += subtree.SizeInBytes
			} else {
				break
			}
		}

		b.logger.Debugf("Fee accumulation complete: included %d subtrees, total_fees=%d satoshis (%.8f BSV)", subtreeCount, totalFees, float64(totalFees)/1e8)

		if len(subtreesToInclude) > 0 {
			coinbaseMerkleProof, err := subtree.GetMerkleProofForCoinbase(subtreesToInclude)
			if err != nil {
				return nil, nil, errors.NewProcessingError("error getting merkle proof for coinbase", err)
			}

			for _, hash := range coinbaseMerkleProof {
				coinbaseMerkleProofBytes = append(coinbaseMerkleProofBytes, hash.CloneBytes())
			}
		}

		id = topTree.RootHash()
	}

	timeNow := time.Now().Unix()
	b.logger.Debugf("Current time: %d", timeNow)
	timeNowUint32, err := safeconversion.Int64ToUint32(timeNow)
	if err != nil {
		return nil, nil, errors.NewProcessingError("error converting time now", err)
	}

	timeBytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(timeBytes, timeNowUint32)

	nBits, err := b.getNextNbits(timeNow)
	if err != nil {
		return nil, nil, err
	}

	// Log coinbase value before adding subsidy
	feesOnly := coinbaseValue
	b.logger.Debugf("Before adding subsidy: coinbase_value=%d satoshis (%.8f BSV) from transaction fees", feesOnly, float64(feesOnly)/1e8)

	// Critical subsidy calculation - add comprehensive logging
	subsidyHeight := b.bestBlockHeight.Load() + 1
	b.logger.Debugf("Calculating block subsidy for height %d", subsidyHeight)

	// Validate ChainCfgParams before using
	if b.settings.ChainCfgParams == nil {
		b.logger.Errorf("CRITICAL: ChainCfgParams is nil! This will cause subsidy calculation to fail")
		return nil, nil, errors.NewProcessingError("ChainCfgParams is nil")
	}

	blockSubsidy := util.GetBlockSubsidyForHeight(subsidyHeight, b.settings.ChainCfgParams)
	b.logger.Debugf("Block subsidy calculated: %d satoshis (%.8f BSV) for height %d", blockSubsidy, float64(blockSubsidy)/1e8, subsidyHeight)

	coinbaseValue += blockSubsidy

	// Log final coinbase value
	b.logger.Debugf("Final coinbase value: fees=%d + subsidy=%d = total=%d satoshis (%.8f BSV)", feesOnly, blockSubsidy, coinbaseValue, float64(coinbaseValue)/1e8)

	// Additional validation - check for suspicious values
	if blockSubsidy == 0 && subsidyHeight < 6930000 {
		b.logger.Errorf("SUSPICIOUS: Block subsidy is 0 for height %d (should be non-zero until height 6930000)", subsidyHeight)
	}

	if coinbaseValue < blockSubsidy {
		b.logger.Errorf("CRITICAL BUG: Final coinbase value %d is less than subsidy %d - this indicates an overflow or logic error", coinbaseValue, blockSubsidy)
	}

	previousHash := b.bestBlockHeader.Load().Hash().CloneBytes()

	lenSubtreesToIncludeUint32, err := safeconversion.IntToUint32(len(subtreesToInclude))
	if err != nil {
		return nil, nil, errors.NewProcessingError("error converting subtree count", err)
	}

	// remove the coinbase tx from the tx count
	if txCount > 0 {
		txCount--
	}

	miningCandidate := &model.MiningCandidate{
		// create a job ID from the top tree hash and the previous block hash, to prevent empty block job id collisions
		Id:                  chainhash.HashB(append(append(id[:], previousHash...), timeBytes...)),
		PreviousHash:        previousHash,
		CoinbaseValue:       coinbaseValue,
		Version:             0x20000000,
		NBits:               nBits.CloneBytes(),
		Height:              b.bestBlockHeight.Load() + 1,
		Time:                timeNowUint32,
		MerkleProof:         coinbaseMerkleProofBytes,
		NumTxs:              txCount,
		SizeWithoutCoinbase: sizeWithoutCoinbase,
		SubtreeCount:        lenSubtreesToIncludeUint32,
		SubtreeHashes:       subtreeBytesToInclude,
	}

	return miningCandidate, subtreesToInclude, nil
}

// handleReorg handles blockchain reorganization.
//
// Parameters:
//   - ctx: Context for cancellation
//   - header: New block header
//   - height: New block height
//
// Returns:
//   - error: Any error encountered during reorganization
func (b *BlockAssembler) handleReorg(ctx context.Context, header *model.BlockHeader, height uint32) error {
	startTime := time.Now()

	prometheusBlockAssemblerReorg.Inc()

	moveBackBlocksWithMeta, moveForwardBlocksWithMeta, err := b.getReorgBlocks(ctx, header, height)
	if err != nil {
		return errors.NewProcessingError("error getting reorg blocks", err)
	}

	if (len(moveBackBlocksWithMeta) > 5 || len(moveForwardBlocksWithMeta) > 5) && b.bestBlockHeight.Load() > 1000 {
		// large reorg, log it and Reset the block assembler
		b.Reset()

		return errors.NewBlockAssemblyResetError("large reorg, moveBackBlocks: %d, moveForwardBlocks: %d, resetting block assembly", len(moveBackBlocksWithMeta), len(moveForwardBlocksWithMeta))
	}

	hasInvalidBlock := false

	moveBackBlocks := make([]*model.Block, len(moveBackBlocksWithMeta))
	for i, moveBackBlockWithMeta := range moveBackBlocksWithMeta {
		if moveBackBlockWithMeta.meta.Invalid {
			hasInvalidBlock = true
		}

		moveBackBlocks[i] = moveBackBlockWithMeta.block
	}

	moveForwardBlocks := make([]*model.Block, len(moveForwardBlocksWithMeta))
	for i, moveForwardBlockWithMeta := range moveForwardBlocksWithMeta {
		moveForwardBlocks[i] = moveForwardBlockWithMeta.block
	}

	reset := hasInvalidBlock

	if !hasInvalidBlock {
		// now do the reorg in the subtree processor
		if err = b.subtreeProcessor.Reorg(moveBackBlocks, moveForwardBlocks); err != nil {
			b.logger.Warnf("[BlockAssembler] error doing reorg, will reset instead: %v", err)
			// fallback to full reset
			reset = true
		}
	}

	if reset {
		// we have an invalid block in the reorg or reorg failed, we need to reset the block assembly and load the unmined transactions again
		b.logger.Warnf("[BlockAssembler] reorg contains invalid block, resetting block assembly, moveBackBlocks: %d, moveForwardBlocks: %d", len(moveBackBlocks), len(moveForwardBlocks))

		if resp := b.subtreeProcessor.Reset(nil, moveBackBlocks, moveForwardBlocks, false); resp.Err != nil {
			return errors.NewProcessingError("error resetting block assembly after reorg with invalid block", resp.Err)
		}

		// wait for any pending blocks to be processed before loading unmined transactions, this will include invalidated blocks
		if err = b.waitForPendingBlocks(ctx); err != nil {
			// we cannot continue if we have not processed all pending blocks
			return errors.NewProcessingError("[BlockAssembler] failed to wait for pending blocks", err)
		}

		// load unmined transactions again, but only after all blocks have been mined_set properly
		if err = b.loadUnminedTransactions(ctx); err != nil {
			// we cannot continue if we have not loaded unmined transactions successfully
			return errors.NewStorageError("[BlockAssembler] failed to load un-mined transactions", err)
		}
	}

	prometheusBlockAssemblerReorgDuration.Observe(float64(time.Since(startTime).Microseconds()) / 1_000_000)

	return nil
}

// getReorgBlocks retrieves blocks involved in reorganization.
//
// Parameters:
//   - ctx: Context for cancellation
//   - header: Target block header
//   - height: Target block height
//
// Returns:
//   - []*model.Block: Blocks to move down
//   - []*model.Block: Blocks to move up
//   - error: Any error encountered
func (b *BlockAssembler) getReorgBlocks(ctx context.Context, header *model.BlockHeader, height uint32) ([]blockWithMeta, []blockWithMeta, error) {
	_, _, deferFn := tracing.Tracer("blockassembly").Start(ctx, "getReorgBlocks",
		tracing.WithParentStat(b.stats),
		tracing.WithHistogram(prometheusBlockAssemblerGetReorgBlocksDuration),
		tracing.WithLogMessage(b.logger, "[getReorgBlocks] called"),
	)
	defer deferFn()

	moveBackBlockHeadersWithMeta, moveForwardBlockHeadersWithMeta, err := b.getReorgBlockHeaders(ctx, header, height)
	if err != nil {
		return nil, nil, errors.NewServiceError("error getting reorg block headers", err)
	}

	// moveForwardBlocks will contain all blocks we need to move up to get to the new tip from the common ancestor
	moveForwardBlocks := make([]blockWithMeta, 0, len(moveForwardBlockHeadersWithMeta))

	// moveBackBlocks will contain all blocks we need to move down to get to the common ancestor
	moveBackBlocks := make([]blockWithMeta, 0, len(moveBackBlockHeadersWithMeta))

	var block *model.Block
	for _, headerWithMeta := range moveForwardBlockHeadersWithMeta {
		block, err = b.blockchainClient.GetBlock(ctx, headerWithMeta.header.Hash())
		if err != nil {
			return nil, nil, errors.NewServiceError("error getting block", err)
		}

		moveForwardBlocks = append(moveForwardBlocks, blockWithMeta{
			block: block,
			meta:  headerWithMeta.meta,
		})
	}

	for _, headerWithMeta := range moveBackBlockHeadersWithMeta {
		block, err = b.blockchainClient.GetBlock(ctx, headerWithMeta.header.Hash())
		if err != nil {
			return nil, nil, errors.NewServiceError("error getting block", err)
		}

		moveBackBlocks = append(moveBackBlocks, blockWithMeta{
			block: block,
			meta:  headerWithMeta.meta,
		})
	}

	return moveBackBlocks, moveForwardBlocks, nil
}

// getReorgBlockHeaders returns the block headers that need to be moved down and up to get to the new tip
// it is based on a common ancestor between the current chain and the new chain
// TODO optimize this function
func (b *BlockAssembler) getReorgBlockHeaders(ctx context.Context, header *model.BlockHeader, height uint32) ([]blockHeaderWithMeta, []blockHeaderWithMeta, error) {
	if header == nil {
		return nil, nil, errors.NewError("header is nil")
	}

	// We want to use block locators to find the common ancestor
	// This is because block locators are more efficient than fetching blocks
	// and we want to avoid fetching blocks that we don't need

	// We will get a block locator for requested header
	// and another block locator for on the best block header

	// Important: in order to compare 2 block locators, it is critical that
	// the starting height of both locators is the same
	// otherwise the common ancestor might be the genesis block
	// because none of the mentioned block hashes in the block locators
	// are necessarily going to be on the same height

	bestBlockHash := b.bestBlockHeader.Load().Hash()
	bestBlockHeight := b.bestBlockHeight.Load()
	startingHeight := height

	if height > bestBlockHeight {
		startingHeight = bestBlockHeight
	}

	// Get block locator for current chain
	currentChainLocator, err := b.blockchainClient.GetBlockLocator(ctx, bestBlockHash, startingHeight)
	if err != nil {
		return nil, nil, errors.NewServiceError("error getting block locator for current chain", err)
	}

	// Get block locator for the new best block
	newChainLocator, err := b.blockchainClient.GetBlockLocator(ctx, header.Hash(), startingHeight)
	if err != nil {
		return nil, nil, errors.NewServiceError("error getting block locator for new chain", err)
	}

	// Find common ancestor using locators
	var (
		commonAncestor     *model.BlockHeader
		commonAncestorMeta *model.BlockHeaderMeta
	)

	for _, currentHash := range currentChainLocator {
		for _, newHash := range newChainLocator {
			if currentHash.IsEqual(newHash) {
				commonAncestor, commonAncestorMeta, err = b.blockchainClient.GetBlockHeader(ctx, currentHash)
				if err != nil {
					return nil, nil, errors.NewServiceError("error getting common ancestor header", err)
				}

				goto FoundAncestor
			}
		}
	}

FoundAncestor:
	if commonAncestor == nil || commonAncestorMeta == nil {
		return nil, nil, errors.NewProcessingError("common ancestor not found, reorg not possible")
	}

	// Get headers from current tip down to common ancestor
	headerCount := bestBlockHeight - commonAncestorMeta.Height + 1

	moveBackBlockHeaders, moveBackBlockHeaderMetas, err := b.blockchainClient.GetBlockHeaders(ctx, b.bestBlockHeader.Load().Hash(), uint64(headerCount))
	if err != nil {
		return nil, nil, errors.NewServiceError("error getting current chain headers", err)
	}

	moveBackBlockHeadersWithMeta := make([]blockHeaderWithMeta, 0, len(moveBackBlockHeaders))
	for i, moveBackBlockHeader := range moveBackBlockHeaders {
		moveBackBlockHeadersWithMeta = append(moveBackBlockHeadersWithMeta, blockHeaderWithMeta{
			header: moveBackBlockHeader,
			meta:   moveBackBlockHeaderMetas[i],
		})
	}

	// Handle empty moveBackBlockHeaders or when length is 1 (only common ancestor)
	var filteredMoveBack []blockHeaderWithMeta
	if len(moveBackBlockHeaders) > 1 {
		filteredMoveBack = moveBackBlockHeadersWithMeta[:len(moveBackBlockHeadersWithMeta)-1]
	}

	// Get headers from new tip down to common ancestor
	moveForwardBlockHeaders, moveForwardBlockHeaderMetas, err := b.blockchainClient.GetBlockHeaders(ctx, header.Hash(), uint64(height-commonAncestorMeta.Height))
	if err != nil {
		return nil, nil, errors.NewServiceError("error getting new chain headers", err)
	}

	moveForwardBlockHeadersWithMeta := make([]blockHeaderWithMeta, 0, len(moveForwardBlockHeaders))
	for i, moveForwardBlockHeader := range moveForwardBlockHeaders {
		moveForwardBlockHeadersWithMeta = append(moveForwardBlockHeadersWithMeta, blockHeaderWithMeta{
			header: moveForwardBlockHeader,
			meta:   moveForwardBlockHeaderMetas[i],
		})
	}

	// reverse moveForwardBlocks slice
	for i := len(moveForwardBlockHeadersWithMeta)/2 - 1; i >= 0; i-- {
		opp := len(moveForwardBlockHeadersWithMeta) - 1 - i
		moveForwardBlockHeadersWithMeta[i], moveForwardBlockHeadersWithMeta[opp] = moveForwardBlockHeadersWithMeta[opp], moveForwardBlockHeadersWithMeta[i]
	}

	maxGetReorgHashes := b.settings.BlockAssembly.MaxGetReorgHashes
	if len(filteredMoveBack) > maxGetReorgHashes {
		b.logger.Errorf("reorg is too big, max block reorg: current hash: %s, current height: %d, new hash: %s, new height: %d, common ancestor hash: %s, common ancestor height: %d, move down block count: %d, move up block count: %d, current locator: %v, new block locator: %v", b.bestBlockHeader.Load().Hash(), b.bestBlockHeight.Load(), header.Hash(), height, commonAncestor.Hash(), commonAncestorMeta.Height, len(filteredMoveBack), len(moveForwardBlockHeaders), currentChainLocator, newChainLocator)
		return nil, nil, errors.NewProcessingError("reorg is too big, max block reorg: %d", maxGetReorgHashes)
	}

	return filteredMoveBack, moveForwardBlockHeadersWithMeta, nil
}

// getNextNbits retrieves the next required work difficulty target.
//
// Returns:
//   - *model.NBit: Next difficulty target
//   - error: Any error encountered during retrieval
func (b *BlockAssembler) getNextNbits(nextBlockTime int64) (*model.NBit, error) {
	nbit, err := b.blockchainClient.GetNextWorkRequired(context.Background(), b.bestBlockHeader.Load().Hash(), nextBlockTime)
	if err != nil {
		return nil, errors.NewProcessingError("error getting next work required", err)
	}

	return nbit, nil
}

// loadUnminedTransactions loads previously unmined transactions from the UTXO store.
// This method is called during block assembler initialization to restore the state
// of transactions that were previously processed but not yet included in a mined block.
// It helps maintain continuity across service restarts by reloading pending transactions.
//
// The function performs the following operations:
// - Checks if a UTXO store is available (logs warning and returns if not)
// - Creates an iterator for unmined transactions from the UTXO store
// - Processes each unmined transaction and adds it to the subtree processor
// - Handles any errors during transaction loading and processing
//
// This is an important initialization step that ensures the block assembler can
// continue processing transactions that were in progress before a restart or
// service interruption.
//
// Parameters:
//   - ctx: Context for the loading operation, allowing for cancellation
//
// Returns:
//   - error: Any error encountered during transaction loading or processing
func (b *BlockAssembler) loadUnminedTransactions(ctx context.Context) (err error) {
	// Set flag to indicate unmined transactions are being loaded
	b.unminedTransactionsLoading.Store(true)
	defer func() {
		// Clear flag when loading is complete
		b.unminedTransactionsLoading.Store(false)
		b.logger.Infof("[loadUnminedTransactions] unmined transaction loading completed")
	}()

	_, _, deferFn := tracing.Tracer("blockassembly").Start(ctx, "loadUnminedTransactions",
		tracing.WithParentStat(b.stats),
		tracing.WithLogMessage(b.logger, "[loadUnminedTransactions] starting cleanup of old unmined transactions before loading"),
	)
	defer deferFn(err)

	if b.utxoStore == nil {
		return errors.NewServiceError("[BlockAssembler] no utxostore")
	}

	currentBlockHeight := b.bestBlockHeight.Load()

	if currentBlockHeight > 0 {
		cleanupCount, err := utxo.PreserveParentsOfOldUnminedTransactions(ctx, b.utxoStore, currentBlockHeight, b.settings, b.logger)

		switch {
		case err != nil:
			b.logger.Errorf("[BlockAssembler] failed to cleanup old unmined transactions: %v", err)
		case cleanupCount > 0:
			b.logger.Infof("[BlockAssembler] cleanup completed - removed %d old unmined transactions", cleanupCount)
		default:
			b.logger.Infof("[BlockAssembler] cleanup completed - no old unmined transactions found")
		}
	} else {
		b.logger.Infof("[BlockAssembler] skipping cleanup - block height is 0")
	}

	b.logger.Infof("[BlockAssembler] now loading remaining unmined transactions")

	it, err := b.utxoStore.GetUnminedTxIterator()
	if err != nil {
		return errors.NewProcessingError("error getting unmined tx iterator", err)
	}

	unminedTransactions := make([]*utxo.UnminedTransaction, 0, 1024*1024) // preallocate a large slice to avoid reallocations
	lockedTransactions := make([]chainhash.Hash, 0, 1024)

	for {
		unminedTransaction, err := it.Next(ctx)
		if err != nil {
			return errors.NewProcessingError("error getting unmined transaction", err)
		}

		if unminedTransaction == nil {
			break
		}

		unminedTransactions = append(unminedTransactions, unminedTransaction)

		if unminedTransaction.Locked {
			// if the transaction is locked, we need to add it to the locked transactions list, so we can unlock them
			lockedTransactions = append(lockedTransactions, *unminedTransaction.Hash)
		}
	}

	// order the transactions by createdAt
	sort.Slice(unminedTransactions, func(i, j int) bool {
		// sort by createdAt, oldest first
		return unminedTransactions[i].CreatedAt < unminedTransactions[j].CreatedAt
	})

	for _, unminedTransaction := range unminedTransactions {
		subtreeNode := subtree.SubtreeNode{
			Hash:        *unminedTransaction.Hash,
			Fee:         unminedTransaction.Fee,
			SizeInBytes: unminedTransaction.Size,
		}

		if err = b.subtreeProcessor.AddDirectly(subtreeNode, unminedTransaction.TxInpoints, true); err != nil {
			return errors.NewProcessingError("error adding unmined transaction to subtree processor", err)
		}
	}

	// unlock any locked transactions
	if len(lockedTransactions) > 0 {
		if err = b.utxoStore.SetLocked(ctx, lockedTransactions, false); err != nil {
			return errors.NewProcessingError("[BlockAssembler] failed to unlock %d unmined transactions: %v", len(lockedTransactions), err)
		} else {
			b.logger.Infof("[BlockAssembler] unlocked %d previously locked unmined transactions", len(lockedTransactions))
		}
	}

	return nil
}

// startUnminedTransactionCleanup starts a background goroutine that periodically cleans up old unmined transactions.
// The cleanup runs every 10 minutes and uses the store-agnostic cleanup function.
func (b *BlockAssembler) startUnminedTransactionCleanup(ctx context.Context) {
	if b.utxoStore == nil {
		b.logger.Warnf("[BlockAssembler] no utxostore, skipping unmined transaction cleanup background job")
		return
	}

	// Don't start if already running
	if b.unminedCleanupTicker != nil {
		b.logger.Debugf("[BlockAssembler] unmined transaction cleanup background job already running")
		return
	}

	// Create a ticker for 10-minute intervals
	b.unminedCleanupTicker = time.NewTicker(10 * time.Minute)

	b.logger.Infof("[BlockAssembler] starting background cleanup of unmined transactions every 10 minutes")

	go func() {
		defer func() {
			b.unminedCleanupTicker.Stop()
			b.unminedCleanupTicker = nil
		}()

		for {
			select {
			case <-ctx.Done():
				b.logger.Infof("[BlockAssembler] stopping unmined transaction cleanup background job")
				return

			case <-b.unminedCleanupTicker.C:
				currentBlockHeight := b.bestBlockHeight.Load()
				if currentBlockHeight > 0 {
					cleanupCount, err := utxo.PreserveParentsOfOldUnminedTransactions(ctx, b.utxoStore, currentBlockHeight, b.settings, b.logger)

					switch {
					case err != nil:
						b.logger.Errorf("[BlockAssembler] background cleanup of unmined transactions failed: %v", err)
					case cleanupCount > 0:
						b.logger.Infof("[BlockAssembler] background cleanup removed %d old unmined transactions", cleanupCount)
					default:
						b.logger.Debugf("[BlockAssembler] background cleanup found no old unmined transactions to remove")
					}
				}
			}
		}
	}()
}

// invalidateMiningCandidateCache invalidates the cached mining candidate
func (b *BlockAssembler) invalidateMiningCandidateCache() {
	b.cachedCandidate.mu.Lock()
	b.cachedCandidate.candidate = nil
	b.cachedCandidate.subtrees = nil
	b.cachedCandidate.lastHeight = 0
	b.cachedCandidate.lastUpdate = time.Time{}
	b.cachedCandidate.generating = false
	b.cachedCandidate.mu.Unlock()
}

// SetSkipWaitForPendingBlocks sets the flag to skip waiting for pending blocks during startup.
// This is primarily used in test environments to prevent blocking on pending blocks.
func (b *BlockAssembler) SetSkipWaitForPendingBlocks(skip bool) {
	b.skipWaitForPendingBlocks = skip
}

// waitForPendingBlocks waits for any pending blocks to be processed before loading unmined transactions.
// This method continuously polls the blockchain client to check if there are any blocks that have not been
// marked as mined yet. It will wait until the list of blocks returned by GetBlocksMinedNotSet is empty,
// indicating that all blocks have been processed and marked as mined.
//
// The method implements a polling loop with a 1-second interval and includes logging to provide visibility
// into the waiting process. This ensures that the BlockAssembly service doesn't start loading unmined
// transactions until all pending blocks have been fully processed.
//
// Parameters:
//   - ctx: Context for cancellation and timeout support
//
// Returns:
//   - error: Any error encountered during the waiting process or blockchain client calls
func (b *BlockAssembler) waitForPendingBlocks(ctx context.Context) error {
	_, _, deferFn := tracing.Tracer("blockassembly").Start(ctx, "waitForPendingBlocks",
		tracing.WithParentStat(b.stats),
		tracing.WithLogMessage(b.logger, "[waitForPendingBlocks] checking for pending blocks"),
	)
	defer deferFn()

	// Use retry utility with infinite retries until no pending blocks remain
	_, err := retry.Retry(ctx, b.logger, func() (interface{}, error) {
		blockNotMined, err := b.blockchainClient.GetBlocksMinedNotSet(ctx)
		if err != nil {
			return nil, err
		}

		if len(blockNotMined) == 0 {
			b.logger.Infof("[waitForPendingBlocks] no pending blocks found, ready to load unmined transactions")
			return nil, nil
		}

		for _, block := range blockNotMined {
			b.logger.Debugf("[waitForPendingBlocks] waiting for block %s to be processed, height %d, ID %d", block.Hash(), block.Height, block.ID)
		}

		// Return an error to trigger retry when blocks are still pending
		return nil, errors.NewProcessingError("waiting for %d blocks to be processed", len(blockNotMined))
	},
		retry.WithMessage("[waitForPendingBlocks] blockchain service check"),
		retry.WithInfiniteRetry(),
		retry.WithExponentialBackoff(),
		retry.WithBackoffDurationType(1*time.Second),
		retry.WithBackoffFactor(2.0),
		retry.WithMaxBackoff(30*time.Second),
	)

	return err
}
