// Package blockassembly provides functionality for assembling Bitcoin blocks in Teranode.
package blockassembly

import (
	"context"
	"database/sql"
	"encoding/binary"
	"fmt"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	safeconversion "github.com/bsv-blockchain/go-safe-conversion"
	"github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockassembly/subtreeprocessor"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/blob"
	"github.com/bsv-blockchain/teranode/stores/tempstore"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/tracing"
	"github.com/ordishs/gocore"
)

// State represents the current operational state of the BlockAssembler.
// It tracks the assembler's lifecycle and processing phases.
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

	// bestBlock atomically stores the current best block header and height together
	bestBlock atomic.Pointer[BestBlockInfo]

	// stateChangeCh notifies listeners of state changes
	// Protected by stateChangeMu to prevent race conditions
	stateChangeMu sync.RWMutex
	stateChangeCh chan BestBlockInfo

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
	resetCh chan resetRequest

	// currentRunningState tracks the current operational state
	currentRunningState atomic.Value

	// stateStartTime tracks when the current state began
	stateStartTime atomic.Value

	// unminedCleanupTicker manages periodic cleanup of old unmined transactions
	unminedCleanupTicker *time.Ticker

	// skipWaitForPendingBlocks allows tests to skip waiting for pending blocks during startup
	skipWaitForPendingBlocks bool

	// unminedTransactionsLoading indicates if unmined transactions are currently being loaded
	unminedTransactionsLoading atomic.Bool

	// wg tracks background goroutines for clean shutdown
	wg sync.WaitGroup
}

// BestBlockInfo holds both the block header and height atomically
type BestBlockInfo struct {
	Header *model.BlockHeader
	Height uint32
}

type blockHeaderWithMeta struct {
	header *model.BlockHeader
	meta   *model.BlockHeaderMeta
}

type blockWithMeta struct {
	block *model.Block
	meta  *model.BlockHeaderMeta
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
		currentChainMap:     make(map[chainhash.Hash]uint32, tSettings.BlockAssembly.MaxBlockReorgCatchup),
		currentChainMapIDs:  make(map[uint32]struct{}, tSettings.BlockAssembly.MaxBlockReorgCatchup),
		defaultMiningNBits:  defaultMiningBits,
		resetCh:             make(chan resetRequest, 2),
		currentRunningState: atomic.Value{},
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

// GetChainedSubtrees returns all chained subtrees from the subtree processor.
//
// Returns:
//   - []*subtree.Subtree: Slice of chained subtrees
func (b *BlockAssembler) GetChainedSubtrees() []*subtree.Subtree {
	return b.subtreeProcessor.GetChainedSubtrees()
}

// GetChainedSubtreesTotalSize returns the total size in bytes of all chained subtrees.
// This uses atomic access and is safe to call from any context without channel-based
// synchronization, avoiding potential deadlocks.
//
// Returns:
//   - uint64: Total size in bytes of all chained subtrees
func (b *BlockAssembler) GetChainedSubtreesTotalSize() uint64 {
	return b.subtreeProcessor.GetChainedSubtreesTotalSize()
}

// startChannelListeners initializes and starts all channel listeners for block assembly operations.
// It handles blockchain notifications, mining candidate requests, and reset operations.
//
// Parameters:
//   - ctx: Context for cancellation
func (b *BlockAssembler) startChannelListeners(ctx context.Context) (err error) {
	// start a subscription for the best block header and the FSM state
	// this will be used to reset the subtree processor when a new block is mined
	b.blockchainSubscriptionCh, err = b.blockchainClient.Subscribe(ctx, "BlockAssembler")
	if err != nil {
		return errors.NewProcessingError("[BlockAssembler] error subscribing to blockchain notifications: %v", err)
	}

	b.wg.Add(1)
	go func() {
		defer b.wg.Done()
		// variables are defined here to prevent unnecessary allocations
		b.setCurrentRunningState(StateRunning)

		for {
			select {
			case <-ctx.Done():
				b.logger.Infof("Stopping blockassembler as ctx is done")
				// Note: We don't close blockchainSubscriptionCh here because we don't own it -
				// it's created by the blockchain client's Subscribe method
				return

			case resetReq := <-b.resetCh:
				b.setCurrentRunningState(StateResetting)

				err := b.reset(ctx, resetReq.FullReset)

				// empty out the reset channel
				for len(b.resetCh) > 0 {
					bufferedCh := <-b.resetCh
					if bufferedCh.ErrCh != nil {
						bufferedCh.ErrCh <- nil
					}
				}

				if resetReq.ErrCh != nil {
					resetReq.ErrCh <- err
				}

				b.setCurrentRunningState(StateRunning)

			case notification := <-b.blockchainSubscriptionCh:
				b.setCurrentRunningState(StateBlockchainSubscription)

				if notification.Type == model.NotificationType_Block {
					b.processNewBlockAnnouncement(ctx)
				}

				b.setCurrentRunningState(StateRunning)
			} // select
		} // for
	}()

	return nil
}

// reset performs a full reset of the block assembler state by clearing all subtrees and reloading from blockchain.
//
// This is the "nuclear option" for handling blockchain reorganizations and is used when:
// 1. Large reorgs (>= CoinbaseMaturity blocks AND height > 1000) where incremental reorg is too expensive
// 2. Failed reorgs where subtreeProcessor.Reorg() encountered errors
// 3. Reorgs involving invalid blocks that require clean state
//
// The reset process:
// 1. Waits for BlockValidation background jobs to complete (WaitForPendingBlocks)
//   - Ensures all blocks have mined_set=true
//   - Invalid blocks: Already have block_ids removed, unmined_since set
//   - moveForward blocks: Already have unmined_since cleared (processed with onLongestChain=true)
//
// 2. Marks transactions from moveBackBlocks as NOT on longest chain (sets unmined_since)
//   - These blocks were on main chain but are now on side chain
//   - They still have mined_set=true (won't be re-processed by BlockValidation)
//   - Must explicitly mark their transactions as unmined
//
// 3. Calls subtreeProcessor.Reset() to clear all subtrees
//
// 4. Calls loadUnminedTransactions() which:
//   - Loads all transactions with unmined_since set into block assembly
//   - Fixes any data inconsistencies (transactions with block_ids on main but unmined_since incorrectly set)
//
// Key insight: BlockValidation handles moveForward and invalid blocks via background jobs.
// reset() only needs to handle moveBack blocks that won't be re-processed.
//
// Parameters:
//   - ctx: Context for cancellation
//   - fullScan: If true, loadUnminedTransactions will scan all records and fix inconsistencies
//     If false, uses index-based query for faster reload
//
// Returns:
//   - error: Any error encountered during reset
func (b *BlockAssembler) reset(ctx context.Context, fullScan bool) error {
	bestBlockchainBlockHeader, meta, err := b.blockchainClient.GetBestBlockHeader(ctx)
	if err != nil {
		return errors.NewProcessingError("[Reset] error getting best block header", err)
	}

	// reset the block assembly
	hash, h := b.CurrentBlock()
	b.logger.Warnf("[BlockAssembler][Reset] resetting: %d: %s -> %d: %s", h, hash.Hash(), meta.Height, bestBlockchainBlockHeader.String())

	moveBackBlocksWithMeta, moveForwardBlocksWithMeta, err := b.getReorgBlocks(ctx, bestBlockchainBlockHeader, meta.Height)
	if err != nil {
		return errors.NewProcessingError("[Reset] error getting reorg blocks", err)
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

	// make sure we have processed all pending blocks before resetting
	if err = b.subtreeProcessor.WaitForPendingBlocks(ctx); err != nil {
		return errors.NewProcessingError("[Reset] error waiting for pending blocks", err)
	}

	// Mark moveBack transactions as unmined (set unmined_since)
	//
	// Division of Responsibility During Reorg:
	// - Invalid blocks: BlockValidation handles via background job (unsetMined=true removes block_ids, sets unmined_since)
	// - moveForward blocks (side→main): BlockValidation handles via background job (mined_set=false → processes with onLongestChain=true → clears unmined_since)
	// - moveBack blocks (main→side): reset() handles HERE (sets unmined_since)
	//
	// Why moveBack needs explicit handling:
	// - These blocks were on main chain (unmined_since=NULL, mined_set=true)
	// - Reorg moved them to side chain
	// - BlockValidation won't re-process them (mined_set still true, not in GetBlocksMinedNotSet queue)
	// - No background job will update them
	// - Must explicitly mark their transactions as unmined here
	//
	// Why we DON'T handle moveForward:
	// - moveForward blocks have mined_set=false (newly processed or re-validated)
	// - BlockValidation background job processes them
	// - Calls setTxMinedStatus with onLongestChain=CheckBlockIsInCurrentChain() = true
	// - unmined_since is automatically cleared
	// - No action needed from reset()
	if len(moveBackBlocksWithMeta) > 0 {
		// First, build a map of transactions in moveForward blocks
		// These are transactions that are ALSO in the new main chain (don't need unmined_since set)
		// Even though BlockValidation handles moveForward, we need this map to avoid marking
		// transactions that appear in BOTH moveBack and moveForward as unmined
		moveForwardTxMap := make(map[chainhash.Hash]bool)
		for _, blockWithMeta := range moveForwardBlocksWithMeta {
			if blockWithMeta.meta.Invalid {
				continue
			}

			block := blockWithMeta.block
			blockSubtrees, err := block.GetSubtrees(ctx, b.logger, b.subtreeStore, b.settings.Block.GetAndValidateSubtreesConcurrency)
			if err != nil {
				continue
			}

			for _, st := range blockSubtrees {
				for _, node := range st.Nodes {
					if !node.Hash.IsEqual(subtree.CoinbasePlaceholderHash) {
						moveForwardTxMap[node.Hash] = true
					}
				}
			}
		}

		// Now collect moveBack transactions, excluding those in moveForward
		// Net unmined = transactions ONLY in moveBack (not also in moveForward)
		moveBackTxs := make([]chainhash.Hash, 0, len(moveBackBlocksWithMeta)*100)

		for _, blockWithMeta := range moveBackBlocksWithMeta {
			if blockWithMeta.meta.Invalid {
				// Skip invalid blocks - BlockValidation already handled them via unsetMined=true
				continue
			}

			block := blockWithMeta.block
			blockSubtrees, err := block.GetSubtrees(ctx, b.logger, b.subtreeStore, b.settings.Block.GetAndValidateSubtreesConcurrency)
			if err != nil {
				b.logger.Warnf("[BlockAssembler][Reset] error getting subtrees for moveBack block %s: %v (will skip)", block.Hash().String(), err)
				continue
			}

			for _, st := range blockSubtrees {
				for _, node := range st.Nodes {
					if !node.Hash.IsEqual(subtree.CoinbasePlaceholderHash) {
						// Only add if NOT in moveForward (these are net unmined)
						if !moveForwardTxMap[node.Hash] {
							moveBackTxs = append(moveBackTxs, node.Hash)
						}
					}
				}
			}
		}

		// Mark net unmined transactions as NOT on longest chain (set unmined_since)
		if len(moveBackTxs) > 0 {
			if err = b.utxoStore.MarkTransactionsOnLongestChain(ctx, moveBackTxs, false); err != nil {
				b.logger.Errorf("[BlockAssembler][Reset] error marking moveBack transactions as unmined: %v", err)
			} else {
				b.logger.Infof("[BlockAssembler][Reset] marked %d net unmined transactions (moveBack minus moveForward)", len(moveBackTxs))
			}
		}
	}

	// define a post process function to be called after the reset is complete, but before we release the lock
	// in the for/select in the subtreeprocessor
	postProcessFn := func() error {
		// reload the unmined transactions
		if err = b.loadUnminedTransactions(ctx, fullScan); err != nil {
			return errors.NewProcessingError("[Reset] error loading unmined transactions", err)
		}

		return nil
	}

	baBestBlockHeader, _ := b.CurrentBlock()

	if response := b.subtreeProcessor.Reset(baBestBlockHeader, moveBackBlocks, moveForwardBlocks, isLegacySync, postProcessFn); response.Err != nil {
		b.logger.Errorf("[BlockAssembler][Reset] resetting error resetting subtree processor: %v", response.Err)
		// something went wrong, we need to set the best block header in the block assembly to be the
		// same as the subtree processor's best block header
		bestBlockchainBlockHeader = b.subtreeProcessor.GetCurrentBlockHeader()

		_, bestBlockchainBlockHeaderMeta, err := b.blockchainClient.GetBlockHeader(ctx, bestBlockchainBlockHeader.Hash())
		if err != nil {
			return errors.NewProcessingError("[Reset] error getting best block header meta", err)
		}

		// set the new height based on the best block header from the subtree processor
		currentHeight = bestBlockchainBlockHeaderMeta.Height
	}

	b.setBestBlockHeader(bestBlockchainBlockHeader, currentHeight)

	if err = b.SetState(ctx); err != nil {
		return errors.NewProcessingError("[Reset] error setting state", err)
	}

	_, height := b.CurrentBlock()
	prometheusBlockAssemblyCurrentBlockHeight.Set(float64(height))

	b.logger.Warnf("[BlockAssembler][Reset] resetting block assembler DONE")

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
		b.setCurrentRunningState(StateRunning)

		deferFn()
	}()

	// Use context-aware logger for trace correlation
	ctxLogger := b.logger.WithTraceContext(ctx)

	bestBlockchainBlockHeader, bestBlockchainBlockHeaderMeta, err := b.blockchainClient.GetBestBlockHeader(ctx)
	if err != nil {
		ctxLogger.Errorf("[BlockAssembler] error getting best block header: %v", err)
		return
	}

	ctxLogger.Infof("[BlockAssembler][%s] new best block header: %d", bestBlockchainBlockHeader.Hash(), bestBlockchainBlockHeaderMeta.Height)

	defer ctxLogger.Infof("[BlockAssembler][%s] new best block header: %d DONE", bestBlockchainBlockHeader.Hash(), bestBlockchainBlockHeaderMeta.Height)

	prometheusBlockAssemblyBestBlockHeight.Set(float64(bestBlockchainBlockHeaderMeta.Height))

	bestBlockAccordingToBlockAssembly, bestBlockAccordingToBlockAssemblyHeight := b.CurrentBlock()
	bestBlockAccordingToBlockchain := bestBlockchainBlockHeader

	ctxLogger.Debugf("[BlockAssembler] best block header according to blockchain: %d: %s", bestBlockchainBlockHeaderMeta.Height, bestBlockAccordingToBlockchain.Hash())
	ctxLogger.Debugf("[BlockAssembler] best block header according to block assembly : %d: %s", bestBlockAccordingToBlockAssemblyHeight, bestBlockAccordingToBlockAssembly.Hash())

	switch {
	case bestBlockAccordingToBlockchain.Hash().IsEqual(bestBlockAccordingToBlockAssembly.Hash()):
		ctxLogger.Infof("[BlockAssembler][%s] best block header is the same as the current best block header: %s", bestBlockchainBlockHeader.Hash(), bestBlockAccordingToBlockAssembly.Hash())
		return

	case !bestBlockchainBlockHeader.HashPrevBlock.IsEqual(bestBlockAccordingToBlockAssembly.Hash()):
		ctxLogger.Infof("[BlockAssembler][%s] best block header is not the same as the previous best block header, reorging: %s", bestBlockchainBlockHeader.Hash(), bestBlockAccordingToBlockAssembly.Hash())
		b.setCurrentRunningState(StateReorging)

		err = b.handleReorg(ctx, bestBlockchainBlockHeader, bestBlockchainBlockHeaderMeta.Height)
		if err != nil {
			if errors.Is(err, errors.ErrBlockAssemblyReset) {
				// only warn about the reset
				ctxLogger.Warnf("[BlockAssembler][%s] error handling reorg: %v", bestBlockchainBlockHeader.Hash(), err)
			} else {
				ctxLogger.Errorf("[BlockAssembler][%s] error handling reorg: %v", bestBlockchainBlockHeader.Hash(), err)
			}

			return
		}
	default:
		ctxLogger.Infof("[BlockAssembler][%s] best block header is the same as the previous best block header, moving up: %s", bestBlockchainBlockHeader.Hash(), bestBlockAccordingToBlockAssembly.Hash())

		var block *model.Block

		if block, err = b.blockchainClient.GetBlock(ctx, bestBlockchainBlockHeader.Hash()); err != nil {
			ctxLogger.Errorf("[BlockAssembler][%s] error getting block from blockchain: %v", bestBlockchainBlockHeader.Hash(), err)
			return
		}

		b.setCurrentRunningState(StateMovingUp)

		if err = b.subtreeProcessor.MoveForwardBlock(block); err != nil {
			ctxLogger.Errorf("[BlockAssembler][%s] error moveForwardBlock in subtree processor: %v", bestBlockchainBlockHeader.Hash(), err)
			return
		}
	}

	b.setBestBlockHeader(bestBlockchainBlockHeader, bestBlockchainBlockHeaderMeta.Height)

	_, height := b.CurrentBlock()
	prometheusBlockAssemblyCurrentBlockHeight.Set(float64(height))

	if err = b.SetState(ctx); err != nil && !errors.Is(err, context.Canceled) {
		ctxLogger.Errorf("[BlockAssembler][%s] error setting state: %v", bestBlockchainBlockHeader.Hash(), err)
	}
}

// setBestBlockHeader updates the internal best block header and height atomically.
// This method is used internally to maintain the current blockchain state within
// the block assembler. It updates both the best block header and height in a
// thread-safe manner using a single atomic operation.
//
// The function performs the following operations:
// - Logs the update operation with block hash and Height
// - Atomically stores the new best block info (Header + height together)
//
// This method is critical for maintaining consistency between the block assembler's
// view of the blockchain state and the actual blockchain tip.
//
// Parameters:
//   - bestBlockchainBlockHeader: The new best block header to set
//   - Height: The height of the new best block
func (b *BlockAssembler) setBestBlockHeader(bestBlockchainBlockHeader *model.BlockHeader, height uint32) {
	b.logger.Infof("[BlockAssembler][%s] setting best block header to height %d", bestBlockchainBlockHeader.Hash(), height)

	b.bestBlock.Store(&BestBlockInfo{
		Header: bestBlockchainBlockHeader,
		Height: height,
	})

	// Send state change notification if a listener is registered
	b.stateChangeMu.RLock()
	stateChangeCh := b.stateChangeCh
	b.stateChangeMu.RUnlock()

	if stateChangeCh != nil {
		// Protect against send on closed channel
		func() {
			defer func() {
				if r := recover(); r != nil {
					b.logger.Debugf("[BlockAssembler] stateChangeCh closed; skipping state change notification")
				}
			}()
			stateChangeCh <- BestBlockInfo{
				Header: bestBlockchainBlockHeader,
				Height: height,
			}
		}()
	}
}

// setCurrentRunningState sets the current operational state.
//
// Parameters:
//   - state: New state to set
func (b *BlockAssembler) setCurrentRunningState(state State) {
	now := time.Now()

	oldStateValue := b.currentRunningState.Load()
	if oldStateValue != nil {
		oldState := oldStateValue.(State)

		if oldState != state {
			startTimeValue := b.stateStartTime.Load()
			if startTimeValue != nil {
				startTime := startTimeValue.(time.Time)
				duration := now.Sub(startTime).Seconds()

				prometheusBlockAssemblerStateDuration.WithLabelValues(StateStrings[oldState]).Observe(duration)
			}

			prometheusBlockAssemblerStateTransitions.WithLabelValues(StateStrings[oldState], StateStrings[state]).Inc()
		}
	}

	b.currentRunningState.Store(state)
	b.stateStartTime.Store(now)
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
		if err = b.subtreeProcessor.WaitForPendingBlocks(ctx); err != nil {
			// we cannot start block assembly if we have not processed all pending blocks
			return errors.NewProcessingError("[BlockAssembler] failed to wait for pending blocks: %v", err)
		}
	}

	// Load unmined transactions (this includes cleanup of old unmined transactions first)
	if err = b.loadUnminedTransactions(ctx, false); err != nil {
		// we cannot start block assembly if we have not loaded unmined transactions successfully
		return errors.NewStorageError("[BlockAssembler] failed to load un-mined transactions: %v", err)
	}

	// Start SubtreeProcessor goroutine after loading unmined transactions to avoid race conditions
	b.subtreeProcessor.Start(ctx)

	if err = b.startChannelListeners(ctx); err != nil {
		return errors.NewProcessingError("[BlockAssembler] failed to start channel listeners: %v", err)
	}

	_, height := b.CurrentBlock()
	prometheusBlockAssemblyCurrentBlockHeight.Set(float64(height))

	return nil
}

// Wait blocks until all background goroutines have finished.
// This should be called after the context is cancelled to ensure clean shutdown.
func (b *BlockAssembler) Wait() {
	b.wg.Wait()
}

func (b *BlockAssembler) initState(ctx context.Context) error {
	var stateFound bool

	bestBlockHeader, bestBlockHeight, err := b.GetState(ctx)
	if err != nil {
		if errors.Is(err, errors.ErrNotFound) || strings.Contains(err.Error(), sql.ErrNoRows.Error()) {
			b.logger.Warnf("[BlockAssembler] no state found in blockchain db")
		} else {
			b.logger.Errorf("[BlockAssembler] error getting state from blockchain db: %v", err)
		}
	} else {
		stateFound = true

		b.logger.Infof("[BlockAssembler] setting best block header from state: %d: %s", bestBlockHeight, bestBlockHeader.Hash())
		b.setBestBlockHeader(bestBlockHeader, bestBlockHeight)
		b.subtreeProcessor.InitCurrentBlockHeader(bestBlockHeader)
	}

	// we did not get any state back from the blockchain db, so we get the current best block header
	if !stateFound {
		baBestBlockHeader, baBestBlockHeight := b.CurrentBlock()
		if baBestBlockHeader == nil || baBestBlockHeight == 0 {
			header, meta, err := b.blockchainClient.GetBestBlockHeader(ctx)
			if err != nil {
				// we must return an error here since we cannot continue without a best block header
				return errors.NewProcessingError("[BlockAssembler] error getting best block header: %v", err)
			} else {
				hash, _ := b.CurrentBlock()
				b.logger.Infof("[BlockAssembler] setting best block header from GetBestBlockHeader: %s", hash.Hash())
				b.setBestBlockHeader(header, meta.Height)
				b.subtreeProcessor.InitCurrentBlockHeader(header)
			}
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
	blockHeader, blockHeight := b.CurrentBlock()
	if blockHeader == nil {
		return errors.NewError("bestBlockHeader is nil")
	}

	blockHeaderBytes := blockHeader.Bytes()

	state := make([]byte, 4+len(blockHeaderBytes))
	binary.LittleEndian.PutUint32(state[:4], blockHeight)
	state = append(state[:4], blockHeaderBytes...)

	b.logger.Debugf("[BlockAssembler] setting state: %d: %s", blockHeight, blockHeader.Hash())

	return b.blockchainClient.SetState(ctx, "BlockAssembler", state)
}

func (b *BlockAssembler) SetStateChangeCh(ch chan BestBlockInfo) {
	b.stateChangeMu.Lock()
	defer b.stateChangeMu.Unlock()
	b.stateChangeCh = ch
}

// CurrentBlock returns the current best block header and height atomically.
// This is the preferred method to access the best block state as it ensures
// the header and height are always consistent with each other.
//
// Returns:
//   - *model.BlockHeader: Current best block header (nil if not set)
//   - uint32: Current block height (0 if not set)
func (b *BlockAssembler) CurrentBlock() (*model.BlockHeader, uint32) {
	info := b.bestBlock.Load()
	if info == nil {
		return nil, 0
	}
	return info.Header, info.Height
}

// AddTxBatch adds a batch of transactions to the block assembler.
//
// Parameters:
//   - nodes: Transaction nodes to add
//   - txInpoints: Parent transaction references for each node
func (b *BlockAssembler) AddTxBatch(nodes []subtree.Node, txInpoints []*subtree.TxInpoints) {
	b.subtreeProcessor.AddBatch(nodes, txInpoints)
}

// RemoveTx removes a transaction from the block assembler.
//
// Parameters:
//   - ctx: Context for the removal operation
//   - hash: Hash of the transaction to remove
//
// Returns:
//   - error: Any error encountered during removal
func (b *BlockAssembler) RemoveTx(ctx context.Context, hash chainhash.Hash) error {
	return b.subtreeProcessor.Remove(ctx, hash)
}

type resetRequest struct {
	FullReset bool
	ErrCh     chan error
}

// Reset triggers a reset of the block assembler state.
// This operation runs asynchronously to prevent blocking.
func (b *BlockAssembler) Reset(fullReset bool) {
	// run in a go routine to prevent blocking
	go func() {
		errCh := make(chan error, 1)

		b.resetCh <- resetRequest{
			FullReset: fullReset,
			ErrCh:     errCh,
		}

		if err := <-errCh; err != nil {
			b.logger.Errorf("[BlockAssembler] error resetting: %v", err)
		}
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
	ctx, _, deferFn := tracing.Tracer("blockassembly").Start(ctx, "GetMiningCandidate",
		tracing.WithParentStat(b.stats),
		tracing.WithHistogram(prometheusBlockAssemblyGetMiningCandidateDuration),
		tracing.WithDebugLogMessage(b.logger, "[GetMiningCandidate] called"),
	)
	defer deferFn()

	prometheusBlockAssemblerGetMiningCandidate.Inc()

	// Handle block-processing-in-progress state
	currentState := b.GetCurrentRunningState()
	if currentState == StateBlockchainSubscription || currentState == StateMovingUp {
		b.logger.Infof("[GetMiningCandidate] Block processing in progress (state: %s), returning empty block template for new height", StateStrings[currentState])

		bestBlockHeader, bestBlockMeta, err := b.blockchainClient.GetBestBlockHeader(ctx)
		if err != nil {
			return nil, nil, errors.NewProcessingError("failed to get best block header during block processing", err)
		}

		return b.generateEmptyBlockCandidate(bestBlockHeader, bestBlockMeta.Height)
	}

	// Get current block state first (single atomic read for consistency)
	baBestBlockHeader, baBestBlockHeight := b.CurrentBlock()
	if baBestBlockHeader == nil {
		return nil, nil, errors.NewError("best block header is not available")
	}

	// Get pre-computed data (atomic read, no locks, no channel sync)
	data := b.subtreeProcessor.GetPrecomputedMiningData()

	// Check if we have valid precomputed data with subtrees
	var subtrees []*subtree.Subtree
	if data != nil && data.PreviousHeader != nil && data.PreviousHeader.Hash().IsEqual(baBestBlockHeader.Hash()) {
		subtrees = data.Subtrees
	} else if data != nil && data.PreviousHeader != nil {
		b.logger.Warnf("[GetMiningCandidate] Pre-computed data is stale (data prev: %s, current best: %s)", data.PreviousHeader.Hash().String(), baBestBlockHeader.Hash().String())
	}

	// If no complete subtrees, try on-demand incomplete subtree snapshot
	if len(subtrees) == 0 {
		incompleteData := b.subtreeProcessor.GetIncompleteSubtreeMiningData(ctx)
		if incompleteData != nil && len(incompleteData.Subtrees) > 0 && incompleteData.PreviousHeader.Hash().IsEqual(baBestBlockHeader.Hash()) {
			data = incompleteData
			subtrees = incompleteData.Subtrees
		} else {
			return b.generateEmptyBlockCandidate(baBestBlockHeader, baBestBlockHeight)
		}
	}

	// Apply max block size limit if configured
	maxBlockSize := b.settings.Policy.BlockMaxSize
	if maxBlockSize > 0 && len(subtrees) > 0 {
		var filterErr error
		subtrees, filterErr = b.filterSubtreesByMaxSize(subtrees, maxBlockSize)
		if filterErr != nil {
			return nil, nil, filterErr
		}
	}

	// Compute derived values from subtrees
	var totalFees uint64
	var txCount uint32
	var sizeWithoutCoinbase = uint64(model.BlockHeaderSize)
	subtreeHashes := make([][]byte, len(subtrees))

	topTree, err := subtree.NewIncompleteTreeByLeafCount(len(subtrees))
	if err != nil {
		return nil, nil, errors.NewProcessingError("error creating top tree", err)
	}

	for i, st := range subtrees {
		totalFees += st.Fees
		sizeWithoutCoinbase += st.SizeInBytes
		subtreeHashes[i] = st.RootHash().CloneBytes()

		lenNodes, convErr := safeconversion.IntToUint32(len(st.Nodes))
		if convErr != nil {
			b.logger.Errorf("[GetMiningCandidate] error converting nodes length: %s", convErr)
			continue
		}
		txCount += lenNodes

		_ = topTree.AddNode(*st.RootHash(), st.Fees, st.SizeInBytes)
	}

	// Remove coinbase from tx count (it's a placeholder in the first subtree)
	if txCount > 0 {
		txCount--
	}

	// Compute merkle proof for coinbase
	coinbaseMerkleProof, err := subtree.GetMerkleProofForCoinbase(subtrees)
	if err != nil {
		return nil, nil, errors.NewProcessingError("error getting merkle proof", err)
	}
	merkleProofBytes := make([][]byte, len(coinbaseMerkleProof))
	for i, hash := range coinbaseMerkleProof {
		merkleProofBytes[i] = hash.CloneBytes()
	}

	subtreeCountUint32, _ := safeconversion.IntToUint32(len(subtrees))

	// Compute time-sensitive fields
	timeNow := time.Now().Unix()
	timeNowUint32, err := safeconversion.Int64ToUint32(timeNow)
	if err != nil {
		return nil, nil, errors.NewProcessingError("error converting time now", err)
	}

	nBits, err := b.getNextNbits(data.PreviousHeader, timeNow)
	if err != nil {
		return nil, nil, err
	}

	if b.settings.ChainCfgParams == nil {
		return nil, nil, errors.NewProcessingError("ChainCfgParams is nil")
	}

	blockSubsidy := util.GetBlockSubsidyForHeight(baBestBlockHeight+1, b.settings.ChainCfgParams)

	// Generate job ID from top tree hash, previous hash, and time
	topTreeRootHash := topTree.RootHash()
	timeBytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(timeBytes, timeNowUint32)
	previousHash := data.PreviousHeader.Hash()
	id := chainhash.HashB(append(append(topTreeRootHash[:], previousHash[:]...), timeBytes...))

	candidate := &model.MiningCandidate{
		Id:                  id,
		PreviousHash:        previousHash[:],
		CoinbaseValue:       totalFees + blockSubsidy,
		Version:             0x20000000,
		NBits:               nBits.CloneBytes(),
		Height:              baBestBlockHeight + 1, // next block height
		Time:                timeNowUint32,
		MerkleProof:         merkleProofBytes,
		NumTxs:              txCount,
		SizeWithoutCoinbase: sizeWithoutCoinbase,
		SubtreeCount:        subtreeCountUint32,
		SubtreeHashes:       subtreeHashes,
	}

	b.logger.Debugf("[GetMiningCandidate] Returning mining candidate: height=%d, fees=%d, subsidy=%d, txCount=%d, subtreeCount=%d", candidate.Height, totalFees, blockSubsidy, txCount, subtreeCountUint32)

	return candidate, subtrees, nil
}

// filterSubtreesByMaxSize filters subtrees to fit within the configured maximum block size.
// Returns the filtered subtrees slice. Derived values are computed by the caller from the result.
// Returns an error if the first subtree doesn't fit within maxBlockSize.
func (b *BlockAssembler) filterSubtreesByMaxSize(subtrees []*subtree.Subtree, maxBlockSize int) ([]*subtree.Subtree, error) {
	maxBlockSizeUint64 := uint64(maxBlockSize)

	var totalSize = uint64(model.BlockHeaderSize)
	includedSubtrees := make([]*subtree.Subtree, 0, len(subtrees))

	for _, st := range subtrees {
		if totalSize+st.SizeInBytes > maxBlockSizeUint64 {
			break
		}
		totalSize += st.SizeInBytes
		includedSubtrees = append(includedSubtrees, st)
	}

	// If all subtrees fit, return original slice
	if len(includedSubtrees) == len(subtrees) {
		return subtrees, nil
	}

	// If no subtrees fit, return an error
	if len(includedSubtrees) == 0 {
		return nil, errors.NewProcessingError("max block size is less than the size of the subtree")
	}

	return includedSubtrees, nil
}

func (b *BlockAssembler) generateEmptyBlockCandidate(bestBlockHeader *model.BlockHeader, bestBlockHeight uint32) (*model.MiningCandidate, []*subtree.Subtree, error) {
	nextBlockHeight := bestBlockHeight + 1
	timeNow := time.Now().Unix()

	b.logger.Infof("[generateEmptyBlockCandidate] Generating empty block template for height %d (prev: %s)", nextBlockHeight, bestBlockHeader.Hash())

	timeNowUint32, err := safeconversion.Int64ToUint32(timeNow)
	if err != nil {
		return nil, nil, errors.NewProcessingError("error converting time", err)
	}

	nBits, err := b.getNextNbits(bestBlockHeader, timeNow)
	if err != nil {
		return nil, nil, err
	}

	blockSubsidy := util.GetBlockSubsidyForHeight(nextBlockHeight, b.settings.ChainCfgParams)

	id := &chainhash.Hash{}
	copy(id[:], bestBlockHeader.Hash()[:])
	id[0] ^= 0xFF

	miningCandidate := &model.MiningCandidate{
		Id:                  id[:],
		PreviousHash:        bestBlockHeader.Hash()[:],
		CoinbaseValue:       blockSubsidy,
		Version:             bestBlockHeader.Version,
		NBits:               nBits[:],
		Time:                timeNowUint32,
		Height:              nextBlockHeight,
		NumTxs:              0,
		SizeWithoutCoinbase: uint64(model.BlockHeaderSize),
		MerkleProof:         [][]byte{},
		SubtreeHashes:       [][]byte{},
	}

	b.logger.Infof("[generateEmptyBlockCandidate] Empty block template: height=%d, subsidy=%d, prev=%s", nextBlockHeight, blockSubsidy, bestBlockHeader.Hash())

	return miningCandidate, []*subtree.Subtree{}, nil
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

	b.logger.Infof("[BlockAssembler] handling reorg, moveBackBlocks: %d, moveForwardBlocks: %d", len(moveBackBlocksWithMeta), len(moveForwardBlocksWithMeta))

	_, currentHeight := b.CurrentBlock()

	if (len(moveBackBlocksWithMeta) >= int(b.settings.ChainCfgParams.CoinbaseMaturity) || len(moveForwardBlocksWithMeta) >= int(b.settings.ChainCfgParams.CoinbaseMaturity)) && currentHeight > 1000 {
		// large reorg, log it and Reset the block assembler
		b.logger.Warnf("[BlockAssembler] large reorg detected, resetting block assembly, moveBackBlocks: %d, moveForwardBlocks: %d", len(moveBackBlocksWithMeta), len(moveForwardBlocksWithMeta))

		// make sure we wait for the reset to complete
		if err = b.reset(ctx, false); err != nil {
			b.logger.Errorf("[BlockAssembler] error resetting after large reorg: %v", err)
		}

		// return an error to indicate we reset due to a large reorg
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

	// now do the reorg in the subtree processor
	if err = b.subtreeProcessor.Reorg(moveBackBlocks, moveForwardBlocks); err != nil {
		b.logger.Warnf("[BlockAssembler] error doing reorg, will reset instead: %v", err)
		// fallback to full reset
		reset = true
	}

	if reset {
		// we have an invalid block in the reorg or reorg failed, we need to reset the block assembly and load the unmined transactions again
		b.logger.Warnf("[BlockAssembler] reorg contains invalid block, resetting block assembly, moveBackBlocks: %d, moveForwardBlocks: %d", len(moveBackBlocks), len(moveForwardBlocks))

		if err = b.reset(ctx, false); err != nil {
			return errors.NewProcessingError("error resetting block assembly after reorg with invalid block", err)
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

	baBestBlockHeader, baBestBlockHeight := b.CurrentBlock()
	if baBestBlockHeader == nil {
		return nil, nil, errors.NewProcessingError("best block header is nil, reorg not possible")
	}

	startingHeight := baBestBlockHeight
	if height < startingHeight {
		startingHeight = height
	}

	currentChainLocator, err := b.blockchainClient.GetBlockLocator(ctx, baBestBlockHeader.Hash(), startingHeight)
	if err != nil {
		return nil, nil, errors.NewServiceError("error getting current chain block locator", err)
	}

	newChainLocator, err := b.blockchainClient.GetBlockLocator(ctx, header.Hash(), startingHeight)
	if err != nil {
		return nil, nil, errors.NewServiceError("error getting new chain block locator", err)
	}

	newChainLocatorSet := make(map[chainhash.Hash]struct{}, len(newChainLocator))
	for _, h := range newChainLocator {
		newChainLocatorSet[*h] = struct{}{}
	}

	var commonAncestorHash *chainhash.Hash
	for _, currentHash := range currentChainLocator {
		if _, ok := newChainLocatorSet[*currentHash]; ok {
			commonAncestorHash = currentHash
			break
		}
	}

	if commonAncestorHash == nil {
		return nil, nil, errors.NewProcessingError("common ancestor not found, reorg not possible")
	}

	commonAncestor, commonAncestorMeta, err := b.blockchainClient.GetBlockHeader(ctx, commonAncestorHash)
	if err != nil {
		return nil, nil, errors.NewServiceError("error getting common ancestor header", err)
	}

	if commonAncestor == nil || commonAncestorMeta == nil {
		return nil, nil, errors.NewProcessingError("common ancestor not found, reorg not possible")
	}

	// Get headers from current tip down to common ancestor
	headerCount := baBestBlockHeight - commonAncestorMeta.Height + 1

	moveBackBlockHeaders, moveBackBlockHeaderMetas, err := b.blockchainClient.GetBlockHeaders(ctx, baBestBlockHeader.Hash(), uint64(headerCount))
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
		currentHeader, currentHeight := b.CurrentBlock()
		b.logger.Errorf("reorg is too big, max block reorg: current hash: %s, current height: %d, new hash: %s, new height: %d, common ancestor hash: %s, common ancestor height: %d, move down block count: %d, move up block count: %d", currentHeader.Hash(), currentHeight, header.Hash(), height, commonAncestor.Hash(), commonAncestorMeta.Height, len(filteredMoveBack), len(moveForwardBlockHeaders))
		return nil, nil, errors.NewProcessingError("reorg is too big, max block reorg: %d", maxGetReorgHashes)
	}

	return filteredMoveBack, moveForwardBlockHeadersWithMeta, nil
}

// getNextNbits retrieves the next required work difficulty target.
//
// Returns:
//   - *model.NBit: Next difficulty target
//   - error: Any error encountered during retrieval
func (b *BlockAssembler) getNextNbits(baBestBlockHeader *model.BlockHeader, nextBlockTime int64) (*model.NBit, error) {
	nbit, err := b.blockchainClient.GetNextWorkRequired(context.Background(), baBestBlockHeader.Hash(), nextBlockTime)
	if err != nil {
		return nil, errors.NewProcessingError("error getting next work required", err)
	}

	return nbit, nil
}

// validateParentChain validates that unmined transactions have their parent transactions
// either on the best chain or also unmined (to be processed together).
//
// Parameters:
//   - ctx: Context for cancellation
//   - unminedTxs: List of unmined transactions to validate
//   - bestBlockHeaderIDsMap: Map of block IDs on the best chain
//
// Returns:
//   - []*utxo.UnminedTransaction: List of transactions (filtered if OnRestartRemoveInvalidParentChainTxs is enabled)
//   - error: Context cancellation error if cancelled, nil otherwise
func (b *BlockAssembler) validateParentChain(
	ctx context.Context,
	unminedTxs []*utxo.UnminedTransaction,
	bestBlockHeaderIDsMap map[uint32]bool,
) ([]*utxo.UnminedTransaction, error) {

	if len(unminedTxs) == 0 {
		return unminedTxs, nil
	}

	b.logger.Infof("[BlockAssembler][validateParentChain] Starting parent chain validation for %d unmined transactions", len(unminedTxs))

	// OPTIMIZATION: Two-pass approach to minimize memory usage
	// Pass 1: Collect only the parent hashes that are actually referenced
	// This is MUCH smaller than indexing all transactions
	referencedParents := make(map[chainhash.Hash]bool)
	for _, tx := range unminedTxs {
		parentHashes := tx.TxInpoints.GetParentTxHashes()
		for _, parentHash := range parentHashes {
			referencedParents[parentHash] = true
		}
	}
	b.logger.Debugf("[BlockAssembler][validateParentChain] Found %d unique parent references out of %d transactions", len(referencedParents), len(unminedTxs))

	// Pass 2: Build index ONLY for transactions that are referenced as parents
	// This dramatically reduces memory usage from O(all_txs) to O(referenced_parents)
	parentIndexMap := make(map[chainhash.Hash]int, len(referencedParents))
	for idx, unminedTx := range unminedTxs {
		if referencedParents[unminedTx.Node.Hash] {
			parentIndexMap[unminedTx.Node.Hash] = idx
		}
	}

	validTxs := make([]*utxo.UnminedTransaction, 0, len(unminedTxs))
	skippedCount := 0
	batchSize := b.settings.BlockAssembly.ParentValidationBatchSize

	// Process transactions in batches for performance
	for i := 0; i < len(unminedTxs); i += batchSize {
		// Check for context cancellation at start of each batch
		select {
		case <-ctx.Done():
			b.logger.Infof("[BlockAssembler][validateParentChain] Parent validation cancelled during batch processing at index %d", i)
			return nil, ctx.Err()
		default:
		}
		end := i + batchSize
		if end > len(unminedTxs) {
			end = len(unminedTxs)
		}
		batch := unminedTxs[i:end]

		// Collect all unique parent transaction IDs in this batch
		parentTxIDs := make([]chainhash.Hash, 0, len(batch)*2) // Assume average 2 inputs per tx
		parentTxIDMap := make(map[chainhash.Hash]bool)

		for _, tx := range batch {
			parentHashes := tx.TxInpoints.GetParentTxHashes()
			for _, parentTxID := range parentHashes {
				if !parentTxIDMap[parentTxID] {
					parentTxIDs = append(parentTxIDs, parentTxID)
					parentTxIDMap[parentTxID] = true
				}
			}
		}

		// Batch query parent transaction metadata from UTXO store
		var parentMetadata map[chainhash.Hash]*meta.Data
		if len(parentTxIDs) > 0 {
			// Use BatchDecorate for efficient batch fetching of parent metadata
			parentMetadata = make(map[chainhash.Hash]*meta.Data)

			// Create UnresolvedMetaData slice for batch operation
			unresolvedParents := make([]*utxo.UnresolvedMetaData, 0, len(parentTxIDs))
			for parentIdx, parentTxID := range parentTxIDs {
				unresolvedParents = append(unresolvedParents, &utxo.UnresolvedMetaData{
					Hash: parentTxID,
					Idx:  parentIdx,
				})
			}

			// Batch fetch all parent metadata at once
			// Request only the fields we need for validation
			err := b.utxoStore.BatchDecorate(ctx, unresolvedParents,
				fields.BlockIDs, fields.UnminedSince, fields.Locked)
			if err != nil {
				// Log the batch error but continue - individual errors are in UnresolvedMetaData
				b.logger.Warnf("[BlockAssembler][validateParentChain] BatchDecorate error (will check individual results): %v", err)
			}

			// Process results - check each parent's fetch result
			for _, unresolved := range unresolvedParents {
				if unresolved.Err != nil {
					// Parent doesn't exist or error retrieving it
					b.logger.Errorf("[BlockAssembler][validateParentChain] Failed to get parent tx %s metadata: %v",
						unresolved.Hash.String(), unresolved.Err)
					continue
				}

				if unresolved.Data != nil {
					parentMetadata[unresolved.Hash] = unresolved.Data
				}
			}
		}

		// Validate each transaction in the batch
		for batchIdx, tx := range batch {
			// First check: Is this transaction already on the best chain?
			// If yes, we filter it out (it shouldn't be in unmined list, but be defensive)
			if len(tx.BlockIDs) > 0 {
				onBestChain := false
				for _, blockID := range tx.BlockIDs {
					if bestBlockHeaderIDsMap[blockID] {
						onBestChain = true
						break
					}
				}
				if onBestChain {
					// Transaction is already on the best chain
					// (though it shouldn't be in unmined list - this is a data inconsistency)
					b.logger.Warnf("[BlockAssembler][validateParentChain] Transaction %s is already on best chain but marked as unmined", tx.Hash.String())
					if b.settings.BlockAssembly.OnRestartRemoveInvalidParentChainTxs {
						// Filtering enabled - skip this transaction
						skippedCount++
						continue
					}
					// Filtering disabled - keep transaction despite being on best chain
				}
				// Transaction has BlockIDs but not on best chain - it's on an orphaned chain
				// Continue to validate its parents to decide if it can be re-included
			}

			allParentsValid := true
			invalidReason := ""

			// Check each parent transaction
			parentHashes := tx.TxInpoints.GetParentTxHashes()
			unminedParents := make([]chainhash.Hash, 0) // Track which parents are unmined

			for _, parentTxID := range parentHashes {
				// Check if parent exists in UTXO store
				parentMeta, exists := parentMetadata[parentTxID]
				if !exists {
					// Parent not found in UTXO store at all
					// This means BatchDecorate couldn't find it - it doesn't exist
					allParentsValid = false
					invalidReason = fmt.Sprintf("parent tx %s not found in UTXO store", parentTxID.String())
					b.logger.Warnf("[BlockAssembler][validateParentChain] Transaction %s has invalid parent: %s", tx.Hash.String(), invalidReason)
					break
				}

				// CRITICAL: Check UnminedSince FIRST (authoritative indicator of mined status)
				// UnminedSince is the authoritative indicator:
				//   - UnminedSince == 0: Transaction is mined on the longest chain
				//   - UnminedSince > 0: Transaction is NOT mined on the longest chain (value is block height when unmarked)
				// BlockIDs is a historical record of ALL blocks containing this tx (can include forks)
				//
				// The key insight: After a reorg, a parent tx may have:
				//   - BlockIDs = [5640] (block on wrong chain)
				//   - UnminedSince = 5650 (marked unmined because 5640 is not on longest chain)
				// We must check UnminedSince FIRST to avoid false "wrong chain" warnings
				if parentMeta.UnminedSince > 0 {
					// Parent is unmined (confirmed by UnminedSince field)
					// Check if it's in our unmined list
					if _, isInUnminedList := parentIndexMap[parentTxID]; isInUnminedList {
						// It's in our list - track it for ordering validation
						unminedParents = append(unminedParents, parentTxID)
					} else {
						// Unmined but not in our list - this is a problem
						allParentsValid = false
						invalidReason = fmt.Sprintf("parent tx %s is unmined but not in processing list", parentTxID.String())
						b.logger.Warnf("[BlockAssembler][validateParentChain] Transaction %s has invalid parent: %s", tx.Hash.String(), invalidReason)
						break
					}
				} else if len(parentMeta.BlockIDs) > 0 {
					// Parent should be mined (UnminedSince == 0)
					// Verify BlockIDs are on best chain for data consistency
					onBestChain := false
					for _, blockID := range parentMeta.BlockIDs {
						if bestBlockHeaderIDsMap[blockID] {
							onBestChain = true
							break
						}
					}

					if !onBestChain {
						// Data inconsistency: unmined_since=0 BUT block_ids not on best chain
						// This indicates transactions weren't properly marked as unmined during a fork
						// This is a data integrity issue that must be fixed at the source (catchup.go)
						allParentsValid = false
						invalidReason = fmt.Sprintf("parent tx %s is on wrong chain (blocks: %v) and not in unmined list - data integrity issue from fork handling",
							parentTxID.String(), parentMeta.BlockIDs)
						b.logger.Warnf("[BlockAssembler][validateParentChain] Transaction %s has invalid parent: %s", tx.Hash.String(), invalidReason)
						break
					}
					// else: parent is mined on best chain - all good, continue
				} else {
					// No BlockIDs and UnminedSince=0 - data inconsistency
					// This should never happen - a tx with unmined_since=0 should have BlockIDs
					allParentsValid = false
					invalidReason = fmt.Sprintf("parent tx %s has data inconsistency (unmined_since=0 but no block_ids)", parentTxID.String())
					b.logger.Warnf("[BlockAssembler][validateParentChain] Transaction %s has invalid parent: %s", tx.Hash.String(), invalidReason)
					break
				}
			}

			// Handle transactions with unmined parents that ARE in our list
			// Check if unmined parents appear BEFORE this transaction in the sorted list
			if len(unminedParents) > 0 {
				// Calculate the global index of this transaction directly using batchIdx
				currentIdx := i + batchIdx

				// Check if all unmined parents come BEFORE this transaction
				hasInvalidOrdering := false
				for _, parentTxID := range unminedParents {
					parentIdx, parentExists := parentIndexMap[parentTxID]
					if !parentExists {
						// Parent not in index map - this means it's not in the unmined list
						// This shouldn't happen as we just checked it was referenced
						b.logger.Errorf("[BlockAssembler][validateParentChain] Parent tx %s not found in index map", parentTxID.String())
						hasInvalidOrdering = true
						break
					}

					// Parent must come BEFORE child (lower index)
					if parentIdx >= currentIdx {
						// Parent comes after or at same position as child - invalid ordering
						hasInvalidOrdering = true
						invalidReason = fmt.Sprintf("parent tx %s (index %d) comes after child tx %s (index %d)",
							parentTxID.String(), parentIdx, tx.Hash.String(), currentIdx)
						b.logger.Warnf("[BlockAssembler][validateParentChain] Skipping tx %s: %s", tx.Hash.String(), invalidReason)
						break
					}
				}

				if hasInvalidOrdering {
					skippedCount++
					continue
				}

				// All unmined parents come before this transaction - this is valid
				// The parent transactions will be processed first due to the sorted order
			}

			if allParentsValid {
				validTxs = append(validTxs, tx)
			} else {
				// Transaction has invalid parent chain - use setting to decide whether to exclude
				if b.settings.BlockAssembly.OnRestartRemoveInvalidParentChainTxs {
					// Filtering enabled - skip this transaction
					skippedCount++
				} else {
					// Filtering disabled (default) - keep transaction despite invalid parents
					validTxs = append(validTxs, tx)
				}
			}
		}
	}

	filteringStatus := "disabled"
	if b.settings.BlockAssembly.OnRestartRemoveInvalidParentChainTxs {
		filteringStatus = "enabled"
	}

	if skippedCount > 0 {
		b.logger.Warnf("[BlockAssembler][validateParentChain] Skipped %d transactions due to invalid/missing parent chains (filtering: %s)", skippedCount, filteringStatus)
	}

	b.logger.Infof("[BlockAssembler][validateParentChain] Parent chain validation complete: %d valid, %d skipped (filtering: %s)",
		len(validTxs), skippedCount, filteringStatus)

	return validTxs, nil
}

// loadUnminedTransactions loads transactions from the UTXO store into block assembly.
//
// Primary responsibility: Load unmined transactions
//   - Iterates through transactions with unmined_since set
//   - Filters out transactions already on main chain (skip those with block_ids on best chain)
//   - Loads remaining transactions into block assembly for potential inclusion in next block
//
// Secondary responsibility: Data integrity safety net (ALWAYS runs)
//   - Identifies transactions with block_ids on main chain BUT unmined_since still set
//   - Calls MarkTransactionsOnLongestChain to clear unmined_since for these transactions
//   - This catches edge cases from: previous bugs, crashes, timing issues
//   - Minimal performance impact since list is usually empty when system is healthy
//
// The fullScan parameter controls iterator behavior:
//   - fullScan=false: Uses index on unmined_since (fast, most common)
//   - fullScan=true: Scans ALL records (Aerospike only; SQL always uses index)
//
// Relationship with reorg handling:
//   - BlockValidation background jobs: Handle moveForward blocks and invalid blocks
//   - reset(): Handles moveBack blocks (sets unmined_since for transactions moved to side chain)
//   - loadUnminedTransactions(): Loads everything + catches any missed edge cases
//
// Note: For moveForward blocks, BlockValidation has already cleared unmined_since via
// background job (mined_set=false triggers setTxMinedStatus with onLongestChain=true).
// This function is just a safety net for any inconsistencies, not primary reorg handling.
//
// Called from:
//   - reset() as postProcessFn (after reorg processing)
//   - Startup initialization
//
// Parameters:
//   - ctx: Context for cancellation
//   - fullScan: true = scan all records, false = use index (faster)
//
// Returns:
//   - error: Any error encountered during loading
func (b *BlockAssembler) loadUnminedTransactions(ctx context.Context, fullScan bool) (err error) {
	_, _, deferFn := tracing.Tracer("blockassembly").Start(ctx, "loadUnminedTransactions",
		tracing.WithParentStat(b.stats),
		tracing.WithLogMessage(b.logger, "[loadUnminedTransactions] called with fullScan=%t", fullScan),
	)
	defer deferFn()

	// Set flag to indicate unmined transactions are being loaded
	b.unminedTransactionsLoading.Store(true)
	defer func() {
		// Clear flag when loading is complete
		b.unminedTransactionsLoading.Store(false)
		b.logger.Infof("[loadUnminedTransactions] unmined transaction loading completed")
	}()

	if b.utxoStore == nil {
		return errors.NewServiceError("[BlockAssembler] no utxostore")
	}

	// Use disk-based sorting for large datasets when:
	// 1. UnminedTxDiskSortEnabled is true
	// 2. OnRestartValidateParentChain is false (parent validation requires in-memory for small datasets)
	if b.settings.BlockAssembly.UnminedTxDiskSortEnabled && !b.settings.BlockAssembly.OnRestartValidateParentChain {
		b.logger.Infof("[loadUnminedTransactions] using disk-based sorting to reduce RAM usage")
		return b.loadUnminedTransactionsWithDiskSort(ctx, fullScan)
	}

	scanHeaders := uint64(1000)

	if !fullScan {
		// Wait for the unmined_since index to be ready before attempting to get the iterator
		// This is similar to how the cleanup service waits for the delete_at_height index
		if indexWaiter, ok := b.utxoStore.(interface {
			WaitForIndexReady(ctx context.Context, indexName string) error
		}); ok {
			indexName := "unminedSinceIndex"
			prometheusBlockAssemblerUtxoIndexReady.WithLabelValues(indexName).Set(0)

			start := time.Now()
			err := indexWaiter.WaitForIndexReady(ctx, indexName)

			duration := time.Since(start).Seconds()
			if err != nil {
				b.logger.Warnf("[BlockAssembler] failed to wait for unmined_since index: %v", err)
				prometheusBlockAssemblerUtxoIndexReady.WithLabelValues(indexName).Set(2)
				prometheusBlockAssemblerUtxoIndexWaitDuration.WithLabelValues(indexName, "error").Observe(duration)
				// Continue anyway as this may be a non-Aerospike store
			} else {
				prometheusBlockAssemblerUtxoIndexReady.WithLabelValues(indexName).Set(1)
				prometheusBlockAssemblerUtxoIndexWaitDuration.WithLabelValues(indexName, "success").Observe(duration)
			}
		} else {
			b.logger.Warnf("[BlockAssembler] utxo store does not support WaitForIndexReady")
			prometheusBlockAssemblerUtxoIndexWaitDuration.WithLabelValues("unminedSinceIndex", "skipped").Observe(0)
		}
	} else {
		// get the full header count so we can do a full scan of all unmined transactions
		_, bestBlockHeaderMeta, err := b.blockchainClient.GetBestBlockHeader(ctx)
		if err != nil {
			return errors.NewProcessingError("error getting best block header meta", err)
		}

		if bestBlockHeaderMeta.Height > 0 {
			scanHeaders = uint64(bestBlockHeaderMeta.Height)
		} else {
			scanHeaders = 1000
		}

		b.logger.Infof("[BlockAssembler] doing full scan of unmined transactions, scanning last %d headers", scanHeaders)
	}

	bestBlockHeader, _ := b.CurrentBlock()
	bestBlockHeaderIDs, err := b.blockchainClient.GetBlockHeaderIDs(ctx, bestBlockHeader.Hash(), scanHeaders)
	if err != nil {
		return errors.NewProcessingError("error getting best block headers", err)
	}

	bestBlockHeaderIDsMap := make(map[uint32]bool, len(bestBlockHeaderIDs))
	for _, id := range bestBlockHeaderIDs {
		bestBlockHeaderIDsMap[id] = true
	}

	b.logger.Infof("[loadUnminedTransactions] requesting unmined tx iterator from UTXO store (fullScan=%t)", fullScan)
	fullScanLabel := "false"
	if fullScan {
		fullScanLabel = "true"
	}
	start := time.Now()
	it, err := b.utxoStore.GetUnminedTxIterator(fullScan)
	duration := time.Since(start).Seconds()
	if err != nil {
		prometheusBlockAssemblerGetUnminedTxIteratorTime.WithLabelValues(fullScanLabel, "error").Observe(duration)
		return errors.NewProcessingError("error getting unmined tx iterator", err)
	}
	prometheusBlockAssemblerGetUnminedTxIteratorTime.WithLabelValues(fullScanLabel, "success").Observe(duration)
	b.logger.Infof("[loadUnminedTransactions] successfully created unmined tx iterator, starting to process transactions")

	unminedTransactions := make([]*utxo.UnminedTransaction, 0, 16*1024*1024) // preallocate a large slice to avoid reallocations
	lockedTransactions := make([]chainhash.Hash, 0, 1024)

	// keep track of transactions that we need to mark as mined on the longest chain
	// this is for transactions that are included in a block that is in the best chain
	// but the transaction itself is still marked as unmined, this can happen if block assembly got dirty
	markAsMinedOnLongestChain := make([]chainhash.Hash, 0, 1024)

	iteratorStart := time.Now()
	totalProcessed := atomic.Int64{}
	skippedCount := atomic.Int64{}
	alreadyMinedCount := atomic.Int64{}
	lockedCount := atomic.Int64{}

	// Worker pool configuration
	numWorkers := runtime.NumCPU() * 4
	workChan := make(chan []*utxo.UnminedTransaction, numWorkers*2)
	var wg sync.WaitGroup

	// Per-worker local buffers to eliminate lock contention
	type workerResult struct {
		unminedTxs              []*utxo.UnminedTransaction
		lockedTxs               []chainhash.Hash
		markAsMinedOnLongestTxs []chainhash.Hash
	}
	workerResults := make([]workerResult, numWorkers)

	// Pre-allocate per-worker buffers
	for i := range workerResults {
		workerResults[i].unminedTxs = make([]*utxo.UnminedTransaction, 0, 1024*128)
		workerResults[i].lockedTxs = make([]chainhash.Hash, 0, 128)
		workerResults[i].markAsMinedOnLongestTxs = make([]chainhash.Hash, 0, 128)
	}

	// Start worker goroutines
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			localResult := &workerResults[workerID]

			for batch := range workChan {
				for _, unminedTransaction := range batch {
					if unminedTransaction.Skip {
						skippedCount.Add(1)
						continue
					}

					if len(unminedTransaction.BlockIDs) > 0 {
						// If the transaction is already included in a block that is in the best chain, skip it
						skipAlreadyMined := false
						for _, blockID := range unminedTransaction.BlockIDs {
							if bestBlockHeaderIDsMap[blockID] {
								skipAlreadyMined = true
								break
							}
						}

						if skipAlreadyMined {
							// b.logger.Debugf("[BlockAssembler] skipping unmined transaction %s already included in best chain", unminedTransaction.Hash)
							alreadyMinedCount.Add(1)

							if unminedTransaction.UnminedSince > 0 {
								localResult.markAsMinedOnLongestTxs = append(localResult.markAsMinedOnLongestTxs, unminedTransaction.Hash)
							}

							continue
						}
					}

					if !b.settings.BlockAssembly.StoreTxInpointsForSubtreeMeta {
						// clear the TxInpoints to save memory if we are not using subtree meta
						unminedTransaction.TxInpoints = &subtree.TxInpoints{}
					}

					localResult.unminedTxs = append(localResult.unminedTxs, unminedTransaction)

					if unminedTransaction.Locked {
						// if the transaction is locked, we need to add it to the locked transactions list, so we can unlock them
						localResult.lockedTxs = append(localResult.lockedTxs, unminedTransaction.Hash)
						lockedCount.Add(1)
					}
				}
			}
		}(i)
	}

	b.logger.Infof("[loadUnminedTransactions] feeding unmined transactions to %d workers", numWorkers)

	// Feed batches from the iterator to workers
	for {
		batch, err := it.Next(ctx)
		if err != nil {
			close(workChan)
			wg.Wait()
			return errors.NewProcessingError("error getting unmined transaction", err)
		}

		if batch == nil || len(batch) == 0 {
			break
		}

		totalProcessed.Add(int64(len(batch)))

		if totalProcessed.Load()%100_000 == 0 {
			b.logger.Infof("[loadUnminedTransactions] processed %d unmined transactions so far", totalProcessed.Load())
		}

		workChan <- batch
	}

	// Close channel and wait for all workers to finish
	close(workChan)
	wg.Wait()

	b.logger.Infof("[loadUnminedTransactions] completed processing unmined transactions from iterator, merging results")

	// Merge per-worker results into final slices
	for idx := range workerResults {
		unminedTransactions = append(unminedTransactions, workerResults[idx].unminedTxs...)
		workerResults[idx].unminedTxs = nil // Clear slice to release memory

		lockedTransactions = append(lockedTransactions, workerResults[idx].lockedTxs...)
		workerResults[idx].lockedTxs = nil // Clear slice to release memory

		markAsMinedOnLongestChain = append(markAsMinedOnLongestChain, workerResults[idx].markAsMinedOnLongestTxs...)
		workerResults[idx].markAsMinedOnLongestTxs = nil // Clear slice to release memory
	}

	workerResults = nil // release memory

	iteratorDuration := time.Since(iteratorStart).Seconds()
	prometheusBlockAssemblerIteratorProcessingTime.WithLabelValues(fullScanLabel).Observe(iteratorDuration)
	prometheusBlockAssemblerIteratorTransactionsTotal.WithLabelValues(fullScanLabel).Add(float64(totalProcessed.Load()))
	prometheusBlockAssemblerIteratorTransactionsStats.WithLabelValues(fullScanLabel, "skipped").Add(float64(skippedCount.Load()))
	prometheusBlockAssemblerIteratorTransactionsStats.WithLabelValues(fullScanLabel, "already_mined").Add(float64(alreadyMinedCount.Load()))
	prometheusBlockAssemblerIteratorTransactionsStats.WithLabelValues(fullScanLabel, "locked").Add(float64(lockedCount.Load()))
	prometheusBlockAssemblerIteratorTransactionsStats.WithLabelValues(fullScanLabel, "added").Add(float64(len(unminedTransactions)))

	// Always fix data inconsistencies: transactions with block_ids on main chain but unmined_since set
	// This ensures data integrity on every load, catching issues from previous bugs, crashes, or edge cases
	// The performance impact is minimal since the list is usually empty when data is correct
	if len(markAsMinedOnLongestChain) > 0 {
		markStart := time.Now()
		if err = b.utxoStore.MarkTransactionsOnLongestChain(ctx, markAsMinedOnLongestChain, true); err != nil {
			return errors.NewProcessingError("error marking transactions as mined on longest chain", err)
		}
		prometheusBlockAssemblerMarkTransactionsTime.Observe(time.Since(markStart).Seconds())
		prometheusBlockAssemblerMarkTransactionsCount.Add(float64(len(markAsMinedOnLongestChain)))

		b.logger.Infof("[BlockAssembler] fixed %d transactions with inconsistent unmined_since (had block_ids on main but unmined_since set)", len(markAsMinedOnLongestChain))
	}

	// order the transactions by createdAt
	sortStart := time.Now()

	b.logger.Infof("[loadUnminedTransactions] sorting %d unmined transactions by createdAt", len(unminedTransactions))

	sort.Slice(unminedTransactions, func(i, j int) bool {
		// sort by createdAt, oldest first
		return unminedTransactions[i].CreatedAt < unminedTransactions[j].CreatedAt
	})

	txCount := len(unminedTransactions)

	var countBucket string

	switch {
	case txCount < 1000:
		countBucket = "<1k"
	case txCount < 10000:
		countBucket = "1k-10k"
	case txCount < 100000:
		countBucket = "10k-100k"
	case txCount < 1000000:
		countBucket = "100k-1M"
	default:
		countBucket = ">1M"
	}

	prometheusBlockAssemblerSortTransactionsTime.WithLabelValues(countBucket).Observe(time.Since(sortStart).Seconds())

	// Apply parent chain validation if enabled
	if b.settings.BlockAssembly.OnRestartValidateParentChain {
		var err error

		validateStart := time.Now()
		beforeCount := len(unminedTransactions)

		unminedTransactions, err = b.validateParentChain(ctx, unminedTransactions, bestBlockHeaderIDsMap)
		if err != nil {
			// Context was cancelled during parent validation
			return err
		}

		prometheusBlockAssemblerValidateParentChainTime.Observe(time.Since(validateStart).Seconds())

		filteredCount := beforeCount - len(unminedTransactions)
		if filteredCount > 0 {
			prometheusBlockAssemblerValidateParentChainFiltered.Add(float64(filteredCount))
		}
	}

	b.logger.Infof("[BlockAssembler] loaded %d unmined transactions into block assembly (total processed: %d, skipped: %d, already mined: %d, locked: %d)",
		len(unminedTransactions), totalProcessed.Load(), skippedCount.Load(), alreadyMinedCount.Load(), lockedCount.Load())

	b.logger.Infof("[loadUnminedTransactions] adding unmined transactions to subtree processor")

	batchStart := time.Now()
	addStart := time.Now()
	addTxs := float64(0)

	// Use batch insertion if UnminedLoadingBatchSize is set (> 0)
	batchSize := b.settings.BlockAssembly.UnminedLoadingBatchSize // default 10 million
	if batchSize > 0 {
		// Batch mode: use AddNodesDirectly for parallel currentTxMap insertion
		// Process directly from original slice to avoid doubling memory usage
		totalTxs := len(unminedTransactions)
		for start := 0; start < totalTxs; start += batchSize {
			end := start + batchSize
			if end > totalTxs {
				end = totalTxs
			}

			// Pass slice segment directly - no copy needed
			batch := unminedTransactions[start:end]
			if err = b.subtreeProcessor.AddNodesDirectly(batch, true); err != nil {
				return errors.NewProcessingError("error adding unmined transactions batch to subtree processor", err)
			}

			batchCount := len(batch)
			addTxs += float64(batchCount)

			// Log progress of batch addition
			prometheusBlockAssemblerAddDirectlyBatchTime.Observe(time.Since(addStart).Seconds())
			prometheusBlockAssemblerAddDirectlyTotal.Add(float64(batchCount))
			addStart = time.Now()

			// Nil out processed elements to allow GC of UnminedTransaction objects
			for j := start; j < end; j++ {
				unminedTransactions[j] = nil
			}
		}
	} else {
		// Sequential mode: use AddDirectly for each transaction
		for idx, unminedTransaction := range unminedTransactions {
			if err = b.subtreeProcessor.AddDirectly(unminedTransaction.Node, unminedTransaction.TxInpoints, true); err != nil {
				return errors.NewProcessingError("error adding unmined transaction to subtree processor", err)
			}

			// add every 10_000 transactions log the time taken
			if (idx+1)%10_000 == 0 {
				prometheusBlockAssemblerAddDirectlyTime.Observe(time.Since(addStart).Seconds())
				prometheusBlockAssemblerAddDirectlyTotal.Add(addTxs)
				addStart = time.Now()
				addTxs = 0
			}

			unminedTransactions[idx] = nil // release memory

			addTxs++
		}
	}

	unminedTransactions = nil // release memory

	prometheusBlockAssemblerAddDirectlyBatchTime.Observe(time.Since(batchStart).Seconds())

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

// sortEntry is a lightweight in-memory structure for sorting.
// Only 12 bytes per transaction instead of full UnminedTransaction.
type sortEntry struct {
	CreatedAt int    // 8 bytes - timestamp with milliseconds for sorting
	Sequence  uint64 // 8 bytes - key to retrieve from temp store
}

// loadUnminedTransactionsWithDiskSort loads unmined transactions using disk-based sorting
// to reduce RAM usage. Instead of loading all transaction data into memory, it:
// 1. Writes transaction data to BadgerDB temp storage
// 2. Keeps only minimal sort entries (12 bytes each) in memory
// 3. Sorts in memory by CreatedAt
// 4. Reads back from disk in sorted order
func (b *BlockAssembler) loadUnminedTransactionsWithDiskSort(ctx context.Context, fullScan bool) error {
	scanHeaders := uint64(1000)

	if !fullScan {
		// Wait for the unmined_since index to be ready
		if indexWaiter, ok := b.utxoStore.(interface {
			WaitForIndexReady(ctx context.Context, indexName string) error
		}); ok {
			indexName := "unminedSinceIndex"
			prometheusBlockAssemblerUtxoIndexReady.WithLabelValues(indexName).Set(0)

			start := time.Now()
			err := indexWaiter.WaitForIndexReady(ctx, indexName)

			duration := time.Since(start).Seconds()
			if err != nil {
				b.logger.Warnf("[BlockAssembler] failed to wait for unmined_since index: %v", err)
				prometheusBlockAssemblerUtxoIndexReady.WithLabelValues(indexName).Set(2)
				prometheusBlockAssemblerUtxoIndexWaitDuration.WithLabelValues(indexName, "error").Observe(duration)
			} else {
				prometheusBlockAssemblerUtxoIndexReady.WithLabelValues(indexName).Set(1)
				prometheusBlockAssemblerUtxoIndexWaitDuration.WithLabelValues(indexName, "success").Observe(duration)
			}
		} else {
			b.logger.Warnf("[BlockAssembler] utxo store does not support WaitForIndexReady")
			prometheusBlockAssemblerUtxoIndexWaitDuration.WithLabelValues("unminedSinceIndex", "skipped").Observe(0)
		}
	} else {
		_, bestBlockHeaderMeta, err := b.blockchainClient.GetBestBlockHeader(ctx)
		if err != nil {
			return errors.NewProcessingError("error getting best block header meta", err)
		}

		if bestBlockHeaderMeta.Height > 0 {
			scanHeaders = uint64(bestBlockHeaderMeta.Height)
		} else {
			scanHeaders = 1000
		}

		b.logger.Infof("[BlockAssembler] doing full scan of unmined transactions, scanning last %d headers", scanHeaders)
	}

	bestBlockHeader, _ := b.CurrentBlock()
	bestBlockHeaderIDs, err := b.blockchainClient.GetBlockHeaderIDs(ctx, bestBlockHeader.Hash(), scanHeaders)
	if err != nil {
		return errors.NewProcessingError("error getting best block headers", err)
	}

	bestBlockHeaderIDsMap := make(map[uint32]bool, len(bestBlockHeaderIDs))
	for _, id := range bestBlockHeaderIDs {
		bestBlockHeaderIDsMap[id] = true
	}

	b.logger.Infof("[loadUnminedTransactionsWithDiskSort] requesting unmined tx iterator from UTXO store (fullScan=%t)", fullScan)
	fullScanLabel := "false"
	if fullScan {
		fullScanLabel = "true"
	}
	start := time.Now()
	it, err := b.utxoStore.GetUnminedTxIterator(fullScan)
	duration := time.Since(start).Seconds()
	if err != nil {
		prometheusBlockAssemblerGetUnminedTxIteratorTime.WithLabelValues(fullScanLabel, "error").Observe(duration)
		return errors.NewProcessingError("error getting unmined tx iterator", err)
	}
	prometheusBlockAssemblerGetUnminedTxIteratorTime.WithLabelValues(fullScanLabel, "success").Observe(duration)
	b.logger.Infof("[loadUnminedTransactionsWithDiskSort] successfully created unmined tx iterator")

	// Create temporary BadgerDB store
	tempStore, err := tempstore.New(tempstore.Options{
		BasePath: b.settings.BlockAssembly.UnminedTxDiskSortPath,
		Prefix:   "unmined-sort",
	})
	if err != nil {
		return errors.NewProcessingError("error creating temp store for disk-based sorting", err)
	}
	defer func() {
		if closeErr := tempStore.Close(); closeErr != nil {
			b.logger.Warnf("[loadUnminedTransactionsWithDiskSort] error closing temp store: %v", closeErr)
		}
	}()

	// Lightweight sort entries - only 12 bytes per tx
	sortEntries := make([]sortEntry, 0, 1024*1024)
	lockedTransactions := make([]chainhash.Hash, 0, 1024)
	markAsMinedOnLongestChain := make([]chainhash.Hash, 0, 1024)

	iteratorStart := time.Now()
	var sequence uint64
	totalProcessed := atomic.Int64{}
	skippedCount := atomic.Int64{}
	alreadyMinedCount := atomic.Int64{}
	lockedCount := atomic.Int64{}

	// Write batch for efficient disk writes
	writeBatch := tempStore.NewWriteBatch()

	b.logger.Infof("[loadUnminedTransactionsWithDiskSort] processing unmined transactions and writing to temp store")

	// Process batches from iterator
	for {
		batch, err := it.Next(ctx)
		if err != nil {
			writeBatch.Cancel()
			return errors.NewProcessingError("error getting unmined transaction", err)
		}

		if batch == nil || len(batch) == 0 {
			break
		}

		for _, unminedTx := range batch {
			totalProcessed.Add(1)

			if unminedTx.Skip {
				skippedCount.Add(1)
				continue
			}

			if len(unminedTx.BlockIDs) > 0 {
				skipAlreadyMined := false
				for _, blockID := range unminedTx.BlockIDs {
					if bestBlockHeaderIDsMap[blockID] {
						skipAlreadyMined = true
						break
					}
				}

				if skipAlreadyMined {
					alreadyMinedCount.Add(1)
					if unminedTx.UnminedSince > 0 {
						markAsMinedOnLongestChain = append(markAsMinedOnLongestChain, unminedTx.Node.Hash)
					}
					continue
				}
			}

			// Serialize and write to temp store
			txData, serErr := utxo.SerializeUnminedTransaction(unminedTx)
			if serErr != nil {
				writeBatch.Cancel()
				return errors.NewProcessingError("error serializing unmined transaction", serErr)
			}

			// Use sequence number as key (8 bytes, big-endian for lexicographic ordering)
			key := make([]byte, 8)
			binary.BigEndian.PutUint64(key, sequence)

			if setErr := writeBatch.Set(key, txData); setErr != nil {
				writeBatch.Cancel()
				return errors.NewProcessingError("error writing to temp store", setErr)
			}

			// Add lightweight sort entry
			sortEntries = append(sortEntries, sortEntry{
				CreatedAt: unminedTx.CreatedAt,
				Sequence:  sequence,
			})

			if unminedTx.Locked {
				lockedTransactions = append(lockedTransactions, unminedTx.Node.Hash)
				lockedCount.Add(1)
			}

			sequence++

			// Flush batch periodically to prevent memory buildup
			if writeBatch.Count() >= 10000 {
				if flushErr := writeBatch.Flush(); flushErr != nil {
					return errors.NewProcessingError("error flushing temp store batch", flushErr)
				}
			}
		}

		if totalProcessed.Load()%100_000 == 0 {
			b.logger.Infof("[loadUnminedTransactionsWithDiskSort] processed %d unmined transactions so far", totalProcessed.Load())
		}
	}

	// Flush any remaining writes
	if flushErr := writeBatch.Flush(); flushErr != nil {
		return errors.NewProcessingError("error flushing final temp store batch", flushErr)
	}

	iteratorDuration := time.Since(iteratorStart).Seconds()
	prometheusBlockAssemblerIteratorProcessingTime.WithLabelValues(fullScanLabel).Observe(iteratorDuration)
	prometheusBlockAssemblerIteratorTransactionsTotal.WithLabelValues(fullScanLabel).Add(float64(totalProcessed.Load()))
	prometheusBlockAssemblerIteratorTransactionsStats.WithLabelValues(fullScanLabel, "skipped").Add(float64(skippedCount.Load()))
	prometheusBlockAssemblerIteratorTransactionsStats.WithLabelValues(fullScanLabel, "already_mined").Add(float64(alreadyMinedCount.Load()))
	prometheusBlockAssemblerIteratorTransactionsStats.WithLabelValues(fullScanLabel, "locked").Add(float64(lockedCount.Load()))
	prometheusBlockAssemblerIteratorTransactionsStats.WithLabelValues(fullScanLabel, "added").Add(float64(len(sortEntries)))

	// Fix data inconsistencies
	if len(markAsMinedOnLongestChain) > 0 {
		markStart := time.Now()
		if err = b.utxoStore.MarkTransactionsOnLongestChain(ctx, markAsMinedOnLongestChain, true); err != nil {
			return errors.NewProcessingError("error marking transactions as mined on longest chain", err)
		}
		prometheusBlockAssemblerMarkTransactionsTime.Observe(time.Since(markStart).Seconds())
		prometheusBlockAssemblerMarkTransactionsCount.Add(float64(len(markAsMinedOnLongestChain)))

		b.logger.Infof("[BlockAssembler] fixed %d transactions with inconsistent unmined_since", len(markAsMinedOnLongestChain))
	}

	// Sort the lightweight entries in memory
	sortStart := time.Now()
	sort.Slice(sortEntries, func(i, j int) bool {
		return sortEntries[i].CreatedAt < sortEntries[j].CreatedAt
	})
	txCount := len(sortEntries)
	var countBucket string
	switch {
	case txCount < 1000:
		countBucket = "<1k"
	case txCount < 10000:
		countBucket = "1k-10k"
	case txCount < 100000:
		countBucket = "10k-100k"
	case txCount < 1000000:
		countBucket = "100k-1M"
	default:
		countBucket = ">1M"
	}
	prometheusBlockAssemblerSortTransactionsTime.WithLabelValues(countBucket).Observe(time.Since(sortStart).Seconds())

	b.logger.Infof("[BlockAssembler] loaded %d unmined transactions into temp store (total processed: %d, skipped: %d, already mined: %d, locked: %d)",
		len(sortEntries), totalProcessed.Load(), skippedCount.Load(), alreadyMinedCount.Load(), lockedCount.Load())

	b.logger.Infof("[loadUnminedTransactionsWithDiskSort] reading back transactions in sorted order and adding to subtree processor")

	// Read back in sorted order and add to subtree processor
	batchStart := time.Now()
	addStart := time.Now()
	addTxs := float64(0)

	for idx, entry := range sortEntries {
		// Get transaction data from temp store
		key := make([]byte, 8)
		binary.BigEndian.PutUint64(key, entry.Sequence)

		txData, getErr := tempStore.Get(key)
		if getErr != nil {
			return errors.NewProcessingError("error reading from temp store", getErr)
		}

		unminedTx, deserErr := utxo.DeserializeUnminedTransaction(txData)
		if deserErr != nil {
			return errors.NewProcessingError("error deserializing unmined transaction", deserErr)
		}

		if err = b.subtreeProcessor.AddDirectly(unminedTx.Node, unminedTx.TxInpoints, true); err != nil {
			return errors.NewProcessingError("error adding unmined transaction to subtree processor", err)
		}

		if (idx+1)%10_000 == 0 {
			prometheusBlockAssemblerAddDirectlyTime.Observe(time.Since(addStart).Seconds())
			prometheusBlockAssemblerAddDirectlyTotal.Add(addTxs)
			addStart = time.Now()
			addTxs = 0
		}

		addTxs++
	}

	prometheusBlockAssemblerAddDirectlyBatchTime.Observe(time.Since(batchStart).Seconds())

	// Unlock any locked transactions
	if len(lockedTransactions) > 0 {
		if err = b.utxoStore.SetLocked(ctx, lockedTransactions, false); err != nil {
			return errors.NewProcessingError("[BlockAssembler] failed to unlock %d unmined transactions: %v", len(lockedTransactions), err)
		}
		b.logger.Infof("[BlockAssembler] unlocked %d previously locked unmined transactions", len(lockedTransactions))
	}

	return nil
}

// SetSkipWaitForPendingBlocks sets the flag to skip waiting for pending blocks during startup.
// This is primarily used in test environments to prevent blocking on pending blocks.
func (b *BlockAssembler) SetSkipWaitForPendingBlocks(skip bool) {
	b.skipWaitForPendingBlocks = skip
}
