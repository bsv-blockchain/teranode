// Package blockassembly provides functionality for assembling Bitcoin blocks in Teranode.
package blockassembly

import (
	"context"
	"database/sql"
	"encoding/binary"
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
	"github.com/bitcoin-sv/teranode/stores/utxo"
	"github.com/bitcoin-sv/teranode/tracing"
	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/bitcoin-sv/teranode/util"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/ordishs/go-utils"
	"github.com/ordishs/gocore"
)

// miningCandidateResponse encapsulates all data needed for a mining candidate response.
type miningCandidateResponse struct {
	// miningCandidate contains the block template for mining
	miningCandidate *model.MiningCandidate

	// subtrees contains all transaction subtrees included in the candidate
	subtrees []*util.Subtree

	// err contains any error encountered during candidate creation
	err error
}

// BlockAssembler manages the assembly of new blocks and coordinates mining operations.
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
	subtreeProcessor *subtreeprocessor.SubtreeProcessor

	// miningCandidateCh coordinates requests for mining candidates
	miningCandidateCh chan chan *miningCandidateResponse

	// bestBlockHeader atomically stores the current best block header
	bestBlockHeader atomic.Pointer[model.BlockHeader]

	// bestBlockHeight atomically stores the current best block height
	bestBlockHeight atomic.Uint32

	// currentChain stores the current blockchain state
	currentChain []*model.BlockHeader

	// currentChainMap maps block hashes to their heights
	currentChainMap map[chainhash.Hash]uint32

	// currentChainMapIDs tracks block IDs in the current chain
	currentChainMapIDs map[uint32]struct{}

	// currentChainMapMu protects access to chain maps
	currentChainMapMu sync.RWMutex

	// blockchainSubscriptionCh receives blockchain notifications
	blockchainSubscriptionCh chan *blockchain.Notification

	// currentDifficulty stores the current mining difficulty target
	currentDifficulty atomic.Pointer[model.NBit]

	// defaultMiningNBits stores the default mining difficulty
	defaultMiningNBits *model.NBit

	// resetCh handles reset requests for the assembler
	resetCh chan struct{}

	// resetWaitCount tracks the number of blocks to wait after reset
	resetWaitCount atomic.Int32

	// resetWaitTime tracks the time to wait after reset
	resetWaitTime atomic.Int32

	// currentRunningState tracks the current operational state
	currentRunningState atomic.Value
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
		resetWaitTime:       atomic.Int32{},
		currentRunningState: atomic.Value{},
	}
	b.currentRunningState.Store("starting")

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
func (b *BlockAssembler) startChannelListeners(ctx context.Context) {
	var err error

	// start a subscription for the best block header and the FSM state
	// this will be used to reset the subtree processor when a new block is mined
	go func() {
		b.blockchainSubscriptionCh, err = b.blockchainClient.Subscribe(ctx, "BlockAssembler")
		if err != nil {
			b.logger.Errorf("[BlockAssembler] error subscribing to blockchain notifications: %v", err)
			return
		}

		// variables are defined here to prevent unnecessary allocations
		var (
			bestBlockchainBlockHeader *model.BlockHeader
			meta                      *model.BlockHeaderMeta
		)

		// send out initial notification to check the best block header
		go func() {
			b.blockchainSubscriptionCh <- &blockchain.Notification{
				Type: model.NotificationType_Block,
			}
		}()

		b.currentRunningState.Store("running")

		for {
			select {
			case <-ctx.Done():
				b.logger.Infof("Stopping blockassembler as ctx is done")
				close(b.miningCandidateCh)

				return

			case <-b.resetCh:
				b.currentRunningState.Store("resetting")

				bestBlockchainBlockHeader, meta, err = b.blockchainClient.GetBestBlockHeader(ctx)
				if err != nil {
					b.logger.Errorf("[BlockAssembler][Reset] error getting best block header: %v", err)
					continue
				}

				// reset the block assembly
				b.logger.Warnf("[BlockAssembler][Reset] resetting: %d: %s -> %d: %s", b.bestBlockHeight.Load(), b.bestBlockHeader.Load().Hash(), meta.Height, bestBlockchainBlockHeader.String())

				moveBackBlocks, moveForwardBlocks, err := b.getReorgBlocks(ctx, bestBlockchainBlockHeader, meta.Height)
				if err != nil {
					b.logger.Errorf("[BlockAssembler][Reset] error getting reorg blocks: %w", err)
					continue
				}

				if len(moveBackBlocks) == 0 && len(moveForwardBlocks) == 0 {
					b.logger.Errorf("[BlockAssembler][Reset] no reorg blocks found, invalid reset")
					continue
				}

				isLegacySync, err := b.blockchainClient.IsFSMCurrentState(ctx, blockchain.FSMStateLEGACYSYNCING)
				if err != nil {
					b.logger.Errorf("[BlockAssembler][Reset] error getting FSM state: %v", err)

					// if we can't get the FSM state, we assume we are not in legacy sync, which is the default, but less optimized
					isLegacySync = false
				}

				currentHeight := meta.Height

				if response := b.subtreeProcessor.Reset(b.bestBlockHeader.Load(), moveBackBlocks, moveForwardBlocks, isLegacySync); response.Err != nil {
					b.logger.Errorf("[BlockAssembler][Reset] resetting error resetting subtree processor: %v", err)
					// something went wrong, we need to set the best block header in the block assembly to be the
					// same as the subtree processor's best block header
					bestBlockchainBlockHeader = b.subtreeProcessor.GetCurrentBlockHeader()

					_, bestBlockchainBlockHeaderMeta, err := b.blockchainClient.GetBlockHeader(ctx, bestBlockchainBlockHeader.Hash())
					if err != nil {
						b.logger.Errorf("[BlockAssembler][Reset] error getting best block header meta: %v", err)
						continue
					}

					// set the new height based on the best block header from the subtree processor
					currentHeight = bestBlockchainBlockHeaderMeta.Height
				}

				b.logger.Warnf("[BlockAssembler][Reset] resetting to new best block header: %d", meta.Height)
				b.bestBlockHeader.Store(bestBlockchainBlockHeader)
				b.bestBlockHeight.Store(currentHeight)

				if err = b.SetState(ctx); err != nil {
					b.logger.Errorf("[BlockAssembler][Reset] error setting state: %v", err)
				}

				prometheusBlockAssemblyCurrentBlockHeight.Set(float64(b.bestBlockHeight.Load()))

				b.logger.Warnf("[BlockAssembler][Reset] setting wait count to 2 for getMiningCandidate")
				b.resetWaitCount.Store(2) // wait 2 blocks before starting to mine again
				// nolint:gosec
				b.resetWaitTime.Store(int32(time.Now().Add(20 * time.Minute).Unix()))

				b.logger.Warnf("[BlockAssembler][Reset] resetting block assembler DONE")

				// empty out the reset channel
				for len(b.resetCh) > 0 {
					<-b.resetCh
				}

				b.currentRunningState.Store("running")

			case responseCh := <-b.miningCandidateCh:
				b.currentRunningState.Store("miningCandidate")
				// start, stat, _ := util.NewStatFromContext(context, "miningCandidateCh", channelStats)
				// wait for the reset to complete before getting a new mining candidate
				// 2 blocks && at least 20 minutes

				// nolint:gosec
				if b.resetWaitCount.Load() > 0 || int32(time.Now().Unix()) <= b.resetWaitTime.Load() {
					b.logger.Warnf("[BlockAssembler] skipping mining candidate, waiting for reset to complete: %d blocks or until %s", b.resetWaitCount.Load(), time.Unix(int64(b.resetWaitTime.Load()), 0).String())
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
					}
				}
				// stat.AddTime(start)
				b.currentRunningState.Store("running")

			case notification := <-b.blockchainSubscriptionCh:
				b.currentRunningState.Store("blockchainSubscription")

				if notification.Type == model.NotificationType_Block {
					b.UpdateBestBlock(ctx)
				}
			} // select
		} // for
	}()
}

// UpdateBestBlock updates the best block information.
//
// Parameters:
//   - ctx: Context for cancellation
func (b *BlockAssembler) UpdateBestBlock(ctx context.Context) {
	_, _, deferFn := tracing.StartTracing(ctx, "UpdateBestBlock",
		tracing.WithParentStat(b.stats),
		tracing.WithHistogram(prometheusBlockAssemblerUpdateBestBlock),
		tracing.WithLogMessage(b.logger, "[UpdateBestBlock] called"),
	)
	defer func() {
		b.currentRunningState.Store("running")
		deferFn()
	}()

	bestBlockchainBlockHeader, meta, err := b.blockchainClient.GetBestBlockHeader(ctx)
	if err != nil {
		b.logger.Errorf("[BlockAssembler] error getting best block header: %v", err)
		return
	}

	b.logger.Infof("[BlockAssembler][%s] new best block header: %d", bestBlockchainBlockHeader.Hash(), meta.Height)

	defer b.logger.Infof("[BlockAssembler][%s] new best block header: %d DONE", bestBlockchainBlockHeader.Hash(), meta.Height)

	prometheusBlockAssemblyBestBlockHeight.Set(float64(meta.Height))

	switch {
	case bestBlockchainBlockHeader.Hash().IsEqual(b.bestBlockHeader.Load().Hash()):
		b.logger.Infof("[BlockAssembler][%s] best block header is the same as the current best block header: %s", bestBlockchainBlockHeader.Hash(), b.bestBlockHeader.Load().Hash())
		return
	case !bestBlockchainBlockHeader.HashPrevBlock.IsEqual(b.bestBlockHeader.Load().Hash()):
		b.logger.Infof("[BlockAssembler][%s] best block header is not the same as the previous best block header, reorging: %s", bestBlockchainBlockHeader.Hash(), b.bestBlockHeader.Load().Hash())
		b.currentRunningState.Store("reorging")

		err = b.handleReorg(ctx, bestBlockchainBlockHeader, meta.Height)
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

		b.currentRunningState.Store("movingUp")

		if err = b.subtreeProcessor.MoveForwardBlock(block); err != nil {
			b.logger.Errorf("[BlockAssembler][%s] error moveForwardBlock in subtree processor: %v", bestBlockchainBlockHeader.Hash(), err)
			return
		}
	}

	b.bestBlockHeader.Store(bestBlockchainBlockHeader)
	b.bestBlockHeight.Store(meta.Height)

	prometheusBlockAssemblyCurrentBlockHeight.Set(float64(b.bestBlockHeight.Load()))

	if b.resetWaitCount.Load() > 0 {
		// decrement the reset wait count, we just found and processed a block
		b.resetWaitCount.Add(-1)
		b.logger.Warnf("[BlockAssembler] decremented getMiningCandidate wait count: %d", b.resetWaitCount.Load())
	}

	if err = b.SetState(ctx); err != nil {
		b.logger.Errorf("[BlockAssembler][%s] error setting state: %v", bestBlockchainBlockHeader.Hash(), err)
	}

	currentDifficulty, err := b.blockchainClient.GetNextWorkRequired(ctx, bestBlockchainBlockHeader.Hash())
	if err != nil {
		b.logger.Errorf("[BlockAssembler][%s] error getting next work required: %v", bestBlockchainBlockHeader.Hash(), err)
	}

	b.currentDifficulty.Store(currentDifficulty)
}

// GetCurrentRunningState returns the current operational state.
//
// Returns:
//   - string: Current state description
func (b *BlockAssembler) GetCurrentRunningState() string {
	return b.currentRunningState.Load().(string)
}

// Start initializes and begins the block assembler operations.
//
// Parameters:
//   - ctx: Context for cancellation
//
// Returns:
//   - error: Any error encountered during startup
func (b *BlockAssembler) Start(ctx context.Context) error {
	bestBlockHeader, bestBlockHeight, err := b.GetState(ctx)
	b.bestBlockHeight.Store(bestBlockHeight)
	b.bestBlockHeader.Store(bestBlockHeader)

	if err != nil {
		// TODO what is the best way to handle errors wrapped in grpc rpc errors?
		if strings.Contains(err.Error(), sql.ErrNoRows.Error()) {
			b.logger.Warnf("[BlockAssembler] no state found in blockchain db")
		} else {
			b.logger.Errorf("[BlockAssembler] error getting state from blockchain db: %v", err)
		}
	} else {
		b.logger.Infof("[BlockAssembler] setting best block header from state: %d: %s", b.bestBlockHeight.Load(), b.bestBlockHeader.Load().Hash())
		b.subtreeProcessor.SetCurrentBlockHeader(b.bestBlockHeader.Load())
	}

	// we did not get any state back from the blockchain db, so we get the current best block header
	if b.bestBlockHeader.Load() == nil || b.bestBlockHeight.Load() == 0 {
		header, meta, err := b.blockchainClient.GetBestBlockHeader(ctx)
		if err != nil {
			b.logger.Errorf("[BlockAssembler] error getting best block header: %v", err)
		} else {
			b.logger.Infof("[BlockAssembler] setting best block header from GetBestBlockHeader: %s", b.bestBlockHeader.Load().Hash())

			b.bestBlockHeader.Store(header)
			b.bestBlockHeight.Store(meta.Height)
			b.subtreeProcessor.SetCurrentBlockHeader(b.bestBlockHeader.Load())
		}
	}

	currentDifficulty, err := b.blockchainClient.GetNextWorkRequired(ctx, b.bestBlockHeader.Load().Hash())
	if err != nil {
		b.logger.Errorf("[BlockAssembler] error getting next work required: %v", err)
	}

	b.currentDifficulty.Store(currentDifficulty)

	if err = b.SetState(ctx); err != nil {
		b.logger.Errorf("[BlockAssembler] error setting state: %v", err)
	}

	b.startChannelListeners(ctx)

	prometheusBlockAssemblyCurrentBlockHeight.Set(float64(b.bestBlockHeight.Load()))

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
func (b *BlockAssembler) AddTx(node util.SubtreeNode) {
	b.subtreeProcessor.Add(node)
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

// DeDuplicateTransactions triggers deduplication of transactions in the subtree processor.
func (b *BlockAssembler) DeDuplicateTransactions() {
	b.subtreeProcessor.DeDuplicateTransactions()
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
func (b *BlockAssembler) GetMiningCandidate(_ context.Context) (*model.MiningCandidate, []*util.Subtree, error) {
	// make sure we call this on the select, so we don't get a candidate when we found a new block
	responseCh := make(chan *miningCandidateResponse)

	utils.SafeSend(b.miningCandidateCh, responseCh, 10*time.Second)

	// wait for 10 seconds for the response
	select {
	case <-time.After(10 * time.Second):
		// make sure to close the channel, otherwise the for select will hang, because no one is reading from it
		close(responseCh)
		return nil, nil, errors.NewServiceError("timeout getting mining candidate")
	case response := <-responseCh:
		return response.miningCandidate, response.subtrees, response.err
	}
}

// getMiningCandidate creates a new mining candidate from the current block state.
// This is an internal method called by GetMiningCandidate.
//
// Returns:
//   - *model.MiningCandidate: Created mining candidate
//   - []*util.Subtree: Associated subtrees
//   - error: Any error encountered during candidate creation
func (b *BlockAssembler) getMiningCandidate() (*model.MiningCandidate, []*util.Subtree, error) {
	prometheusBlockAssemblerGetMiningCandidate.Inc()

	if b.bestBlockHeader.Load() == nil {
		return nil, nil, errors.NewError("best block header is not available")
	}

	b.logger.Debugf("[BlockAssembler] getting mining candidate for header: %s", b.bestBlockHeader.Load().Hash())

	// Get the list of completed containers for the current chaintip and height...
	subtrees := b.subtreeProcessor.GetCompletedSubtreesForMiningCandidate()

	//nolint:gosec // G115: integer overflow conversion uint64 -> int (gosec)
	if b.settings.Policy.BlockMaxSize > 0 && len(subtrees) > 0 && uint64(b.settings.Policy.BlockMaxSize) < subtrees[0].SizeInBytes {
		b.logger.Warnf("[BlockAssembler] max block size is less than the size of the subtree: %d < %d", b.settings.Policy.BlockMaxSize, subtrees[0].SizeInBytes)

		return nil, nil, errors.NewProcessingError("max block size is less than the size of the subtree")
	}

	var coinbaseValue uint64

	// Get the hash of the last subtree in the list...
	// We do this by using the same subtree processor logic to get the top tree hash.
	id := &chainhash.Hash{}

	var txCount uint32

	var sizeWithoutCoinbase uint64

	var subtreesToInclude []*util.Subtree

	var subtreeBytesToInclude [][]byte

	var coinbaseMerkleProofBytes [][]byte

	// set the size without the coinbase to the size of the block header
	// This should probably have the size of the varint for the tx count as well
	// but bitcoin-sv node doesn't do that.
	sizeWithoutCoinbase = 80

	if len(subtrees) == 0 {
		txCount = 1
	} else {
		currentBlockSize := uint64(0)

		topTree, err := util.NewIncompleteTreeByLeafCount(len(subtrees))
		if err != nil {
			return nil, nil, errors.NewProcessingError("error creating top tree", err)
		}

		for _, subtree := range subtrees {
			//nolint:gosec // G115: integer overflow conversion uint64 -> int (gosec)
			if b.settings.Policy.BlockMaxSize == 0 || currentBlockSize+subtree.SizeInBytes <= uint64(b.settings.Policy.BlockMaxSize) {
				subtreesToInclude = append(subtreesToInclude, subtree)
				subtreeBytesToInclude = append(subtreeBytesToInclude, subtree.RootHash().CloneBytes())
				coinbaseValue += subtree.Fees
				currentBlockSize += subtree.SizeInBytes
				_ = topTree.AddNode(*subtree.RootHash(), subtree.Fees, subtree.SizeInBytes)
				// nolint:gosec // G404: Use of weak random number generator (math/rand instead of crypto/rand) (gosec)
				txCount += uint32(len(subtree.Nodes))

				sizeWithoutCoinbase += subtree.SizeInBytes
			} else {
				break
			}
		}

		if len(subtreesToInclude) > 0 {
			coinbaseMerkleProof, err := util.GetMerkleProofForCoinbase(subtreesToInclude)
			if err != nil {
				return nil, nil, errors.NewProcessingError("error getting merkle proof for coinbase", err)
			}

			for _, hash := range coinbaseMerkleProof {
				coinbaseMerkleProofBytes = append(coinbaseMerkleProofBytes, hash.CloneBytes())
			}
		}

		id = topTree.RootHash()
	}

	nBits, err := b.getNextNbits()
	if err != nil {
		return nil, nil, err
	}

	currentDifficulty := b.currentDifficulty.Load()
	if nBits == nil {
		if currentDifficulty != nil {
			b.logger.Warnf("nextNbits is nil. Setting to current difficulty")

			nBits = currentDifficulty
		} else {
			b.logger.Warnf("nextNbits and current difficulty are nil. Setting to pow limit bits")

			bitsBytes := make([]byte, 4)
			binary.LittleEndian.PutUint32(bitsBytes, b.settings.ChainCfgParams.PowLimitBits)

			nBits, err = model.NewNBitFromSlice(bitsBytes)
			if err != nil {
				return nil, nil, errors.NewBlockInvalidError("failed to create NBit from Bits", err)
			}

			b.currentDifficulty.Store(nBits)
		}
	} else {
		b.currentDifficulty.Store(nBits)
	}
	//nolint:gosec // G404: Use of weak random number generator (math/rand instead of crypto/rand) (gosec)
	timeNow := uint32(time.Now().Unix())
	timeBytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(timeBytes, timeNow)

	coinbaseValue += util.GetBlockSubsidyForHeight(b.bestBlockHeight.Load()+1, b.settings.ChainCfgParams)

	previousHash := b.bestBlockHeader.Load().Hash().CloneBytes()
	miningCandidate := &model.MiningCandidate{
		// create a job ID from the top tree hash and the previous block hash, to prevent empty block job id collisions
		Id:                  chainhash.HashB(append(append(id[:], previousHash...), timeBytes...)),
		PreviousHash:        previousHash,
		CoinbaseValue:       coinbaseValue,
		Version:             0x20000000,
		NBits:               nBits.CloneBytes(),
		Height:              b.bestBlockHeight.Load() + 1,
		Time:                timeNow,
		MerkleProof:         coinbaseMerkleProofBytes,
		NumTxs:              txCount,
		SizeWithoutCoinbase: sizeWithoutCoinbase,
		// nolint:gosec
		SubtreeCount:  uint32(len(subtreesToInclude)),
		SubtreeHashes: subtreeBytesToInclude,
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

	moveBackBlocks, moveForwardBlocks, err := b.getReorgBlocks(ctx, header, height)
	if err != nil {
		return errors.NewProcessingError("error getting reorg blocks", err)
	}

	if (len(moveBackBlocks) > 5 || len(moveForwardBlocks) > 5) && b.bestBlockHeight.Load() > 1000 {
		// large reorg, log it and Reset the block assembler
		b.Reset()

		return errors.NewBlockAssemblyResetError("large reorg, moveBackBlocks: %d, moveForwardBlocks: %d, resetting block assembly", len(moveBackBlocks), len(moveForwardBlocks))
	}

	// now do the reorg in the subtree processor
	if err = b.subtreeProcessor.Reorg(moveBackBlocks, moveForwardBlocks); err != nil {
		return errors.NewProcessingError("error doing reorg", err)
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
func (b *BlockAssembler) getReorgBlocks(ctx context.Context, header *model.BlockHeader, height uint32) ([]*model.Block, []*model.Block, error) {
	_, _, deferFn := tracing.StartTracing(ctx, "getReorgBlocks",
		tracing.WithParentStat(b.stats),
		tracing.WithHistogram(prometheusBlockAssemblerGetReorgBlocksDuration),
		tracing.WithLogMessage(b.logger, "[getReorgBlocks] called"),
	)
	defer deferFn()

	moveBackBlockHeaders, moveForwardBlockHeaders, err := b.getReorgBlockHeaders(ctx, header, height)
	if err != nil {
		return nil, nil, errors.NewServiceError("error getting reorg block headers", err)
	}

	// moveForwardBlocks will contain all blocks we need to move up to get to the new tip from the common ancestor
	moveForwardBlocks := make([]*model.Block, 0, len(moveForwardBlockHeaders))

	// moveBackBlocks will contain all blocks we need to move down to get to the common ancestor
	moveBackBlocks := make([]*model.Block, 0, len(moveBackBlockHeaders))

	var block *model.Block
	for _, blockHeader := range moveForwardBlockHeaders {
		block, err = b.blockchainClient.GetBlock(ctx, blockHeader.Hash())
		if err != nil {
			return nil, nil, errors.NewServiceError("error getting block", err)
		}

		moveForwardBlocks = append(moveForwardBlocks, block)
	}

	for _, blockHeader := range moveBackBlockHeaders {
		block, err = b.blockchainClient.GetBlock(ctx, blockHeader.Hash())
		if err != nil {
			return nil, nil, errors.NewServiceError("error getting block", err)
		}

		moveBackBlocks = append(moveBackBlocks, block)
	}

	return moveBackBlocks, moveForwardBlocks, nil
}

// getReorgBlockHeaders returns the block headers that need to be moved down and up to get to the new tip
// it is based on a common ancestor between the current chain and the new chain
// TODO optimize this function
func (b *BlockAssembler) getReorgBlockHeaders(ctx context.Context, header *model.BlockHeader, height uint32) ([]*model.BlockHeader, []*model.BlockHeader, error) {
	if header == nil {
		return nil, nil, errors.NewError("header is nil")
	}

	bestBlockHeight := b.bestBlockHeight.Load()
	// Get block locators for both chains
	currentChainLocator, err := b.blockchainClient.GetBlockLocator(ctx, b.bestBlockHeader.Load().Hash(), bestBlockHeight)
	if err != nil {
		return nil, nil, errors.NewServiceError("error getting block locator for current chain", err)
	}

	// newChainLocator, err := b.blockchainClient.GetBlockLocator(ctx, header.Hash(), height)
	newChainLocator, err := b.blockchainClient.GetBlockLocator(ctx, header.Hash(), bestBlockHeight+1) // start from the best block header so that the locator returned works in step with the current chain locator
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
	if commonAncestor == nil {
		return nil, nil, errors.NewProcessingError("common ancestor not found, reorg not possible")
	}

	// Get headers from current tip down to common ancestor
	headerCount := bestBlockHeight - commonAncestorMeta.Height + 1

	moveBackBlockHeaders, _, err := b.blockchainClient.GetBlockHeaders(ctx, b.bestBlockHeader.Load().Hash(), uint64(headerCount))
	if err != nil {
		return nil, nil, errors.NewServiceError("error getting current chain headers", err)
	}

	// Handle empty moveBackBlockHeaders or when length is 1 (only common ancestor)
	var filteredMoveBack []*model.BlockHeader
	if len(moveBackBlockHeaders) > 1 {
		filteredMoveBack = moveBackBlockHeaders[:len(moveBackBlockHeaders)-1]
	}

	// Get headers from new tip down to common ancestor
	moveForwardBlockHeaders, _, err := b.blockchainClient.GetBlockHeaders(ctx, header.Hash(), uint64(height-commonAncestorMeta.Height))
	if err != nil {
		return nil, nil, errors.NewServiceError("error getting new chain headers", err)
	}

	// reverse moveForwardBlocks slice
	for i := len(moveForwardBlockHeaders)/2 - 1; i >= 0; i-- {
		opp := len(moveForwardBlockHeaders) - 1 - i
		moveForwardBlockHeaders[i], moveForwardBlockHeaders[opp] = moveForwardBlockHeaders[opp], moveForwardBlockHeaders[i]
	}

	maxGetReorgHashes := b.settings.BlockAssembly.MaxGetReorgHashes
	if len(filteredMoveBack) > maxGetReorgHashes {
		b.logger.Errorf("reorg is too big, max block reorg: current hash: %s, current height: %d, new hash: %s, new height: %d, common ancestor hash: %s, common ancestor height: %d, move down block count: %d, move up block count: %d, current locator: %v, new block locator: %v", b.bestBlockHeader.Load().Hash(), b.bestBlockHeight.Load(), header.Hash(), height, commonAncestor.Hash(), commonAncestorMeta.Height, len(filteredMoveBack), len(moveForwardBlockHeaders), currentChainLocator, newChainLocator)
		return nil, nil, errors.NewProcessingError("reorg is too big, max block reorg: %d", maxGetReorgHashes)
	}

	return filteredMoveBack, moveForwardBlockHeaders, nil
}

// getNextNbits retrieves the next required work difficulty target.
//
// Returns:
//   - *model.NBit: Next difficulty target
//   - error: Any error encountered during retrieval
func (b *BlockAssembler) getNextNbits() (*model.NBit, error) {
	nbit, err := b.blockchainClient.GetNextWorkRequired(context.Background(), b.bestBlockHeader.Load().Hash())
	if err != nil {
		return nil, errors.NewProcessingError("error getting next work required", err)
	}

	return nbit, nil
}
