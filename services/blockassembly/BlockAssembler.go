package blockassembly

import (
	"context"
	"database/sql"
	"encoding/binary"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bitcoin-sv/ubsv/errors"
	"github.com/bitcoin-sv/ubsv/model"
	"github.com/bitcoin-sv/ubsv/services/blockassembly/subtreeprocessor"
	"github.com/bitcoin-sv/ubsv/services/blockchain"
	"github.com/bitcoin-sv/ubsv/settings"
	"github.com/bitcoin-sv/ubsv/stores/blob"
	"github.com/bitcoin-sv/ubsv/stores/utxo"
	"github.com/bitcoin-sv/ubsv/tracing"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/ordishs/go-utils"
	"github.com/ordishs/gocore"
)

type miningCandidateResponse struct {
	miningCandidate *model.MiningCandidate
	subtrees        []*util.Subtree
	err             error
}

type BlockAssembler struct {
	logger           ulogger.Logger
	stats            *gocore.Stat
	settings         *settings.Settings
	utxoStore        utxo.Store
	subtreeStore     blob.Store
	blockchainClient blockchain.ClientI
	subtreeProcessor *subtreeprocessor.SubtreeProcessor

	miningCandidateCh        chan chan *miningCandidateResponse
	bestBlockHeader          atomic.Pointer[model.BlockHeader]
	bestBlockHeight          atomic.Uint32
	currentChain             []*model.BlockHeader
	currentChainMap          map[chainhash.Hash]uint32
	currentChainMapIDs       map[uint32]struct{}
	currentChainMapMu        sync.RWMutex
	blockchainSubscriptionCh chan *blockchain.Notification
	currentDifficulty        *model.NBit
	defaultMiningNBits       *model.NBit
	resetCh                  chan struct{}
	resetWaitCount           atomic.Int32
	resetWaitTime            atomic.Int32
	currentRunningState      atomic.Value
}

const DifficultyAdjustmentWindow = 144

func NewBlockAssembler(ctx context.Context, logger ulogger.Logger, tSettings *settings.Settings, stats *gocore.Stat, utxoStore utxo.Store,
	subtreeStore blob.Store, blockchainClient blockchain.ClientI, newSubtreeChan chan subtreeprocessor.NewSubtreeRequest) *BlockAssembler {

	bytesLittleEndian := make([]byte, 4)

	if tSettings.ChainCfgParams == nil {
		logger.Errorf("[BlockAssembler] chain cfg params are nil")
		return nil
	}
	binary.LittleEndian.PutUint32(bytesLittleEndian, tSettings.ChainCfgParams.PowLimitBits)

	defaultMiningBits, _ := model.NewNBitFromSlice(bytesLittleEndian)

	subtreeProcessor, _ := subtreeprocessor.NewSubtreeProcessor(ctx, logger, subtreeStore, utxoStore, newSubtreeChan)
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

	return b
}

func (b *BlockAssembler) TxCount() uint64 {
	return b.subtreeProcessor.TxCount()
}

func (b *BlockAssembler) QueueLength() int64 {
	return b.subtreeProcessor.QueueLength()
}

func (b *BlockAssembler) SubtreeCount() int {
	return b.subtreeProcessor.SubtreeCount()
}

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

				moveDownBlocks, moveUpBlocks, err := b.getReorgBlocks(ctx, bestBlockchainBlockHeader)
				if err != nil {
					b.logger.Errorf("[BlockAssembler][Reset] error getting reorg blocks: %w", err)
					continue
				}

				if len(moveDownBlocks) == 0 && len(moveUpBlocks) == 0 {
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

				if response := b.subtreeProcessor.Reset(b.bestBlockHeader.Load(), moveDownBlocks, moveUpBlocks, isLegacySync); response.Err != nil {
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

		err = b.handleReorg(ctx, bestBlockchainBlockHeader)
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

		if err = b.subtreeProcessor.MoveUpBlock(block); err != nil {
			b.logger.Errorf("[BlockAssembler][%s] error moveUpBlock in subtree processor: %v", bestBlockchainBlockHeader.Hash(), err)
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

	b.currentDifficulty, err = b.blockchainClient.GetNextWorkRequired(ctx, bestBlockchainBlockHeader.Hash())
	if err != nil {
		b.logger.Errorf("[BlockAssembler][%s] error getting next work required: %v", bestBlockchainBlockHeader.Hash(), err)
	}
}

func (b *BlockAssembler) GetCurrentRunningState() string {
	return b.currentRunningState.Load().(string)
}

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

	b.currentDifficulty, err = b.blockchainClient.GetNextWorkRequired(ctx, b.bestBlockHeader.Load().Hash())
	if err != nil {
		b.logger.Errorf("[BlockAssembler] error getting next work required: %v", err)
	}

	if err = b.SetState(ctx); err != nil {
		b.logger.Errorf("[BlockAssembler] error setting state: %v", err)
	}

	b.startChannelListeners(ctx)

	prometheusBlockAssemblyCurrentBlockHeight.Set(float64(b.bestBlockHeight.Load()))

	return nil
}

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

func (b *BlockAssembler) CurrentBlock() (*model.BlockHeader, uint32) {
	return b.bestBlockHeader.Load(), b.bestBlockHeight.Load()
}

func (b *BlockAssembler) AddTx(node util.SubtreeNode) {
	b.subtreeProcessor.Add(node)
}

func (b *BlockAssembler) RemoveTx(hash chainhash.Hash) error {
	return b.subtreeProcessor.Remove(hash)
}

func (b *BlockAssembler) DeDuplicateTransactions() {
	b.subtreeProcessor.DeDuplicateTransactions()
}

func (b *BlockAssembler) Reset() {
	// run in a go routine to prevent blocking
	go func() {
		b.resetCh <- struct{}{}
	}()
}

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

	var sizeWithoutCoinbase uint32

	var subtreesToInclude []*util.Subtree

	var coinbaseMerkleProofBytes [][]byte

	if len(subtrees) > 0 {
		currentBlockSize := uint64(0)

		topTree, err := util.NewIncompleteTreeByLeafCount(len(subtrees))
		if err != nil {
			return nil, nil, errors.NewProcessingError("error creating top tree", err)
		}

		for _, subtree := range subtrees {
			//nolint:gosec // G115: integer overflow conversion uint64 -> int (gosec)
			if b.settings.Policy.BlockMaxSize == 0 || currentBlockSize+subtree.SizeInBytes <= uint64(b.settings.Policy.BlockMaxSize) {
				subtreesToInclude = append(subtreesToInclude, subtree)
				coinbaseValue += subtree.Fees
				currentBlockSize += subtree.SizeInBytes
				_ = topTree.AddNode(*subtree.RootHash(), subtree.Fees, subtree.SizeInBytes)
				// nolint:gosec // G404: Use of weak random number generator (math/rand instead of crypto/rand) (gosec)
				txCount += uint32(len(subtree.Nodes))
				// nolint:gosec // G404: Use of weak random number generator (math/rand instead of crypto/rand) (gosec)
				sizeWithoutCoinbase += uint32(subtree.SizeInBytes)
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

	if nBits == nil {
		if b.currentDifficulty != nil {
			b.logger.Warnf("nextNbits is nil. Setting to current difficulty")
			nBits = b.currentDifficulty
		} else {
			b.logger.Warnf("nextNbits and current difficulty are nil. Setting to pow limit bits")

			bitsBytes := make([]byte, 4)
			binary.LittleEndian.PutUint32(bitsBytes, b.settings.ChainCfgParams.PowLimitBits)

			nBits, err = model.NewNBitFromSlice(bitsBytes)
			if err != nil {
				return nil, nil, errors.NewBlockInvalidError("failed to create NBit from Bits", err)
			}

			b.currentDifficulty = nBits
		}
	}
	//nolint:gosec // G404: Use of weak random number generator (math/rand instead of crypto/rand) (gosec)
	timeNow := uint32(time.Now().Unix())
	timeBytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(timeBytes, timeNow)

	coinbaseValue += util.GetBlockSubsidyForHeight(b.bestBlockHeight.Load() + 1)

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
		SubtreeCount: uint32(len(subtreesToInclude)),
	}

	return miningCandidate, subtreesToInclude, nil
}

func (b *BlockAssembler) handleReorg(ctx context.Context, header *model.BlockHeader) error {
	startTime := time.Now()

	prometheusBlockAssemblerReorg.Inc()

	moveDownBlocks, moveUpBlocks, err := b.getReorgBlocks(ctx, header)
	if err != nil {
		return errors.NewProcessingError("error getting reorg blocks", err)
	}

	if (len(moveDownBlocks) > 5 || len(moveUpBlocks) > 5) && b.bestBlockHeight.Load() > 1000 {
		// large reorg, log it and Reset the block assembler
		b.Reset()

		return errors.NewBlockAssemblyResetError("large reorg, moveDownBlocks: %d, moveUpBlocks: %d, resetting block assembly", len(moveDownBlocks), len(moveUpBlocks))
	}

	// now do the reorg in the subtree processor
	if err = b.subtreeProcessor.Reorg(moveDownBlocks, moveUpBlocks); err != nil {
		return errors.NewProcessingError("error doing reorg", err)
	}

	prometheusBlockAssemblerReorgDuration.Observe(float64(time.Since(startTime).Microseconds()) / 1_000_000)

	return nil
}

func (b *BlockAssembler) getReorgBlocks(ctx context.Context, header *model.BlockHeader) ([]*model.Block, []*model.Block, error) {
	_, _, deferFn := tracing.StartTracing(ctx, "getReorgBlocks",
		tracing.WithParentStat(b.stats),
		tracing.WithHistogram(prometheusBlockAssemblerGetReorgBlocksDuration),
		tracing.WithLogMessage(b.logger, "[getReorgBlocks] called"),
	)
	defer deferFn()

	moveDownBlockHeaders, moveUpBlockHeaders, err := b.getReorgBlockHeaders(ctx, header)
	if err != nil {
		return nil, nil, errors.NewServiceError("error getting reorg block headers", err)
	}

	// moveUpBlocks will contain all blocks we need to move up to get to the new tip from the common ancestor
	moveUpBlocks := make([]*model.Block, 0, len(moveUpBlockHeaders))

	// moveDownBlocks will contain all blocks we need to move down to get to the common ancestor
	moveDownBlocks := make([]*model.Block, 0, len(moveDownBlockHeaders))

	var block *model.Block
	for _, blockHeader := range moveUpBlockHeaders {
		block, err = b.blockchainClient.GetBlock(ctx, blockHeader.Hash())
		if err != nil {
			return nil, nil, errors.NewServiceError("error getting block", err)
		}

		moveUpBlocks = append(moveUpBlocks, block)
	}

	for _, blockHeader := range moveDownBlockHeaders {
		block, err = b.blockchainClient.GetBlock(ctx, blockHeader.Hash())
		if err != nil {
			return nil, nil, errors.NewServiceError("error getting block", err)
		}

		moveDownBlocks = append(moveDownBlocks, block)
	}

	return moveDownBlocks, moveUpBlocks, nil
}

// getReorgBlockHeaders returns the block headers that need to be moved down and up to get to the new tip
// it is based on a common ancestor between the current chain and the new chain
// TODO optimize this function
func (b *BlockAssembler) getReorgBlockHeaders(ctx context.Context, header *model.BlockHeader) ([]*model.BlockHeader, []*model.BlockHeader, error) {
	if header == nil {
		return nil, nil, errors.NewError("header is nil")
	}

	// allow this to get up to 10,000 hashes
	// this is needed for large resets of the block assembly
	// the maxBlockReorgCatchup is used to limit the number of blocks we can catch up to in the subtree processor
	maxGetReorgHashes, _ := gocore.Config().GetInt("blockassembly_maxGetReorgHashes", 10_000)

	// nolint:gosec
	maxGetHashes := uint64(maxGetReorgHashes)

	// get a much larger chain map from the current chain to determine the common ancestor
	currentChainMap := make(map[chainhash.Hash]uint32, maxGetHashes)

	currentChain, _, err := b.blockchainClient.GetBlockHeaders(ctx, b.bestBlockHeader.Load().Hash(), maxGetHashes)
	if err != nil {
		return nil, nil, errors.NewServiceError("error getting current chain", err)
	}

	for _, blockHeader := range currentChain {
		currentChainMap[*blockHeader.Hash()] = blockHeader.Timestamp
	}

	newChain, _, err := b.blockchainClient.GetBlockHeaders(ctx, header.Hash(), maxGetHashes)
	if err != nil {
		return nil, nil, errors.NewServiceError("error getting new chain", err)
	}

	// moveUpBlockHeaders will contain all block headers we need to move up to get to the new tip from the common ancestor
	moveUpBlockHeaders := make([]*model.BlockHeader, 0, len(newChain))

	// moveDownBlocks will contain all blocks we need to move down to get to the common ancestor
	moveDownBlockHeaders := make([]*model.BlockHeader, 0, len(newChain))

	// find the first blockHeader that is the same in both chains
	var commonAncestor *model.BlockHeader

	for _, blockHeader := range newChain {
		// check whether the blockHeader is in the current chain
		if _, ok := currentChainMap[*blockHeader.Hash()]; ok {
			commonAncestor = blockHeader
			break
		}

		moveUpBlockHeaders = append(moveUpBlockHeaders, blockHeader)
	}

	if commonAncestor == nil {
		return nil, nil, errors.NewProcessingError("common ancestor not found, reorg not possible")
	}

	// reverse moveUpBlocks slice
	for i := len(moveUpBlockHeaders)/2 - 1; i >= 0; i-- {
		opp := len(moveUpBlockHeaders) - 1 - i
		moveUpBlockHeaders[i], moveUpBlockHeaders[opp] = moveUpBlockHeaders[opp], moveUpBlockHeaders[i]
	}

	// traverse currentChain in reverse order until we find the common ancestor
	// skipping the current block, start at the previous block (len-2)
	for _, blockHeader := range currentChain {
		if blockHeader.Hash().IsEqual(commonAncestor.Hash()) {
			break
		}

		moveDownBlockHeaders = append(moveDownBlockHeaders, blockHeader)
	}

	//nolint:gosec // G404: Use of weak random number generator (math/rand instead of crypto/rand) (gosec)
	if len(moveDownBlockHeaders) > int(maxGetHashes) {
		return nil, nil, errors.NewProcessingError("reorg is too big, max block reorg: %d", b.settings.BlockAssembly.MaxBlockReorgRollback)
	}

	return moveDownBlockHeaders, moveUpBlockHeaders, nil
}

func (b *BlockAssembler) getNextNbits() (*model.NBit, error) {
	nbit, err := b.blockchainClient.GetNextWorkRequired(context.Background(), b.bestBlockHeader.Load().Hash())
	if err != nil {
		return nil, errors.NewProcessingError("error getting next work required", err)
	}

	return nbit, nil
}
