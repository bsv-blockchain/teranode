package blockassembly

import (
	"context"
	"database/sql"
	"encoding/binary"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bitcoin-sv/ubsv/stores/utxo"

	"github.com/bitcoin-sv/ubsv/model"
	"github.com/bitcoin-sv/ubsv/services/blockassembly/subtreeprocessor"
	"github.com/bitcoin-sv/ubsv/services/blockchain"
	"github.com/bitcoin-sv/ubsv/services/blockchain/blockchain_api"

	"github.com/bitcoin-sv/ubsv/stores/blob"
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
	utxoStore        utxo.Store
	subtreeStore     blob.Store
	blockchainClient blockchain.ClientI
	subtreeProcessor *subtreeprocessor.SubtreeProcessor

	miningCandidateCh          chan chan *miningCandidateResponse
	bestBlockHeader            atomic.Pointer[model.BlockHeader]
	bestBlockHeight            atomic.Uint32
	currentChain               []*model.BlockHeader
	currentChainMap            map[chainhash.Hash]uint32
	currentChainMapIDs         map[uint32]struct{}
	currentChainMapMu          sync.RWMutex
	blockchainSubscriptionCh   chan *model.Notification
	maxBlockReorgRollback      int
	maxBlockReorgCatchup       int
	difficultyAdjustmentWindow int
	difficultyAdjustment       bool
	currentDifficulty          *model.NBit
	defaultMiningNBits         *model.NBit
	resetCh                    chan struct{}
	resetWaitCount             atomic.Int32
	resetWaitTime              atomic.Int32
	currentRunningState        atomic.Value
}

func NewBlockAssembler(ctx context.Context, logger ulogger.Logger, utxoStore utxo.Store,
	subtreeStore blob.Store, blockchainClient blockchain.ClientI, newSubtreeChan chan subtreeprocessor.NewSubtreeRequest) *BlockAssembler {

	maxBlockReorgRollback, _ := gocore.Config().GetInt("blockassembly_maxBlockReorgRollback", 100)
	maxBlockReorgCatchup, _ := gocore.Config().GetInt("blockassembly_maxBlockReorgCatchup", 100)

	difficultyAdjustmentWindow, _ := gocore.Config().GetInt("difficulty_adjustment_window", 144)
	difficultyAdjustment := gocore.Config().GetBool("difficulty_adjustment", false)

	nBitsString, _ := gocore.Config().Get("mining_n_bits", "2000ffff") // TEMP By default, we want hashes with 2 leading zeros. genesis was 1d00ffff
	defaultMiningBits := model.NewNBitFromString(nBitsString)
	b := &BlockAssembler{
		logger:                     logger,
		utxoStore:                  utxoStore,
		subtreeStore:               subtreeStore,
		blockchainClient:           blockchainClient,
		subtreeProcessor:           subtreeprocessor.NewSubtreeProcessor(ctx, logger, subtreeStore, utxoStore, newSubtreeChan),
		miningCandidateCh:          make(chan chan *miningCandidateResponse),
		currentChainMap:            make(map[chainhash.Hash]uint32, maxBlockReorgCatchup),
		currentChainMapIDs:         make(map[uint32]struct{}, maxBlockReorgCatchup),
		maxBlockReorgRollback:      maxBlockReorgRollback,
		maxBlockReorgCatchup:       maxBlockReorgCatchup,
		difficultyAdjustmentWindow: difficultyAdjustmentWindow,
		difficultyAdjustment:       difficultyAdjustment,
		defaultMiningNBits:         &defaultMiningBits,
		resetCh:                    make(chan struct{}, 2),
		resetWaitCount:             atomic.Int32{},
		resetWaitTime:              atomic.Int32{},
		currentRunningState:        atomic.Value{},
	}
	b.currentRunningState.Store("starting")

	return b
}

func (b *BlockAssembler) SetMaxBlockReorg(maxBlockReorg int) {
	b.maxBlockReorgRollback = maxBlockReorg
}

func (b *BlockAssembler) SetMaxBlockCatchup(maxBlockCatchup int) {
	b.maxBlockReorgCatchup = maxBlockCatchup
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

	// start a subscription for the best block header
	// this will be used to reset the subtree processor when a new block is mined
	go func() {
		b.blockchainSubscriptionCh, err = b.blockchainClient.Subscribe(ctx, "BlockAssembler")
		if err != nil {
			b.logger.Errorf("[BlockAssembler] error subscribing to blockchain notifications: %v", err)
			return
		}

		// variables are defined here to prevent unnecessary allocations
		var block *model.Block
		var bestBlockchainBlockHeader *model.BlockHeader
		var meta *model.BlockHeaderMeta

		b.currentRunningState.Store("running")
		for {
			select {
			case <-ctx.Done():
				b.logger.Infof("Stopping blockassembler as ctx is done")
				close(b.miningCandidateCh)
				close(b.blockchainSubscriptionCh)
				return

			case <-b.resetCh:
				b.currentRunningState.Store("resetting")
				bestBlockchainBlockHeader, meta, err = b.blockchainClient.GetBestBlockHeader(ctx)
				if err != nil {
					b.logger.Errorf("[BlockAssembler][Reset] error getting best block header: %v", err)
					continue
				}

				// reset the block assembly
				b.logger.Warnf("[BlockAssembler][Reset] resetting block assembler to new best block header: %d: %s", meta.Height, bestBlockchainBlockHeader.String())

				moveDownBlocks, moveUpBlocks, err := b.getReorgBlocks(ctx, bestBlockchainBlockHeader)
				if err != nil {
					b.logger.Errorf("[BlockAssembler][Reset] error getting reorg blocks: %w", err)
					continue
				}

				currentHeight := meta.Height
				if response := b.subtreeProcessor.Reset(b.bestBlockHeader.Load(), moveDownBlocks, moveUpBlocks); response.Err != nil {
					b.logger.Errorf("[BlockAssembler][Reset] resetting error resetting subtree processor: %v", err)
					// something went wrong, we need to set the best block header in the block assembly to be the
					// same as the subtree processor's best block header
					bestBlockchainBlockHeader = b.subtreeProcessor.GetCurrentBlockHeader()

					// set the new height based on the response counts
					currentHeight = uint32(int(meta.Height) - len(response.MovedDownBlocks) + len(response.MovedUpBlocks))
				}

				b.logger.Warnf("[BlockAssembler][Reset] resetting to new best block header: %d", meta.Height)
				b.bestBlockHeader.Store(bestBlockchainBlockHeader)
				b.bestBlockHeight.Store(currentHeight)
				if err = b.SetState(ctx); err != nil {
					b.logger.Errorf("[BlockAssembler][Reset] error setting state: %v", err)
				}

				prometheusBlockAssemblyCurrentBlockHeight.Set(float64(b.bestBlockHeight.Load()))

				if err = b.setCurrentChain(ctx); err != nil {
					b.logger.Errorf("[BlockAssembler][Reset][%s] error setting current chain: %v", bestBlockchainBlockHeader.Hash(), err)
				}

				b.logger.Warnf("[BlockAssembler][Reset] setting wait count to 2 for getMiningCandidate")
				b.resetWaitCount.Store(2) // wait 2 blocks before starting to mine again
				b.resetWaitTime.Store(int32(time.Now().Add(20 * time.Minute).Unix()))

				b.logger.Warnf("[BlockAssembler][Reset] resetting block assembler DONE")
				b.currentRunningState.Store("running")

			case responseCh := <-b.miningCandidateCh:
				b.currentRunningState.Store("miningCandidate")
				// start, stat, _ := util.NewStatFromContext(context, "miningCandidateCh", channelStats)
				// wait for the reset to complete before getting a new mining candidate
				// 2 blocks && at least 20 minutes
				if b.resetWaitCount.Load() > 0 || int32(time.Now().Unix()) <= b.resetWaitTime.Load() {
					b.logger.Warnf("[BlockAssembler] skipping mining candidate, waiting for reset to complete: %d blocks or until %s", b.resetWaitCount.Load(), time.Unix(int64(b.resetWaitTime.Load()), 0).String())
					utils.SafeSend(responseCh, &miningCandidateResponse{
						err: fmt.Errorf("waiting for reset to complete"),
					})
				} else {
					// check if current state is mining
					state, err := b.blockchainClient.GetFSMCurrentState(ctx)
					if err != nil {
						// TODO: should we add retry? or do something else?
						b.logger.Errorf("[BlockValidation][checkIfMiningShouldStop] failed to get current state [%w]", err)
					}

					if state != nil && *state == blockchain_api.FSMStateType_MINING {
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
				switch notification.Type {
				case model.NotificationType_Block:
					bestBlockchainBlockHeader, meta, err = b.blockchainClient.GetBestBlockHeader(ctx)
					if err != nil {
						b.logger.Errorf("[BlockAssembler] error getting best block header: %v", err)
						continue
					}
					b.logger.Infof("[BlockAssembler][%s] new best block header: %d", bestBlockchainBlockHeader.Hash(), meta.Height)

					prometheusBlockAssemblyBestBlockHeight.Set(float64(meta.Height))

					// if the bestBlockchainBlockHeader is the same as the current best block header, we already have this block, nothing to do, skip
					if bestBlockchainBlockHeader.Hash().IsEqual(b.bestBlockHeader.Load().Hash()) {
						b.logger.Infof("[BlockAssembler][%s] best block header is the same as the current best block header: %s", bestBlockchainBlockHeader.Hash(), b.bestBlockHeader.Load().Hash())
						continue
					} else if !bestBlockchainBlockHeader.HashPrevBlock.IsEqual(b.bestBlockHeader.Load().Hash()) { // if the bestBlockchainBlockHeader's previous block is not the same as the current best block header, reorg
						b.logger.Infof("[BlockAssembler][%s] best block header is not the same as the previous best block header, reorging: %s", bestBlockchainBlockHeader.Hash(), b.bestBlockHeader.Load().Hash())
						b.currentRunningState.Store("reorging")
						err = b.handleReorg(ctx, bestBlockchainBlockHeader)
						if err != nil {
							b.logger.Errorf("[BlockAssembler][%s] error handling reorg: %v", bestBlockchainBlockHeader.Hash(), err)
							continue
						}
					} else { // if the bestBlockchainBlockHeader's previous block is the same as the current best block header, move up
						b.logger.Infof("[BlockAssembler][%s] best block header is the same as the previous best block header, moving up: %s", bestBlockchainBlockHeader.Hash(), b.bestBlockHeader.Load().Hash())
						if block, err = b.blockchainClient.GetBlock(ctx, bestBlockchainBlockHeader.Hash()); err != nil {
							b.logger.Errorf("[BlockAssembler][%s] error getting block from blockchain: %v", bestBlockchainBlockHeader.Hash(), err)
							continue
						}

						b.currentRunningState.Store("movingUp")
						if err = b.subtreeProcessor.MoveUpBlock(block); err != nil {
							b.logger.Errorf("[BlockAssembler][%s] error moveUpBlock in subtree processor: %v", bestBlockchainBlockHeader.Hash(), err)
							continue
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

					err = b.SetState(ctx)
					if err != nil {
						b.logger.Errorf("[BlockAssembler][%s] error setting state: %v", bestBlockchainBlockHeader.Hash(), err)
					}
					b.currentDifficulty, err = b.blockchainClient.GetNextWorkRequired(ctx, bestBlockchainBlockHeader.Hash())
					if err != nil {
						b.logger.Errorf("[BlockAssembler][%s] error getting next work required: %v", bestBlockchainBlockHeader.Hash(), err)
					}

					err = b.setCurrentChain(ctx)
					if err != nil {
						b.logger.Errorf("[BlockAssembler][%s] error setting current chain: %v", bestBlockchainBlockHeader.Hash(), err)
					}

					b.logger.Infof("[BlockAssembler][%s] new best block header: %d DONE", bestBlockchainBlockHeader.Hash(), meta.Height)
				}
				b.currentRunningState.Store("running")
			}
		}
	}()
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

	err = b.setCurrentChain(ctx)
	if err != nil {
		b.logger.Errorf("[BlockAssembler] error setting current chain: %v", err)
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
	if b.bestBlockHeader.Load() == nil {
		return fmt.Errorf("bestBlockHeader is nil")
	}

	state := make([]byte, 4+len(b.bestBlockHeader.Load().Bytes()))
	binary.LittleEndian.PutUint32(state[:4], b.bestBlockHeight.Load())
	state = append(state[:4], b.bestBlockHeader.Load().Bytes()...)

	b.logger.Debugf("[BlockAssembler] setting state: %d: %s", b.bestBlockHeight.Load(), b.bestBlockHeader.Load().Hash())
	return b.blockchainClient.SetState(ctx, "BlockAssembler", state)
}

func (b *BlockAssembler) setCurrentChain(ctx context.Context) (err error) {
	b.currentChain, _, err = b.blockchainClient.GetBlockHeaders(ctx, b.bestBlockHeader.Load().Hash(), uint64(b.maxBlockReorgCatchup))
	if err != nil {
		return fmt.Errorf("error getting block headers from blockchain: %v", err)
	}

	ids, err := b.blockchainClient.GetBlockHeaderIDs(ctx, b.bestBlockHeader.Load().Hash(), uint64(b.maxBlockReorgCatchup))
	if err != nil {
		return fmt.Errorf("error getting block headers from blockchain: %v", err)
	}

	b.currentChainMapMu.Lock()

	b.currentChainMap = make(map[chainhash.Hash]uint32, len(b.currentChain))
	for _, blockHeader := range b.currentChain {
		b.currentChainMap[*blockHeader.Hash()] = blockHeader.Timestamp
	}

	b.currentChainMapIDs = make(map[uint32]struct{}, len(ids))
	for _, id := range ids {
		b.currentChainMapIDs[id] = struct{}{}
	}

	b.currentChainMapMu.Unlock()

	return nil
}

func (b *BlockAssembler) GetCurrentChainMap() map[chainhash.Hash]uint32 {
	b.currentChainMapMu.RLock()
	defer b.currentChainMapMu.RUnlock()

	return b.currentChainMap
}

func (b *BlockAssembler) GetCurrentChainMapIDs() map[uint32]struct{} {
	b.currentChainMapMu.RLock()
	defer b.currentChainMapMu.RUnlock()

	return b.currentChainMapIDs
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
	b.resetCh <- struct{}{}
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
		return nil, nil, fmt.Errorf("timeout getting mining candidate")
	case response := <-responseCh:
		return response.miningCandidate, response.subtrees, response.err
	}
}

func (b *BlockAssembler) getMiningCandidate() (*model.MiningCandidate, []*util.Subtree, error) {
	prometheusBlockAssemblerGetMiningCandidate.Inc()

	if b.bestBlockHeader.Load() == nil {
		return nil, nil, fmt.Errorf("best block header is not available")
	}

	b.logger.Debugf("[BlockAssembler] getting mining candidate for header: %s", b.bestBlockHeader.Load().Hash())

	// Get the list of completed containers for the current chaintip and height...
	subtrees := b.subtreeProcessor.GetCompletedSubtreesForMiningCandidate()

	var coinbaseValue uint64
	for _, subtree := range subtrees {
		coinbaseValue += subtree.Fees
	}
	coinbaseValue += util.GetBlockSubsidyForHeight(b.bestBlockHeight.Load() + 1)

	// Get the hash of the last subtree in the list...
	// We do this by using the same subtree processor logic to get the top tree hash.
	id := &chainhash.Hash{}
	if len(subtrees) > 0 {
		topTree, err := util.NewIncompleteTreeByLeafCount(len(subtrees))
		if err != nil {
			return nil, nil, fmt.Errorf("error creating top tree: %w", err)
		}
		for _, subtree := range subtrees {
			_ = topTree.AddNode(*subtree.RootHash(), subtree.Fees, subtree.SizeInBytes)
		}
		id = topTree.RootHash()
	}

	nBits, err := b.getNextNbits()
	if err != nil {
		return nil, nil, err
	}

	var coinbaseMerkleProofBytes [][]byte
	if len(subtrees) > 0 {
		coinbaseMerkleProof, err := util.GetMerkleProofForCoinbase(subtrees)
		if err != nil {
			return nil, nil, fmt.Errorf("error getting merkle proof for coinbase: %w", err)
		}

		for _, hash := range coinbaseMerkleProof {
			coinbaseMerkleProofBytes = append(coinbaseMerkleProofBytes, hash.CloneBytes())
		}
	} else {
		coinbaseMerkleProofBytes = [][]byte{}
	}

	timeNow := uint32(time.Now().Unix())
	timeBytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(timeBytes, timeNow)

	previousHash := b.bestBlockHeader.Load().Hash().CloneBytes()
	miningCandidate := &model.MiningCandidate{
		// create a job ID from the top tree hash and the previous block hash, to prevent empty block job id collisions
		Id:            chainhash.HashB(append(append(id[:], previousHash...), timeBytes...)),
		PreviousHash:  previousHash,
		CoinbaseValue: coinbaseValue,
		Version:       1,
		NBits:         nBits.CloneBytes(),
		Height:        b.bestBlockHeight.Load() + 1,
		Time:          timeNow,
		MerkleProof:   coinbaseMerkleProofBytes,
		SubtreeCount:  uint32(len(subtrees)),
	}

	return miningCandidate, subtrees, nil
}

func (b *BlockAssembler) handleReorg(ctx context.Context, header *model.BlockHeader) error {
	startTime := time.Now()
	prometheusBlockAssemblerReorg.Inc()

	moveDownBlocks, moveUpBlocks, err := b.getReorgBlocks(ctx, header)
	if err != nil {
		return fmt.Errorf("error getting reorg blocks: %w", err)
	}

	if (len(moveDownBlocks) > 5 || len(moveUpBlocks) > 5) && b.bestBlockHeight.Load() > 1000 {
		// large reorg, log it and Reset the block assembler
		b.logger.Warnf("large reorg, moveDownBlocks: %d, moveUpBlocks: %d, resetting block assembly", len(moveDownBlocks), len(moveUpBlocks))
		b.Reset()
		return nil
	}

	// now do the reorg in the subtree processor
	if err = b.subtreeProcessor.Reorg(moveDownBlocks, moveUpBlocks); err != nil {
		return fmt.Errorf("error doing reorg: %w", err)
	}

	prometheusBlockAssemblerReorgDuration.Observe(float64(time.Since(startTime).Microseconds()) / 1_000_000)

	return nil
}

func (b *BlockAssembler) getReorgBlocks(ctx context.Context, header *model.BlockHeader) ([]*model.Block, []*model.Block, error) {
	moveDownBlockHeaders, moveUpBlockHeaders, err := b.getReorgBlockHeaders(ctx, header)
	if err != nil {
		return nil, nil, fmt.Errorf("error getting reorg block headers: %w", err)
	}

	// moveUpBlocks will contain all blocks we need to move up to get to the new tip from the common ancestor
	moveUpBlocks := make([]*model.Block, 0, len(moveUpBlockHeaders))

	// moveDownBlocks will contain all blocks we need to move down to get to the common ancestor
	moveDownBlocks := make([]*model.Block, 0, len(moveDownBlockHeaders))

	var block *model.Block
	for _, blockHeader := range moveUpBlockHeaders {
		block, err = b.blockchainClient.GetBlock(ctx, blockHeader.Hash())
		if err != nil {
			return nil, nil, fmt.Errorf("error getting block: %w", err)
		}

		moveUpBlocks = append(moveUpBlocks, block)
	}

	for _, blockHeader := range moveDownBlockHeaders {
		block, err = b.blockchainClient.GetBlock(ctx, blockHeader.Hash())
		if err != nil {
			return nil, nil, fmt.Errorf("error getting block: %w", err)
		}

		moveDownBlocks = append(moveDownBlocks, block)
	}

	return moveDownBlocks, moveUpBlocks, nil
}

func (b *BlockAssembler) getReorgBlockHeaders(ctx context.Context, header *model.BlockHeader) ([]*model.BlockHeader, []*model.BlockHeader, error) {
	if header == nil {
		return nil, nil, fmt.Errorf("header is nil")
	}

	newChain, _, err := b.blockchainClient.GetBlockHeaders(ctx, header.Hash(), uint64(b.maxBlockReorgCatchup))
	if err != nil {
		return nil, nil, fmt.Errorf("error getting new chain: %w", err)
	}

	// moveUpBlockHeaders will contain all block headers we need to move up to get to the new tip from the common ancestor
	moveUpBlockHeaders := make([]*model.BlockHeader, 0, b.maxBlockReorgCatchup)

	// moveDownBlocks will contain all blocks we need to move down to get to the common ancestor
	moveDownBlockHeaders := make([]*model.BlockHeader, 0, b.maxBlockReorgRollback)

	// find the first blockHeader that is the same in both chains
	var commonAncestor *model.BlockHeader
	for _, blockHeader := range newChain {
		// check whether the blockHeader is in the current chain
		if _, ok := b.currentChainMap[*blockHeader.Hash()]; ok {
			commonAncestor = blockHeader
			break
		}

		moveUpBlockHeaders = append(moveUpBlockHeaders, blockHeader)
	}

	if commonAncestor == nil {
		return nil, nil, fmt.Errorf("common ancestor not found, reorg not possible")
	}

	// reverse moveUpBlocks slice
	for i := len(moveUpBlockHeaders)/2 - 1; i >= 0; i-- {
		opp := len(moveUpBlockHeaders) - 1 - i
		moveUpBlockHeaders[i], moveUpBlockHeaders[opp] = moveUpBlockHeaders[opp], moveUpBlockHeaders[i]
	}

	// traverse b.currentChain in reverse order until we find the common ancestor
	// skipping the current block, start at the previous block (len-2)
	for _, blockHeader := range b.currentChain {
		if blockHeader.Hash().IsEqual(commonAncestor.Hash()) {
			break
		}

		moveDownBlockHeaders = append(moveDownBlockHeaders, blockHeader)
	}

	if len(moveDownBlockHeaders) > b.maxBlockReorgRollback {
		return nil, nil, fmt.Errorf("reorg is too big, max block reorg: %d", b.maxBlockReorgRollback)
	}

	return moveDownBlockHeaders, moveUpBlockHeaders, nil
}

func (b *BlockAssembler) getNextNbits() (*model.NBit, error) {

	now := time.Now()
	targetTimePerBlock, _ := gocore.Config().GetInt("difficulty_target_time_per_block", 600)

	thresholdSeconds := 2 * uint32(targetTimePerBlock)
	//nolint:gosec // G404: Use of weak random number generator (math/rand instead of crypto/rand) (gosec)
	randomOffset := rand.Int31n(21) - 10

	timeDifference := uint32(now.Unix()) - b.bestBlockHeader.Load().Timestamp

	b.logger.Debugf("timeDifference: %d", timeDifference)
	b.logger.Debugf("bestBlockHeader.Hash().String(): %s", b.bestBlockHeader.Load().Hash().String())

	if !b.difficultyAdjustment || (b.bestBlockHeight.Load() < uint32(b.difficultyAdjustmentWindow)+3) {
		b.logger.Debugf("no difficulty adjustment. Difficulty set to %s", b.defaultMiningNBits.String())
		return b.defaultMiningNBits, nil
	} else if timeDifference > thresholdSeconds+uint32(randomOffset) {
		// If the new block's timestamp is more than 2* 10 minutes then allow mining of a min-difficulty block.
		b.logger.Debugf("applying special difficulty rule")
		// set to start difficulty
		return b.defaultMiningNBits, nil
	} else if b.currentDifficulty != nil {
		b.logger.Debugf("setting difficulty to current difficulty %s", b.currentDifficulty.String())
		return b.currentDifficulty, nil
	} else {
		b.logger.Debugf("setting difficulty to default mining bits %s", b.defaultMiningNBits.String())
		return b.defaultMiningNBits, nil
	}
}
