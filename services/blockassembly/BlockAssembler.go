package blockassembly

import (
	"context"
	"database/sql"
	"encoding/binary"
	"fmt"
	"math"
	"strings"
	"sync"
	"time"

	"github.com/bitcoin-sv/ubsv/model"
	"github.com/bitcoin-sv/ubsv/services/blockassembly/subtreeprocessor"
	"github.com/bitcoin-sv/ubsv/services/blockchain"
	"github.com/bitcoin-sv/ubsv/stores/blob"
	utxostore "github.com/bitcoin-sv/ubsv/stores/utxo"
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
	logger           utils.Logger
	utxoStore        utxostore.Interface
	subtreeStore     blob.Store
	blockchainClient blockchain.ClientI
	subtreeProcessor *subtreeprocessor.SubtreeProcessor

	miningCandidateCh        chan chan *miningCandidateResponse
	bestBlockHeader          *model.BlockHeader
	bestBlockHeight          uint32
	currentChain             []*model.BlockHeader
	currentChainMap          map[chainhash.Hash]uint32
	currentChainMapMu        sync.RWMutex
	blockchainSubscriptionCh chan *model.Notification
	maxBlockReorgRollback    int
	maxBlockReorgCatchup     int
}

func NewBlockAssembler(ctx context.Context, logger utils.Logger, utxoStore utxostore.Interface,
	subtreeStore blob.Store, blockchainClient blockchain.ClientI, newSubtreeChan chan *util.Subtree) *BlockAssembler {

	maxBlockReorgRollback, _ := gocore.Config().GetInt("blockassembly_maxBlockReorgRollback", 100)
	maxBlockReorgCatchup, _ := gocore.Config().GetInt("blockassembly_maxBlockReorgCatchup", 100)

	b := &BlockAssembler{
		logger:                logger,
		utxoStore:             utxoStore,
		subtreeStore:          subtreeStore,
		blockchainClient:      blockchainClient,
		subtreeProcessor:      subtreeprocessor.NewSubtreeProcessor(ctx, logger, subtreeStore, utxoStore, newSubtreeChan),
		miningCandidateCh:     make(chan chan *miningCandidateResponse),
		currentChainMap:       make(map[chainhash.Hash]uint32, maxBlockReorgCatchup),
		maxBlockReorgRollback: maxBlockReorgRollback,
		maxBlockReorgCatchup:  maxBlockReorgCatchup,
	}

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

func (b *BlockAssembler) startChannelListeners(context context.Context) {
	var err error

	// start a subscription for the best block header
	// this will be used to reset the subtree processor when a new block is mined
	go func() {
		b.blockchainSubscriptionCh, err = b.blockchainClient.Subscribe(context, "BlockAssembler")
		if err != nil {
			b.logger.Errorf("[BlockAssembler] error subscribing to blockchain notifications: %v", err)
			return
		}

		// variables are defined here to prevent unnecessary allocations
		var block *model.Block
		var bestBlockchainBlockHeader *model.BlockHeader
		var meta *model.BlockHeaderMeta

		for {
			select {
			case <-context.Done():
				b.logger.Infof("Stopping blockassembler as ctx is done")
				close(b.miningCandidateCh)
				close(b.blockchainSubscriptionCh)
				return

			case responseCh := <-b.miningCandidateCh:
				start, stat, _ := util.NewStatFromContext(context, "miningCandidateCh", channelStats)
				miningCandidate, subtrees, err := b.getMiningCandidate()
				responseCh <- &miningCandidateResponse{
					miningCandidate: miningCandidate,
					subtrees:        subtrees,
					err:             err,
				}
				stat.AddTime(start)

			case notification := <-b.blockchainSubscriptionCh:
				switch notification.Type {
				case model.NotificationType_Block:
					_, _, ctx := util.NewStatFromContext(context, "blockchainSubscriptionCh", channelStats)
					bestBlockchainBlockHeader, meta, err = b.blockchainClient.GetBestBlockHeader(ctx)
					if err != nil {
						b.logger.Errorf("[BlockAssembler] error getting best block header: %v", err)
						continue
					}
					b.logger.Infof("[BlockAssembler] new best block header: %d: %s", meta.Height, bestBlockchainBlockHeader.Hash())

					if bestBlockchainBlockHeader.Hash().IsEqual(b.bestBlockHeader.Hash()) {
						b.logger.Infof("[BlockAssembler] best block header is the same as the current best block header: %s", b.bestBlockHeader.Hash())
						// we already have this block, nothing to do
						continue
					} else if !bestBlockchainBlockHeader.HashPrevBlock.IsEqual(b.bestBlockHeader.Hash()) {
						b.logger.Infof("[BlockAssembler] best block header is not the same as the previous best block header, reorging: %s", b.bestBlockHeader.Hash())
						err = b.handleReorg(ctx, bestBlockchainBlockHeader)
						if err != nil {
							b.logger.Errorf("[BlockAssembler] error handling reorg: %v", err)
							continue
						}
					} else {
						b.logger.Infof("[BlockAssembler] best block header is the same as the previous best block header, moving up: %s", b.bestBlockHeader.Hash())
						if block, err = b.blockchainClient.GetBlock(ctx, bestBlockchainBlockHeader.Hash()); err != nil {
							b.logger.Errorf("[BlockAssembler] error getting block from blockchain: %v", err)
							continue
						}

						if err = b.subtreeProcessor.MoveUpBlock(block); err != nil {
							b.logger.Errorf("[BlockAssembler] error moveUpBlock in subtree processor: %v", err)
							continue
						}
					}

					b.bestBlockHeader = bestBlockchainBlockHeader
					b.bestBlockHeight = meta.Height

					err = b.SetState(ctx)
					if err != nil {
						b.logger.Errorf("[BlockAssembler] error setting state: %v", err)
					}

					err = b.setCurrentChain(ctx)
					if err != nil {
						b.logger.Errorf("[BlockAssembler] error setting current chain: %v", err)
					}
				}
			}
		}
	}()
}

func (b *BlockAssembler) Start(ctx context.Context) (err error) {
	b.bestBlockHeader, b.bestBlockHeight, err = b.GetState(ctx)
	if err != nil {
		// TODO what is the best way to handle errors wrapped in grpc rpc errors?
		if strings.Contains(err.Error(), sql.ErrNoRows.Error()) {
			b.logger.Warnf("[BlockAssembler] no state found in blockchain db")
		} else {
			b.logger.Errorf("[BlockAssembler] error getting state from blockchain db: %v", err)
		}
	} else {
		b.logger.Infof("[BlockAssembler] setting best block header from state: %d: %s", b.bestBlockHeight, b.bestBlockHeader.Hash())
		b.subtreeProcessor.SetCurrentBlockHeader(b.bestBlockHeader)
	}

	// we did not get any state back from the blockchain db, so we get the current best block header
	if b.bestBlockHeader == nil || b.bestBlockHeight == 0 {
		header, meta, err := b.blockchainClient.GetBestBlockHeader(ctx)
		if err != nil {
			b.logger.Errorf("[BlockAssembler] error getting best block header: %v", err)
		} else {
			b.logger.Infof("[BlockAssembler] setting best block header from GetBestBlockHeader: %s", b.bestBlockHeader.Hash())

			b.bestBlockHeader = header
			b.bestBlockHeight = meta.Height
			b.subtreeProcessor.SetCurrentBlockHeader(b.bestBlockHeader)
		}
	}

	if err = b.SetState(ctx); err != nil {
		b.logger.Errorf("[BlockAssembler] error setting state: %v", err)
	}

	err = b.setCurrentChain(ctx)
	if err != nil {
		b.logger.Errorf("[BlockAssembler] error setting current chain: %v", err)
	}

	b.startChannelListeners(ctx)

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
	if b.bestBlockHeader == nil {
		return fmt.Errorf("bestBlockHeader is nil")
	}

	state := make([]byte, 4+len(b.bestBlockHeader.Bytes()))
	binary.LittleEndian.PutUint32(state[:4], b.bestBlockHeight)
	state = append(state[:4], b.bestBlockHeader.Bytes()...)

	b.logger.Debugf("[BlockAssembler] setting state: %d: %s", b.bestBlockHeight, b.bestBlockHeader.Hash())
	return b.blockchainClient.SetState(ctx, "BlockAssembler", state)
}

func (b *BlockAssembler) setCurrentChain(ctx context.Context) (err error) {
	b.currentChain, _, err = b.blockchainClient.GetBlockHeaders(ctx, b.bestBlockHeader.Hash(), uint64(b.maxBlockReorgCatchup))
	if err != nil {
		return fmt.Errorf("error getting block headers from blockchain: %v", err)
	}

	b.currentChainMapMu.Lock()
	b.currentChainMap = make(map[chainhash.Hash]uint32, len(b.currentChain))
	for _, blockHeader := range b.currentChain {
		b.currentChainMap[*blockHeader.Hash()] = blockHeader.Timestamp
	}
	b.currentChainMapMu.Unlock()

	return nil
}

func (b *BlockAssembler) GetCurrentChainMap() map[chainhash.Hash]uint32 {
	b.currentChainMapMu.RLock()
	defer b.currentChainMapMu.RUnlock()

	return b.currentChainMap
}

func (b *BlockAssembler) CurrentBlock() (*model.BlockHeader, uint32) {
	return b.bestBlockHeader, b.bestBlockHeight
}

func (b *BlockAssembler) AddTx(node *util.SubtreeNode) error {
	b.subtreeProcessor.Add(node)
	return nil
}

func (b *BlockAssembler) GetMiningCandidate(_ context.Context) (*model.MiningCandidate, []*util.Subtree, error) {
	// make sure we call this on the select, so we don't get a candidate when we found a new block
	responseCh := make(chan *miningCandidateResponse)
	utils.SafeSend(b.miningCandidateCh, responseCh)
	response := <-responseCh

	return response.miningCandidate, response.subtrees, response.err
}

func (b *BlockAssembler) getMiningCandidate() (*model.MiningCandidate, []*util.Subtree, error) {
	prometheusBlockAssemblerGetMiningCandidate.Inc()

	if b.bestBlockHeader == nil {
		return nil, nil, fmt.Errorf("best block header is not available")
	}

	b.logger.Debugf("[BlockAssembler] getting mining candidate for header: %s", b.bestBlockHeader.Hash())

	// Get the list of completed containers for the current chaintip and height...
	subtrees := b.subtreeProcessor.GetCompletedSubtreesForMiningCandidate()

	var coinbaseValue uint64
	for _, subtree := range subtrees {
		coinbaseValue += subtree.Fees
	}
	coinbaseValue += util.GetBlockSubsidyForHeight(b.bestBlockHeight + 1)

	// Get the hash of the last subtree in the list...
	// We do this by using the same subtree processor logic to get the top tree hash.
	id := &chainhash.Hash{}
	if len(subtrees) > 0 {
		height := int(math.Ceil(math.Log2(float64(len(subtrees)))))
		topTree := util.NewTree(height)
		for _, subtree := range subtrees {
			_ = topTree.AddNode(subtree.RootHash(), subtree.Fees, subtree.SizeInBytes)
		}
		id = topTree.RootHash()
	}

	// TODO this will need to be calculated but for now we will keep the same difficulty for all blocks
	// nBits := bestBlockHeader.Bits
	// TEMP for testing only - moved from blockchain sql store

	nBitsString, _ := gocore.Config().Get("mining_n_bits", "2000ffff") // TEMP By default, we want hashes with 2 leading zeros
	nBits := model.NewNBitFromString(nBitsString)

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

	previousHash := b.bestBlockHeader.Hash().CloneBytes()
	miningCandidate := &model.MiningCandidate{
		// create a job ID from the top tree hash and the previous block hash, to prevent empty block job id collisions
		Id:            chainhash.HashB(append(append(id[:], previousHash...), timeBytes...)),
		PreviousHash:  previousHash,
		CoinbaseValue: coinbaseValue,
		Version:       1,
		NBits:         nBits.CloneBytes(),
		Height:        b.bestBlockHeight + 1,
		Time:          timeNow,
		MerkleProof:   coinbaseMerkleProofBytes,
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

	// now do the reorg in the subtree processor
	if err = b.subtreeProcessor.Reorg(moveDownBlocks, moveUpBlocks); err != nil {
		return fmt.Errorf("error doing reorg: %w", err)
	}

	prometheusBlockAssemblerReorgDuration.Observe(time.Since(startTime).Seconds())

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
