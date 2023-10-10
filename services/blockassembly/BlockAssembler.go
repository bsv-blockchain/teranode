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
	txmetastore "github.com/bitcoin-sv/ubsv/stores/txmeta"
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
	txMetaClient     txmetastore.Store
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
	maxBlockReorg            int
	maxBlockCatchup          int
}

func NewBlockAssembler(ctx context.Context, logger utils.Logger, txMetaClient txmetastore.Store, utxoStore utxostore.Interface,
	subtreeStore blob.Store, blockchainClient blockchain.ClientI, newSubtreeChan chan *util.Subtree) *BlockAssembler {

	maxBlockReorg, _ := gocore.Config().GetInt("block_assembler_max_block_reorg", 100)
	maxBlockCatchup, _ := gocore.Config().GetInt("block_assembler_max_block_catchup", 100)

	b := &BlockAssembler{
		logger:            logger,
		txMetaClient:      txMetaClient,
		utxoStore:         utxoStore,
		subtreeStore:      subtreeStore,
		blockchainClient:  blockchainClient,
		subtreeProcessor:  subtreeprocessor.NewSubtreeProcessor(ctx, logger, subtreeStore, utxoStore, newSubtreeChan),
		miningCandidateCh: make(chan chan *miningCandidateResponse),
		currentChainMap:   make(map[chainhash.Hash]uint32, 100),
		maxBlockReorg:     maxBlockReorg,
		maxBlockCatchup:   maxBlockCatchup,
	}

	return b
}

func (b *BlockAssembler) SetMaxBlockReorg(maxBlockReorg int) {
	b.maxBlockReorg = maxBlockReorg
}

func (b *BlockAssembler) SetMaxBlockCatchup(maxBlockCatchup int) {
	b.maxBlockCatchup = maxBlockCatchup
}

func (b *BlockAssembler) TxCount() uint64 {
	return b.subtreeProcessor.TxCount()
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
		var header *model.BlockHeader
		var height uint32
		for {
			select {
			case <-ctx.Done():
				b.logger.Infof("Stopping blockassembler as ctx is done")
				close(b.miningCandidateCh)
				close(b.blockchainSubscriptionCh)
				return

			case responseCh := <-b.miningCandidateCh:
				miningCandidate, subtrees, err := b.getMiningCandidate()
				responseCh <- &miningCandidateResponse{
					miningCandidate: miningCandidate,
					subtrees:        subtrees,
					err:             err,
				}

			case notification := <-b.blockchainSubscriptionCh:
				switch notification.Type {
				case model.NotificationType_Block:
					header, height, err = b.blockchainClient.GetBestBlockHeader(ctx)
					if err != nil {
						b.logger.Errorf("[BlockAssembler] error getting best block header: %v", err)
						continue
					}

					if header.Hash().IsEqual(b.bestBlockHeader.Hash()) {
						// we already have this block, nothing to do
						continue
					} else if !header.HashPrevBlock.IsEqual(b.bestBlockHeader.Hash()) {
						err = b.handleReorg(ctx, header)
						if err != nil {
							b.logger.Errorf("[BlockAssembler] error handling reorg: %v", err)
							continue
						}
					} else {
						if block, err = b.blockchainClient.GetBlock(ctx, header.Hash()); err != nil {
							b.logger.Errorf("[BlockAssembler] error getting block from blockchain: %v", err)
							continue
						}

						if err = b.subtreeProcessor.MoveUpBlock(block); err != nil {
							b.logger.Errorf("[BlockAssembler] error moveUpBlock in subtree processor: %v", err)
							continue
						}
					}

					b.bestBlockHeader = header
					b.bestBlockHeight = height

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
		b.bestBlockHeader, b.bestBlockHeight, err = b.blockchainClient.GetBestBlockHeader(ctx)
		if err != nil {
			b.logger.Errorf("[BlockAssembler] error getting best block header: %v", err)
		} else {
			b.logger.Infof("[BlockAssembler] setting best block header from GetBestBlockHeader: %s", b.bestBlockHeader.Hash())
			b.subtreeProcessor.SetCurrentBlockHeader(b.bestBlockHeader)
		}
	}

	if err = b.SetState(ctx); err != nil {
		b.logger.Errorf("[BlockAssembler] error setting state: %v", err)
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
	b.currentChain, _, err = b.blockchainClient.GetBlockHeaders(ctx, b.bestBlockHeader.Hash(), 100)
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

	if len(moveDownBlocks) > b.maxBlockReorg {
		return fmt.Errorf("reorg is too big, not handling: %d", len(moveDownBlocks))
	}

	if len(moveUpBlocks) > b.maxBlockCatchup {
		return fmt.Errorf("catchup is too big, not handling: %d", len(moveUpBlocks))
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

	newChain, _, err := b.blockchainClient.GetBlockHeaders(ctx, header.Hash(), 100)
	if err != nil {
		return nil, nil, fmt.Errorf("error getting new chain: %w", err)
	}

	// moveUpBlockHeaders will contain all block headers we need to move up to get to the new tip from the common ancestor
	moveUpBlockHeaders := make([]*model.BlockHeader, 0, 100)

	// moveDownBlocks will contain all blocks we need to move down to get to the common ancestor
	moveDownBlockHeaders := make([]*model.BlockHeader, 0, 100)

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

	return moveDownBlockHeaders, moveUpBlockHeaders, nil
}
