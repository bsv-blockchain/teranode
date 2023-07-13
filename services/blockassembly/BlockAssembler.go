package blockassembly

import (
	"context"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/TAAL-GmbH/ubsv/model"
	"github.com/TAAL-GmbH/ubsv/services/blockassembly/subtreeprocessor"
	"github.com/TAAL-GmbH/ubsv/services/blockchain"
	"github.com/TAAL-GmbH/ubsv/stores/blob"
	txmeta_store "github.com/TAAL-GmbH/ubsv/stores/txmeta"
	utxostore "github.com/TAAL-GmbH/ubsv/stores/utxo"
	"github.com/TAAL-GmbH/ubsv/util"
	"github.com/libsv/go-p2p/chaincfg/chainhash"
	"github.com/ordishs/go-utils"
)

type miningCandidateResponse struct {
	miningCandidate *model.MiningCandidate
	subtrees        []*util.Subtree
	err             error
}

type BlockAssembler struct {
	mu               sync.RWMutex
	logger           utils.Logger
	txMetaClient     txmeta_store.Store
	utxoStore        utxostore.Interface
	subtreeStore     blob.Store
	blockchainClient blockchain.ClientI
	subtreeProcessor *subtreeprocessor.SubtreeProcessor

	miningCandidateCh chan chan *miningCandidateResponse
	bestBlockHeader   *model.BlockHeader
	bestBlockHeight   uint32
}

func NewBlockAssembler(logger utils.Logger, txMetaClient txmeta_store.Store, utxoStore utxostore.Interface,
	subtreeStore blob.Store, blockchainClient blockchain.ClientI, newSubtreeChan chan *util.Subtree) *BlockAssembler {

	b := &BlockAssembler{
		logger:            logger,
		txMetaClient:      txMetaClient,
		utxoStore:         utxoStore,
		subtreeStore:      subtreeStore,
		blockchainClient:  blockchainClient,
		subtreeProcessor:  subtreeprocessor.NewSubtreeProcessor(logger, subtreeStore, newSubtreeChan),
		miningCandidateCh: make(chan chan *miningCandidateResponse),
	}

	// start a subscription for the best block header
	// this will be used to reset the subtree processor when a new block is mined
	var err error
	var bestBlockHeaderCh chan *blockchain.BestBlockHeader
	go func() {
		b.bestBlockHeader, b.bestBlockHeight, err = b.blockchainClient.GetBestBlockHeader(context.Background())
		if err != nil {
			logger.Errorf("[BlockAssembler] error getting best block header: %v", err)
			return
		}

		bestBlockHeaderCh, err = b.blockchainClient.SubscribeBestBlockHeader(context.Background())
		if err != nil {
			logger.Errorf("[BlockAssembler] error subscribing to best block header: %v", err)
			return
		}

		var block *model.Block
		for {
			select {
			case responseCh := <-b.miningCandidateCh:
				miningCandidate, subtrees, err := b.getMiningCandidate()
				responseCh <- &miningCandidateResponse{
					miningCandidate: miningCandidate,
					subtrees:        subtrees,
					err:             err,
				}

			case header := <-bestBlockHeaderCh:
				b.logger.Infof("[BlockAssembler] best block header subscription received new header: %s", header.Header.Hash().String())
				// reset the subtree processor

				if header.Header.Hash().IsEqual(b.bestBlockHeader.Hash()) {
					// we already have this block, nothing to do
					continue
				} else if !header.Header.HashPrevBlock.IsEqual(b.bestBlockHeader.Hash()) {
					// TODO check what is going on here, maybe we are on a different tip, or need to
					// reorg, or something else

					b.logger.Errorf("[BlockAssembler] best block header subscription received new header with incorrect prev hash: %s", header.Header.HashPrevBlock.String())
					continue
				}

				b.bestBlockHeader = header.Header
				b.bestBlockHeight = header.Height

				if block, err = b.blockchainClient.GetBlock(context.Background(), header.Header.Hash()); err != nil {
					b.logger.Errorf("[BlockAssembler] error getting block from blockchain: %v", err)
					continue
				}

				if err = b.subtreeProcessor.MoveUpBlock(block); err != nil {
					b.logger.Errorf("[BlockAssembler] error resetting subtree processor: %v", err)
					continue
				}
			}
		}
	}()

	return b
}

func (b *BlockAssembler) CurrentBlock() (*model.BlockHeader, uint32) {
	return b.bestBlockHeader, b.bestBlockHeight
}

func (b *BlockAssembler) AddTx(ctx context.Context, txHash *chainhash.Hash) error {
	startTime := time.Now()

	txMetadata, err := b.txMetaClient.Get(ctx, txHash)
	if err != nil {
		return err
	}

	prometheusTxMetaGetDuration.Observe(float64(time.Since(startTime).Microseconds()))

	startTime = time.Now()

	// Add all the utxo hashes to the utxostore
	for _, hash := range txMetadata.UtxoHashes {
		if resp, err := b.utxoStore.Store(context.Background(), hash); err != nil {
			return fmt.Errorf("error storing utxo (%v): %w", resp, err)
		}
	}

	prometheusUtxoStoreDuration.Observe(float64(time.Since(startTime).Microseconds()))

	startTime = time.Now()

	b.subtreeProcessor.Add(*txHash, txMetadata.Fee)

	prometheusSubtreeAddToChannelDuration.Observe(float64(time.Since(startTime).Microseconds()))

	prometheusBlockAssemblyAddTx.Inc()

	return nil
}

func (b *BlockAssembler) GetMiningCandidate(_ context.Context) (*model.MiningCandidate, []*util.Subtree, error) {
	// make sure we call this on the select, so we don't get a candidate when we found a new block
	responseCh := make(chan *miningCandidateResponse)
	b.miningCandidateCh <- responseCh
	response := <-responseCh

	return response.miningCandidate, response.subtrees, response.err
}

func (b *BlockAssembler) getMiningCandidate() (*model.MiningCandidate, []*util.Subtree, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	// Get the list of completed containers for the current chaintip and height...
	subtrees := b.subtreeProcessor.GetCompletedSubtreesForMiningCandidate()

	var coinbaseValue uint64
	for _, subtree := range subtrees {
		coinbaseValue += subtree.Fees
	}
	coinbaseValue += util.GetBlockSubsidyForHeight(b.bestBlockHeight + 1)

	// Get the hash of the last subtree in the list...
	id := &chainhash.Hash{}
	if len(subtrees) > 0 {
		height := int(math.Ceil(math.Log2(float64(len(subtrees)))))
		topTree := util.NewTree(height)
		for _, subtree := range subtrees {
			_ = topTree.AddNode(subtree.RootHash(), subtree.Fees)
		}
		id = topTree.RootHash()
	}

	// TODO this will need to be calculated but for now we will keep the same difficulty for all blocks
	// nBits := bestBlockHeader.Bits
	// TEMP for testing only - moved from blockchain sql store
	nBits := model.NewNBitFromString("2000ffff") // TEMP We want hashes with 2 leading zeros

	coinbaseMerkleProof, err := util.GetMerkleProofForCoinbase(subtrees)
	if err != nil {
		return nil, nil, fmt.Errorf("error getting merkle proof for coinbase: %w", err)
	}

	var coinbaseMerkleProofBytes [][]byte
	for _, hash := range coinbaseMerkleProof {
		coinbaseMerkleProofBytes = append(coinbaseMerkleProofBytes, hash.CloneBytes())
	}

	miningCandidate := &model.MiningCandidate{
		Id:            id[:],
		PreviousHash:  b.bestBlockHeader.Hash().CloneBytes(),
		CoinbaseValue: coinbaseValue,
		Version:       1,
		NBits:         nBits.CloneBytes(),
		Height:        b.bestBlockHeight + 1,
		Time:          uint32(time.Now().Unix()),
		MerkleProof:   coinbaseMerkleProofBytes,
	}

	return miningCandidate, subtrees, nil
}
