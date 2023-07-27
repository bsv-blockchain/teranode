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
	"github.com/TAAL-GmbH/ubsv/services/blockchain/blockchain_api"
	"github.com/TAAL-GmbH/ubsv/stores/blob"
	txmetastore "github.com/TAAL-GmbH/ubsv/stores/txmeta"
	utxostore "github.com/TAAL-GmbH/ubsv/stores/utxo"
	"github.com/TAAL-GmbH/ubsv/util"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-p2p/chaincfg/chainhash"
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
	txStore          blob.Store
	subtreeStore     blob.Store
	blockchainClient blockchain.ClientI
	subtreeProcessor *subtreeprocessor.SubtreeProcessor

	miningCandidateCh chan chan *miningCandidateResponse
	bestBlockHeader   *model.BlockHeader
	bestBlockHeight   uint32
	currentChainMap   map[chainhash.Hash]uint32
	currentChainMapMu sync.RWMutex
}

func NewBlockAssembler(ctx context.Context, logger utils.Logger, txMetaClient txmetastore.Store, utxoStore utxostore.Interface,
	txStore blob.Store, subtreeStore blob.Store, blockchainClient blockchain.ClientI, newSubtreeChan chan *util.Subtree) *BlockAssembler {

	b := &BlockAssembler{
		logger:            logger,
		txMetaClient:      txMetaClient,
		utxoStore:         utxoStore,
		txStore:           txStore,
		subtreeStore:      subtreeStore,
		blockchainClient:  blockchainClient,
		subtreeProcessor:  subtreeprocessor.NewSubtreeProcessor(logger, subtreeStore, newSubtreeChan),
		miningCandidateCh: make(chan chan *miningCandidateResponse),
		currentChainMap:   make(map[chainhash.Hash]uint32, 100),
	}

	var err error
	b.bestBlockHeader, b.bestBlockHeight, err = b.blockchainClient.GetBestBlockHeader(ctx)
	if err != nil {
		logger.Errorf("[BlockAssembler] error getting best block header: %v", err)
	} else {
		b.subtreeProcessor.SetCurrentBlockHeader(b.bestBlockHeader)
	}

	// start a subscription for the best block header
	// this will be used to reset the subtree processor when a new block is mined
	var blockchainSubscriptionCh chan *model.Notification
	go func() {
		blockchainSubscriptionCh, err = b.blockchainClient.Subscribe(context.Background())
		if err != nil {
			logger.Errorf("[BlockAssembler] error subscribing to blockchain notifications: %v", err)
			return
		}

		// variables are defined here to prevent unnecessary allocations
		var block *model.Block
		var blockHeaders []*model.BlockHeader
		var header *model.BlockHeader
		var height uint32
		var blockHeight uint32
		var txIDHash *chainhash.Hash
		var utxoHash *chainhash.Hash

		for {
			select {
			case <-ctx.Done():
				logger.Infof("Stopping blockassembler as ctx is done")
				return

			case responseCh := <-b.miningCandidateCh:
				miningCandidate, subtrees, err := b.getMiningCandidate()
				responseCh <- &miningCandidateResponse{
					miningCandidate: miningCandidate,
					subtrees:        subtrees,
					err:             err,
				}

			case notification := <-blockchainSubscriptionCh:
				switch notification.Type {
				case int32(blockchain_api.Type_Block):
					header, height, err = b.blockchainClient.GetBestBlockHeader(context.Background())
					if err != nil {
						b.logger.Errorf("[BlockAssembler] error getting best block header: %v", err)
						continue
					}

					if header.Hash().IsEqual(b.bestBlockHeader.Hash()) {
						// we already have this block, nothing to do
						continue
					} else if !header.HashPrevBlock.IsEqual(b.bestBlockHeader.Hash()) {
						// TODO check what is going on here, maybe we are on a different tip, or need to
						// reorg, or something else

						b.logger.Errorf("[BlockAssembler] best block header subscription received new header with incorrect prev utxoHash: %s", header.HashPrevBlock.String())
						continue
					}

					if block, err = b.blockchainClient.GetBlock(context.Background(), header.Hash()); err != nil {
						b.logger.Errorf("[BlockAssembler] error getting block from blockchain: %v", err)
						continue
					}

					err = b.txStore.Set(context.Background(), bt.ReverseBytes(block.CoinbaseTx.TxIDBytes()), block.CoinbaseTx.ExtendedBytes())
					if err != nil {
						b.logger.Errorf("[BlockAssembler] error storing coinbase tx in tx store: %v", err)
						continue
					}

					// Build up the items we need to store the outputs in the utxostore.  We do this here so that
					// any errors that occur will happen before we do any further processing.
					txIDHash, err = chainhash.NewHashFromStr(block.CoinbaseTx.TxID())
					if err != nil {
						b.logger.Errorf("[BlockAssembler] error getting txid utxoHash from string: %v", err)
						continue
					}

					blockHeight, err = block.ExtractCoinbaseHeight()
					if err != nil {
						b.logger.Errorf("[BlockAssembler] error extracting coinbase height: %v", err)
						continue
					}

					utxoHashes := make([]*chainhash.Hash, 0, len(block.CoinbaseTx.Outputs))
					success := true

					for i, output := range block.CoinbaseTx.Outputs {
						if output.Satoshis > 0 {
							utxoHash, err = util.UTXOHashFromOutput(txIDHash, output, uint32(i))
							if err != nil {
								b.logger.Errorf("[BlockAssembler] error getting utxo utxoHash from output: %v", err)
								continue
							}

							if resp, err := b.utxoStore.Store(ctx, utxoHash, blockHeight+100); err != nil {
								b.logger.Errorf("[BlockAssembler] error storing utxo (%v): %w", resp, err)
								success = false
								break
							}

							utxoHashes = append(utxoHashes, utxoHash)
						}
					}

					if !success {
						b.removeAllAdded(ctx, utxoHashes)
						continue
					}

					if err = b.subtreeProcessor.MoveUpBlock(block); err != nil {
						b.logger.Errorf("[BlockAssembler] error resetting subtree processor: %v", err)
						b.removeAllAdded(ctx, utxoHashes)
						continue
					}

					// Add the outputs of the coinbase to the utxostore with locktime of 100 blocks

					b.bestBlockHeader = header
					b.bestBlockHeight = height

					blockHeaders, err = b.blockchainClient.GetBlockHeaders(context.Background(), b.bestBlockHeader.Hash(), 100)
					if err != nil {
						b.logger.Errorf("[BlockAssembler] error getting block headers from blockchain: %v", err)
						// todo should we stop here, we are in a weird state
						continue
					}

					b.currentChainMapMu.Lock()
					b.currentChainMap = make(map[chainhash.Hash]uint32, 100)
					for _, blockHeader := range blockHeaders {
						// todo set the height instead of timestamp
						b.currentChainMap[*blockHeader.Hash()] = blockHeader.Timestamp
					}
					b.currentChainMapMu.Unlock()
				}
			}
		}
	}()

	return b
}

func (b *BlockAssembler) removeAllAdded(ctx context.Context, hashes []*chainhash.Hash) {
	// Remove all the utxos we added
	for _, hash := range hashes {
		// TODO should we be deleting here?
		if resp, err := b.utxoStore.Reset(ctx, hash); err != nil {
			b.logger.Errorf("[BlockAssembler] error resetting utxo (%v): %w", resp, err)
		}
	}
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

	// looking this up here and adding to the subtree processor, might create a situation where a transaction
	// that was in a block from a competing miner, is added to the subtree processor when it shouldn't
	// TODO should this be done in the subtree processor?
	if len(txMetadata.BlockHashes) > 0 {
		b.currentChainMapMu.RLock()
		for _, hash := range txMetadata.BlockHashes {
			if _, ok := b.currentChainMap[*hash]; ok {
				// the tx is already in a block on our chain, nothing to do
				return fmt.Errorf("tx already in a block on the active chain: %s", hash)
			}
		}
		b.currentChainMapMu.RUnlock()
	}

	prometheusTxMetaGetDuration.Observe(float64(time.Since(startTime).Microseconds()))

	startTime = time.Now()

	// Add all the utxo hashes to the utxostore
	for _, hash := range txMetadata.UtxoHashes {
		if resp, err := b.utxoStore.Store(context.Background(), hash, txMetadata.LockTime); err != nil {
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
	if b.bestBlockHeader == nil {
		return nil, nil, fmt.Errorf("best block header is not available")
	}

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

	previousHash := b.bestBlockHeader.Hash().CloneBytes()
	miningCandidate := &model.MiningCandidate{
		// create a job ID from the top tree hash and the previous block hash, to prevent empty block job id collisions
		Id:            chainhash.HashB(append(id[:], previousHash...)),
		PreviousHash:  previousHash,
		CoinbaseValue: coinbaseValue,
		Version:       1,
		NBits:         nBits.CloneBytes(),
		Height:        b.bestBlockHeight + 1,
		Time:          uint32(time.Now().Unix()),
		MerkleProof:   coinbaseMerkleProofBytes,
	}

	return miningCandidate, subtrees, nil
}
