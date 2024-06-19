package blockpersisterintegrity

import (
	"context"
	"fmt"

	"github.com/bitcoin-sv/ubsv/model"
	p_model "github.com/bitcoin-sv/ubsv/services/blockpersister/utxoset/model"

	"github.com/bitcoin-sv/ubsv/stores/blob"
	"github.com/bitcoin-sv/ubsv/stores/blob/options"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/bitcoin-sv/ubsv/util"
)

type BlockProcessor struct {
	logger ulogger.Logger
	store  blob.Store
}

func NewBlockProcessor(logger ulogger.Logger, store blob.Store) *BlockProcessor {
	return &BlockProcessor{
		logger: logger,
		store:  store,
	}
}

func (bp *BlockProcessor) ProcessBlock(ctx context.Context, blockHeader *model.BlockHeader, blockMeta *model.BlockHeaderMeta) error {
	blockFees := uint64(0)
	height := blockMeta.Height

	blockReader, err := bp.store.GetIoReader(ctx, blockHeader.Hash().CloneBytes(), options.WithFileExtension("block"))
	// blockBytes, err := blockStore.Get(ctx, blockHeader.Hash().CloneBytes(), options.WithFileExtension("block"))
	if err != nil {
		return fmt.Errorf("failed to get block %s: %s", blockHeader.Hash(), err)
	}

	var block *model.Block
	block, err = model.NewBlockFromReader(blockReader)
	// block, err = model.NewBlockFromBytes(blockBytes)
	if err != nil {
		return fmt.Errorf("failed to parse block %s: %s", blockHeader.Hash(), err)
	}

	bp.logger.Debugf("checking block %d %s\n", height, block.Hash())
	if block.CoinbaseTx == nil || !block.CoinbaseTx.IsCoinbase() {
		return fmt.Errorf("block %s does not have a valid coinbase transaction", block.Hash())
	}

	coinbaseHeight, err := util.ExtractCoinbaseHeight(block.CoinbaseTx)
	if err != nil {
		return fmt.Errorf("failed to extract coinbase height from block coinbase %s: %s", block.Hash(), err)
	}

	if coinbaseHeight != height {
		return fmt.Errorf("coinbase height %d does not match block height %d", coinbaseHeight, height)
	}

	diff := p_model.NewUTXODiff(bp.logger, blockHeader.Hash())
	stp := NewSubtreeProcessor(bp.logger, bp.store, block, NewTxProcessor(bp.logger, diff))

	stp.tp.ProcessUtxoDiffForTx(block.CoinbaseTx)

	for _, subtreeHash := range block.Subtrees {
		err := stp.ProcessSubtree(ctx, *subtreeHash)
		if err != nil {
			bp.logger.Errorf("failed to process subtree %s: %s\n", subtreeHash, err)
		}
	}

	p := NewUTXOProcessor(bp.logger, bp.store)

	if exists, err := p.DiffExists(*blockHeader.Hash()); err != nil {
		return fmt.Errorf("failed to check if diff exists for block %s: %s", blockHeader.Hash(), err)
	} else if exists {
		if err := p.VerifyDiff(blockHeader, diff); err != nil {
			bp.logger.Errorf("failed to verify diff for block %s: %s\n", blockHeader.Hash(), err)
		}
	}

	if exists, err := p.SetExists(*blockHeader.Hash()); err != nil {
		bp.logger.Errorf("failed to check if set exists for block %s: %s\n", blockHeader.Hash(), err)
	} else if exists {
		if err := p.VerifySet(blockHeader, diff); err != nil {
			bp.logger.Errorf("failed to verify diff for block %s: %s\n", blockHeader.Hash(), err)
		}
	}

	blockReward := block.CoinbaseTx.TotalOutputSatoshis()
	blockSubsidy := util.GetBlockSubsidyForHeight(height)
	if blockFees+blockSubsidy != blockReward {
		return fmt.Errorf("block %s has incorrect fees: %d != %d\n", block.Hash(), blockFees, blockReward)
		// } else {
		// bp.logger.Debugf("block %s has %d in fees, subsidy %d\n", block.Hash(), blockFees, blockSubsidy)
	}

	return nil
}
