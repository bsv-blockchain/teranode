package netsync

import (
	"bytes"
	"context"
	"fmt"
	"runtime"
	"time"

	"github.com/bitcoin-sv/ubsv/errors"
	"github.com/bitcoin-sv/ubsv/model"
	"github.com/bitcoin-sv/ubsv/services/legacy/bsvutil"
	"github.com/bitcoin-sv/ubsv/services/legacy/peer"
	"github.com/bitcoin-sv/ubsv/stores/blob/options"
	"github.com/bitcoin-sv/ubsv/stores/utxo/meta"
	"github.com/bitcoin-sv/ubsv/tracing"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/bscript"
	"github.com/libsv/go-bt/v2/chainhash"
	"golang.org/x/sync/errgroup"
)

func (sm *SyncManager) HandleBlockDirect(ctx context.Context, peer *peer.Peer, block *bsvutil.Block) error {
	// Make sure we have the correct height for this block before continuing
	var blockHeight uint32

	if block.Height() <= 0 {
		// Lookup block height from blockchain
		_, previousBlockHeaderMeta, err := sm.blockchainClient.GetBlockHeader(ctx, &block.MsgBlock().Header.PrevBlock)
		if err != nil {
			return errors.NewProcessingError("failed to get block header for previous block %s", block.MsgBlock().Header.PrevBlock, err)
		}
		blockHeight = previousBlockHeaderMeta.Height + 1
		block.SetHeight(int32(blockHeight))
	} else {
		blockHeight = uint32(block.Height())
	}

	ctx, _, deferFn := tracing.StartTracing(ctx, "HandleBlockDirect",
		tracing.WithDebugLogMessage(
			sm.logger,
			"[HandleBlockDirect][%s %d] processing block found from peer %s",
			block.Hash().String(),
			blockHeight,
			peer.String(),
		),
		tracing.WithTag("blockHash", block.Hash().String()),
		tracing.WithTag("peer", peer.String()),
	)
	defer deferFn()

	// 3. Create a block message with (block hash, coinbase tx and slice if 1 subtree)
	var headerBytes bytes.Buffer
	if err := block.MsgBlock().Header.Serialize(&headerBytes); err != nil {
		return errors.NewProcessingError("failed to serialize header", err)
	}

	// create the Teranode compatible block header
	header, err := model.NewBlockHeaderFromBytes(headerBytes.Bytes())
	if err != nil {
		return errors.NewProcessingError("failed to create block header from bytes", err)
	}

	var coinbase bytes.Buffer
	if err = block.Transactions()[0].MsgTx().Serialize(&coinbase); err != nil {
		return errors.NewProcessingError("failed to serialize coinbase", err)
	}

	coinbaseTx, err := bt.NewTxFromBytes(coinbase.Bytes())
	if err != nil {
		return errors.NewProcessingError("failed to create bt.Tx for coinbase", err)
	}

	// validate all subtrees and store all subtree data
	// this also should spend and create all utxos
	subtrees, err := sm.prepareSubtrees(ctx, block)
	if err != nil {
		return err
	}

	// create valid teranode block, with the subtree hash
	blockSize := block.MsgBlock().SerializeSize()
	teranodeBlock, err := model.NewBlock(header, coinbaseTx, subtrees, uint64(len(block.Transactions())), uint64(blockSize), blockHeight)
	if err != nil {
		return errors.NewProcessingError("failed to create model.NewBlock", err)
	}

	// call the process block wrapper, which will add tracing and logging
	err = sm.processBlock(ctx, teranodeBlock)
	if err != nil {
		return err
	}

	return nil
}

func (sm *SyncManager) processBlock(ctx context.Context, teranodeBlock *model.Block) error {
	ctx, _, deferFn := tracing.StartTracing(ctx, "SyncManager:processBlock",
		tracing.WithLogMessage(
			sm.logger,
			"[SyncManager:processBlock][%s %d] processing block",
			teranodeBlock.Hash().String(),
			teranodeBlock.Height,
		),
	)
	defer deferFn()

	// send the block to the blockValidation for processing and validation
	// all the block subtrees should have been validated in processSubtrees
	if err := sm.blockValidation.ProcessBlock(ctx, teranodeBlock, teranodeBlock.Height); err != nil {
		return errors.NewProcessingError("failed to process block", err)
	}

	return nil
}

type txMapWrapper struct {
	tx                 *bt.Tx
	someParentsInBlock bool
	childLevelInBlock  uint32
}

func (sm *SyncManager) prepareSubtrees(ctx context.Context, block *bsvutil.Block) ([]*chainhash.Hash, error) {
	ctx, _, deferFn := tracing.StartTracing(ctx, "prepareSubtrees",
		tracing.WithLogMessage(
			sm.logger,
			"[prepareSubtrees][%s %d] processing subtree for block",
			block.Hash().String(),
			block.Height(),
		),
	)
	defer deferFn()

	subtrees := make([]*chainhash.Hash, 0)
	blockHeight := uint32(block.Height())

	// create 1 subtree + subtree.subtreeData
	// then validate the subtree through the subtreeValidation service
	if len(block.Transactions()) > 1 {
		subtree, err := util.NewIncompleteTreeByLeafCount(len(block.Transactions()))
		if err != nil {
			return nil, errors.NewSubtreeError("failed to create subtree", err)
		}

		if err := subtree.AddNode(model.CoinbasePlaceholder, 0, 0); err != nil {
			return nil, fmt.Errorf("failed to add coinbase placeholder: %w", err)
		}

		// subtreeData contains the extended tx bytes of all transactions references in the subtree
		// except the coinbase transaction
		subtreeData := util.NewSubtreeData(subtree)

		txMap, err := sm.createTxMap(block)
		if err != nil {
			return nil, err
		}

		g, gCtx := errgroup.WithContext(ctx)
		g.SetLimit(runtime.NumCPU() * 4)
		for _, wireTx := range block.Transactions() {
			txHash := *wireTx.Hash()

			// the coinbase transaction is not part of the txMap
			if txWrapper, found := txMap[txHash]; found {
				tx := txWrapper.tx
				txSize := uint64(tx.Size())

				if err = subtree.AddNode(txHash, 0, txSize); err != nil {
					return nil, fmt.Errorf("failed to add node (%s) to subtree: %w", txHash, err)
				}
				// we need to match the indexes of the subtree and the tx data in subtreeData
				currentIdx := subtree.Length() - 1

				g.Go(func() error {
					if err := sm.extendTransaction(tx, txMap); err != nil {
						return fmt.Errorf("failed to extend transaction: %w", err)
					}

					// store the extended transaction in our subtree tx data file
					if err = subtreeData.AddTx(tx, currentIdx); err != nil {
						return fmt.Errorf("failed to add tx to subtree data: %w", err)
					}

					return nil
				})
			}
		}

		// wait for all tx to be processed - we don't need to process errors here
		if err = g.Wait(); err != nil {
			return nil, errors.NewProcessingError("failed to process transactions", err)
		}

		maxLevel, blockTxsPerLevel := sm.prepareTxsPerLevel(ctx, block, txMap)

		// try to pre-validate the transactions through the validation, to speed up subtree validation later on.
		// This allows us to process all the transactions that are not referencing transactions from this current block
		// to be processed in parallel.
		for i := uint32(0); i <= maxLevel; i++ {
			// process all the transactions on a certain level in parallel
			g, gCtx = errgroup.WithContext(ctx)
			g.SetLimit(runtime.NumCPU() * 4)
			for _, tx := range blockTxsPerLevel[i] {
				g.Go(func() error {
					// send to validation, but only if the parent is not in the same block
					_ = sm.validationClient.Validate(gCtx, tx, blockHeight)

					return nil
				})
			}

			// we don't care about errors here, we are just pre-warming caches for a quicker subtree validation
			_ = g.Wait()
		}

		subtreeBytes, err := subtree.Serialize()
		if err != nil {
			return nil, errors.NewStorageError("failed to serialize subtree", err)
		}
		if err = sm.subtreeStore.Set(ctx,
			subtree.RootHash()[:],
			subtreeBytes,
			options.WithFileExtension("subtree"),
			options.WithTTL(2*time.Minute),
		); err != nil {
			return nil, errors.NewStorageError("failed to store subtree", err)
		}

		subtreeDataBytes, err := subtreeData.Serialize()
		if err != nil {
			return nil, errors.NewStorageError("failed to serialize subtree data", err)
		}
		if err = sm.subtreeStore.Set(ctx,
			subtreeData.RootHash()[:],
			subtreeDataBytes,
			options.WithFileExtension("subtreeData"),
			options.WithTTL(2*time.Minute),
		); err != nil {
			return nil, errors.NewStorageError("failed to store subtree data", err)
		}

		if err = sm.subtreeValidation.CheckSubtree(ctx, *subtree.RootHash(), "legacy", blockHeight); err != nil {
			return nil, errors.NewSubtreeError("failed to check subtree", err)
		}

		subtrees = append(subtrees, subtree.RootHash())
	}

	return subtrees, nil
}

func (sm *SyncManager) createTxMap(block *bsvutil.Block) (map[chainhash.Hash]*txMapWrapper, error) {
	// Create a map of all transactions in the block
	txMap := make(map[chainhash.Hash]*txMapWrapper)

	for _, wireTx := range block.Transactions() {
		txHash := *wireTx.Hash()

		// Serialize the tx
		var txBytes bytes.Buffer
		if err := wireTx.MsgTx().Serialize(&txBytes); err != nil {
			return nil, fmt.Errorf("could not serialize msgTx: %w", err)
		}

		tx, err := bt.NewTxFromBytes(txBytes.Bytes())
		if err != nil {
			return nil, fmt.Errorf("failed to create bt.Tx: %w", err)
		}

		// don't add the coinbase to the txMap, we cannot process it anyway
		if !tx.IsCoinbase() {
			txMap[txHash] = &txMapWrapper{tx: tx}
		}

		// check if has parents in the block

	}

	return txMap, nil
}

// prepareTxsPerLevel prepares the transactions per level for processing
// levels are determined by the number of parents in the block
func (sm *SyncManager) prepareTxsPerLevel(ctx context.Context, block *bsvutil.Block,
	txMap map[chainhash.Hash]*txMapWrapper) (uint32, map[uint32][]*bt.Tx) {

	ctx, _, deferFn := tracing.StartTracing(ctx, "prepareTxsPerLevel")
	defer deferFn()

	maxLevel := uint32(0)
	sizePerLevel := make(map[uint32]uint64)
	blockTxsPerLevel := make(map[uint32][]*bt.Tx)
	for _, wireTx := range block.Transactions() {
		txHash := *wireTx.Hash()
		if _, found := txMap[txHash]; found {
			if txMap[txHash].someParentsInBlock {
				for _, input := range txMap[txHash].tx.Inputs {
					parentTxHash := *input.PreviousTxIDChainHash()
					if parentTxWrapper, found := txMap[parentTxHash]; found {
						// if the parent from this input is at the same level or higher,
						// we need to increase the child level of this transaction
						if parentTxWrapper.childLevelInBlock >= txMap[txHash].childLevelInBlock {
							txMap[txHash].childLevelInBlock = parentTxWrapper.childLevelInBlock + 1
						}
						if txMap[txHash].childLevelInBlock > maxLevel {
							maxLevel = txMap[txHash].childLevelInBlock
						}
					}
				}
			}
			sizePerLevel[txMap[txHash].childLevelInBlock] += uint64(txMap[txHash].tx.Size())
		}
	}

	// pre-allocation of the blockTxsPerLevel map
	for i := uint32(0); i <= maxLevel; i++ {
		blockTxsPerLevel[i] = make([]*bt.Tx, 0, sizePerLevel[i])
	}

	// put all transactions in a map per level for processing
	for _, txWrapper := range txMap {
		blockTxsPerLevel[txWrapper.childLevelInBlock] = append(blockTxsPerLevel[txWrapper.childLevelInBlock], txWrapper.tx)
	}

	return maxLevel, blockTxsPerLevel
}

func (sm *SyncManager) extendTransaction(tx *bt.Tx, txMap map[chainhash.Hash]*txMapWrapper) error {
	previousOutputs := make([]*meta.PreviousOutput, 0, len(tx.Inputs))

	txWrapper, found := txMap[*tx.TxIDChainHash()]
	if !found {
		return fmt.Errorf("tx %s not found in txMap", tx.TxIDChainHash())
	}

	for i, input := range tx.Inputs {
		prevTxHash := *input.PreviousTxIDChainHash()
		if prevTxWrapper, found := txMap[prevTxHash]; found {
			txWrapper.someParentsInBlock = true
			tx.Inputs[i].PreviousTxSatoshis = prevTxWrapper.tx.Outputs[input.PreviousTxOutIndex].Satoshis
			tx.Inputs[i].PreviousTxScript = bscript.NewFromBytes(*prevTxWrapper.tx.Outputs[input.PreviousTxOutIndex].LockingScript)
		} else {
			previousOutputs = append(previousOutputs, &meta.PreviousOutput{
				PreviousTxID: prevTxHash,
				Vout:         input.PreviousTxOutIndex,
				Idx:          i,
			})
		}
	}

	if err := sm.utxoStore.PreviousOutputsDecorate(sm.ctx, previousOutputs); err != nil {
		return fmt.Errorf("failed to decorate previous outputs for tx %s: %w", tx.TxIDChainHash(), err)
	}

	// run through the previous outputs and extend the transaction
	for _, po := range previousOutputs {
		if po.LockingScript == nil {
			return fmt.Errorf("previous output script is empty for %s:%d", po.PreviousTxID, po.Vout)
		}

		tx.Inputs[po.Idx].PreviousTxSatoshis = po.Satoshis
		tx.Inputs[po.Idx].PreviousTxScript = bscript.NewFromBytes(po.LockingScript)
	}
	return nil
}
