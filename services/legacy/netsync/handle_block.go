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
		_, blockHeaderMeta, err := sm.blockchainClient.GetBlockHeader(ctx, &block.MsgBlock().Header.PrevBlock)
		if err != nil {
			return errors.New(errors.ERR_PROCESSING, "failed to get block header", err)
		}
		blockHeight = blockHeaderMeta.Height
		block.SetHeight(int32(blockHeight))
	} else {
		blockHeight = uint32(block.Height())
	}

	ctx, stat, deferFn := tracing.StartTracing(ctx, "HandleBlockDirect",
		tracing.WithLogMessage(sm.logger, "[HandleBlockDirect][%s %d] processing block found from peer %s",
			block.Hash().String(), blockHeight, peer.String()),
	)
	defer deferFn()

	stat.NewStat("SubtreeStore").AddRanges(0, 1, 10, 100, 1000, 10000, 100000, 1000000)

	// 3. Create a block message with (block hash, coinbase tx and slice if 1 subtree)
	var headerBytes bytes.Buffer
	if err := block.MsgBlock().Header.Serialize(&headerBytes); err != nil {
		return errors.New(errors.ERR_PROCESSING, "failed to serialize header", err)
	}

	// create the Teranode compatible block header
	header, err := model.NewBlockHeaderFromBytes(headerBytes.Bytes())
	if err != nil {
		return errors.New(errors.ERR_PROCESSING, "failed to create block header from bytes", err)
	}

	var coinbase bytes.Buffer
	if err = block.Transactions()[0].MsgTx().Serialize(&coinbase); err != nil {
		return errors.New(errors.ERR_PROCESSING, "failed to serialize coinbase", err)
	}

	coinbaseTx, err := bt.NewTxFromBytes(coinbase.Bytes())
	if err != nil {
		return errors.New(errors.ERR_PROCESSING, "failed to create bt.Tx for coinbase", err)
	}

	// validate all subtrees and store all subtree data
	// this also should spend and create all utxos
	subtrees, err := sm.prepareSubtrees(ctx, block)
	if err != nil {
		return err
	}

	// create valid teranode block, with the subtree hash
	blockSize := block.MsgBlock().SerializeSize()
	teranodeBlock, err := model.NewBlock(header, coinbaseTx, subtrees, uint64(len(block.Transactions())), uint64(blockSize), uint32(block.Height()))
	if err != nil {
		return errors.New(errors.ERR_PROCESSING, "failed to create model.NewBlock", err)
	}

	// send the block to the blockValidation for processing and validation
	// all the block subtrees should have been validated in processSubtrees
	if err = sm.blockValidation.ProcessBlock(ctx, teranodeBlock, blockHeight); err != nil {
		return errors.New(errors.ERR_PROCESSING, "failed to process block", err)
	}

	return nil
}

type txMapWrapper struct {
	tx                 *bt.Tx
	someParentsInBlock bool
}

func (sm *SyncManager) prepareSubtrees(ctx context.Context, block *bsvutil.Block) ([]*chainhash.Hash, error) {
	subtrees := make([]*chainhash.Hash, 0)

	// create 1 subtree + subtree.subtreeData
	// then validate the subtree through the subtreeValidation service
	if len(block.Transactions()) > 1 {
		subtree, err := util.NewIncompleteTreeByLeafCount(len(block.Transactions()))
		if err != nil {
			return nil, errors.New(errors.ERR_SUBTREE_ERROR, "failed to create subtree", err)
		}

		if err := subtree.AddNode(model.CoinbasePlaceholder, 0, 0); err != nil {
			return nil, fmt.Errorf("failed to add coinbase placeholder: %w", err)
		}

		// subtreeData contains the extended tx bytes of all transactions references in the subtree
		// except the coinbase transaction
		subtreeData := util.NewSubtreeData(subtree)

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

			txMap[txHash] = &txMapWrapper{tx: tx}
		}

		g, gCtx := errgroup.WithContext(ctx)
		g.SetLimit(runtime.NumCPU() * 4)
		for _, wireTx := range block.Transactions() {
			txHash := *wireTx.Hash()

			tx := txMap[txHash].tx

			txSize := uint64(tx.Size())

			if !tx.IsCoinbase() {
				if err := subtree.AddNode(txHash, 0, txSize); err != nil {
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
			return nil, errors.New(errors.ERR_PROCESSING, "failed to process transactions", err)
		}

		blockHeight := uint32(block.Height())

		// try to pre-validate the transactions through the validation, to speed up subtree validation later on.
		// This allows us to process all the transactions that are not referencing transactions from this current block
		// to be processed in parallel.
		g, gCtx = errgroup.WithContext(ctx)
		g.SetLimit(runtime.NumCPU() * 4)
		for _, wireTx := range block.Transactions() {
			txHash := *wireTx.Hash()
			g.Go(func() error {
				// send to validation, but only if the parent is not in the same block
				if !txMap[txHash].someParentsInBlock && !txMap[txHash].tx.IsCoinbase() {
					if err := sm.validationClient.Validate(gCtx, txMap[txHash].tx, blockHeight); err != nil {
						sm.logger.Warnf("failed to validate transaction in pre-validation: %v", err)
					}
				}

				return nil
			})
		}

		// we don't care about errors here, we are just pre-warming caches for a quicker subtree validation
		_ = g.Wait()

		// try to pre-validate the transactions that did have a parent in the same block after doing the parents
		//g, gCtx = errgroup.WithContext(ctx)
		//g.SetLimit(runtime.NumCPU() * 4)
		//for _, txWrapper := range txMap {
		//	if txWrapper.someParentsInBlock {
		//		tx := txWrapper.tx
		//		g.Go(func() error {
		//			// send to validation, but only if the parent is not in the same block
		//			_ = sm.validationClient.Validate(gCtx, tx, blockHeight)
		//
		//			return nil
		//		})
		//	}
		//}
		//
		//// we don't care about errors here, we are just pre-warming caches for a quicker subtree validation
		//_ = g.Wait()

		subtreeBytes, err := subtree.Serialize()
		if err != nil {
			return nil, errors.New(errors.ERR_STORAGE_ERROR, "failed to serialize subtree", err)
		}
		if err = sm.subtreeStore.Set(ctx,
			subtree.RootHash()[:],
			subtreeBytes,
			options.WithSubDirectory("legacy"),
			options.WithFileExtension("subtree"),
			options.WithTTL(2*time.Minute),
		); err != nil {
			return nil, errors.New(errors.ERR_STORAGE_ERROR, "failed to store subtree", err)
		}

		subtreeDataBytes, err := subtreeData.Serialize()
		if err != nil {
			return nil, errors.New(errors.ERR_STORAGE_ERROR, "failed to serialize subtree data", err)
		}
		if err = sm.subtreeStore.Set(ctx,
			subtreeData.RootHash()[:],
			subtreeDataBytes,
			options.WithSubDirectory("legacy"),
			options.WithFileExtension("subtreeData"),
			options.WithTTL(2*time.Minute),
		); err != nil {
			return nil, errors.New(errors.ERR_STORAGE_ERROR, "failed to store subtree data", err)
		}

		if err = sm.subtreeValidation.CheckSubtree(ctx, *subtree.RootHash(), "legacy", uint32(block.Height())); err != nil {
			return nil, errors.New(errors.ERR_SUBTREE_ERROR, "failed to check subtree", err)
		}

		subtrees = append(subtrees, subtree.RootHash())
	}

	return subtrees, nil
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

	if err := sm.utxoStore.PreviousOutputsDecorate(context.Background(), previousOutputs); err != nil {
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
