package netsync

import (
	"bytes"
	"context"
	"fmt"
	"github.com/bitcoin-sv/ubsv/services/legacy/peer"
	"github.com/bitcoin-sv/ubsv/stores/blob/options"
	"github.com/bitcoin-sv/ubsv/tracing"

	"github.com/bitcoin-sv/ubsv/errors"
	"github.com/bitcoin-sv/ubsv/model"
	"github.com/bitcoin-sv/ubsv/services/legacy/bsvutil"
	"github.com/bitcoin-sv/ubsv/stores/utxo"
	"github.com/bitcoin-sv/ubsv/stores/utxo/meta"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/bscript"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/ordishs/gocore"
)

func (sm *SyncManager) HandleBlockDirect(ctx context.Context, peer *peer.Peer, block *bsvutil.Block) error {
	ctx, stat, deferFn := tracing.StartTracing(ctx, "HandleBlockDirect",
		tracing.WithLogMessage(sm.logger, "[HandleBlockDirect][%s] processing block found from peer %s",
			block.Hash().String(), peer.String()),
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

	blockSize := block.MsgBlock().SerializeSize()

	start := gocore.CurrentTime()

	subtrees := make([]*chainhash.Hash, 0)

	// create 1 subtree + subtree.data
	// then validate the subtree through the subtreeValidation service
	if len(block.Transactions()) > 1 {
		subtree, err := util.NewIncompleteTreeByLeafCount(len(block.Transactions()))
		if err != nil {
			return errors.New(errors.ERR_SUBTREE_ERROR, "failed to create subtree", err)
		}

		subtreeData := util.NewSubtreeData(subtree)

		if err = subtree.AddNode(model.CoinbasePlaceholder, 0, 0); err != nil {
			return fmt.Errorf("failed to add coinbase placeholder: %w", err)
		}

		var tx *bt.Tx
		for _, wireTx := range block.Transactions() {
			txHash := *wireTx.Hash()

			// Serialize the tx
			var txBytes bytes.Buffer
			if err = wireTx.MsgTx().Serialize(&txBytes); err != nil {
				return fmt.Errorf("could not serialize msgTx: %w", err)
			}

			txSize := uint64(txBytes.Len())

			tx, err = bt.NewTxFromBytes(txBytes.Bytes())
			if err != nil {
				return fmt.Errorf("failed to create bt.Tx: %w", err)
			}

			if !tx.IsCoinbase() {
				if err = subtree.AddNode(txHash, 0, txSize); err != nil {
					return fmt.Errorf("failed to add node (%s) to subtree: %w", txHash, err)
				}
				// we need to match the indexes of the subtree and the tx data in subtreeData
				currentIdx := subtree.Length() - 1

				// Extend the tx with additional information
				previousOutputs := make([]*meta.PreviousOutput, len(tx.Inputs))

				for i, input := range tx.Inputs {
					previousOutputs[i] = &meta.PreviousOutput{
						PreviousTxID: *input.PreviousTxIDChainHash(),
						Vout:         input.PreviousTxOutIndex,
					}
				}

				err = sm.utxoStore.PreviousOutputsDecorate(context.Background(), previousOutputs)
				if err != nil {
					return fmt.Errorf("failed to decorate previous outputs for tx %s: %w", txHash, err)
				}

				// run through the previous outputs and extend the transaction
				for i, po := range previousOutputs {
					if po.LockingScript == nil {
						return fmt.Errorf("previous output script is empty for %s:%d", po.PreviousTxID, po.Vout)
					}

					tx.Inputs[i].PreviousTxSatoshis = po.Satoshis
					tx.Inputs[i].PreviousTxScript = bscript.NewFromBytes(po.LockingScript)
				}

				// store the extended transaction in our subtree tx data file
				if err = subtreeData.AddTx(tx, currentIdx); err != nil {
					return fmt.Errorf("failed to add tx to subtree data: %w", err)
				}
			}
		}
		subtreeBytes, err := subtree.Serialize()
		if err != nil {
			return errors.New(errors.ERR_STORAGE_ERROR, "failed to serialize subtree", err)
		}
		if err = sm.subtreeStore.Set(ctx, subtree.RootHash()[:], subtreeBytes); err != nil {
			return errors.New(errors.ERR_STORAGE_ERROR, "failed to store subtree", err)
		}

		subtreeDataBytes, err := subtreeData.Serialize()
		if err != nil {
			return errors.New(errors.ERR_STORAGE_ERROR, "failed to serialize subtree data", err)
		}
		if err = sm.subtreeStore.Set(ctx, subtreeData.RootHash()[:], subtreeDataBytes, options.WithFileExtension("subtreeData")); err != nil {
			return errors.New(errors.ERR_STORAGE_ERROR, "failed to store subtree data", err)
		}

		if err = sm.subtreeValidation.CheckSubtree(ctx, *subtree.RootHash(), "", uint32(block.Height())); err != nil {
			return errors.New(errors.ERR_SUBTREE_ERROR, "failed to check subtree", err)
		}

		subtrees = append(subtrees, subtree.RootHash())
	}

	// create valid teranode block, with the subtree hash
	teranodeBlock, err := model.NewBlock(header, coinbaseTx, subtrees, uint64(len(block.Transactions())), uint64(blockSize), uint32(block.Height()))
	if err != nil {
		return errors.New(errors.ERR_PROCESSING, "failed to create model.NewBlock", err)
	}

	// send the block to the blockValidation
	err = sm.blockValidation.BlockFound(ctx, teranodeBlock)

	err = sm.blockchainClient.AddBlock(ctx, teranodeBlock, "LEGACY")
	if err != nil {
		if errors.Is(err, errors.ErrBlockExists) {
			sm.logger.Warnf("Block already exists in the database: %s", block.Hash().String())
		} else {
			return errors.New(errors.ERR_PROCESSING, "failed to store block", err)
		}
	}

	blockIDs, err := sm.blockchainClient.GetBlockHeaderIDs(ctx, block.Hash(), 1)
	if err != nil {
		return errors.New(errors.ERR_PROCESSING, "failed to get block header IDs for %s", block.Hash(), err)
	}

	start = stat.NewStat("StoreBlock").AddTime(start)

	// Add the placeholder to the subtree
	if err := subtree.AddNode(model.CoinbasePlaceholder, 0, 0); err != nil {
		return fmt.Errorf("Failed to add coinbase placeholder: %w", err)
	}

	for _, wireTx := range block.Transactions() {
		txHash := *wireTx.Hash()

		// Serialize the tx
		var txBytes bytes.Buffer
		if err := wireTx.MsgTx().Serialize(&txBytes); err != nil {
			return fmt.Errorf("Could not serialize msgTx: %w", err)
		}

		txSize := uint64(txBytes.Len())

		tx, err := bt.NewTxFromBytes(txBytes.Bytes())
		if err != nil {
			return fmt.Errorf("Failed to create bt.Tx: %w", err)
		}

		if !tx.IsCoinbase() {
			spends := make([]*utxo.Spend, len(tx.Inputs))

			if err = subtree.AddNode(txHash, 0, txSize); err != nil {
				return fmt.Errorf("Failed to add node (%s) to subtree: %w", txHash, err)
			}

			// Extend the tx with additional information
			previousOutputs := make([]*meta.PreviousOutput, len(tx.Inputs))

			for i, input := range tx.Inputs {
				previousOutputs[i] = &meta.PreviousOutput{
					PreviousTxID: *input.PreviousTxIDChainHash(),
					Vout:         input.PreviousTxOutIndex,
				}

				if input.PreviousTxSatoshis > 0 {
					hash, err := util.UTXOHashFromInput(input)
					if err != nil {
						return fmt.Errorf("error getting input utxo hash: %s", err.Error())
					}

					// v.logger.Debugf("spending utxo %s:%d -> %s", input.PreviousTxIDChainHash().String(), input.PreviousTxOutIndex, hash.String())
					spends[i] = &utxo.Spend{
						TxID:         input.PreviousTxIDChainHash(),
						Vout:         input.PreviousTxOutIndex,
						UTXOHash:     hash,
						SpendingTxID: &txHash,
					}
				}
			}

			err = sm.utxoStore.PreviousOutputsDecorate(context.Background(), previousOutputs)
			if err != nil {
				return fmt.Errorf("Failed to decorate previous outputs for tx %s: %w", txHash, err)
			}

			for i, po := range previousOutputs {
				if po.LockingScript == nil {
					return fmt.Errorf("Previous output script is empty for %s:%d", po.PreviousTxID, po.Vout)
				}

				tx.Inputs[i].PreviousTxSatoshis = uint64(po.Satoshis)
				tx.Inputs[i].PreviousTxScript = bscript.NewFromBytes(po.LockingScript)
			}

			// Spend the inputs
			if err = sm.utxoStore.Spend(ctx, spends, uint32(block.Height())); err != nil {
				return fmt.Errorf("Failed to spend utxos: %w", err)
			}
		}

		// Store the tx in the store
		if _, err = sm.utxoStore.Create(ctx, tx, blockIDs[0]); err != nil {
			if !errors.Is(err, errors.ErrTxAlreadyExists) {
				return fmt.Errorf("Failed to store tx: %w", err)
			}
		}
	}

	if len(block.Transactions()) > 1 {
		// Add the subtree to the cache
		subtreeBytes, err := subtree.SerializeNodes()
		if err != nil {
			return fmt.Errorf("Failed to serialize subtree: %w", err)
		}

		_ = sm.subtreeStore.Set(ctx, subtree.RootHash()[:], subtreeBytes)

		stat.NewStat("SubtreeStore").AddTimeForRange(start, len(block.Transactions()))

		// subtrees = append(subtrees, subtree.RootHash())

		// Update the block with the correct subtree, if necessary
		// TODO s.blockchainStore.Se(ctx context.Context, blockHash *chainhash.Hash)
	}

	height := block.Height()
	_ = sm.utxoStore.SetBlockHeight(uint32(height))

	return nil
}
