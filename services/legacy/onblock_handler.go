package legacy

import (
	"bytes"
	"context"
	"fmt"
	"github.com/bitcoin-sv/ubsv/errors"
	"github.com/bitcoin-sv/ubsv/model"
	"github.com/bitcoin-sv/ubsv/services/legacy/bsvutil"
	"github.com/bitcoin-sv/ubsv/services/legacy/peer"
	"github.com/bitcoin-sv/ubsv/services/legacy/wire"
	"github.com/bitcoin-sv/ubsv/stores/utxo"
	"github.com/bitcoin-sv/ubsv/stores/utxo/meta"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/lib/pq"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/bscript"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/ordishs/gocore"
)

// onBlock processes a block message, after a block is requested by us via getBlockMsg
func (pm *PeerManager) onBlock(ctx context.Context) func(p *peer.Peer, msg *wire.MsgBlock, buf []byte) {
	return func(p *peer.Peer, msg *wire.MsgBlock, buf []byte) {
		pm.cond.L.Lock() // Lock the mutex to prevent processing the next block before the signal is received

		pm.height++

		pm.logger.Warnf("Received block   %s with %d txns (Height: %d)\n", msg.BlockHash(), len(msg.Transactions), pm.height)

		block := bsvutil.NewBlock(msg)
		block.SetHeight(int32(pm.height))

		if gocore.Config().GetBool("legacy_direct", true) {
			if err := pm.HandleBlockDirect(ctx, block); err != nil {
				pm.logger.Fatalf("Failed to handle block: %v", err)
			}
		} else {
			//if err := s.tb.HandleBlock(ctx, block); err != nil {
			//	pm.logger.Fatalf("Failed to handle block: %v", err)
			//}
		}

		pm.cond.Signal()
		pm.cond.L.Unlock()

		// s.wg.Done()
	}
}

func (pm *PeerManager) HandleBlockDirect(ctx context.Context, block *bsvutil.Block) error {
	startTotal, stat, ctx := util.StartStatFromContext(ctx, "HandleBlockDirect")

	defer func() {
		stat.AddTime(startTotal)
	}()

	stat.NewStat("SubtreeStore").AddRanges(0, 1, 10, 100, 1000, 10000, 100000, 1000000)

	subtrees := make([]*chainhash.Hash, 0)

	subtree, err := util.NewIncompleteTreeByLeafCount(len(block.Transactions()))
	if err != nil {
		return fmt.Errorf("Failed to create subtree: %w", err)
	}

	// 3. Create a block message with (block hash, coinbase tx and slice if 1 subtree)
	var headerBytes bytes.Buffer
	if err := block.MsgBlock().Header.Serialize(&headerBytes); err != nil {
		return fmt.Errorf("Failed to serialize header: %w", err)
	}

	header, err := model.NewBlockHeaderFromBytes(headerBytes.Bytes())
	if err != nil {
		return fmt.Errorf("Failed to create block header from bytes: %w", err)
	}

	var coinbase bytes.Buffer
	if err := block.Transactions()[0].MsgTx().Serialize(&coinbase); err != nil {
		return fmt.Errorf("Failed to serialize coinbase: %w", err)
	}

	coinbaseTx, err := bt.NewTxFromBytes(coinbase.Bytes())
	if err != nil {
		return fmt.Errorf("Failed to create bt.Tx for coinbase: %w", err)
	}

	blockSize := block.MsgBlock().SerializeSize()

	// We will first store this block with an empty slice of subtrees. If this block has more than just the 1 coinbase tx, we will
	// update the row to have a subtree after processing all the txs

	teranodeBlock, err := model.NewBlock(header, coinbaseTx, subtrees, uint64(len(block.Transactions())), uint64(blockSize), uint32(block.Height()))
	if err != nil {
		return fmt.Errorf("Failed to create model.NewBlock: %w", err)
	}

	start := gocore.CurrentTime()

	dbID, err := pm.blockchainStore.StoreBlock(ctx, teranodeBlock, "LEGACY")
	if err != nil {
		var pqErr *pq.Error
		if errors.As(err, &pqErr) {
			if pqErr.Code == "23505" { // Duplicate constraint violation
				pm.logger.Warnf("Block already exists in the database: %s", block.Hash().String())
			} else {
				return fmt.Errorf("Failed to store block: %w", err)
			}
		} else {
			return fmt.Errorf("Failed to store block: %w", err)
		}
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

			err = pm.utxoStore.PreviousOutputsDecorate(context.Background(), previousOutputs)
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
			if err = pm.utxoStore.Spend(ctx, spends, uint32(block.Height())); err != nil {
				return fmt.Errorf("Failed to spend utxos: %w", err)
			}
		}

		// Store the tx in the store
		if _, err = pm.utxoStore.Create(ctx, tx, uint32(dbID)); err != nil {
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

		_ = pm.subtreeStore.Set(ctx, subtree.RootHash()[:], subtreeBytes)

		stat.NewStat("SubtreeStore").AddTimeForRange(start, len(block.Transactions()))

		// subtrees = append(subtrees, subtree.RootHash())

		// Update the block with the correct subtree, if necessary
		// TODO s.blockchainStore.Se(ctx context.Context, blockHash *chainhash.Hash)
	}

	height := block.Height()
	_ = pm.utxoStore.SetBlockHeight(uint32(height))

	return nil
}
