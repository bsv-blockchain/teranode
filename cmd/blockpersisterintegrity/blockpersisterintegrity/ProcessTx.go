package blockpersisterintegrity

import (
	"context"
	"fmt"

	"github.com/bitcoin-sv/ubsv/model"
	p_model "github.com/bitcoin-sv/ubsv/services/blockpersister/utxoset/model"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/libsv/go-bt/v2"
)

type TxProcessor struct {
	logger ulogger.Logger
	diff   *p_model.UTXODiff
	idx    int
}

func NewTxProcessor(logger ulogger.Logger, diff *p_model.UTXODiff) *TxProcessor {
	return &TxProcessor{
		logger: logger,
		diff:   diff,
		idx:    0,
	}
}

func (tp *TxProcessor) ProcessTx(ctx context.Context, tx *bt.Tx) error {
	defer func() { tp.idx++ }()

	tp.diff.ProcessTx(tx)

	if tp.idx == 0 {
		// this must be a coinbase transaction

		// our .subtree file appears to store the placeholder coinbase tx with the correct hash but doesn't bother with the rest of the tx
		isCoinbasePlaceholder := len(tx.Inputs) == 0 && len(tx.Outputs) == 0 && tx.Version == bt.DefaultSequenceNumber && tx.LockTime == bt.DefaultSequenceNumber

		if !isCoinbasePlaceholder {
			return fmt.Errorf("first transaction in a block must be a coinbase transaction")
		}

		// if !model.CoinbasePlaceholderHash.Equal(*tx.TxIDChainHash()) && !tx.IsCoinbase() {
		// 	return fmt.Errorf("first transaction in a block must be a coinbase transaction")
		// }
		return nil
	}

	// make sure we don't have a coinbase transaction in the middle of the block
	if model.CoinbasePlaceholderHash.Equal(*tx.TxIDChainHash()) || tx.IsCoinbase() {
		return fmt.Errorf("coinbase transaction must be the first transaction in a block")
	}

	// if !model.CoinbasePlaceholderHash.Equal(*tx.TxIDChainHash()) {
	// logger.Debugf("checking transaction %s", *btTx.TxIDChainHash())

	// check that the transaction does not already exist in another block
	// previotx := usBlockSubtree, ok := transactionMap[node.Hash]
	// if ok {
	// 	logger.Debugf("current subtree %s in block %s", subtreeHash, block.Hash())
	// 	return fmt.Errorf("transaction %s already exists in subtree %s in block %s", node.Hash, previousBlockSubtree.Subtree, previousBlockSubtree.Block)
	// } else {
	// 	transactionMap[node.Hash] = BlockSubtree{Block: *block.Hash(), Subtree: *subtreeHash, Index: nodeIdx}
	// }

	// check that the transaction exists in the tx store
	// tx, err = txStore.Get(ctx, node.Hash[:])
	// if err != nil {
	// 	txMeta, err := utxoStore.GetMeta(ctx, &node.Hash)
	// 	if err != nil {
	// 		return fmt.Errorf("failed to get transaction %s from txmeta store: %s", node.Hash, err)
	// 		continue
	// 	}
	// 	if txMeta.Tx != nil {
	// 		tx = txMeta.Tx.ExtendedBytes()
	// 	} else {
	// 		return fmt.Errorf("failed to get transaction %s from tx store: %s", node.Hash, err)
	// 		continue
	// 	}
	// }

	// check the topological order of the transactions
	// for inputIdx, input := range btTx.Inputs {
	// 	// the input tx id (parent tx) should already be in the transaction map
	// 	inputHash := chainhash.Hash(input.PreviousTxID())
	// 	if !inputHash.Equal(chainhash.Hash{}) { // coinbase is parent
	// 		_, ok = transactionMap[inputHash]
	// 		if !ok {
	// 			missingParents[inputHash] = BlockSubtree{Block: *block.Hash(), Subtree: *subtreeHash, Index: nodeIdx}
	// 			return fmt.Errorf("the parent %s does not appear before the transaction %s, in block %s, subtree %s:%d", inputHash, node.Hash.String(), block.Hash(), subtreeHash, nodeIdx)
	// 		} else {
	// 			// check that parent inputs are marked as spent by this tx in the utxo store
	// 			utxoHash, err := util.UTXOHashFromInput(input)
	// 			if err != nil {
	// 				return fmt.Errorf("failed to get utxo hash for parent tx input %s in transaction %s: %s", input, btTx.TxIDChainHash(), err)
	// 				continue
	// 			}
	// 			utxo, err := utxoStore.GetSpend(ctx, &utxostore.Spend{
	// 				TxID:         input.PreviousTxIDChainHash(),
	// 				SpendingTxID: btTx.TxIDChainHash(),
	// 				Vout:         uint32(inputIdx),
	// 				Hash:         utxoHash,
	// 			})
	// 			if err != nil {
	// 				return fmt.Errorf("failed to get parent utxo %s from utxo store: %s", utxoHash, err)
	// 				continue
	// 			}
	// 			if utxo == nil {
	// 				return fmt.Errorf("parent utxo %s does not exist in utxo store", utxoHash)
	// 			} else if !utxo.SpendingTxID.IsEqual(btTx.TxIDChainHash()) {
	// 				return fmt.Errorf("parent utxo %s is not marked as spent by transaction %s", utxoHash, btTx.TxIDChainHash())
	// 			} else {
	// 				logger.Debugf("transaction %s parent utxo %s exists in utxo store with status %s, spending tx %s, locktime %d", btTx.TxIDChainHash(), utxoHash, utxostore.Status(utxo.Status), utxo.SpendingTxID, utxo.LockTime)
	// 			}
	// 		}
	// 	}
	//}

	// check outputs in utxo store
	// var utxoHash *chainhash.Hash
	// for vout, output := range btTx.Outputs {
	// 	utxoHash, err = util.UTXOHashFromOutput(btTx.TxIDChainHash(), output, uint32(vout))
	// 	if err != nil {
	// 		return fmt.Errorf("failed to get utxo hash for output %d in transaction %s: %s", vout, btTx.TxIDChainHash(), err)
	// 		continue
	// 	}
	// 	utxo, err := utxoStore.GetSpend(ctx, &utxostore.Spend{
	// 		TxID: btTx.TxIDChainHash(),
	// 		Vout: uint32(vout),
	// 		Hash: utxoHash,
	// 	})
	// 	if err != nil {
	// 		return fmt.Errorf("failed to get utxo %s from utxo store: %s", utxoHash, err)
	// 		continue
	// 	}
	// 	if utxo == nil {
	// 		return fmt.Errorf("utxo %s does not exist in utxo store", utxoHash)
	// 	} else {
	// 		logger.Debugf("transaction %s vout %d utxo %s exists in utxo store with status %s, spending tx %s, locktime %d", btTx.TxIDChainHash(), vout, utxoHash, utxostore.Status(utxo.Status), utxo.SpendingTxID, utxo.LockTime)
	// 	}
	// }

	// the coinbase fees are calculated differently to check if everything matches up
	// if !tx.IsCoinbase() {
	// 	fees, err := util.GetFees(tx) // only works for extended transactions
	// 	if err != nil {
	// 		return blockFees, fmt.Errorf("failed to get the fees for tx: %s", btTx.String())
	// 	}
	// 	subtreeFees += fees
	// }

	// check whether this transaction was missing before and write out info if it was
	// if blockOfChild, ok := missingParents[node.Hash]; ok {
	// 	logger.Warnf("found missing parent %s in block %s, subtree %s:%d", node.Hash, block.Hash(), subtreeHash, nodeIdx)
	// 	logger.Warnf("-- child was in block %s, subtree %s:%d", blockOfChild.Block, blockOfChild.Subtree, blockOfChild.Index)
	// }
	// } // if !coinbaseTx

	return nil
}
