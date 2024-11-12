package unspend

import (
	"context"
	"fmt"
	"os"

	"github.com/bitcoin-sv/ubsv/errors"
	utxostore "github.com/bitcoin-sv/ubsv/stores/utxo"
	utxofactory "github.com/bitcoin-sv/ubsv/stores/utxo/_factory"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/ordishs/gocore"
)

func Start() {
	logger := ulogger.New("unspend")
	ctx := context.Background()

	fmt.Println()

	if len(os.Args) != 2 {
		fmt.Printf("Usage: %s <txid>\n", os.Args[0])
		os.Exit(1)
	}

	if len(os.Args[1]) != 64 {
		fmt.Printf("Invalid txid: %s\n", os.Args[1])
		os.Exit(1)
	}

	txHash, err := chainhash.NewHashFromStr(os.Args[1])
	if err != nil {
		fmt.Printf("Invalid txid: %s\n", os.Args[1])
		os.Exit(1)
	}

	utxoStoreURL, err, found := gocore.Config().GetURL("utxostore")
	if err != nil {
		fmt.Printf("error reading utxostore setting: %s\n", err)
		os.Exit(1)
	}

	if !found {
		fmt.Printf("no utxostore setting found\n")
		os.Exit(1)
	}

	utxoStore, err := utxofactory.NewStore(ctx, logger, utxoStoreURL, "main", false)
	if err != nil {
		fmt.Printf("error creating utxostore: %s\n", err)
		os.Exit(1)
	}

	if err = unSpendTransaction(ctx, logger, utxoStore, txHash); err != nil {
		fmt.Printf("error unspending tx: %s\n", err)
		os.Exit(1)
	}
}

func unSpendTransaction(ctx context.Context, logger ulogger.Logger, utxoStore utxostore.Store, txHash *chainhash.Hash) (err error) {
	logger.Infof("Un-spending tx %s", txHash.String())

	txMeta, err := utxoStore.Get(ctx, txHash)
	if err != nil {
		return errors.NewProcessingError("error getting tx meta: %s", err)
	}

	if txMeta == nil || txMeta.Tx == nil {
		return errors.NewProcessingError("tx not found")
	}

	if len(txMeta.BlockIDs) > 0 {
		// TODO the blocks this transaction has been mined into needs to be opened and the transaction
		//      marked as conflicting in the subtree it was added into
		return errors.NewProcessingError("tx has already been mined, cannot un-spend")
	}

	tx := txMeta.Tx

	if !util.IsExtended(tx, 0) {
		return errors.NewProcessingError("tx is not an extended tx")
	}

	// check the parent txs	and undo the spends
	unSpends := make([]*utxostore.Spend, len(tx.Inputs))

	for idx, input := range tx.Inputs {
		parentHash := input.PreviousTxIDChainHash()

		parentMeta, err := utxoStore.GetMeta(ctx, parentHash)
		if err != nil {
			return errors.NewProcessingError("error getting parent tx utxo record: %s", err)
		}

		if parentMeta == nil {
			return errors.NewProcessingError("parent tx not found")
		}

		utxoHash, err := util.UTXOHashFromInput(input)
		if err != nil {
			return errors.NewProcessingError("error getting utxo hash: %s", err)
		}

		unSpends[idx] = &utxostore.Spend{
			TxID:     parentHash,
			Vout:     input.PreviousTxOutIndex,
			UTXOHash: utxoHash,
		}
	}

	// check outputs of transaction and that they have not been spent, if spent, un-spend those as well
	for idx, output := range tx.Outputs {
		//nolint:gosec
		utxoHash, err := util.UTXOHashFromOutput(txHash, output, uint32(idx))
		if err != nil {
			return errors.NewProcessingError("error getting utxo hash: %s\n", err)
		}

		spendResponse, err := utxoStore.GetSpend(ctx, &utxostore.Spend{
			TxID: txHash,
			//nolint:gosec
			Vout:     uint32(idx),
			UTXOHash: utxoHash,
		})
		if err != nil {
			return errors.NewProcessingError("error getting utxo record: %s\n", err)
		}

		if spendResponse != nil && spendResponse.SpendingTxID != nil {
			// recursively un-spend this output spending tx
			if err = unSpendTransaction(ctx, logger, utxoStore, spendResponse.SpendingTxID); err != nil {
				return errors.NewProcessingError("error un-spending spending tx: %s\n", err)
			}
		}
	}

	// all parents are still there, so we can safely un-spend and delete the tx

	// undo the spend
	logger.Infof("-- Un-spending %d utxos for %s", len(unSpends), txHash.String())

	if err = utxoStore.UnSpend(context.TODO(), unSpends); err != nil {
		return errors.NewProcessingError("error un-spending tx: %s\n", err)
	}

	// delete this tx from the utxo store
	// TODO what to do if this transaction has already been mined?
	logger.Infof("-- Deleting tx %s", txHash.String())

	if err = utxoStore.Delete(context.TODO(), txHash); err != nil {
		return errors.NewProcessingError("error deleting tx: %s\n", err)
	}

	return nil
}
