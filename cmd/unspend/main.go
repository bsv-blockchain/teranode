package main

import (
	"context"
	"fmt"
	"os"

	utxostore "github.com/bitcoin-sv/ubsv/stores/utxo"
	utxofactory "github.com/bitcoin-sv/ubsv/stores/utxo/_factory"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/ordishs/gocore"
)

func main() {
	logger := ulogger.NewGoCoreLogger("unspend", ulogger.WithLevel("WARN"))

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

	utxoStore, err := utxofactory.NewStore(context.TODO(), logger, utxoStoreURL, "main")
	if err != nil {
		fmt.Printf("error creating utxostore: %s\n", err)
		os.Exit(1)
	}

	txMeta, err := utxoStore.Get(context.TODO(), txHash)
	if err != nil {
		fmt.Printf("error getting tx meta: %s\n", err)
		os.Exit(1)
	}

	if txMeta == nil || txMeta.Tx == nil {
		fmt.Printf("tx not found\n")
		os.Exit(1)
	}

	tx := txMeta.Tx

	if !util.IsExtended(tx, 0) {
		fmt.Printf("tx is not an extended tx\n")
		os.Exit(1)
	}

	// check the parent txs	and undo the spends
	unSpends := make([]*utxostore.Spend, len(tx.Inputs))

	for idx, input := range tx.Inputs {
		parentHash := input.PreviousTxIDChainHash()

		parentMeta, err := utxoStore.Get(context.TODO(), parentHash)
		if err != nil {
			fmt.Printf("error getting parent tx utxo record: %s\n", err)
			os.Exit(1)
		}

		if parentMeta == nil || parentMeta.Tx == nil {
			fmt.Printf("parent tx not found\n")
			os.Exit(1)
		}

		utxoHash, err := util.UTXOHashFromInput(input)
		if err != nil {
			fmt.Printf("error getting utxo hash: %s\n", err)
			os.Exit(1)
		}

		unSpends[idx] = &utxostore.Spend{
			TxID:     input.PreviousTxIDChainHash(),
			Vout:     input.PreviousTxOutIndex,
			UTXOHash: utxoHash,
		}
	}

	// TODO: check outputs of transaction and that they have not been spent

	// all parents are still there, so we can safely un-spend and delete the tx

	// undo the spend
	err = utxoStore.UnSpend(context.TODO(), unSpends)
	if err != nil {
		fmt.Printf("error unspending tx: %s\n", err)
		os.Exit(1)
	}

	// delete this tx from the utxo store
	err = utxoStore.Delete(context.TODO(), txHash)
	if err != nil {
		fmt.Printf("error deleting tx: %s\n", err)
		os.Exit(1)
	}
}
