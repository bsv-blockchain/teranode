package propagation

import (
	"context"
	"fmt"

	"github.com/TAAL-GmbH/ubsv/services/propagation/store"
	"github.com/libsv/go-bt/v2"
)

func ExtendTransaction(tx *bt.Tx, txStore store.TransactionStore) (err error) {
	parentTxBytes := make(map[[32]byte][]byte)
	var btParentTx *bt.Tx

	// get the missing input data for the tx
	for _, input := range tx.Inputs {
		parentTxID := [32]byte(bt.ReverseBytes(input.PreviousTxID()))
		b, ok := parentTxBytes[parentTxID]
		if !ok {
			b, err = txStore.Get(context.Background(), parentTxID[:])
			if err != nil {
				return err
			}
			parentTxBytes[parentTxID] = b
		}

		btParentTx, err = bt.NewTxFromBytes(b)
		if err != nil {
			return err
		}

		if len(btParentTx.Outputs) < int(input.PreviousTxOutIndex) {
			return fmt.Errorf("output %d not found in tx %x", input.PreviousTxOutIndex, bt.ReverseBytes(parentTxID[:]))
		}
		output := btParentTx.Outputs[input.PreviousTxOutIndex]

		input.PreviousTxScript = output.LockingScript
		input.PreviousTxSatoshis = output.Satoshis
	}

	return nil
}
