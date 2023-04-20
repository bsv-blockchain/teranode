package propagation

import (
	"context"
	"encoding/hex"
	"fmt"
	"log"

	"github.com/TAAL-GmbH/ubsv/services/propagation/store"
	"github.com/libsv/go-bt/v2"
	"github.com/ordishs/go-bitcoin"
	"github.com/ordishs/gocore"
)

var bitcoinClient *bitcoin.Bitcoind

func init() {
	bitcoinRpcUri, err, ok := gocore.Config().GetURL("peer_1_rpc")
	if err == nil && ok {
		bitcoinClient, err = bitcoin.NewFromURL(bitcoinRpcUri, false)
		if err != nil {
			log.Printf("ERROR could not create bitcoin bitcoinClient: %v", err)
		}
	}
}

func ExtendTransaction(ctx context.Context, tx *bt.Tx, txStore store.TransactionStore) (err error) {
	parentTxBytes := make(map[[32]byte][]byte)
	var btParentTx *bt.Tx

	// get the missing input data for the tx
	for _, input := range tx.Inputs {
		parentTxID := [32]byte(bt.ReverseBytes(input.PreviousTxID()))
		b, ok := parentTxBytes[parentTxID]
		if !ok {
			b, err = txStore.Get(ctx, parentTxID[:])
			if err != nil {
				if bitcoinClient != nil {
					fmt.Printf("tx %x not found in store, trying bitcoin node\n", bt.ReverseBytes(parentTxID[:]))
					txHex, txErr := bitcoinClient.GetRawTransactionHex(input.PreviousTxIDStr())
					if txErr != nil {
						return fmt.Errorf("error getting tx %x from bitcoin node: %v", bt.ReverseBytes(parentTxID[:]), txErr)
					}
					if txHex == nil {
						return fmt.Errorf("tx %x not found", bt.ReverseBytes(parentTxID[:]))
					}

					b, txErr = hex.DecodeString(*txHex)
					if txErr != nil {
						return fmt.Errorf("error decoding tx %x: %v", bt.ReverseBytes(parentTxID[:]), txErr)
					}

					if b != nil {
						fmt.Printf("tx %x not found in store, but found in bitcoin node\n", bt.ReverseBytes(parentTxID[:]))
					} else {
						return fmt.Errorf("tx %x not found in store", bt.ReverseBytes(parentTxID[:]))
					}
				} else {
					return fmt.Errorf("tx %x not found in store", bt.ReverseBytes(parentTxID[:]))
				}
			}
			parentTxBytes[parentTxID] = b
		}

		btParentTx, err = bt.NewTxFromBytes(b)
		if err != nil {
			return fmt.Errorf("error decoding tx %x: %v", bt.ReverseBytes(parentTxID[:]), err)
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
