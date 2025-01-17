package utxo

import (
	"math/rand/v2"

	"github.com/libsv/go-bt/v2"
)

func GetSpendingTx(tx *bt.Tx, vOut ...uint32) *bt.Tx {
	newTx := bt.NewTx()
	for _, outIdx := range vOut {
		_ = newTx.From(tx.TxIDChainHash().String(), outIdx, tx.Outputs[outIdx].LockingScript.String(), tx.Outputs[outIdx].Satoshis)
	}

	//nolint:gosec
	_ = newTx.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", rand.Uint64())
	//nolint:gosec
	_ = newTx.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", rand.Uint64())
	//nolint:gosec
	_ = newTx.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", rand.Uint64())
	_ = newTx.ChangeToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", &bt.FeeQuote{})

	return newTx
}
