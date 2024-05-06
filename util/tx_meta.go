package util

import (
	"github.com/bitcoin-sv/ubsv/stores/txmeta"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/chainhash"
)

func TxMetaDataFromTx(tx *bt.Tx) (*txmeta.Data, error) {
	fee, err := GetFees(tx)
	if err != nil {
		return nil, err
	}

	var parentTxHashes []chainhash.Hash
	if tx.IsCoinbase() {
		parentTxHashes = make([]chainhash.Hash, 0)
	} else {
		parentTxHashes = make([]chainhash.Hash, len(tx.Inputs))
		for index, input := range tx.Inputs {
			parentTxHashes[index] = *input.PreviousTxIDChainHash()
		}
	}

	s := txmeta.Data{
		Tx:             tx,
		Fee:            fee,
		SizeInBytes:    uint64(tx.Size()),
		ParentTxHashes: parentTxHashes,
		IsCoinbase:     tx.IsCoinbase(),
	}

	return &s, nil
}
