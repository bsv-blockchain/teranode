package util

import (
	"github.com/bitcoin-sv/ubsv/stores/utxo/meta"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/chainhash"
)

func TxMetaDataFromTx(tx *bt.Tx) (*meta.Data, error) {
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

	s := meta.Data{
		Tx:             tx,
		ParentTxHashes: parentTxHashes,
		BlockIDs:       make([]uint32, 0),
		Fee:            fee,
		SizeInBytes:    uint64(tx.Size()),
		IsCoinbase:     tx.IsCoinbase(),
		LockTime:       tx.LockTime,
	}

	return &s, nil
}
