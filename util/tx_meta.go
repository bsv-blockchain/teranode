package util

import (
	"time"

	"github.com/bitcoin-sv/ubsv/stores/txmeta"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/chainhash"
)

func TxMetaDataFromTx(tx *bt.Tx) (*txmeta.Data, error) {
	hash := *tx.TxIDChainHash()
	fee, err := GetFees(tx)
	if err != nil {
		return nil, err
	}

	// I assume this is an alternative way of creating same thing without using UTXOHashFromInput
	// parentTxHashes := make([]*chainhash.Hash, len(request.ParentTxHashes))
	// var parentTxHash *chainhash.Hash
	// for index, parentTxHashBytes := range request.ParentTxHashes {
	// 	parentTxHash, err = chainhash.NewHash(parentTxHashBytes)
	// 	if err != nil {
	// 		return nil, err
	// 	}
	// 	parentTxHashes[index] = parentTxHash
	// }

	var parentTxHashes []*chainhash.Hash
	if tx.IsCoinbase() {
		parentTxHashes = make([]*chainhash.Hash, 0)
	} else {
		parentTxHashes = make([]*chainhash.Hash, len(tx.Inputs))
		for index, input := range tx.Inputs {
			parentTxHash, err := UTXOHashFromInput(input)
			if err != nil {
				return nil, err
			}
			parentTxHashes[index] = parentTxHash
		}
	}

	utxoHashes := make([]*chainhash.Hash, len(tx.Outputs))
	var utxoHash *chainhash.Hash
	for index, output := range tx.Outputs {
		// utxoHash, err = chainhash.NewHash(utxoHashBytes)
		utxoHash, err = UTXOHashFromOutput(&hash, output, uint32(index))
		if err != nil {
			return nil, err
		}
		utxoHashes[index] = utxoHash
	}

	s := txmeta.Data{
		Tx:             tx,
		Fee:            fee,
		SizeInBytes:    uint64(tx.Size()),
		FirstSeen:      uint32(time.Now().Unix()),
		ParentTxHashes: parentTxHashes,
		UtxoHashes:     utxoHashes,
	}

	return &s, nil
}
