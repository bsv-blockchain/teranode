package util

import (
	"github.com/bitcoin-sv/teranode/stores/utxo/meta"
	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-subtree"
)

func TxMetaDataFromTx(tx *bt.Tx) (*meta.Data, error) {
	fee, err := GetFees(tx)
	if err != nil {
		return nil, err
	}

	var txInpoints subtree.TxInpoints
	if tx.IsCoinbase() {
		// For coinbase transactions, we do not have inputs, so we create an empty TxInpoints.
		txInpoints = subtree.TxInpoints{}
	} else {
		txInpoints, err = subtree.NewTxInpointsFromTx(tx)
		if err != nil {
			return nil, err
		}
	}

	s := meta.Data{
		Tx:         tx,
		TxInpoints: txInpoints,
		BlockIDs:   make([]uint32, 0),
		Fee:        fee,
		IsCoinbase: tx.IsCoinbase(),
		LockTime:   tx.LockTime,
	}

	// For partially populated utxos, we will have no inputs and possibly some nil outputs.
	// Therefore, we do not call tx.Size() as it will panic.
	if len(tx.Inputs) > 0 {
		s.SizeInBytes = uint64(tx.Size())
	}

	return &s, nil
}
