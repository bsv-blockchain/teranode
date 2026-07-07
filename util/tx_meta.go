package util

import (
	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
)

// txMetaDataFromTx is the single construction site for meta.Data, shared by
// TxMetaDataFromTx (real fee) and TxMetaDataFromTxNoFee (fee=0). Keeping one builder
// ensures the two entry points cannot drift if a field is added to meta.Data.
func txMetaDataFromTx(tx *bt.Tx, fee uint64) (*meta.Data, error) {
	var txInpoints subtree.TxInpoints
	if tx.IsCoinbase() {
		// For coinbase transactions, we do not have inputs, so we create an empty TxInpoints.
		txInpoints = subtree.TxInpoints{}
	} else {
		var err error
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

// TxMetaDataFromTxNoFee builds tx metadata without computing fees. Used by minimal (below-checkpoint)
// create where inputs are not decorated, so GetFees would error. Fee is set to 0; TxInpoints (parent
// outpoints, used by the pruner) and size are computed exactly as in TxMetaDataFromTx.
func TxMetaDataFromTxNoFee(tx *bt.Tx) (*meta.Data, error) {
	return txMetaDataFromTx(tx, 0)
}

func TxMetaDataFromTx(tx *bt.Tx) (*meta.Data, error) {
	fee, err := GetFees(tx)
	if err != nil {
		return nil, err
	}

	return txMetaDataFromTx(tx, fee)
}
