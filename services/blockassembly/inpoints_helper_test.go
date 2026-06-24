package blockassembly

import (
	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-subtree"
)

// singleParentInpointsPtr builds a *TxInpoints with one parent and one vout,
// matching the pre-packed-layout pattern
// `&subtree.TxInpoints{ParentTxHashes: []chainhash.Hash{X}, Idxs: [][]uint32{{vout}}}`.
//
// Tests use this to replace direct struct-literal construction now that the
// Idxs field has been removed.
func singleParentInpointsPtr(parent chainhash.Hash, vout uint32) *subtree.TxInpoints {
	in := &bt.Input{PreviousTxOutIndex: vout}
	if err := in.PreviousTxIDAdd(&parent); err != nil {
		panic(err)
	}

	ti, err := subtree.NewTxInpointsFromInputs([]*bt.Input{in})
	if err != nil {
		panic(err)
	}

	return &ti
}
