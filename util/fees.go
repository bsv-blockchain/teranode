package util

import (
	"fmt"

	"github.com/libsv/go-bt/v2"
)

func GetFees(btTx *bt.Tx) (uint64, error) {
	fees := uint64(0)

	if btTx.IsCoinbase() {
		// fmt.Printf("coinbase tx: %s\n", btTx.String())

		for _, output := range btTx.Outputs {
			if output.Satoshis > 0 {
				fees += output.Satoshis
			}
		}

		return fees, nil
	}

	if !btTx.IsExtended() {
		return 0, fmt.Errorf("cannot get fees for non extended tx")
	}

	for _, input := range btTx.Inputs {
		fees += input.PreviousTxSatoshis
	}
	for _, output := range btTx.Outputs {
		if output.Satoshis > 0 {
			fees -= output.Satoshis
		}
	}

	return fees, nil
}
