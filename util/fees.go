package util

import (
	"fmt"

	"github.com/libsv/go-bt/v2"
)

func GetFees(btTx *bt.Tx) (uint64, error) {
	if !IsExtended(btTx) {
		return 0, fmt.Errorf("cannot get fees for non extended tx")
	}

	fees := uint64(0)

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
