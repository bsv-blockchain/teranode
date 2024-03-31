package util

import (
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

	// SAO - there are some transactions (e.g. d5a13dcb1ad24dbffab91c3c2ffe7aea38d5e84b444c0014eb6c7c31fe8e23fc) that have 0 satoshi inputs and
	// therefore look like they are not extended.  So for now, we will not check if the tx is extended.
	// if !btTx.IsExtended() {
	// 	return 0, fmt.Errorf("cannot get fees for non extended tx")
	// }

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
