package util

import "github.com/libsv/go-bt/v2"

func IsExtended(tx *bt.Tx) bool {
	if tx == nil || tx.Inputs == nil {
		return false
	}

	for _, input := range tx.Inputs {
		if input.PreviousTxScript == nil || (input.PreviousTxSatoshis == 0 && !input.PreviousTxScript.IsData()) {
			return false
		}
	}

	return true
}
