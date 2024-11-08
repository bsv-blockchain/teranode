package util

import (
	"github.com/libsv/go-bt/v2"
)

// IsExtended checks if a transaction is extended
// NOTE: 0 satoshi inputs are valid in older transactions
func IsExtended(tx *bt.Tx, blockHeight uint32) bool {
	if tx == nil || tx.Inputs == nil {
		return false
	}

	for _, input := range tx.Inputs {
		if input.PreviousTxScript == nil {
			return false
		}
	}

	return true
}
