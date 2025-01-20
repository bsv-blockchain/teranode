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
		// on both testnet and mainnet there are outputs with empty scripts
		// that get spent later, e.g. `8d1c472b6584844c204cb546a3ad6bfd924b4816bc886d6693b5a93888fc019e`
		//  || len(*input.PreviousTxScript) == 0
		if input.PreviousTxScript == nil {
			return false
		}
	}

	return true
}
