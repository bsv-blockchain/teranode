package util

import (
	"github.com/libsv/go-bt/v2"
)

const (
	// https://en.wikipedia.org/wiki/Bitcoin_Cash#:~:text=The%20fork%20that%20created%20Bitcoin,second%20version%20or%20an%20altcoin.
	ForkIDActivationHeight  = 478559
	GenesisActivationHeight = 620538
)

func IsExtended(tx *bt.Tx, blockHeight uint32) bool {
	if blockHeight < GenesisActivationHeight {
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

	return tx.IsExtended()
}
