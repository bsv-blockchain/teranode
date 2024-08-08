package util

import (
	"github.com/libsv/go-bt/v2"
)

const LockTimeBIP113 = 419328

// ValidLockTime checks whether a lock time is valid in the context of a block height and median block time
// the block height and median block time are the values of the block in which the transaction is mined
func ValidLockTime(lockTime uint32, blockHeight uint32, medianBlockTime uint32) bool {
	blockLockTime := lockTime < 500000000 && blockHeight >= lockTime

	// Note that since the adoption of BIP 113, the time-based nLockTime is compared to the 11-block median time
	// past (the median timestamp of the 11 blocks preceding the block in which the transaction is mined), and not the
	// block time itself.
	timeLockTime := lockTime >= 500000000 && medianBlockTime >= lockTime

	return blockLockTime || timeLockTime
}

// IsTransactionFinal checks whether a transaction is final
// Consensus rule, referenced in requirements document as TNJ-13:
// A transaction must be final, meaning that either of the following conditions is met:
// - The sequence number in all inputs is equal to 0xffffffff, or
// - The lock time is: Equal to zero, or <500000000 and smaller than block height, or >=500000000 and smaller than timestamp
//
// Any transaction that does not adhere to this consensus rule is to be rejected. It is up to the user to properly set
// up payment channels or use an escrow service for their non-final transactions. Teranode shouldn't be aware or care
// about them. These transactions should be rejected as per the consensus rules they are invalid.
func IsTransactionFinal(tx *bt.Tx, blockHeight uint32, medianBlockTime uint32) bool {
	if len(tx.Inputs) == 0 {
		return false // transactions with no inputs are not valid, and therefore not final
	}

	// check that the sequence number of all inputs is final
	allSequenceNumbersFinal := true
	for _, input := range tx.Inputs {
		allSequenceNumbersFinal = allSequenceNumbersFinal && input.SequenceNumber == bt.DefaultSequenceNumber
	}
	if allSequenceNumbersFinal {
		return true
	}

	// check that the locktime is final
	if tx.LockTime > 0 {
		return ValidLockTime(tx.LockTime, blockHeight, medianBlockTime)
	}

	// if the locktime is 0, then the transaction is final, even if the sequence numbers are not final
	return true
}
