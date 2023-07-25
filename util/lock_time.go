package util

import "time"

func ValidLockTime(lockTime uint32, blockHeight uint32) bool {
	blockLockTime := lockTime < 500000000 && blockHeight >= lockTime

	// TODO Note that since the adoption of BIP 113, the time-based nLockTime is compared to the 11-block median time
	// past (the median timestamp of the 11 blocks preceding the block in which the transaction is mined), and not the
	// block time itself.
	timeLockTime := lockTime >= 500000000 && time.Unix(int64(lockTime), 0).After(time.Now())

	return blockLockTime || timeLockTime
}
