// Package utxo provides UTXO (Unspent Transaction Output) management for the Bitcoin SV Teranode implementation.
package utxo

import (
	"fmt"
	"time"

	"github.com/bitcoin-sv/teranode/errors"
)

const (
	// isoFormat defines the ISO 8601 time format used for locktime error messages
	isoFormat = "2006-01-02T15:04:05Z"
	nilString = "nil"
)

// NewErrLockTime creates a new transaction locktime error with context-specific messaging.
// It handles both block height-based and timestamp-based locktimes.
//
// Parameters:
//   - lockTime: The locktime value to check against (block height or Unix timestamp)
//   - blockHeight: Current block height for comparison with height-based locktimes
//   - optionalErrs: Optional errors to include in the error context
//
// Returns:
//   - A formatted TxLockTimeError with appropriate context message
func NewErrLockTime(lockTime uint32, blockHeight uint32, optionalErrs ...error) error {
	var errorString string

	switch {
	case lockTime == 0:
		errorString = "ErrLockTime"
	case lockTime < 500_000_000:
		// This is a timestamp based locktime
		spendableAt := time.Unix(int64(lockTime), 0)
		errorString = fmt.Sprintf("utxo is locked until %s", spendableAt.UTC().Format(isoFormat))
	default:
		errorString = fmt.Sprintf("utxo is locked until block %d (height check: %d)", lockTime, blockHeight)
	}

	return errors.NewTxLockTimeError(errorString)
}
