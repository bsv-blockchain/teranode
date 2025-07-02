// Package utxo provides UTXO (Unspent Transaction Output) management for the Bitcoin SV Teranode implementation.
//
// The UTXO package implements functionality for tracking, managing, and validating Unspent Transaction
// Outputs within the Bitcoin SV blockchain. It provides comprehensive error handling for transaction
// validation scenarios including locktime enforcement, freeze management, and conflicting transaction
// detection.
//
// Error handling in this package follows a structured approach where domain-specific errors
// are created with appropriate context information to facilitate debugging and client feedback.
package utxo

import (
	"fmt"
	"time"

	"github.com/bitcoin-sv/teranode/errors"
)

const (
	// isoFormat defines the ISO 8601 time format used for locktime error messages.
	// This format provides a standardized timestamp representation (ISO 8601) for error messages
	// related to time-based locktimes.
	isoFormat = "2006-01-02T15:04:05Z"
	// nilString is a constant representing a nil value in string form.
	// Used for string representation of nil values in error messages.
	nilString = "nil"
)

// NewErrLockTime creates a new transaction locktime error with context-specific messaging.
// It handles both block height-based and timestamp-based locktimes.
//
// The function differentiates between three locktime scenarios:
//  1. lockTime == 0: Simple locktime error with no time constraints
//  2. lockTime < 500,000,000: Timestamp-based locktime (Unix timestamp in seconds)
//  3. lockTime >= 500,000,000: Block height-based locktime
//
// The threshold of 500,000,000 is the standard Bitcoin protocol distinction between
// timestamp-based and block height-based locktimes.
//
// Parameters:
//   - lockTime: The locktime value to check against (block height or Unix timestamp)
//   - blockHeight: Current block height for comparison with height-based locktimes
//   - optionalErrs: Optional errors to include in the error context
//
// Returns:
//   - A formatted TxLockTimeError with appropriate context message that clearly
//     indicates why the transaction is locked and when it will become valid
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
