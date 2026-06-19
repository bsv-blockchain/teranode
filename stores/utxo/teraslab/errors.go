package teraslab

import (
	teraslab "github.com/icellan/teraslab/client/go"

	"github.com/bsv-blockchain/teranode/errors"
)

// mapErrorCode converts a TeraSlab error code to a Teranode error.
func mapErrorCode(code uint16) error {
	switch code {
	case teraslab.ErrCodeOK:
		return nil
	case teraslab.ErrCodeTxNotFound:
		return errors.ErrTxNotFound
	case teraslab.ErrCodeAlreadySpent:
		return errors.ErrSpent
	case teraslab.ErrCodeAlreadyFrozen:
		return errors.ErrFrozen
	case teraslab.ErrCodeUtxoNotFrozen:
		return errors.NewUtxoError("utxo is not frozen")
	case teraslab.ErrCodeFrozen:
		return errors.ErrFrozen
	case teraslab.ErrCodeConflicting:
		return errors.ErrTxConflicting
	case teraslab.ErrCodeLocked:
		return errors.ErrTxLocked
	case teraslab.ErrCodeCoinbaseImmature:
		return errors.ErrNonFinal
	case teraslab.ErrCodeAlreadyExists:
		return errors.ErrTxExists
	case teraslab.ErrCodeVoutOutOfRange:
		return errors.NewUtxoError("vout out of range")
	case teraslab.ErrCodeUtxoHashMismatch:
		return errors.NewUtxoError("utxo hash mismatch")
	case teraslab.ErrCodeInvalidSpend:
		return errors.NewUtxoError("invalid spend")
	case teraslab.ErrCodeFrozenUntil:
		return errors.ErrFrozen
	case teraslab.ErrCodeInternal:
		return errors.NewStorageError("teraslab internal error", nil)
	default:
		return errors.NewStorageError("teraslab unknown error code", nil)
	}
}
