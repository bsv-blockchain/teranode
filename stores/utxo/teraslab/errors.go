package teraslab

import (
	teraslab "github.com/icellan/teraslab/client/go"

	"github.com/bsv-blockchain/teranode/errors"
)

// partialErrorToError converts a TeraSlab PartialError into a single joined
// Teranode error — one mapped error per failed item, tagged with the operation
// name and item index. Returns nil only if there are no item errors.
//
// Used by the batch mutation paths so per-item failures propagate to the caller
// instead of being silently swallowed (a money-path correctness requirement;
// the Aerospike backend likewise aggregates and returns per-item errors).
func partialErrorToError(op string, pe *teraslab.PartialError) error {
	if pe == nil || len(pe.Errors) == 0 {
		return nil
	}

	errs := make([]error, 0, len(pe.Errors))
	for _, ie := range pe.Errors {
		errs = append(errs, errors.NewProcessingError("%s item %d failed", op, ie.ItemIndex, mapErrorCode(ie.Code)))
	}

	return errors.Join(errs...)
}

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
	case teraslab.ErrCodeRedirect:
		// Redirects are resolved client-side; surfacing one here means the
		// client failed to follow it, which is an internal/storage-class fault.
		return errors.NewStorageError("teraslab unexpected redirect", nil)
	default:
		return errors.NewStorageError("teraslab unknown error code", nil)
	}
}
