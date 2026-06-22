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
		return errors.ErrTxCoinbaseImmature
	case teraslab.ErrCodeAlreadyExists:
		return errors.ErrTxExists
	case teraslab.ErrCodeVoutOutOfRange:
		return errors.NewUtxoError("vout out of range")
	case teraslab.ErrCodeUtxoHashMismatch:
		return errors.ErrUtxoHashMismatch
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

	// Cluster / replication / migration codes (15-37). The client v0.5.1 does
	// not expose named constants for these, so they are matched by their wire
	// values from src/protocol/opcodes.rs.
	//
	// Transient codes: the operation can succeed on retry once the cluster
	// converges, so they map to a service-unavailable (retryable) error.
	case 15: // ERR_NO_QUORUM
		return errors.NewServiceUnavailableError("teraslab no quorum")
	case 19: // ERR_MIGRATION_IN_PROGRESS
		return errors.NewServiceUnavailableError("teraslab migration in progress")
	case 20: // ERR_REPLICATION_FAILED
		return errors.NewServiceUnavailableError("teraslab replication failed")
	case 24: // ERR_STALE_EPOCH
		return errors.NewServiceUnavailableError("teraslab stale epoch")
	case 25: // ERR_CLUSTER_NOT_READY
		// Distinct from the transient codes above so startup gating can tell a
		// not-yet-ready cluster apart from a mid-flight convergence stall.
		return errors.NewStorageError("teraslab cluster not ready")

	// Streaming / blob codes.
	case 16: // ERR_STREAM_NOT_FOUND
		return errors.NewStorageError("teraslab stream not found")
	case 17: // ERR_BLOB_NOT_FOUND
		return errors.NewStorageError("teraslab blob not found")

	// Protocol / invariant codes: the request or server state is malformed, so
	// these are processing-class faults rather than retryable ones.
	case 18: // ERR_STREAM_OFFSET_MISMATCH
		return errors.NewProcessingError("teraslab stream offset mismatch")
	case 28: // ERR_PAYLOAD_MALFORMED
		return errors.NewProcessingError("teraslab payload malformed")
	case 29: // ERR_OPCODE_UNSUPPORTED
		return errors.NewProcessingError("teraslab opcode unsupported")
	case 33: // ERR_INVARIANT_VIOLATION
		return errors.NewProcessingError("teraslab invariant violation")
	case 34: // ERR_STREAM_INVARIANT
		return errors.NewProcessingError("teraslab stream invariant violation")

	// Remaining cluster / admin / storage codes.
	case 21: // ERR_MIGRATION_MANIFEST_REQUIRED
		return errors.NewStorageError("teraslab migration manifest required")
	case 22: // ERR_MIGRATION_MANIFEST_MISMATCH
		return errors.NewStorageError("teraslab migration manifest mismatch")
	case 23: // ERR_TOPOLOGY_PERSIST_FAILED
		return errors.NewStorageError("teraslab topology persist failed")
	case 26: // ERR_INDEX_DEGRADED
		return errors.NewStorageError("teraslab index degraded")
	case 27: // ERR_CLUSTER_AUTH_FAILED
		return errors.NewStorageError("teraslab cluster auth failed")
	case 30: // ERR_STORAGE_IO
		return errors.NewStorageError("teraslab storage io error")
	case 31: // ERR_RATE_LIMITED
		return errors.NewStorageError("teraslab rate limited")
	case 32: // ERR_NOT_CLUSTERED
		return errors.NewStorageError("teraslab not clustered")
	case 35: // ERR_DELETED_CHILDREN
		return errors.NewStorageError("teraslab deleted children")
	case 36: // ERR_NOT_DUE
		return errors.NewStorageError("teraslab not due")
	case 37: // ERR_MIGRATION_TARGET_NOT_READY
		return errors.NewStorageError("teraslab migration target not ready")

	default:
		return errors.NewStorageError("teraslab unknown error code", nil)
	}
}
