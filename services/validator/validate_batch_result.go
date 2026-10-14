package validator

import (
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
)

// ValidatePhase identifies which phase of ValidateBatch a per-tx error
// originated in. Used for telemetry and debugging; the zero value
// PhaseNone indicates success.
type ValidatePhase uint8

const (
	PhaseNone ValidatePhase = iota
	PhaseGetParents
	PhaseCPU
	PhaseSpend
	PhaseCreate
	PhaseBlockAssembly
	PhaseSetLocked
)

// ValidationResult is one element of the slice returned by ValidateBatch,
// indexed positionally against the input txs. A nil Err means success;
// Meta is then populated. A non-nil Err means failure at Phase.
//
// Phase A (BatchGetParents) failures are surfaced with the existing
// errors.ErrTxMissingParent sentinel from the teranode errors package
// rather than introducing a new local sentinel.
type ValidationResult struct {
	TxHash chainhash.Hash
	Meta   *meta.Data
	Err    error
	Phase  ValidatePhase
}
