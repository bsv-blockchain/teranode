/*
Package validator implements BSV Blockchain transaction validation functionality.

This file contains the core transaction validation logic and implements the standard
Bitcoin transaction validation rules and policies. The TxValidator component is responsible
for enforcing both consensus rules (which all nodes must follow) and policy rules
(which can be configured per node).

The implementation uses GoBDK for BSV transaction validation and keeps only the
Teranode-owned checks that need node context outside BDK.

The validation process enforces rules including but not limited to:
- BDK transaction structure, standardness, sigops, and script validation
- Teranode-specific input and node-context checks
- Fee policy enforcement
- Locktime and sequence number verification

This component is designed to be highly performant and configurable to support
different validation scenarios from development to high-volume production environments.
*/
package validator

import (
	"math"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/ulogger"
)

// BIP68 sequence lock constants
// These constants are used for relative lock-time enforcement via input sequence numbers
const (
	// SequenceLockTimeDisableFlag is the flag bit that disables the relative locktime feature
	// If this bit is set, the sequence number is not interpreted as a relative lock-time
	SequenceLockTimeDisableFlag uint32 = 1 << 31

	// SequenceLockTimeTypeFlag is the flag bit that determines the lock-time type
	// If set, the sequence number specifies a relative time lock in 512-second units
	// If not set, the sequence number specifies a relative block height lock
	SequenceLockTimeTypeFlag uint32 = 1 << 22

	// SequenceLockTimeMask is the bitmask to extract the lock-time value from sequence number
	// Only the lower 16 bits are used for the actual lock-time value
	SequenceLockTimeMask uint32 = 0x0000ffff

	// SequenceLockTimeGranularity is the granularity for time-based sequence locks
	// Time-based locks use 512-second (2^9 seconds) granularity
	SequenceLockTimeGranularity = 9
)

// TxValidatorI defines the interface for transaction validation operations.
// This interface serves as the contract for all transaction validators, abstracting
// the implementation details from the rest of the system. This enables different
// validation strategies to be used (including mocks for testing) while maintaining
// a consistent API.
//
// The validator is responsible for enforcing Teranode-owned checks that need
// node context and then running BDK-side validation.
type TxValidatorI interface {
	// ValidateTransaction performs Teranode-owned validation that needs node
	// context and BDK-side transaction validation, excluding BIP68 sequence-lock
	// checks. BIP68 validation is performed separately via ValidateBIP68 so that
	// MTP lookups are skipped when the transaction fails normal validation first.
	//
	// Parameters:
	//   - tx: The extended transaction to validate, with previous-output data populated
	//   - blockHeight: The current block height for validation context
	//   - utxoHeights: Block heights where each input UTXO was created (nil if not available)
	//   - validationOptions: Optional validation options to customize validation behavior
	// Returns:
	//   - error: Specific validation error with reason if validation fails, nil on success
	ValidateTransaction(tx *bt.Tx, blockHeight uint32, utxoHeights []uint32, validationOptions *Options) error

	// ValidateBIP68 verifies that BIP68 relative lock-time constraints are satisfied.
	// This must only be called for block validation (SkipPolicyChecks=true) and only
	// after ValidateTransaction succeeds. Keeping BIP68 separate avoids the cost of
	// MTP lookups when the transaction fails normal validation.
	//
	// Parameters:
	//   - tx: The transaction to validate
	//   - blockHeight: Height of the block being validated
	//   - utxoHeights: Block heights where each input UTXO was created
	//   - utxoMTPs: Median Time Past values for each UTXO height (stored_mtp(utxoHeight))
	//   - blockMTP: Median Time Past for the block (stored_mtp(blockHeight-1))
	// Returns:
	//   - error: Validation error if sequence locks are not satisfied, nil on success
	ValidateBIP68(tx *bt.Tx, blockHeight uint32, utxoHeights []uint32, utxoMTPs []uint32, blockMTP uint32) error
}

// TxValidator implements transaction validation logic
type TxValidator struct {
	logger   ulogger.Logger
	settings *settings.Settings
	bdk      bdkValidator
	options  *TxValidatorOptions
}

// NewTxValidator creates a new transaction validator with the specified configuration
// Parameters:
//   - logger: Logger instance for validation operations
//   - policy: Policy settings for validation rules
//   - params: Network parameters
//   - opts: Optional validator settings
//
// Returns:
//   - TxValidatorI: The created transaction validator
func NewTxValidator(logger ulogger.Logger, tSettings *settings.Settings, opts ...TxValidatorOption) *TxValidator {
	options := NewTxValidatorOptions(opts...)

	return &TxValidator{
		logger:   logger,
		settings: tSettings,
		bdk:      newScriptVerifierGoBDK(logger, tSettings.Policy, tSettings.ChainCfgParams),
		options:  options,
	}
}

// ValidateTransaction performs Teranode-owned transaction validation and BDK
// transaction validation, excluding BIP68 sequence-lock checks (use
// ValidateBIP68 for that).
//
// Parameters:
//   - tx: The extended transaction to validate, with previous-output data populated
//   - blockHeight: Current block height for validation context
//   - utxoHeights: Block heights where each input UTXO was created
//   - validationOptions: Optional validation options
//
// Returns:
//   - error: Any validation errors encountered
func (tv *TxValidator) ValidateTransaction(tx *bt.Tx, blockHeight uint32, utxoHeights []uint32, validationOptions *Options) error {
	if validationOptions == nil {
		validationOptions = NewDefaultOptions()
	}

	// BDK rejects coinbase transactions in both modes. Keep coinbase routing
	// outside BDK so the adapter only sees regular transactions.
	if tx.IsCoinbase() {
		return errors.NewTxInvalidError("coinbase transactions are not supported")
	}

	if err := tv.checkInputs(tx, blockHeight, validationOptions); err != nil {
		return err
	}

	// Legacy catchup below the highest hard-coded checkpoint sets this: PoW +
	// checkpoint linkage already establish the chain as canonical, so re-running
	// scripts is pure overhead. The caller is responsible for ensuring the block
	// is actually trusted (see SyncManager.quickValidationAllowed).
	if validationOptions.SkipScriptValidation {
		// The normal path leans on BDK to reject MEMPOOL_HEIGHT in consensus mode
		// (bdk/core/txvalidator.cpp:779), but skipping script validation bypasses
		// BDK entirely. Without this guard the unconfirmedParentHeight sentinel
		// would propagate to BIP68 (height conversion produces -1 from 0xFFFFFFFF
		// in sequenceLocks; MTP lookup in Validator.readMTPsLocked clamps to
		// blockMTP) and the tx would be silently accepted. Mirror BDK's
		// UnconfirmedInputInBlock rejection here.
		if validationOptions.SkipPolicyChecks {
			for _, h := range utxoHeights {
				if h == unconfirmedParentHeight {
					return errors.NewTxInvalidError("bad-txns-unconfirmed-input-in-block")
				}
			}
		}
		return nil
	}

	// Fee enforcement (including the consolidation-fee exemption) is performed by
	// BDK's ValidateTransaction in policy mode. Setters pushed at startup carry
	// MinMiningTxFee plus the four consolidation-policy values into BDK.
	// SkipPolicyChecks is equivalent to BDK consensus=true.
	// https://github.com/bsv-blockchain/teranode/issues/2367
	return tv.bdk.ValidateTransaction(tx, blockHeight, validationOptions.SkipPolicyChecks, utxoHeights)
}

// ValidateBIP68 verifies that BIP68 relative lock-time constraints are satisfied.
// Must be called separately after ValidateTransaction succeeds, and only for block
// validation (SkipPolicyChecks=true). This separation avoids the cost of MTP lookups
// when a transaction fails normal validation.
func (tv *TxValidator) ValidateBIP68(tx *bt.Tx, blockHeight uint32, utxoHeights []uint32, utxoMTPs []uint32, blockMTP uint32) error {
	return tv.sequenceLocks(tx, blockHeight, utxoHeights, utxoMTPs, blockMTP)
}

// sequenceLocks verifies that relative lock-time constraints (BIP68) are satisfied for block validation.
// This function implements the SequenceLocks check from SV Node validation.cpp.
//
// BIP68 allows transaction inputs to specify minimum block heights or times before they can be spent
// using the sequence number field. This enables relative lock-times for smart contracts and
// payment channels.
//
// Parameters:
//   - tx: The transaction to validate
//   - blockHeight: Height of the block being validated
//   - utxoHeights: Heights where each input UTXO was created
//   - utxoMTPs: Median Time Past values for inputHeight for each UTXO
//   - blockMTP: Median Time Past value for blockHeight
//
// Returns:
//   - error: Validation error if sequence locks are not satisfied, nil on success
func (tv *TxValidator) sequenceLocks(tx *bt.Tx, blockHeight uint32, utxoHeights []uint32, utxoMTPs []uint32, blockMTP uint32) error {
	// BIP68 is only active from CSVHeight onwards.
	// BSV C++ block validation: if (pindex_->GetHeight() >= consensusParams.CSVHeight)
	if blockHeight < tv.settings.ChainCfgParams.CSVHeight {
		return nil
	}

	// BSV Genesis restored the original Bitcoin semantics for nSequence: it is
	// used for RBF signalling only, not for relative lock-time enforcement. The
	// reference implementation drops LOCKTIME_VERIFY_SEQUENCE post-Genesis (see
	// src/policy/policy.h::StandardNonFinalVerifyFlags and
	// src/validation.cpp::CheckSequenceLocks), which makes CalculateSequenceLocks
	// a no-op. Mirror that here so blocks containing non-zero-sequence inputs are
	// accepted post-Genesis.
	//
	// This check uses >= to match BSV's IsGenesisEnabled() semantics: the
	// activation block itself is considered post-Genesis.
	if blockHeight >= tv.settings.ChainCfgParams.GenesisActivationHeight {
		return nil
	}

	// Version 2 transactions are required for BIP68
	// Transactions with version < 2 bypass relative lock-time enforcement
	if tx.Version < 2 {
		return nil
	}

	// Calculate sequence locks - find the minimum block height and time.
	// Initial value -1 means "no constraint": the semantics of nLockTime are the
	// last INVALID height/time, so -1 means any height or time is valid.
	// This matches BSV C++: int32_t nMinHeight = -1; int64_t nMinTime = -1;
	minHeight := int32(-1)
	minTime := int64(-1)

	// Process each input to determine lock requirements
	for i, input := range tx.Inputs {
		// If sequence has the disable flag set, skip this input
		if input.SequenceNumber&SequenceLockTimeDisableFlag != 0 {
			continue
		}

		// Extract the lock value from the sequence number (lower 16 bits)
		sequenceMasked := input.SequenceNumber & SequenceLockTimeMask

		// Check if this is a time-based or height-based lock
		if input.SequenceNumber&SequenceLockTimeTypeFlag != 0 {
			// Time-based relative lock-time
			// Calculate the minimum time required using the UTXO's MTP
			if i >= len(utxoMTPs) {
				return errors.NewTxInvalidError("missing MTP value for input %d", i)
			}

			// Time is in 512-second units (2^9 seconds)
			// Add the relative time offset to the UTXO's MTP, minus 1
			// (matching Bitcoin Core: nMinTime = nCoinTime + (sequence << granularity) - 1,
			// so the tx is valid starting from blockMTP >= nCoinTime + (sequence << granularity)).
			utxoMTP := int64(utxoMTPs[i])
			nTxTime := utxoMTP + (int64(sequenceMasked) << SequenceLockTimeGranularity) - 1

			// Update minimum time if this input requires a later time
			if nTxTime > minTime {
				minTime = nTxTime
			}
		} else {
			// Height-based relative lock-time
			// Calculate the minimum height required
			if i >= len(utxoHeights) {
				return errors.NewTxInvalidError("missing height value for input %d", i)
			}

			// Add the relative height offset to the UTXO's height, minus 1
			// (matching Bitcoin Core: nMinHeight = coinHeight + nSequence - 1,
			// so the tx is valid starting from blockHeight >= coinHeight + nSequence)
			nTxHeight := int32(utxoHeights[i]) + int32(sequenceMasked) - 1

			// Update minimum height if this input requires a later height
			if nTxHeight > minHeight {
				minHeight = nTxHeight
			}
		}
	}

	// Evaluate the calculated locks against the block being validated
	// The transaction can only be included if both height and time requirements are met

	// Check height requirement: minimum required height must be less than current block height.
	// blockHeight is uint32 but int32 conversion would wrap for values > math.MaxInt32; reject
	// such heights as invalid since no realistic block will ever reach that range.
	if blockHeight > math.MaxInt32 {
		return errors.NewTxInvalidError("block height %d exceeds maximum safe int32 value", blockHeight)
	}
	blockHeightInt32 := int32(blockHeight)
	if minHeight >= blockHeightInt32 {
		return errors.NewTxInvalidError("transaction sequence lock height not satisfied: required %d, current %d", minHeight, blockHeight)
	}

	// Check time requirement: minimum required time must be less than block's MTP
	if minTime >= int64(blockMTP) {
		return errors.NewTxInvalidError("transaction sequence lock time not satisfied: required %d, current %d", minTime, blockMTP)
	}

	return nil
}

// checkInputs validates transaction inputs according to consensus rules.
func (tv *TxValidator) checkInputs(tx *bt.Tx, blockHeight uint32, validationOptions *Options) error {
	accumulatedPrevUTXOSize := uint64(0)
	maxCoinsViewCacheSize := tv.settings.Policy.GetMaxCoinsViewCacheSize()

	// blockHeight is not used, but it is required by the interface
	_ = blockHeight

	for _, input := range tx.Inputs {
		// Null-prevout rejection is done in BDK
		// which replicates bitcoin-sv CheckRegularTransaction's bad-txns-prevout-null
		// check with the correct prevout.IsNull() semantics.

		// Check accumulated previous utxo size if maxcoinsviewcachesize is enabled
		// See BSV Node CCoinsViewCache::Shard::HaveInputsLimited
		//    https://github.com/teranode-group/bitcoin-sv-staging/blob/develop/src/coins.cpp#L131
		if !validationOptions.SkipPolicyChecks && maxCoinsViewCacheSize > 0 {
			if input.PreviousTxScript == nil {
				return errors.NewTxPolicyError("bad-txns-inputs-too-large")
			}

			accumulatedPrevUTXOSize += uint64(len(*input.PreviousTxScript))
			if accumulatedPrevUTXOSize > maxCoinsViewCacheSize {
				return errors.NewTxPolicyError("bad-txns-inputs-too-large")
			}
		}
	}

	return nil
}
