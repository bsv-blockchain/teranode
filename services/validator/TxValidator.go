/*
Package validator implements BSV Blockchain transaction validation functionality.

This file contains the core transaction validation logic and implements the standard
Bitcoin transaction validation rules and policies. The TxValidator component is responsible
for enforcing both consensus rules (which all nodes must follow) and policy rules
(which can be configured per node).

The implementation supports multiple script interpreters through a plugin architecture,
allowing different script verification engines to be used based on configuration. Currently
supported interpreters include:
- Go-BT: Pure Go implementation from the libsv/go-bt library
- Go-SDK: BSV SDK implementation
- Go-BDK: Bitcoin Development Kit implementation

The validation process enforces rules including but not limited to:
- Transaction size limits
- Input and output structure verification
- Non-dust output values
- Script operation count limits
- Signature operation (SIGOPS) counting with full CScriptNum parsing support
- Signature verification
- Fee policy enforcement
- Locktime and sequence number verification

This component is designed to be highly performant and configurable to support
different validation scenarios from development to high-volume production environments.
*/
package validator

import (
	"math"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/bscript/interpreter"
	"github.com/bsv-blockchain/go-chaincfg"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/ulogger"
)

// TxInterpreter defines the type of script interpreter to be used
// for transaction validation
type TxInterpreter string

const (
	// TxInterpreterGoBT specifies the Go-BT library interpreter
	TxInterpreterGoBT TxInterpreter = "GoBT"

	// TxInterpreterGoSDK specifies the Go-SDK library interpreter
	TxInterpreterGoSDK TxInterpreter = "GoSDK"

	// TxInterpreterGoBDK specifies the Go-BDK library interpreter
	TxInterpreterGoBDK TxInterpreter = "GoBDK"
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
// The validator is responsible for enforcing Bitcoin consensus rules and configurable
// policy rules across the full range of transaction properties. This includes
// script verification, size limits, fee policies, and structure validation.
type TxValidatorI interface {
	// ValidateTransaction performs comprehensive validation of a transaction,
	// excluding BIP68 sequence-lock checks. This method enforces all consensus
	// and policy rules including format, structure, inputs/outputs, script
	// verification, and fees. BIP68 validation is performed separately via
	// ValidateBIP68 so that MTP lookups are skipped when the transaction fails
	// normal validation first.
	//
	// Parameters:
	//   - tx: The transaction to validate, must be properly initialized
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

	// ValidateTransactionScripts performs script validation for a transaction.
	// This method specifically handles the script execution and signature verification
	// portion of validation, which is typically the most computationally intensive part.
	// It can be called independently from ValidateTransaction when only script
	// validation is needed.
	//
	// Parameters:
	//   - tx: The transaction containing the scripts to validate
	//   - blockHeight: Current block height for validation context (affects script flags)
	//   - utxoHeights: Heights of the UTXOs being spent, used for BIP68 relative locktime
	//   - validationOptions: Optional validation options to customize validation behavior
	// Returns:
	//   - error: Specific script validation error if validation fails, nil on success
	ValidateTransactionScripts(tx *bt.Tx, blockHeight uint32, utxoHeights []uint32, validationOptions *Options) error
}

// TxValidator implements transaction validation logic
type TxValidator struct {
	logger      ulogger.Logger
	settings    *settings.Settings
	interpreter TxScriptInterpreter
	options     *TxValidatorOptions
}

// TxScriptInterpreter defines the interface for script verification operations
type TxScriptInterpreter interface {
	// VerifyScript implements script verification for a transaction
	// Parameters:
	//   - tx: The transaction containing the scripts to verify
	//   - blockHeight: Current block height for validation context
	// Returns:
	//   - error: Any script verification errors encountered
	// Logger return the encapsulated logger

	// VerifyScript implement the method to verify a script for a transaction
	VerifyScript(tx *bt.Tx, blockHeight uint32, consensus bool, utxoHeights []uint32) error

	// Interpreter returns the interpreter being used
	Interpreter() TxInterpreter
}

// TxScriptInterpreterCreator defines a function type for creating script interpreters
// Parameters:
//   - logger: Logger instance for the interpreter
//   - policy: Policy settings for validation
//   - params: Network parameters
//
// Returns:
//   - TxScriptInterpreter: The created script interpreter
type TxScriptInterpreterCreator func(logger ulogger.Logger, policy *settings.PolicySettings, params *chaincfg.Params) TxScriptInterpreter

// TxScriptInterpreterFactory stores registered TxValidator creator methods
// The factory is populated at build time based on build tags
var TxScriptInterpreterFactory = make(map[TxInterpreter]TxScriptInterpreterCreator)

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

	var txScriptInterpreter TxScriptInterpreter

	// If a creator was not registered to the factory, then return nil
	if createTxScriptInterpreter, ok := TxScriptInterpreterFactory[TxInterpreterGoBDK]; ok {
		txScriptInterpreter = createTxScriptInterpreter(logger, tSettings.Policy, tSettings.ChainCfgParams)
	}

	// Make sure script interpreter is created
	if txScriptInterpreter == nil {
		panic("unable to create script interpreter")
	}

	return &TxValidator{
		logger:      logger,
		settings:    tSettings,
		interpreter: txScriptInterpreter,
		options:     options,
	}
}

// ValidateTransaction performs comprehensive validation of a transaction,
// excluding BIP68 sequence-lock checks (use ValidateBIP68 for that).
// This includes checking:
//  1. Input and output presence
//  2. Transaction size limits
//  3. Input values and coinbase restrictions
//  4. Output values and dust limits
//  5. Lock time requirements
//  6. Script operation limits
//  7. Script validation
//  8. Fee requirements
//
// Parameters:
//   - tx: The transaction to validate
//   - blockHeight: Current block height for validation context
//   - utxoHeights: Block heights where each input UTXO was created
//   - validationOptions: Optional validation options
//
// Returns:
//   - error: Any validation errors encountered
func (tv *TxValidator) ValidateTransaction(tx *bt.Tx, blockHeight uint32, utxoHeights []uint32, validationOptions *Options) error {
	//
	// Each node will verify every transaction against a long checklist of criteria:
	//
	txSize := tx.Size()

	// 1) Neither lists of inputs nor outputs are empty
	if len(tx.Inputs) == 0 || len(tx.Outputs) == 0 {
		return errors.NewTxInvalidError("transaction has no inputs or outputs")
	}

	// 2) Check transaction size against both consensus and policy limits
	// Consensus limits are ALWAYS checked, policy limits only for mempool transactions
	if err := tv.checkTxSize(txSize, blockHeight, validationOptions.SkipPolicyChecks); err != nil {
		return err
	}

	// 3) check that each input value, as well as the sum, are in the allowed range of values (less than 21m coins)
	if err := tv.checkInputs(tx, blockHeight, validationOptions); err != nil {
		return err
	}

	// 4) Each output value, as well as the total, must be within the allowed range of values (less than 21m coins,
	//    more than the dust threshold if 1 unless it's OP_RETURN, which is allowed to be 0)
	if err := tv.checkOutputs(tx, blockHeight, validationOptions); err != nil {
		return err
	}

	// 5) Check that no inputs have null prevouts (hash=0, N=0xFFFFFFFF)
	// This is a consensus check that prevents invalid input references
	if err := tv.checkPrevOutputs(tx); err != nil {
		return err
	}

	// 6) Consensus: Check signature operations count (pre-Genesis only)
	// This check is ALWAYS enforced (even for block transactions) as it's a consensus rule.
	// Before Genesis: limit is 20,000 sigops (counts only tx inputs/outputs, not P2SH redeem scripts)
	// After Genesis: unlimited (no check needed)
	// Matches C++ bitcoin-sv: CheckTransactionCommon in validation.cpp:561-567
	if err := tv.checkConsensusSigops(tx, blockHeight); err != nil {
		return err
	}

	// 7) nLocktime is equal to INT_MAX, or nLocktime and nSequence values are satisfied according to MedianTimePast
	//    => checked by the node, we do not want to have to know the current block height

	// 8) The transaction size in bytes is greater than or equal to 100
	//    => This is a BCH only check, not applicable to BSV

	// 9) Policy: The number of signature operations (SIGOPS) contained in the transaction is less than the policy limit
	// This is a policy check (not consensus) and only applies to mempool transactions.
	if !validationOptions.SkipPolicyChecks {
		if err := tv.sigOpsCheck(tx, blockHeight, utxoHeights, validationOptions); err != nil {
			return err
		}
	}

	// 10) Reject if the sum of input values is less than sum of output values
	// 11) Reject if transaction fee would be too low (minRelayTxFee) to get into an empty block.
	if !validationOptions.SkipPolicyChecks {
		if err := tv.checkFees(tx, blockHeight, utxoHeights); err != nil {
			return err
		}
	}

	return nil
}

// ValidateBIP68 verifies that BIP68 relative lock-time constraints are satisfied.
// Must be called separately after ValidateTransaction succeeds, and only for block
// validation (SkipPolicyChecks=true). This separation avoids the cost of MTP lookups
// when a transaction fails normal validation.
func (tv *TxValidator) ValidateBIP68(tx *bt.Tx, blockHeight uint32, utxoHeights []uint32, utxoMTPs []uint32, blockMTP uint32) error {
	return tv.sequenceLocks(tx, blockHeight, utxoHeights, utxoMTPs, blockMTP)
}

// ValidateTransactionScripts performs script validation for all transaction inputs.
func (tv *TxValidator) ValidateTransactionScripts(tx *bt.Tx, blockHeight uint32, utxoHeights []uint32, validationOptions *Options) error {
	if tv == nil {
		return errors.NewTxInvalidError("tx validator is nil")
	}

	if tv.interpreter == nil {
		return errors.NewTxInvalidError("tx interpreter is nil, available interpreters: %v", TxScriptInterpreterFactory)
	}

	// SkipPolicy is equivalent to execute the script with consensus = true
	// https://github.com/bsv-blockchain/teranode/issues/2367
	consensus := true
	if validationOptions != nil {
		consensus = validationOptions.SkipPolicyChecks
	}

	// 12) The unlocking scripts for each input must validate against the corresponding output locking scripts
	if err := tv.interpreter.VerifyScript(tx, blockHeight, consensus, utxoHeights); err != nil {
		return err
	}

	// everything checks out
	return nil
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
			// Add the relative time offset to the UTXO's MTP
			utxoMTP := int64(utxoMTPs[i])
			nTxTime := utxoMTP + (int64(sequenceMasked) << SequenceLockTimeGranularity)

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

// checkOutputs validates transaction outputs according to consensus and policy rules.
func (tv *TxValidator) checkOutputs(tx *bt.Tx, blockHeight uint32, validationOptions *Options) error {
	total := uint64(0)

	for index, output := range tx.Outputs {

		if output.Satoshis > MaxSatoshis {
			return errors.NewTxInvalidError("transaction output %d satoshis is invalid", index)
		}
		total += output.Satoshis
	}

	if total > MaxSatoshis {
		return errors.NewTxInvalidError("transaction output total satoshis is too high")
	}

	// Consensus: After Genesis, P2SH outputs are not allowed in new transactions
	// This check is ALWAYS enforced (for both block and mempool transactions) as it's a consensus rule.
	// Matches C++ bitcoin-sv implementation: CheckRegularTransaction in validation.cpp:611-623
	// where it checks: if (IsProtocolActive(era, ProtocolName::Genesis)) { check for P2SH outputs }
	genesisActivationHeight := tv.settings.ChainCfgParams.GenesisActivationHeight
	isPostGenesis := blockHeight >= genesisActivationHeight
	if isPostGenesis {
		for _, output := range tx.Outputs {
			if output.LockingScript != nil && output.LockingScript.IsP2SH() {
				return errors.NewTxInvalidError("bad-txns-vout-p2sh")
			}
		}
	}

	return nil
}

// checkPrevOutputs validates that no transaction inputs have null prevouts.
// A null prevout is one where the previous transaction ID is all zeros AND the output index is 0xFFFFFFFF.
// This check is ALWAYS enforced (for both block and mempool transactions) as it's a consensus rule.
// Matches C++ bitcoin-sv implementation: CheckRegularTransaction in validation.cpp:628-631
// where it checks: if (txin.prevout.IsNull()) return error
//
// Parameters:
//   - tx: The transaction to validate
//
// Returns:
//   - error: Returns "bad-txns-prevout-null" if a null prevout is found, nil otherwise
func (tv *TxValidator) checkPrevOutputs(tx *bt.Tx) error {
	// Consensus check: No null prevouts allowed
	// Matches C++ bitcoin-sv implementation: CheckRegularTransaction in validation.cpp:628-631
	// A prevout is null if: txid.IsNull() && n == uint32_t(-1)
	// In Go: all bytes of PreviousTxID are zero AND PreviousTxOutIndex is 0xFFFFFFFF (4294967295)

	for _, input := range tx.Inputs {
		// Check if this is a null prevout
		// txid is all zeros
		previousTxID := input.PreviousTxID()
		isNullTxID := true
		for _, b := range previousTxID {
			if b != 0 {
				isNullTxID = false
				break
			}
		}

		// output index is 0xFFFFFFFF (max uint32)
		isMaxOutIndex := input.PreviousTxOutIndex == 0xFFFFFFFF

		// If both conditions are true, this is a null prevout
		if isNullTxID && isMaxOutIndex {
			return errors.NewTxInvalidError("bad-txns-prevout-null")
		}
	}

	return nil
}

// checkConsensusSigops validates that the transaction's signature operations count complies with consensus limits.
// This check is ONLY for pre-Genesis transactions and ALWAYS runs (even with SkipPolicyChecks=true).
// After Genesis, sigops are unlimited so this check is skipped.
//
// This implements the consensus check from C++ bitcoin-sv: CheckTransactionCommon in validation.cpp:561-567
// where it counts sigops using GetSigOpCountWithoutP2SH (does NOT include P2SH redeem scripts).
//
// Key differences from policy sigops check:
// - This is a CONSENSUS rule (always enforced), policy check is optional
// - This counts WITHOUT P2SH redeem scripts (doesn't need UTXO data)
// - This uses consensus limit (20,000), policy check uses configurable limit
// - This only applies pre-Genesis, policy check can apply at any height
//
// Parameters:
//   - tx: The transaction to validate
//   - blockHeight: Current block height to determine if Genesis is active
//
// Returns:
//   - error: Returns "bad-txn-sigops" if sigops exceed consensus limit, nil otherwise
func (tv *TxValidator) checkConsensusSigops(tx *bt.Tx, blockHeight uint32) error {
	// Consensus check: Only applies before Genesis (after Genesis, sigops are unlimited)
	// Matches C++ bitcoin-sv implementation: CheckTransactionCommon in validation.cpp:561-567
	genesisActivationHeight := tv.settings.ChainCfgParams.GenesisActivationHeight
	isPostGenesis := blockHeight >= genesisActivationHeight

	// After Genesis, sigops are unlimited, no consensus check needed
	if isPostGenesis {
		return nil
	}

	// Count sigops WITHOUT P2SH (only in transaction inputs and outputs)
	// This matches C++ GetSigOpCountWithoutP2SH behavior
	var totalSigOps uint64 = 0

	// Count sigops in transaction inputs (unlocking scripts)
	for _, input := range tx.Inputs {
		sigOps, err := tv.countSigOpsInScript(input.UnlockingScript, false, false)
		if err != nil {
			// If we can't count sigops, treat it as exceeding the limit
			return errors.NewTxInvalidError("bad-txn-sigops")
		}
		totalSigOps += sigOps
	}

	// Count sigops in transaction outputs (locking scripts)
	for _, output := range tx.Outputs {
		sigOps, err := tv.countSigOpsInScript(output.LockingScript, false, false)
		if err != nil {
			// If we can't count sigops, treat it as exceeding the limit
			return errors.NewTxInvalidError("bad-txn-sigops")
		}
		totalSigOps += sigOps
	}

	// Check against consensus limit (20,000 sigops before Genesis)
	if totalSigOps > MaxTxSigopsCountConsensusBeforeGenesis {
		return errors.NewTxInvalidError("bad-txn-sigops")
	}

	return nil
}

// checkInputs validates transaction inputs according to consensus rules.
func (tv *TxValidator) checkInputs(tx *bt.Tx, blockHeight uint32, validationOptions *Options) error {
	total := uint64(0)
	accumulatedPrevUTXOSize := uint64(0)
	maxCoinsViewCacheSize := tv.settings.Policy.GetMaxCoinsViewCacheSize()

	// blockHeight is not used, but it is required by the interface
	_ = blockHeight

	// Use a map to track seen inputs with fixed-size 36-byte array key (32 bytes txid + 4 bytes output index)
	seenInputs := make(map[[36]byte]struct{})

	for index, input := range tx.Inputs {
		// Check each input for duplicates
		var key [36]byte

		copy(key[:32], input.PreviousTxID())

		// Convert uint32 output index to 4 bytes
		outIdx := input.PreviousTxOutIndex
		key[32] = byte(outIdx >> 24)
		key[33] = byte(outIdx >> 16)
		key[34] = byte(outIdx >> 8)
		key[35] = byte(outIdx)

		// Check if we've seen this input before
		if _, exists := seenInputs[key]; exists {
			return errors.NewTxInvalidError("duplicate input found at index %d", index)
		}

		// Mark this input as seen
		seenInputs[key] = struct{}{}

		if input.PreviousTxIDStr() == coinbaseTxID {
			return errors.NewTxInvalidError("transaction input %d is a coinbase input", index)
		}
		/* lots of our valid test transactions have this sequence number, is this not allowed?
		if input.SequenceNumber == 0xffffffff {
			fmt.Printf("input %d has sequence number 0xffffffff, txid = %s", index, tx.TxID())
			return errors.NewTxInvalidError("transaction input %d sequence number is invalid", index)
		}
		*/

		if input.PreviousTxSatoshis > MaxSatoshis {
			return errors.NewTxInvalidError("transaction input %d satoshis is too high", index)
		}

		total += input.PreviousTxSatoshis

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

	// if total == 0 && blockHeight >= tv.Params().GenesisActivationHeight {
	// TODO there is a lot of shit transactions on-chain with 0 inputs and 0 outputs - WTF
	// return errors.NewTxInvalidError("transaction input total satoshis cannot be zero")
	// }

	if total > MaxSatoshis {
		return errors.NewTxInvalidError("transaction input total satoshis is too high")
	}

	return nil
}

// checkTxSize validates that the transaction size complies with consensus and policy limits.
// This method enforces two types of checks:
// 1. Consensus check (ALWAYS enforced): Ensures transaction doesn't exceed consensus size limit
//   - Before Genesis: 1 MB (MaxTxSizeConsensusBeforeGenesis)
//   - After Genesis: 1 GB (MaxTxSizeConsensusAfterGenesis)
//   - Matches C++ bitcoin-sv: CheckTransactionCommon in validation.cpp:536
//
// 2. Policy check (only when skipPolicy=false): Ensures transaction doesn't exceed policy size limit
//
// Parameters:
//   - txSize: The transaction size in bytes
//   - blockHeight: Current block height to determine if Genesis is active
//   - skipPolicy: If true, skip policy checks (used for block validation)
func (tv *TxValidator) checkTxSize(txSize int, blockHeight uint32, skipPolicy bool) error {
	// Consensus check: ALWAYS enforced regardless of skipPolicy
	// Matches C++ bitcoin-sv implementation: CheckTransactionCommon in validation.cpp:536
	// where it checks: if (::GetSerializeSize(tx, SER_NETWORK, PROTOCOL_VERSION) > maxTxSizeConsensus)
	genesisActivationHeight := tv.settings.ChainCfgParams.GenesisActivationHeight
	isPostGenesis := blockHeight >= genesisActivationHeight
	maxTxSizeConsensus := MaxTxSizeConsensusBeforeGenesis
	if isPostGenesis {
		maxTxSizeConsensus = MaxTxSizeConsensusAfterGenesis
	}
	if txSize > maxTxSizeConsensus {
		return errors.NewTxInvalidError("bad-txns-oversize")
	}

	// Policy check: Only enforced for mempool transactions (when skipPolicy=false)
	if !skipPolicy {
		maxTxSizePolicy := tv.settings.Policy.GetMaxTxSizePolicy()
		if maxTxSizePolicy == 0 {
			// no policy found for tx size, use max block size
			maxTxSizePolicy = MaxBlockSize
		}

		if txSize > maxTxSizePolicy {
			return errors.NewTxInvalidError("transaction size in bytes is greater than max tx size policy %d", maxTxSizePolicy)
		}
	}

	return nil
}

// checkFees validates transaction fees according to policy requirements.
func (tv *TxValidator) checkFees(tx *bt.Tx, blockHeight uint32, utxoHeights []uint32) error {
	// Check for consolidation transaction with proper UTXO height verification
	isConsolidation := tv.isConsolidationTx(tx, utxoHeights, blockHeight)
	if isConsolidation {
		return nil // We return nil here to say there was no issue with the fees
	}

	inputSats := tx.TotalInputSatoshis()
	outputSats := tx.TotalOutputSatoshis()

	if inputSats < outputSats {
		return errors.NewTxInvalidError("transaction input satoshis is less than output satoshis: %d < %d", inputSats, outputSats)
	}

	minFeeRateBSVPerKB := tv.settings.Policy.GetMinMiningTxFee() // BSV per kilobyte

	if minFeeRateBSVPerKB == 0 {
		return nil // no fee policy found, skip fee check
	}

	actualFeePaid := inputSats - outputSats

	// Convert BSV/kB to satoshis/byte
	// 1 BSV = 1e8 satoshis
	// 1 kB = 1000 bytes
	// So BSV/kB * 1e8 / 1000 = satoshis/byte
	satoshisPerByte := minFeeRateBSVPerKB * 1e8 / 1000

	// Calculate minimum relay fee based on transaction size
	txSize := tx.Size()
	minRequiredFee := uint64(satoshisPerByte * float64(txSize))

	// Ensure minimum 1 satoshi for non-zero sized transactions (matching SV Node)
	if minRequiredFee == 0 && txSize > 0 && minFeeRateBSVPerKB > 0 {
		minRequiredFee = 1
	}

	if actualFeePaid < minRequiredFee {
		return errors.NewTxInvalidError("transaction fee is too low: %d < %d required", actualFeePaid, minRequiredFee)
	}

	return nil
}

// isDustReturnTx checks if a transaction is a dust return transaction.
// A dust return transaction has a single output with 0 satoshis and an unspendable script
// (OP_FALSE OP_RETURN pattern). These transactions are used to clean up dust UTXOs.
//
// Parameters:
//   - tx: The transaction to check
//
// Returns:
//   - bool: true if the transaction is a dust return transaction, false otherwise
func (tv *TxValidator) isDustReturnTx(tx *bt.Tx) bool {
	if tx == nil {
		return false
	}

	// Must have exactly one output
	if len(tx.Outputs) != 1 {
		return false
	}

	output := tx.Outputs[0]

	// Output must have 0 satoshis
	if output.Satoshis != 0 {
		return false
	}

	// Check if the locking script matches the dust return pattern
	// OP_FALSE, OP_RETURN, OP_PUSHDATA(4), 'dust'
	// This implement the equivalent IsDustReturnScript in C++
	dustReturnScript := []byte{0x00, 0x6a, 0x04, 0x64, 0x75, 0x73, 0x74}
	if output.LockingScript == nil {
		return false
	}
	scriptBytes := *output.LockingScript
	if len(scriptBytes) != len(dustReturnScript) {
		return false
	}
	for i := range dustReturnScript {
		if scriptBytes[i] != dustReturnScript[i] {
			return false
		}
	}

	return true
}

// isStandardOutput checks if a scriptPubKey is a standard output type.
// Standard outputs include: P2PKH, P2PK, P2MS (multisig), and OP_RETURN (data).
//
// Parameters:
//   - scriptPubKey: The script to check
//
// Returns:
//   - bool: true if the script is a standard output type
func (tv *TxValidator) isStandardOutput(scriptPubKey *bscript.Script) bool {
	if scriptPubKey == nil {
		return false
	}

	// Check for standard script types
	return scriptPubKey.IsP2PKH() ||
		scriptPubKey.IsP2PK() ||
		scriptPubKey.IsMultiSigOut() ||
		scriptPubKey.IsData()
}

// isConsolidationTx checks if a transaction qualifies as a free consolidation transaction
// following Bitcoin SV rules. This implementation replicates the C++ IsFreeConsolidationTxn logic.
//
// A consolidation transaction reduces the UTXO set sufficiently that miners accept it without fees.
//
// Parameters:
//   - tx: The transaction to check
//   - utxoHeights: Block heights of the UTXOs being spent (nil for fee checks only)
//   - currentHeight: Current block height (ignored if utxoHeights is nil)
//
// Returns:
//   - bool: true if the transaction qualifies as a free consolidation transaction
func (tv *TxValidator) isConsolidationTx(tx *bt.Tx, utxoHeights []uint32, currentHeight uint32) bool {
	if tx == nil {
		return false
	}

	// Allow disabling free consolidation txns via configuring the consolidation factor to zero or negative
	minConsolidationFactor := tv.settings.Policy.GetMinConsolidationFactor()
	if minConsolidationFactor <= 0 {
		tv.logger.Debugf("Consolidation disabled: minConsolidationFactor is %d", minConsolidationFactor)
		return false
	}

	// Coinbase transactions cannot be consolidation transactions
	if tx.IsCoinbase() {
		tv.logger.Debugf("Not a consolidation tx: coinbase transaction")
		return false
	}

	// Check if it's a dust donation transaction (special case)
	isDustDonation := tv.isDustReturnTx(tx)

	// Dynamic factor and minConf based on donation status
	factor := minConsolidationFactor
	minConf := tv.settings.Policy.GetMinConfConsolidationInput()

	if isDustDonation {
		// Dust donations use actual input count as factor and require 0 confirmations
		factor = len(tx.Inputs)
		minConf = 0
	}

	numInputs := len(tx.Inputs)
	numOutputs := len(tx.Outputs)

	// Rule 1: Input/Output Ratio
	// The consolidation transaction needs to reduce the count of UTXOs
	if numInputs < factor*numOutputs {
		// Provide hint if close to consolidation factor
		if numInputs > 2*numOutputs {
			tv.logger.Debugf("Consolidation tx has too few inputs in relation to outputs. Consolidation factor: %d", factor)
		}
		return false
	}

	// Check if transaction is extended (has PreviousTxScript for all inputs)
	for _, input := range tx.Inputs {
		if input.PreviousTxScript == nil {
			tv.logger.Debugf("Not a consolidation tx: missing PreviousTxScript")
			return false
		}
	}

	// Rule 2: Script Size Comparison
	// Check all UTXOs are confirmed and prevent spam via big scriptSig sizes
	sumScriptPubKeySizeOfTxInputs := uint64(0)
	for i, input := range tx.Inputs {
		// If we have UTXO heights, perform full validation
		if utxoHeights != nil {
			// Rule 3: Input Maturity - accept only with many confirmations
			if i < len(utxoHeights) {
				inputHeight := utxoHeights[i]

				// Check for mempool/unconfirmed inputs
				if minConf > 0 && inputHeight >= currentHeight {
					tv.logger.Debugf("Consolidation tx has input from unconfirmed transaction")
					return false
				}

				// Check minimum confirmations
				seenConf := int32(currentHeight+1) - int32(inputHeight)
				if minConf > 0 && inputHeight > 0 && seenConf < int32(minConf) {
					tv.logger.Debugf("Consolidation tx has input with %d confirmations, minimum required: %d", seenConf, minConf)
					return false
				}
			}

			// Rule 4: Input Script Size Limit - spam detection
			maxInputScriptSize := tv.settings.Policy.GetMaxConsolidationInputScriptSize()
			if input.UnlockingScript != nil && len(*input.UnlockingScript) > maxInputScriptSize {
				tv.logger.Debugf("Consolidation tx has input with scriptSig size %d, maximum: %d", len(*input.UnlockingScript), maxInputScriptSize)
				return false
			}

			// Rule 5: Standard Script Rule
			// If not acceptNonStdConsolidationInput then check if inputs are standard
			stdInputOnly := !tv.settings.Policy.GetAcceptNonStdConsolidationInput()
			if stdInputOnly {
				scriptPubKey := bscript.Script(*input.PreviousTxScript)
				if !tv.isStandardOutput(&scriptPubKey) {
					tv.logger.Debugf("Consolidation tx has non-standard input")
					return false
				}
			}
		}

		// Sum up scriptPubKey sizes from inputs
		sumScriptPubKeySizeOfTxInputs += uint64(len(*input.PreviousTxScript))
	}

	// Calculate sum of output scriptPubKey sizes
	sumScriptPubKeySizeOfTxOutputs := uint64(0)
	for _, output := range tx.Outputs {
		if output.LockingScript != nil {
			sumScriptPubKeySizeOfTxOutputs += uint64(len(*output.LockingScript))
		}
	}

	// Prevent consolidation transactions that are not advantageous enough for miners
	if sumScriptPubKeySizeOfTxInputs < uint64(factor)*sumScriptPubKeySizeOfTxOutputs {
		tv.logger.Debugf("Consolidation tx script size ratio insufficient: input=%d, output=%d, factor=%d",
			sumScriptPubKeySizeOfTxInputs, sumScriptPubKeySizeOfTxOutputs, factor)
		return false
	}

	// Transaction qualifies as a consolidation transaction
	if isDustDonation {
		tv.logger.Debugf("Free donation transaction: %s", tx.TxID())
	} else {
		tv.logger.Debugf("Free consolidation transaction: %s", tx.TxID())
	}
	return true
}

// sigOpsCheck validates that the transaction's signature operations count complies with policy limits.
// This reimplements GetTransactionSigOpCount from bitcoin-sv/src/validation.cpp:496
//
// The function counts signature operations in three places:
// 1. Transaction inputs (unlocking scripts) - GetSigOpCountWithoutP2SH for inputs
// 2. Transaction outputs (locking scripts) - GetSigOpCountWithoutP2SH for outputs
// 3. P2SH redeem scripts (pre-Genesis only) - GetP2SHSigOpCount
//
// Differences from C++ implementation:
//   - No MEMPOOL_HEIGHT constant needed: teranode always has actual utxoHeights from UTXO store,
//     so we can always determine the protocol era for each UTXO
//   - Script number parsing: Implements CScriptNum-compatible parsing (little-endian with sign bit)
//     via helper functions checkMinimalEncoding() and parseScriptNumber() to handle non-OP_N
//     multisig operands with full validation (size checks, minimal encoding, negative value rejection)
//
// This implementation provides 100% compatibility with the C++ bitcoin-sv sigops counting logic.
func (tv *TxValidator) sigOpsCheck(tx *bt.Tx, blockHeight uint32, utxoHeights []uint32, validationOptions *Options) error {
	// Get max sigops policy limit
	maxSigOps := tv.settings.Policy.GetMaxTxSigopsCountsPolicy()
	if maxSigOps == 0 || validationOptions.SkipPolicyChecks {
		maxSigOps = int64(MaxTxSigopsCountPolicyAfterGenesis)
	}

	genesisActivationHeight := tv.settings.ChainCfgParams.GenesisActivationHeight
	isCurrentBlockPostGenesis := blockHeight >= genesisActivationHeight

	var totalSigOps uint64 = 0

	// ============================================================================
	// SECTION 1: Count sigops in transaction inputs (unlocking scripts)
	// Corresponds to GetSigOpCountWithoutP2SH for tx.vin in C++
	// Post-Genesis: Input scripts should only contain push data, so this should return 0
	// Pre-Genesis: May contain OP_CHECKSIG operations
	// ============================================================================
	for _, input := range tx.Inputs {
		sigOps, err := tv.countSigOpsInScript(input.UnlockingScript, false, isCurrentBlockPostGenesis)
		if err != nil {
			return errors.NewTxInvalidError("failed to count sigops in input script: %v", err)
		}
		totalSigOps += sigOps
		if totalSigOps > uint64(maxSigOps) {
			return errors.NewTxInvalidError("transaction has too many sigops (%d)", totalSigOps)
		}
	}

	// ============================================================================
	// SECTION 2: Count sigops in transaction outputs (locking scripts)
	// Corresponds to GetSigOpCountWithoutP2SH for tx.vout in C++
	// ============================================================================
	for _, output := range tx.Outputs {
		sigOps, err := tv.countSigOpsInScript(output.LockingScript, false, isCurrentBlockPostGenesis)
		if err != nil {
			return errors.NewTxInvalidError("failed to count sigops in output script: %v", err)
		}
		totalSigOps += sigOps
		if totalSigOps > uint64(maxSigOps) {
			return errors.NewTxInvalidError("transaction has too many sigops (%d)", totalSigOps)
		}
	}

	// ============================================================================
	// SECTION 3: Count P2SH sigops (pre-Genesis only)
	// Corresponds to GetP2SHSigOpCount in C++
	// After Genesis, P2SH is not supported and redeem scripts are not executed
	// ============================================================================
	if tx.IsCoinbase() {
		// Coinbase transactions have no P2SH sigops to count
		return nil
	}

	// Validate utxoHeights array length matches inputs
	if len(utxoHeights) != len(tx.Inputs) {
		return errors.NewTxInvalidError("utxoHeights length (%d) does not match inputs length (%d)",
			len(utxoHeights), len(tx.Inputs))
	}

	for i, input := range tx.Inputs {
		// Determine the protocol era when this UTXO was created
		// In C++, if coin->GetHeight() == MEMPOOL_HEIGHT, they use current era
		// In teranode, we always have actual heights, so we always determine the era from the height
		utxoHeight := utxoHeights[i]
		isUTXOPostGenesis := utxoHeight >= genesisActivationHeight

		// After Genesis, P2SH is not supported, skip counting
		if isUTXOPostGenesis {
			continue
		}

		// Check if previous output is P2SH (pre-Genesis only)
		if input.PreviousTxScript == nil {
			return errors.NewTxInvalidError("input %d missing PreviousTxScript", i)
		}

		if !input.PreviousTxScript.IsP2SH() {
			continue
		}

		// For P2SH outputs, we need to count sigops in the redeem script
		// The redeem script is the last item pushed by the unlocking script
		// P2SH scriptPubKey format: OP_HASH160 <20 bytes> OP_EQUAL
		// P2SH scriptSig format: <sig> ... <redeemScript>

		redeemScript, err := tv.extractRedeemScript(input.UnlockingScript)
		if err != nil {
			// Invalid P2SH unlocking script format, return 0 sigops (will fail later in script execution)
			continue
		}

		if redeemScript == nil {
			// No redeem script found
			continue
		}

		// Count sigops in the redeem script with accurate counting (fAccurate = true)
		sigOps, err := tv.countSigOpsInScript(redeemScript, true, isUTXOPostGenesis)
		if err != nil {
			return errors.NewTxInvalidError("failed to count sigops in P2SH redeem script: %v", err)
		}

		totalSigOps += sigOps
		if totalSigOps > uint64(maxSigOps) {
			return errors.NewTxInvalidError("transaction has too many sigops (%d)", totalSigOps)
		}
	}

	return nil
}

// countSigOpsInScript counts signature operations in a script.
// Reimplements CScript::GetSigOpCount from bitcoin-sv/src/script/script.cpp:26
//
// Parameters:
//   - script: The script to analyze
//   - fAccurate: If true, uses accurate counting (looks at previous opcode for CHECKMULTISIG)
//   - isPostGenesis: If true, applies post-Genesis rules (accurate counting, scope tracking)
//
// Returns the count of signature operations and any error encountered.
//
// Key behaviors:
// - Tracks IF/ENDIF scope depth to handle nested conditionals
// - Stops counting after OP_RETURN at top-level scope (post-Genesis or accurate mode)
// - OP_CHECKSIG/OP_CHECKSIGVERIFY: Always count as 1
// - OP_CHECKMULTISIG/OP_CHECKMULTISIGVERIFY:
//   - Pre-Genesis (fAccurate=false): Count as 20 (MAX_PUBKEYS_PER_MULTISIG_BEFORE_GENESIS)
//   - Pre-Genesis (fAccurate=true): If previous op is OP_1 to OP_16, count as N; else count as 20
//   - Post-Genesis: Always accurate with full validation:
//   - OP_0: count as 0
//   - OP_1 to OP_16: decode as N (1-16)
//   - Push data: parse as CScriptNum with size check, minimal encoding validation, and negative check
func (tv *TxValidator) countSigOpsInScript(script *bscript.Script, fAccurate bool, isPostGenesis bool) (uint64, error) {
	if script == nil {
		return 0, nil
	}

	parser := interpreter.DefaultOpcodeParser{}
	parsedOps, err := parser.Parse(script)
	if err != nil {
		// If we can't parse the script, return 0 (script will fail execution later)
		return 0, nil
	}

	var nSigOps uint64 = 0
	var lastOp interpreter.ParsedOpcode
	var lastOpcode byte = 0
	scopeDepth := 0 // Track IF/ENDIF nesting depth

	for _, op := range parsedOps {
		opcode := op.Value()

		// Handle invalid opcodes
		if opcode == 0xff { // OP_INVALIDOPCODE
			break
		}

		// Scope tracking for accurate counting or post-Genesis
		if fAccurate || isPostGenesis {
			// Stop counting after OP_RETURN at top-level scope
			if opcode == bscript.OpRETURN && scopeDepth == 0 {
				break
			}

			// Track scope depth for IF/ENDIF blocks
			if opcode == bscript.OpIF || opcode == bscript.OpNOTIF ||
				opcode == bscript.OpVERIF || opcode == bscript.OpVERNOTIF {
				scopeDepth++
			} else if opcode == bscript.OpENDIF {
				scopeDepth--
				if scopeDepth < 0 {
					// Unbalanced IF/ENDIF - invalid script
					return 0, errors.NewTxInvalidError("unbalanced IF/ENDIF in script")
				}
			}
		}

		// Count OP_CHECKSIG and OP_CHECKSIGVERIFY
		if opcode == bscript.OpCHECKSIG || opcode == bscript.OpCHECKSIGVERIFY {
			nSigOps++
		} else if opcode == bscript.OpCHECKMULTISIG || opcode == bscript.OpCHECKMULTISIGVERIFY {
			// Handle multisig signature operations
			if (fAccurate || isPostGenesis) && lastOpcode >= bscript.Op1 && lastOpcode <= bscript.Op16 {
				// Previous opcode is OP_1 to OP_16, decode the number
				// OP_1 = 0x51 (81), OP_16 = 0x60 (96)
				n := uint64(lastOpcode - bscript.Op1 + 1)
				nSigOps += n
			} else if isPostGenesis {
				// Post-Genesis: Must accurately count multisig operations
				if lastOpcode == bscript.Op0 {
					// OP_0 CHECKMULTISIG - checking with 0 keys, nothing to add
				} else if lastOp.Data != nil && len(lastOp.Data) > 0 {
					// Non-OP_N operand before CHECKMULTISIG
					// Parse the operand data as CScriptNum with full validation
					// This matches the C++ implementation: script.cpp:85-112

					// Check operand size - pre-Genesis uses 4 bytes max
					// Post-Genesis allows larger but we check against a reasonable limit
					maxScriptNumLen := 4 // CScriptNum::MAXIMUM_ELEMENT_SIZE before Genesis
					if isPostGenesis {
						// Post-Genesis allows up to 750KB per script number
						maxScriptNumLen = 750000 // 750KB as per go-bt interpreter config
					}

					if len(lastOp.Data) > maxScriptNumLen {
						// Operand too large - when trying to spend such output, EvalScript would fail
						// Making the coin unspendable
						return 0, errors.NewTxInvalidError("multisig operand exceeds maximum size (%d > %d)", len(lastOp.Data), maxScriptNumLen)
					}

					// Validate minimal encoding - required by EvalScript
					// Matches IsMinimallyEncoded check in C++: script.cpp:98-104
					if err := tv.checkMinimalEncoding(lastOp.Data); err != nil {
						return 0, err
					}

					// Parse as script number (little-endian with sign bit)
					// Matches CScriptNum constructor: script.cpp:106
					numSigs, err := tv.parseScriptNumber(lastOp.Data)
					if err != nil {
						return 0, err
					}

					// Check that the result is non-negative
					// Matches check in C++: script.cpp:107-111
					if numSigs < 0 {
						return 0, errors.NewTxInvalidError("multisig pubkey count cannot be negative (%d)", numSigs)
					}

					nSigOps += uint64(numSigs)
				} else {
					// No operand data, treat as malformed
					return 0, errors.NewTxInvalidError("malformed CHECKMULTISIG operation")
				}
			} else {
				// Pre-Genesis without accurate counting: Use maximum (20)
				nSigOps += 20 // MAX_PUBKEYS_PER_MULTISIG_BEFORE_GENESIS
			}
		}

		// Remember last opcode and operation for next iteration
		lastOpcode = opcode
		lastOp = op
	}

	return nSigOps, nil
}

// extractRedeemScript extracts the redeem script from a P2SH unlocking script.
// The redeem script is the last item pushed onto the stack by the unlocking script.
//
// For P2SH, the unlocking script must only contain push operations.
// Returns nil if the script is invalid or doesn't follow P2SH rules.
func (tv *TxValidator) extractRedeemScript(unlockingScript *bscript.Script) (*bscript.Script, error) {
	if unlockingScript == nil {
		return nil, nil
	}

	parser := interpreter.DefaultOpcodeParser{}
	parsedOps, err := parser.Parse(unlockingScript)
	if err != nil {
		return nil, nil
	}

	var lastPushData []byte

	// Iterate through all operations, ensuring they are all push operations
	for _, op := range parsedOps {
		opcode := op.Value()

		// Check if this is a valid push operation (OP_0 to OP_PUSHDATA4, or OP_1-OP_16)
		// According to P2SH BIP16, only push operations are valid
		if opcode > bscript.Op16 {
			// Non-push operation found, invalid P2SH unlocking script
			return nil, nil
		}

		if opcode == 0xff { // OP_INVALIDOPCODE
			return nil, nil
		}

		// Save the pushed data
		if op.Data != nil {
			lastPushData = op.Data
		} else if opcode == bscript.Op0 {
			lastPushData = []byte{}
		} else if opcode >= bscript.Op1 && opcode <= bscript.Op16 {
			// OP_1 to OP_16 push small numbers
			// For redeem script extraction, we treat these as empty data
			// (actual P2SH scripts won't use these as redeem scripts)
			lastPushData = []byte{opcode - bscript.Op1 + 1}
		}
	}

	if lastPushData == nil {
		return nil, nil
	}

	// Create a new script from the last pushed data
	redeemScript := bscript.NewFromBytes(lastPushData)
	return redeemScript, nil
}

// checkMinimalEncoding validates that a byte array adheres to minimal encoding requirements.
// This matches the checkMinimalDataEncoding function in go-bt/bscript/interpreter/number.go:404
// and IsMinimallyEncoded in bitcoin-sv C++ code.
//
// Minimal encoding means:
// - No unnecessary leading zeros (except when needed to avoid sign bit conflict)
// - Negative zero [0x80] is not allowed
//
// For example:
//   - 127 encodes as [0x7f] ✓
//   - 127 encodes as [0x7f 0x00] ✗ (not minimal)
//   - 255 encodes as [0xff 0x00] ✓ (0x00 needed because 0xff would set sign bit)
//   - -128 encodes as [0x80 0x80] ✓
func (tv *TxValidator) checkMinimalEncoding(v []byte) error {
	if len(v) == 0 {
		return nil
	}

	// Check that the number is encoded with the minimum possible number of bytes.
	//
	// If the most-significant-byte - excluding the sign bit - is zero,
	// then we're not minimal. Note how this test also rejects the
	// negative-zero encoding, [0x80].
	if v[len(v)-1]&0x7f == 0 {
		// One exception: if there's more than one byte and the most
		// significant bit of the second-most-significant-byte is set,
		// it would conflict with the sign bit. An example of this case
		// is +-255, which encode to 0xff00 and 0xff80 respectively
		// (big-endian).
		if len(v) == 1 || v[len(v)-2]&0x80 == 0 {
			return errors.NewTxInvalidError("numeric value encoded as %x is not minimally encoded", v)
		}
	}

	return nil
}

// parseScriptNumber interprets serialized bytes as an encoded integer
// and returns the result as an int64.
// This matches the makeScriptNumber function in go-bt/bscript/interpreter/number.go:72
// and CScriptNum in bitcoin-sv C++ code.
//
// Bitcoin script numbers are encoded as little-endian with a sign bit in the
// most significant bit of the most significant byte.
//
// Examples:
//   - [0x7f] = 127
//   - [0xff] = -127 (0x7f with sign bit set)
//   - [0x80 0x00] = 128
//   - [0x80 0x80] = -128
//   - [] = 0 (empty byte slice)
func (tv *TxValidator) parseScriptNumber(bb []byte) (int64, error) {
	// Zero is encoded as an empty byte slice
	if len(bb) == 0 {
		return 0, nil
	}

	// Decode from little endian
	// Each byte is shifted left by its position * 8 bits
	var v int64
	for i, b := range bb {
		v |= int64(b) << uint(8*i)
	}

	// When the most significant byte has the sign bit set (0x80),
	// the result is negative. Remove the sign bit and make the value negative.
	if bb[len(bb)-1]&0x80 != 0 {
		// Remove the sign bit: AND with NOT(0x80 << shift)
		// where shift = 8 * (len(bb) - 1)
		signMask := int64(0x80) << uint(8*(len(bb)-1))
		v &= ^signMask
		v = -v
	}

	return v, nil
}
