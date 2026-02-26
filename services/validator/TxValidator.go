/*
Package validator implements Bitcoin SV transaction validation functionality.

This file contains the core transaction validation logic and implements the standard
Bitcoin transaction validation rules and policies. The TxValidator component is responsible
for enforcing both consensus rules (which all nodes must follow) and policy rules
(which can be configured per node).

The implementation supports multiple script interpreters through a plugin architecture,
allowing different script verification engines to be used based on configuration. Currently
supported interpreters include:
- Go-BT: Pure Go implementation from the libsv/go-bt library
- Go-SDK: Bitcoin SV SDK implementation
- Go-BDK: Bitcoin Development Kit implementation

The validation process enforces rules including but not limited to:
- Transaction size limits
- Input and output structure verification
- Non-dust output values
- Script operation count limits
- Signature verification
- Fee policy enforcement
- Locktime and sequence number verification

This component is designed to be highly performant and configurable to support
different validation scenarios from development to high-volume production environments.
*/
package validator

import (
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

	// 2) The transaction size in bytes is less than maxtxsizepolicy.
	if !validationOptions.SkipPolicyChecks {
		if err := tv.checkTxSize(txSize); err != nil {
			return err
		}
	}

	// 3) check that each input value, as well as the sum, are in the allowed range of values (less than 21m coins)
	// 5) None of the inputs have hash=0, N=–1 (coinbase transactions should not be relayed)
	if err := tv.checkInputs(tx, blockHeight); err != nil {
		return err
	}

	// 4) Each output value, as well as the total, must be within the allowed range of values (less than 21m coins,
	//    more than the dust threshold if 1 unless it's OP_RETURN, which is allowed to be 0)
	if err := tv.checkOutputs(tx, blockHeight, validationOptions); err != nil {
		return err
	}

	// 6) nLocktime is equal to INT_MAX, or nLocktime and nSequence values are satisfied according to MedianTimePast
	//    => checked by the node, we do not want to have to know the current block height

	// 7) The transaction size in bytes is greater than or equal to 100
	//    => This is a BCH only check, not applicable to BSV

	// 8) The number of signature operations (SIGOPS) contained in the transaction is less than the signature operation limit
	// --------- TURN OFF -> unlimited ---------------------
	// if err := tv.sigOpsCheck(tx, validationOptions); err != nil {
	// 	return err
	// }

	// SAO - https://bitcoin.stackexchange.com/questions/83805/did-the-introduction-of-verifyscript-cause-a-backwards-incompatible-change-to-co
	// SAO - The rule enforcing that unlocking scripts must be "push only" became more relevant and started being enforced with the
	//       introduction of Segregated Witness (SegWit) which activated at height 481824.  BCH Forked before this at height 478559
	//       and therefore let's not enforce this check until then.
	if tv.interpreter.Interpreter() != TxInterpreterGoBDK && blockHeight > tv.settings.ChainCfgParams.UahfForkHeight {
		// 9) The unlocking script (scriptSig) can only push numbers on the stack
		if err := tv.pushDataCheck(tx); err != nil {
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
// This function implements the SequenceLocks check from Bitcoin SV validation.cpp.
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

	// Check height requirement: minimum required height must be less than current block height
	if minHeight >= int32(blockHeight) {
		return errors.NewTxInvalidError("transaction sequence lock height not satisfied: required %d, current %d", minHeight, blockHeight)
	}

	// Check time requirement: minimum required time must be less than block's MTP
	if minTime >= int64(blockMTP) {
		return errors.NewTxInvalidError("transaction sequence lock time not satisfied: required %d, current %d", minTime, blockMTP)
	}

	return nil
}

// isUnspendableOutput checks if an output script is unspendable (starts with OP_FALSE OP_RETURN)
func isUnspendableOutput(script *bscript.Script) bool {
	if script == nil {
		return false
	}
	// Convert script to bytes
	scriptBytes := *script
	// Check if script starts with OP_FALSE (0x00) followed by OP_RETURN (0x6a)
	if len(scriptBytes) >= 2 && scriptBytes[0] == 0x00 && scriptBytes[1] == 0x6a {
		return true
	}

	return false
}

// isStandardInputScript checks if an input script (unlocking script) is standard
// Standard input scripts should only contain data pushes (no other opcodes)
// This uses the same interpreter as the SV node for consistency
// Before UAHF height, all scripts are considered standard (no push-only requirement)
func isStandardInputScript(script *bscript.Script, blockHeight uint32, uahfHeight uint32) bool {
	// Before UAHF, there was no push-only requirement for input scripts
	if blockHeight <= uahfHeight {
		// Any parseable script is considered standard before UAHF
		// Return true unless script is nil
		return script != nil
	}

	if script == nil {
		// Nil scripts are not standard (matches pushDataCheck behavior)
		return false
	}

	// Use the same parser as pushDataCheck for consistency
	parser := interpreter.DefaultOpcodeParser{}
	parsedScript, err := parser.Parse(script)
	if err != nil {
		// If we can't parse the script, it's not standard
		return false
	}

	// Check if the parsed script is push-only
	return parsedScript.IsPushOnly()
}

// checkOutputs validates transaction outputs according to consensus and policy rules.
func (tv *TxValidator) checkOutputs(tx *bt.Tx, blockHeight uint32, validationOptions *Options) error {
	total := uint64(0)

	// Note: We use > instead of >= to exclude the Genesis activation block itself
	// because transactions in block 620538 were created before Genesis rules existed
	isGenesisActivated := blockHeight > tv.settings.ChainCfgParams.GenesisActivationHeight

	for index, output := range tx.Outputs {
		// Check P2SH output after genesis activation
		if !validationOptions.SkipPolicyChecks && isGenesisActivated && output.LockingScript.IsP2SH() {
			// See https://github.com/bitcoin-sv/teranode/issues/4333
			return errors.NewTxInvalidError("transaction output %d is p2sh after genesis activation", index)
		}

		if output.Satoshis > MaxSatoshis {
			return errors.NewTxInvalidError("transaction output %d satoshis is invalid", index)
		}

		// Check dust limit after genesis activation
		// Dust checks are policy rules, not consensus rules - they only apply to mempool/relay
		if !validationOptions.SkipPolicyChecks && isGenesisActivated {
			// Only enforce dust limit for spendable outputs when RequireStandard is true
			if tv.settings.ChainCfgParams.RequireStandard && output.Satoshis < DustLimit && !isUnspendableOutput(output.LockingScript) {
				return errors.NewTxInvalidError("zero-satoshi outputs require 'OP_FALSE OP_RETURN' prefix")
			}
		}

		total += output.Satoshis
	}

	if total > MaxSatoshis {
		return errors.NewTxInvalidError("transaction output total satoshis is too high")
	}

	return nil
}

// checkInputs validates transaction inputs according to consensus rules.
func (tv *TxValidator) checkInputs(tx *bt.Tx, blockHeight uint32) error {
	total := uint64(0)

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

// checkTxSize validates that the transaction size complies with policy limits.
func (tv *TxValidator) checkTxSize(txSize int) error {
	maxTxSizePolicy := tv.settings.Policy.GetMaxTxSizePolicy()
	if maxTxSizePolicy == 0 {
		// no policy found for tx size, use max block size
		maxTxSizePolicy = MaxBlockSize
	}

	if txSize > maxTxSizePolicy {
		return errors.NewTxInvalidError("transaction size in bytes is greater than max tx size policy %d", maxTxSizePolicy)
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

	// Ensure minimum 1 satoshi for non-zero sized transactions (matching Bitcoin SV)
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

	// Output script must be unspendable (OP_FALSE OP_RETURN)
	return isUnspendableOutput(output.LockingScript)
}

// isConsolidationTx checks if a transaction qualifies as a consolidation transaction
// following Bitcoin SV rules.
//
// Parameters:
//   - tx: The transaction to check
//   - utxoHeights: Block heights of the UTXOs being spent (nil for fee checks only)
//   - currentHeight: Current block height (ignored if utxoHeights is nil)
//
// Returns:
//   - bool: true if the transaction qualifies as a consolidation transaction
func (tv *TxValidator) isConsolidationTx(tx *bt.Tx, utxoHeights []uint32, currentHeight uint32) bool {
	if tx == nil {
		return false
	}

	// Coinbase transactions cannot be consolidation transactions
	if tx.IsCoinbase() {
		return false
	}

	// Get policy settings
	minConsolidationFactor := tv.settings.Policy.GetMinConsolidationFactor()
	if minConsolidationFactor <= 0 {
		return false
	}

	numInputs := len(tx.Inputs)
	numOutputs := len(tx.Outputs)

	// Check if it's a dust return transaction (special case)
	isDustReturn := tv.isDustReturnTx(tx)

	// Rule 1: Input/Output Ratio
	// The number of inputs must be >= minConsolidationFactor × number of outputs
	if !isDustReturn && numInputs < minConsolidationFactor*numOutputs {
		return false
	}

	// Rule 2: Script Size Comparison (Bitcoin SV rule)
	// Sum of input scriptPubKey sizes >= minConsolidationFactor × sum of output scriptPubKey sizes
	if !isDustReturn {
		// Check if transaction is extended (has PreviousTxScript for all inputs)
		for _, input := range tx.Inputs {
			if input.PreviousTxScript == nil {
				return false
			}
		}

		// Calculate total size of scriptPubKeys from UTXOs being spent
		totalInputScriptPubKeySize := 0
		for _, input := range tx.Inputs {
			totalInputScriptPubKeySize += len(*input.PreviousTxScript)
		}

		// Calculate total size of output scriptPubKeys
		totalOutputScriptPubKeySize := 0
		for _, output := range tx.Outputs {
			if output.LockingScript != nil {
				totalOutputScriptPubKeySize += len(*output.LockingScript)
			}
		}

		// Check the script size ratio
		if totalInputScriptPubKeySize < minConsolidationFactor*totalOutputScriptPubKeySize {
			return false
		}
	}

	// If no UTXO heights provided, we're done (fee exemption check)
	if utxoHeights == nil {
		return true
	}

	// FULL VALIDATION - Only performed when UTXO heights are provided

	// Get configuration settings
	minConf := tv.settings.Policy.GetMinConfConsolidationInput()
	maxInputScriptSize := tv.settings.Policy.GetMaxConsolidationInputScriptSize()
	acceptNonStdInputs := tv.settings.Policy.GetAcceptNonStdConsolidationInput()

	// Dust return transactions don't require confirmations
	if isDustReturn {
		minConf = 0
	}

	// Check each input
	for i, input := range tx.Inputs {
		// Rule 3: Input Maturity
		// All inputs must have at least minConfConsolidationInput confirmations
		if minConf > 0 && i < len(utxoHeights) {
			inputHeight := utxoHeights[i]
			confirmations := int(currentHeight - inputHeight)
			if confirmations < minConf {
				return false
			}
		}

		// Rule 4: Input Script Size Limit
		// Each input's scriptSig must be <= maxConsolidationInputScriptSize bytes
		if maxInputScriptSize > 0 && input.UnlockingScript != nil {
			scriptSize := len(*input.UnlockingScript)
			if scriptSize > maxInputScriptSize {
				return false
			}
		}

		// Rule 5: Standard Script Rule
		// If acceptNonStdConsolidationInput = 0, all inputs must use standard scripts
		if !acceptNonStdInputs && !isStandardInputScript(input.UnlockingScript, currentHeight, tv.settings.ChainCfgParams.UahfForkHeight) {
			return false
		}
	}

	// Transaction qualifies as a consolidation transaction
	return true
}

// sigOpsCheck validates that the transaction's signature operations count complies with policy limits.
func (tv *TxValidator) sigOpsCheck(tx *bt.Tx, validationOptions *Options) error {
	maxSigOps := tv.settings.Policy.GetMaxTxSigopsCountsPolicy()

	if maxSigOps == 0 || validationOptions.SkipPolicyChecks {
		maxSigOps = int64(MaxTxSigopsCountPolicyAfterGenesis)
	}

	numSigOps := int64(0)

	for _, input := range tx.Inputs {
		parser := interpreter.DefaultOpcodeParser{}
		parsedUnlockingScript, err := parser.Parse(input.PreviousTxScript)

		if err != nil {
			return err
		}

		for _, op := range parsedUnlockingScript {
			if op.Value() == bscript.OpCHECKSIG || op.Value() == bscript.OpCHECKSIGVERIFY {
				numSigOps++
				if numSigOps > maxSigOps {
					return errors.NewTxInvalidError("transaction unlocking scripts have too many sigops (%d)", numSigOps)
				}
			}
		}
	}

	return nil
}

// pushDataCheck validates that transaction input scripts contain only data pushes.
func (tv *TxValidator) pushDataCheck(tx *bt.Tx) error {
	for index, input := range tx.Inputs {
		if input.UnlockingScript == nil {
			return errors.NewTxInvalidError("transaction input %d unlocking script is empty", index)
		}

		parser := interpreter.DefaultOpcodeParser{}
		parsedUnlockingScript, err := parser.Parse(input.UnlockingScript)

		if err != nil {
			return err
		}

		if !parsedUnlockingScript.IsPushOnly() {
			return errors.NewTxInvalidError("transaction input %d unlocking script is not push only", index)
		}
	}

	return nil
}
