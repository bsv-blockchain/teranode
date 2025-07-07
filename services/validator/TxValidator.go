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
	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/settings"
	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/bscript/interpreter"
	"github.com/bsv-blockchain/go-chaincfg"
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
	// ValidateTransaction performs comprehensive validation of a transaction.
	// This method enforces all consensus and policy rules against the transaction,
	// including format, structure, inputs/outputs, signature verification, and fees.
	// The validation context includes the current blockchain height and configuration
	// options that may modify validation behavior (e.g., skip certain checks).
	//
	// Parameters:
	//   - tx: The transaction to validate, must be properly initialized
	//   - blockHeight: The current block height for validation context
	//   - validationOptions: Optional validation options to customize validation behavior
	// Returns:
	//   - error: Specific validation error with reason if validation fails, nil on success
	ValidateTransaction(tx *bt.Tx, blockHeight uint32, validationOptions *Options) error

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
func NewTxValidator(logger ulogger.Logger, tSettings *settings.Settings, opts ...TxValidatorOption) TxValidatorI {
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

// ValidateTransaction performs comprehensive validation of a transaction
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
//
// Returns:
//   - error: Any validation errors encountered
func (tv *TxValidator) ValidateTransaction(tx *bt.Tx, blockHeight uint32, validationOptions *Options) error {
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
	if err := tv.checkOutputs(tx, blockHeight); err != nil {
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
		if err := tv.checkFees(tx, feesToBtFeeQuote(tv.settings.Policy.GetMinMiningTxFee())); err != nil {
			return err
		}
	}

	return nil
}

func (tv *TxValidator) ValidateTransactionScripts(tx *bt.Tx, blockHeight uint32, utxoHeights []uint32, validationOptions *Options) error {
	if tv == nil {
		return errors.NewTxInvalidError("tx validator is nil")
	}

	if tv.interpreter == nil {
		return errors.NewTxInvalidError("tx interpreter is nil, available interpreters: %v", TxScriptInterpreterFactory)
	}

	// SkipPolicy is equivalent to execute the script with consensus = true
	// https://github.com/bitcoin-sv/teranode/issues/2367
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

func (tv *TxValidator) checkOutputs(tx *bt.Tx, blockHeight uint32) error {
	total := uint64(0)

	for index, output := range tx.Outputs {
		if output.Satoshis > MaxSatoshis {
			return errors.NewTxInvalidError("transaction output %d satoshis is invalid", index)
		}

		// Check dust limit after genesis activation
		if blockHeight >= tv.settings.ChainCfgParams.GenesisActivationHeight {
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

func (tv *TxValidator) checkFees(tx *bt.Tx, feeQuote *bt.FeeQuote) error {
	inputSats := tx.TotalInputSatoshis()
	outputSats := tx.TotalOutputSatoshis()

	if inputSats < outputSats {
		return errors.NewTxInvalidError("transaction input satoshis is less than output satoshis: %d < %d", inputSats, outputSats)
	}

	actualFeePaid := inputSats - outputSats

	minRequiredFee := tv.settings.Policy.GetMinMiningTxFee() * 1e8

	if float64(actualFeePaid) < minRequiredFee {
		return errors.NewTxInvalidError("transaction fee is too low")
	}

	feesOK, err := tx.IsFeePaidEnough(feeQuote)
	if err != nil {
		return err
	}

	if !feesOK {
		return errors.NewTxInvalidError("transaction fee is too low")
	}

	return nil
}

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
