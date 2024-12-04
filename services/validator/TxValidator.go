/*
Package validator implements Bitcoin SV transaction validation functionality.

This file contains the core transaction validation logic and implements the standard
Bitcoin transaction validation rules and policies.
*/
package validator

import (
	"encoding/hex"

	"github.com/bitcoin-sv/ubsv/chaincfg"
	"github.com/bitcoin-sv/ubsv/errors"
	"github.com/bitcoin-sv/ubsv/settings"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/bscript"
	"github.com/libsv/go-bt/v2/bscript/interpreter"
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

	// defaultTxVerifier specifies the default interpreter to use
	defaultTxVerifier = TxInterpreterGoBT
)

// TxValidatorI defines the interface for transaction validation operations
type TxValidatorI interface {
	// ValidateTransaction performs comprehensive validation of a transaction
	// Parameters:
	//   - tx: The transaction to validate
	//   - blockHeight: The current block height for validation context
	// Returns:
	//   - error: Any validation errors encountered
	ValidateTransaction(tx *bt.Tx, blockHeight uint32, validationOptions *Options) error
}

// TxValidator implements transaction validation logic
type TxValidator struct {
	logger      ulogger.Logger
	settings    *settings.Settings
	interpreter TxScriptInterpreter
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
	VerifyScript(tx *bt.Tx, blockHeight uint32) error
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
	options := &TxValidatorOptions{}
	for _, opt := range opts {
		opt(options)
	}

	var scriptInterpreter TxInterpreter

	if options.scriptInterpreter != "" {
		scriptInterpreter = options.scriptInterpreter
	} else {
		// Get the type of verifier from config
		scriptValidatorStr := tSettings.Validator.ScriptVerificationLibrary
		if scriptValidatorStr != "" {
			scriptInterpreter = TxInterpreter(scriptValidatorStr)
		} else {
			scriptInterpreter = defaultTxVerifier
		}
	}

	var txScriptInterpreter TxScriptInterpreter

	// If a creator was not registered to the factory, then return nil
	if createTxScriptInterpreter, ok := TxScriptInterpreterFactory[scriptInterpreter]; ok {
		txScriptInterpreter = createTxScriptInterpreter(logger, tSettings.Policy, tSettings.ChainCfgParams)
	} else {
		// default to GoSDK
		txScriptInterpreter = TxScriptInterpreterFactory[defaultTxVerifier](logger, tSettings.Policy, tSettings.ChainCfgParams)
	}

	return &TxValidator{
		logger:      logger,
		settings:    tSettings,
		interpreter: txScriptInterpreter,
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
	if !validationOptions.skipPolicyChecks {
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
	if err := tv.sigOpsCheck(tx, validationOptions); err != nil {
		return err
	}

	// SAO - https://bitcoin.stackexchange.com/questions/83805/did-the-introduction-of-verifyscript-cause-a-backwards-incompatible-change-to-co
	// SAO - The rule enforcing that unlocking scripts must be "push only" became more relevant and started being enforced with the
	//       introduction of Segregated Witness (SegWit) which activated at height 481824.  BCH Forked before this at height 478559
	//       and therefore let's not enforce this check until then.
	if blockHeight > tv.settings.ChainCfgParams.UahfForkHeight {
		// 9) The unlocking script (scriptSig) can only push numbers on the stack
		if err := tv.pushDataCheck(tx); err != nil {
			return err
		}
	}

	// 10) Reject if the sum of input values is less than sum of output values
	// 11) Reject if transaction fee would be too low (minRelayTxFee) to get into an empty block.
	if !validationOptions.skipPolicyChecks {
		if err := tv.checkFees(tx, feesToBtFeeQuote(tv.settings.Policy.GetMinMiningTxFee())); err != nil {
			return err
		}
	}

	// 12) The unlocking scripts for each input must validate against the corresponding output locking scripts
	if err := tv.interpreter.VerifyScript(tx, blockHeight); err != nil {
		return err
	}

	// everything checks out
	return nil
}

func (tv *TxValidator) checkOutputs(tx *bt.Tx, blockHeight uint32) error {
	total := uint64(0)

	// blockHeight is not used, but it is required by the interface
	_ = blockHeight
	// minOutput := uint64(0)
	// if blockHeight >= tv.Params().GenesisActivationHeight {
	//	minOutput = bt.DustLimit
	// }

	for index, output := range tx.Outputs {
		if output.Satoshis > MaxSatoshis {
			return errors.NewTxInvalidError("transaction output %d satoshis is invalid", index)
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

	for index, input := range tx.Inputs {
		if hex.EncodeToString(input.PreviousTxID()) == coinbaseTxID {
			return errors.NewTxInvalidError("transaction input %d is a coinbase input", index)
		}
		/* lots of our valid test transactions have this sequence number, is this not allowed?
		if input.SequenceNumber == 0xffffffff {
			fmt.Printf("input %d has sequence number 0xffffffff, txid = %s", index, tx.TxID())
			return errors.NewTxInvalidError("transaction input %d sequence number is invalid", index)
		}
		*/
		// if input.PreviousTxSatoshis == 0 && !input.PreviousTxScript.IsData() {
		// 	return errors.NewTxInvalidError("transaction input %d satoshis cannot be zero", index)
		// }
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

	if maxSigOps == 0 || validationOptions.skipPolicyChecks {
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
