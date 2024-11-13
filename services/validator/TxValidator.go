package validator

import (
	"encoding/hex"

	"github.com/bitcoin-sv/ubsv/chaincfg"
	"github.com/bitcoin-sv/ubsv/errors"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/bscript"
	"github.com/libsv/go-bt/v2/bscript/interpreter"
	"github.com/ordishs/gocore"
)

var (
	txWhitelist = map[string]struct{}{
		"c99c49da4c38af669dea436d3e73780dfdb6c1ecf9958baa52960e8baee30e73": {},
		"0ad07700151caa994c0bc3087ad79821adf071978b34b8b3f0838582e45ef305": {},
		"7c451f68e15303ab3e28450405cfa70f2c2cc9fa29e92cb2d8ed6ca6edb13645": {},
		"a6c116351836d9cc223321ba4b38d68c8f0db53661f8c2229acabbc269c1b2c8": {},
		"f5efee46ccfa4191ccd9d9f645e2f5d09bbe195f95ef5608e992d6794cd653cd": {},
		"904bda3a7d3e3b8402793334a75fb1ce5a6ff5cf1c2d3bcbd7bd25872d0e8c1e": {},
		"8ac76995ce4ac10dd02aa819e7e6535854a2271e44f908570f71bc418ffe3f02": {},
		"e218970e8f810be99d60aa66262a1d382bc4b1a26a69af07ac47d622885db1a7": {},
		"ba4f9786bb34571bd147448ab3c303ae4228b9c22c89e58cc50e26ff7538bf80": {},
		"38df010716e13254fb5fc16065c1cf62ee2aeaed2fad79973f8a76ba91da36da": {},
		"b3146a5012e75fa06eaf92171416796e141e984b29bf23999726f6d698957cef": {}, // spending of OP_RETURN
		"ad74a116639ea654ed8ba4170781199c2f37004b14dc9a5c54df55788c9ab50c": {}, // spending of weird OP_SHIFT script causing panic
		"cc95cdc21ff31afb8295d8015b222d0e54dcae634010bea8e79c09325ac173cf": {}, // spending of weird OP_SHIFT script causing panic
		"6e1d88f10e829fa2dd9691ef5cf9550ba6f0eed51d676f1b74df3fa894fe7035": {}, // spending of weird OP_SHIFT script causing panic
		"7562141b4a26e2482f43e9e123222579c8c9f704d465aacf11ed041a85d2e50d": {}, // spending of weird OP_SHIFT script causing panic
		"6974a4c575c661a918e50d735852c29541a3263dcc4ff46bf90eb9f8f0ec485e": {}, // spending of weird OP_SHIFT script causing panic
		"65cbf31895f6cab997e6c3688b2263808508adc69bcc9054eef5efac6f7895d3": {}, //
	}
)

type TxInterpreter string

const (
	TxInterpreterGoBT  TxInterpreter = "GoBT"
	TxInterpreterGoSDK TxInterpreter = "GoSDK"
	TxInterpreterGoBDK TxInterpreter = "GoBDK"

	defaultTxVerifier = TxInterpreterGoBT
)

// TxValidatorI interface implement method to validate transactions
type TxValidatorI interface {
	// ValidateTransaction implements the method to validate a transaction
	ValidateTransaction(tx *bt.Tx, blockHeight uint32) error
}

type TxValidator struct {
	logger      ulogger.Logger
	policy      *PolicySettings
	params      *chaincfg.Params
	interpreter TxScriptInterpreter
}

type TxScriptInterpreter interface {
	// VerifyScript implement the method to verify a script for a transaction
	VerifyScript(tx *bt.Tx, blockHeight uint32) error
}

// TxValidatorCreator is the creator method to be registered in the factory
type TxValidatorCreator func(logger ulogger.Logger, policy *PolicySettings, params *chaincfg.Params) TxScriptInterpreter

// ScriptVerificationFactory store all TxValidator creator methods.
// They are registered at build time depending to the build tags
var ScriptVerificationFactory = make(map[TxInterpreter]TxValidatorCreator)

// NewTxValidator lookup from the factory and return the appropriate TxValidator
func NewTxValidator(logger ulogger.Logger, policy *PolicySettings, params *chaincfg.Params, opts ...TxValidatorOption) TxValidatorI {
	options := &TxValidatorOptions{}
	for _, opt := range opts {
		opt(options)
	}

	var scriptInterpreter TxInterpreter

	if options.scriptInterpreter != "" {
		scriptInterpreter = options.scriptInterpreter
	} else {
		// Get the type of verifier from config
		scriptValidatorStr, ok := gocore.Config().Get("validator_scriptVerificationLibrary", string(defaultTxVerifier))
		if ok {
			scriptInterpreter = TxInterpreter(scriptValidatorStr)
		} else {
			scriptInterpreter = defaultTxVerifier
		}
	}

	var txScriptInterpreter TxScriptInterpreter

	// If a creator was not registered to the factory, then return nil
	if createTxScriptInterpreter, ok := ScriptVerificationFactory[scriptInterpreter]; ok {
		txScriptInterpreter = createTxScriptInterpreter(logger, policy, params)
	} else {
		// default to GoSDK
		txScriptInterpreter = ScriptVerificationFactory[defaultTxVerifier](logger, policy, params)
	}

	return &TxValidator{
		logger:      logger,
		policy:      policy,
		params:      params,
		interpreter: txScriptInterpreter,
	}
}

// ValidateTransaction use the TxValidator method VerifyScript to verify a script
func (tv *TxValidator) ValidateTransaction(tx *bt.Tx, blockHeight uint32) error {
	if _, ok := txWhitelist[tx.TxIDChainHash().String()]; ok {
		return nil
	}

	//
	// Each node will verify every transaction against a long checklist of criteria:
	//
	txSize := tx.Size()

	// 1) Neither lists of inputs nor outputs are empty
	if len(tx.Inputs) == 0 || len(tx.Outputs) == 0 {
		return errors.NewTxInvalidError("transaction has no inputs or outputs")
	}

	// 2) The transaction size in bytes is less than maxtxsizepolicy.
	if err := tv.checkTxSize(txSize); err != nil {
		return err
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
	if err := tv.sigOpsCheck(tx); err != nil {
		return err
	}

	// SAO - https://bitcoin.stackexchange.com/questions/83805/did-the-introduction-of-verifyscript-cause-a-backwards-incompatible-change-to-co
	// SAO - The rule enforcing that unlocking scripts must be "push only" became more relevant and started being enforced with the
	//       introduction of Segregated Witness (SegWit) which activated at height 481824.  BCH Forked before this at height 478559
	//       and therefore let's not enforce this check until then.
	if blockHeight > tv.params.UahfForkHeight {
		// 9) The unlocking script (scriptSig) can only push numbers on the stack
		if err := tv.pushDataCheck(tx); err != nil {
			return err
		}
	}

	// 10) Reject if the sum of input values is less than sum of output values
	// 11) Reject if transaction fee would be too low (minRelayTxFee) to get into an empty block.
	if err := tv.checkFees(tx, feesToBtFeeQuote(tv.policy.GetMinMiningTxFee())); err != nil {
		return err
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
	maxTxSizePolicy := tv.policy.GetMaxTxSizePolicy()
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

func (tv *TxValidator) sigOpsCheck(tx *bt.Tx) error {
	maxSigOps := tv.policy.GetMaxTxSigopsCountsPolicy()

	if maxSigOps == 0 {
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
