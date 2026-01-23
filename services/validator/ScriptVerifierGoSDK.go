/*
Package validator implements Bitcoin SV transaction validation functionality.

This file implements the Go-SDK script verification functionality, providing
script validation using the Bitcoin SV Go-SDK library implementation.
*/
package validator

import (
	"strings"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-chaincfg"
	"github.com/bsv-blockchain/go-sdk/chainhash"
	"github.com/bsv-blockchain/go-sdk/script"
	interpreter_sdk "github.com/bsv-blockchain/go-sdk/script/interpreter"
	"github.com/bsv-blockchain/go-sdk/transaction"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/ulogger"
)

// init registers the Go-SDK script verifier with the verification factory
// This is called automatically when the package is imported
func init() {
	TxScriptInterpreterFactory[TxInterpreterGoSDK] = newScriptVerifierGoSDK
}

// newScriptVerifierGoSDK creates a new Go-SDK script verifier instance
// Parameters:
//   - l: Logger instance for verification operations
//   - po: Policy settings for validation rules
//   - pa: Network parameters
//
// Returns:
//   - TxScriptInterpreter: The created script interpreter
func newScriptVerifierGoSDK(l ulogger.Logger, po *settings.PolicySettings, pa *chaincfg.Params) TxScriptInterpreter {
	l.Infof("Use Script verifier with GoSDK")

	return &scriptVerifierGoSDK{
		logger: l,
		policy: po,
		params: pa,
	}
}

// scriptVerifierGoSDK implements the TxScriptInterpreter interface using Go-SDK
type scriptVerifierGoSDK struct {
	logger ulogger.Logger
	policy *settings.PolicySettings
	params *chaincfg.Params
}

// VerifyScript implements script verification using the Go-SDK
// This method verifies all inputs of a transaction against their corresponding
// locking scripts from the previous outputs.
//
// The verification process:
// 1. Converts the transaction to Go-SDK format
// 2. Iterates through all inputs
// 3. Verifies each input's unlocking script against its corresponding locking script
// 4. Handles special cases and historical quirks
//
// Parameters:
//   - tx: The transaction containing scripts to verify
//   - blockHeight: Current block height for validation context
//
// Returns:
//   - error: Any script verification errors encountered
//
// Note: Contains special handling for negative shift amount errors
// which are bypassed for historical compatibility
func (v *scriptVerifierGoSDK) VerifyScript(tx *bt.Tx, blockHeight uint32, consensus bool, utxoHeights []uint32) (err error) {
	defer func() {
		if r := recover(); r != nil {
			if rErr, ok := r.(error); ok {
				if strings.Contains(rErr.Error(), "negative shift amount") {
					err = errors.NewTxInvalidError("negative shift amount for tx %s: %v", tx.TxIDChainHash().String(), rErr)
					return
				}
			}

			err = errors.NewTxInvalidError("script execution failed: %v", r)
		}
	}()

	// TODO add the utxo heights to the tx verifier
	_ = utxoHeights

	sdkTx := goBt2GoSDKTransaction(tx)
	// sdkTx, _ := transaction.NewTransactionFromBytes(tx.Bytes())

	for i, in := range tx.Inputs {
		prevOutput := &transaction.TransactionOutput{
			Satoshis:      in.PreviousTxSatoshis,
			LockingScript: (*script.Script)(in.PreviousTxScript),
		}

		opts := make([]interpreter_sdk.ExecutionOptionFunc, 0, 3)
		opts = append(opts, interpreter_sdk.WithTx(sdkTx, i, prevOutput))

		if blockHeight > v.params.UahfForkHeight {
			opts = append(opts, interpreter_sdk.WithForkID())
		}

		if blockHeight >= v.params.GenesisActivationHeight {
			opts = append(opts, interpreter_sdk.WithAfterGenesis())
		}

		// opts = append(opts, interpreter.WithDebugger(&LogDebugger{}),

		if err = interpreter_sdk.NewEngine().Execute(opts...); err != nil {
			return errors.NewTxInvalidError("script execution error", err)
		}
	}

	return nil
}

// goBt2GoSDKTransaction converts a go-bt transaction to Go-SDK format
// This is a helper function that performs deep copying of transaction data
// to ensure proper conversion between different transaction representations.
//
// Parameters:
//   - tx: The go-bt transaction to convert
//
// Returns:
//   - *transaction.Transaction: The converted Go-SDK transaction
func goBt2GoSDKTransaction(tx *bt.Tx) *transaction.Transaction {
	sdkTx := &transaction.Transaction{
		Version:  tx.Version,
		LockTime: tx.LockTime,
	}

	sdkTx.Inputs = make([]*transaction.TransactionInput, len(tx.Inputs))

	for i, in := range tx.Inputs {
		// clone the bytes of the unlocking script
		unlockingScript := make([]byte, len(*in.UnlockingScript))
		copy(unlockingScript, *in.UnlockingScript)

		sourceTxHash := chainhash.Hash(in.PreviousTxID())
		sdkTx.Inputs[i] = &transaction.TransactionInput{
			SourceTXID:       &sourceTxHash,
			SourceTxOutIndex: in.PreviousTxOutIndex,
			UnlockingScript:  (*script.Script)(&unlockingScript),
			SequenceNumber:   in.SequenceNumber,
		}
	}

	sdkTx.Outputs = make([]*transaction.TransactionOutput, len(tx.Outputs))
	for i, out := range tx.Outputs {
		sdkTx.Outputs[i] = &transaction.TransactionOutput{
			Satoshis:      out.Satoshis,
			LockingScript: (*script.Script)(out.LockingScript),
		}
	}

	return sdkTx
}

func (v *scriptVerifierGoSDK) Interpreter() TxInterpreter {
	return TxInterpreterGoSDK
}
