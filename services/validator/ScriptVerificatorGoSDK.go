package validator

import (
	"log"
	"strings"

	"github.com/bitcoin-sv/go-sdk/chainhash"
	"github.com/bitcoin-sv/go-sdk/script"
	interpreter_sdk "github.com/bitcoin-sv/go-sdk/script/interpreter"
	"github.com/bitcoin-sv/go-sdk/transaction"
	"github.com/bitcoin-sv/ubsv/chaincfg"
	"github.com/bitcoin-sv/ubsv/errors"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/libsv/go-bt/v2"
)

func init() {
	ScriptVerificatorFactory["scriptVerificatorGoSDK"] = newScriptVerificatorGoSDK
	log.Println("Registered scriptVerificatorGoSDK")
}

func newScriptVerificatorGoSDK(l ulogger.Logger, po *PolicySettings, pa *chaincfg.Params) TxValidator {
	l.Infof("Use Script Verificator with Go-SDK")
	return &scriptVerificatorGoSDK{
		logger: l,
		policy: po,
		params: pa,
	}
}

type scriptVerificatorGoSDK struct {
	logger ulogger.Logger
	policy *PolicySettings
	params *chaincfg.Params
}

func (v *scriptVerificatorGoSDK) Logger() ulogger.Logger {
	return v.logger
}

func (v *scriptVerificatorGoSDK) Params() *chaincfg.Params {
	return v.params
}

func (v *scriptVerificatorGoSDK) PolicySettings() *PolicySettings {
	return v.policy
}

// VerifyScript verifies script using Go-SDK
func (v *scriptVerificatorGoSDK) VerifyScript(tx *bt.Tx, blockHeight uint32) (err error) {
	defer func() {
		if r := recover(); r != nil {
			// TODO - remove this when script engine is fixed
			if rErr, ok := r.(error); ok {
				if strings.Contains(rErr.Error(), "negative shift amount") {
					v.logger.Errorf("negative shift amount for tx %s: %v", tx.TxIDChainHash().String(), rErr)

					err = nil

					return
				}
			}

			err = errors.NewTxInvalidError("script execution failed: %v", r)
		}
	}()

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
			if blockHeight < 850_000 {
				v.logger.Errorf("script execution error for tx %s: %v", tx.TxIDChainHash().String(), err)
				return nil
			}

			return errors.NewTxInvalidError("script execution error: %w", err)
		}
	}

	return nil
}

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
