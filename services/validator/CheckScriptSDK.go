//go:build !bdk

package validator

import (
	"log"
	"strings"

	"github.com/bitcoin-sv/go-sdk/chainhash"
	"github.com/bitcoin-sv/go-sdk/script"
	interpreter_sdk "github.com/bitcoin-sv/go-sdk/script/interpreter"
	"github.com/bitcoin-sv/go-sdk/transaction"
	"github.com/bitcoin-sv/ubsv/errors"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/bscript/interpreter"
	"github.com/ordishs/gocore"
)

func init() {
	if gocore.Config().GetBool("validator_useSDKInterpreter", false) {
		log.Println("Using go-sdk script validation")

		validatorFunc = checkScriptsWithSDK
	} else {
		log.Println("Using generic go-bt script validation")

		validatorFunc = checkScripts
	}

	for k := range txWhitelist {
		hash, _ := chainhash.NewHashFromHex(k)
		txWhitelistHashes[*hash] = struct{}{}
	}
}

// checkScripts verifies script using go-bt
func checkScripts(tv *TxValidator, tx *bt.Tx, blockHeight uint32) (err error) {
	defer func() {
		if r := recover(); r != nil {
			// TODO - remove this when script engine is fixed
			if rErr, ok := r.(error); ok {
				if strings.Contains(rErr.Error(), "negative shift amount") {
					tv.logger.Errorf("negative shift amount for tx %s: %v", tx.TxIDChainHash().String(), rErr)
					err = nil
					return
				}
			}
			err = errors.NewTxInvalidError("script execution failed: %v", r)
		}
	}()

	for i, in := range tx.Inputs {
		prevOutput := &bt.Output{
			Satoshis:      in.PreviousTxSatoshis,
			LockingScript: in.PreviousTxScript,
		}

		opts := make([]interpreter.ExecutionOptionFunc, 0, 3)
		opts = append(opts, interpreter.WithTx(tx, i, prevOutput))

		if blockHeight > tv.chainParams.UahfForkHeight {
			opts = append(opts, interpreter.WithForkID())
		}

		if blockHeight >= tv.chainParams.GenesisActivationHeight {
			opts = append(opts, interpreter.WithAfterGenesis())
		}

		// opts = append(opts, interpreter.WithDebugger(&LogDebugger{}),

		if err = interpreter.NewEngine().Execute(opts...); err != nil {
			// TODO - in the interests of completeing the IBD, we should not fail the node on script errors
			// and instead log them and continue. This is a temporary measure until we can fix the script engine
			if blockHeight < 800_000 {
				tv.logger.Errorf("script execution error for tx %s: %v", tx.TxIDChainHash().String(), err)
				return nil
			}

			return errors.NewTxInvalidError("script execution error: %w", err)
		}
	}

	return nil
}

// checkScriptsWithSDK verifies script using Go-SDK
func checkScriptsWithSDK(tv *TxValidator, tx *bt.Tx, blockHeight uint32) (err error) {
	defer func() {
		if r := recover(); r != nil {
			// TODO - remove this when script engine is fixed
			if rErr, ok := r.(error); ok {
				if strings.Contains(rErr.Error(), "negative shift amount") {
					tv.logger.Errorf("negative shift amount for tx %s: %v", tx.TxIDChainHash().String(), rErr)
					err = nil
					return
				}
			}
			err = errors.NewTxInvalidError("script execution failed: %v", r)
		}
	}()

	sdkTx := GoBt2GoSDKTransaction(tx)
	// sdkTx, _ := transaction.NewTransactionFromBytes(tx.Bytes())

	for i, in := range tx.Inputs {
		prevOutput := &transaction.TransactionOutput{
			Satoshis:      in.PreviousTxSatoshis,
			LockingScript: (*script.Script)(in.PreviousTxScript),
		}

		opts := make([]interpreter_sdk.ExecutionOptionFunc, 0, 3)
		opts = append(opts, interpreter_sdk.WithTx(sdkTx, i, prevOutput))

		if blockHeight > tv.chainParams.UahfForkHeight {
			opts = append(opts, interpreter_sdk.WithForkID())
		}

		if blockHeight >= tv.chainParams.GenesisActivationHeight {
			opts = append(opts, interpreter_sdk.WithAfterGenesis())
		}

		// opts = append(opts, interpreter.WithDebugger(&LogDebugger{}),

		if err = interpreter_sdk.NewEngine().Execute(opts...); err != nil {
			if blockHeight < 850_000 {
				tv.logger.Errorf("script execution error for tx %s: %v", tx.TxIDChainHash().String(), err)
				return nil
			}

			return errors.NewTxInvalidError("script execution error: %w", err)
		}
	}

	return nil
}
