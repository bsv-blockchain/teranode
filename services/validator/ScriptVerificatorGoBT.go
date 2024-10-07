package validator

import (
	"log"
	"strings"

	"github.com/bitcoin-sv/ubsv/chaincfg"
	"github.com/bitcoin-sv/ubsv/errors"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/bscript/interpreter"
)

func init() {
	ScriptVerificatorFactory["scriptVerificatorGoBt"] = newScriptVerificatorGoBt
	log.Println("Registered ScriptVerificatorGoBt")
}

func newScriptVerificatorGoBt(l ulogger.Logger, po *PolicySettings, pa *chaincfg.Params) TxValidator {
	l.Infof("Use Script Verificator with GoBt")
	return &scriptVerificatorGoBt{
		logger: l,
		policy: po,
		params: pa,
	}
}

type scriptVerificatorGoBt struct {
	logger ulogger.Logger
	policy *PolicySettings
	params *chaincfg.Params
}

func (v *scriptVerificatorGoBt) Logger() ulogger.Logger {
	return v.logger
}

func (v *scriptVerificatorGoBt) Params() *chaincfg.Params {
	return v.params
}

func (v *scriptVerificatorGoBt) PolicySettings() *PolicySettings {
	return v.policy
}

// VerifyScript verifies script using go-bt
func (v *scriptVerificatorGoBt) VerifyScript(tx *bt.Tx, blockHeight uint32) (err error) {
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

	for i, in := range tx.Inputs {
		prevOutput := &bt.Output{
			Satoshis:      in.PreviousTxSatoshis,
			LockingScript: in.PreviousTxScript,
		}

		opts := make([]interpreter.ExecutionOptionFunc, 0, 3)
		opts = append(opts, interpreter.WithTx(tx, i, prevOutput))

		if blockHeight > v.params.UahfForkHeight {
			opts = append(opts, interpreter.WithForkID())
		}

		if blockHeight >= v.params.GenesisActivationHeight {
			opts = append(opts, interpreter.WithAfterGenesis())
		}

		// opts = append(opts, interpreter.WithDebugger(&LogDebugger{}),

		if err = interpreter.NewEngine().Execute(opts...); err != nil {
			// TODO - in the interests of completing the IBD, we should not fail the node on script errors
			// and instead log them and continue. This is a temporary measure until we can fix the script engine
			if blockHeight < 800_000 {
				v.logger.Errorf("script execution error for tx %s: %v", tx.TxIDChainHash().String(), err)
				return nil
			}

			return errors.NewTxInvalidError("script execution error: %w", err)
		}
	}

	return nil
}
