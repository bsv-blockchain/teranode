//go:build bdk

package validator

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"

	gobdk "github.com/bitcoin-sv/bdk/module/gobdk"
	bdkconfig "github.com/bitcoin-sv/bdk/module/gobdk/config"
	bdkscript "github.com/bitcoin-sv/bdk/module/gobdk/script"
	"github.com/bitcoin-sv/ubsv/chaincfg"
	"github.com/bitcoin-sv/ubsv/errors"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/libsv/go-bt/v2"
)

type bdkDebugVerification struct {
	UScript     string `mapstructure:"uScript" json:"uScript" validate:"required"`
	LScript     string `mapstructure:"lScript" json:"lScript" validate:"required"`
	TxBytes     string `mapstructure:"txBytes" json:"txBytes" validate:"required"`
	Flags       uint32 `mapstructure:"flags" json:"flags" validate:"required"`
	Input       int    `mapstructure:"input" json:"input" validate:"required"`
	Satoshis    uint64 `mapstructure:"satoshis" json:"satoshis" validate:"required"`
	BlockHeight uint32 `mapstructure:"blockHeight" json:"blockHeight" validate:"required"`
	Err         string `mapstructure:"err" json:"err" validate:"required"`
}

func init() {
	ScriptVerificationFactory[TxInterpreterGoBDK] = newScriptVerifierGoBDK

	log.Println("Registered scriptVerifierGoBDK")
}

// getChainNameFromParams map the chain name from the ubsv way to bsv C++ code.
func getBDKChainNameFromParams(pa *chaincfg.Params) string {
	// ubsv : mainnet  testnet3  regtest  stn
	// bdk  :    main      test  regtest  stn
	chainNameMap := map[string]string{
		"mainnet":  "main",
		"stn":      "stn",
		"testnet3": "test",
		"regtest":  "regtest",
	}

	return chainNameMap[pa.Name]
}

func newScriptVerifierGoBDK(l ulogger.Logger, po *PolicySettings, pa *chaincfg.Params) TxScriptInterpreter {
	l.Infof("Use Script Verifier with GoBDK, version : %v", gobdk.BDK_VERSION_STRING())

	bdkScriptConfig := bdkconfig.ScriptConfig{
		ChainNetwork:                 getBDKChainNameFromParams(pa),
		MaxOpsPerScriptPolicy:        uint64(po.MaxOpsPerScriptPolicy),
		MaxScriptNumLengthPolicy:     uint64(po.MaxScriptNumLengthPolicy),
		MaxScriptSizePolicy:          uint64(po.MaxScriptSizePolicy),
		MaxPubKeysPerMultiSig:        uint64(po.MaxPubKeysPerMultisigPolicy),
		MaxStackMemoryUsageConsensus: uint64(po.MaxStackMemoryUsageConsensus),
		MaxStackMemoryUsagePolicy:    uint64(po.MaxStackMemoryUsagePolicy),
	}
	bdkscript.SetGlobalScriptConfig(bdkScriptConfig)

	return &scriptVerifierGoBDK{
		logger: l,
		policy: po,
		params: pa,
	}
}

type scriptVerifierGoBDK struct {
	logger ulogger.Logger
	policy *PolicySettings
	params *chaincfg.Params
}

func (v *scriptVerifierGoBDK) Logger() ulogger.Logger {
	return v.logger
}

func (v *scriptVerifierGoBDK) Params() *chaincfg.Params {
	return v.params
}

func (v *scriptVerifierGoBDK) PolicySettings() *PolicySettings {
	return v.policy
}

// VerifyScript verifies script using Go-BDK
func (v *scriptVerifierGoBDK) VerifyScript(tx *bt.Tx, blockHeight uint32) (err error) {
	for i, in := range tx.Inputs {
		if in.PreviousTxScript == nil || in.UnlockingScript == nil {
			continue
		}

		// TODO : For now, as there are only one way to pass go []byte to C++ array and assume
		// the array is not empty, we actually cannot pass empty []byte to C++ array
		// In future, we must handle this case where empty array is still valid and need to verify
		// See https://github.com/bitcoin-sv/ubsv/issues/1270
		if len(*in.PreviousTxScript) < 1 || len(*in.UnlockingScript) < 1 {
			continue
		}

		// isPostChronicle now is unknow, in future, it need to be calculate based on block height
		//flags, errF := bdkscript.ScriptVerificationFlags(*in.PreviousTxScript, false)
		flags, errF := bdkscript.ScriptVerificationFlagsV2(*in.PreviousTxScript, blockHeight)
		if errF != nil {
			return errors.NewTxInvalidError("failed to calculate flags from prev locking script, flags : %v, error: %v", flags, errF)
		}

		txBinary := tx.Bytes()
		if errV := bdkscript.Verify(*in.UnlockingScript, *in.PreviousTxScript, true, uint(flags), txBinary, i, in.PreviousTxSatoshis); errV != nil {
			// Helpful logging to get full information to debug separately in GoBDK

			errLog := bdkDebugVerification{
				UScript:     hex.EncodeToString(*in.UnlockingScript),
				LScript:     hex.EncodeToString(*in.PreviousTxScript),
				TxBytes:     tx.String(),
				Flags:       flags,
				Input:       i,
				Satoshis:    in.PreviousTxSatoshis,
				BlockHeight: blockHeight,
				Err:         errV.Error(),
			}

			errLogData, _ := json.MarshalIndent(errLog, "", "    ")

			errorLogMsg := fmt.Sprintf("Failed to verify script in go-bdk, error : \n\n%v\n\n", string(errLogData))
			//fmt.Println(errorLogMsg)
			v.logger.Warnf(errorLogMsg)
			return errors.NewTxInvalidError("Failed to verify script: %w", errV)
		}
	}

	return nil
}
