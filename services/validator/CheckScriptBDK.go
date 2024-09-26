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
	"github.com/bitcoin-sv/ubsv/errors"
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
	// Set the validator function to use the go-bdk script validation
	log.Println("Script Validation using GoBDK version : ", gobdk.BDK_VERSION_STRING())
	validatorFunc = checkScriptsWithGoBDK

	//"main";"test";"regtest";"stn";
	bdkScriptConfig := bdkconfig.ScriptConfig{
		ChainNetwork: "main",
		// MaxOpsPerScriptPolicy:        uint64(10000),
		// MaxScriptNumLengthPolicy:     uint64(20000),
		// MaxScriptSizePolicy:          uint64(4294967295), // MAX_SCRIPT_SIZE_AFTER_GENESIS
		// MaxPubKeysPerMultiSig:        uint64(40000),
		// MaxStackMemoryUsageConsensus: uint64(500000),
		// MaxStackMemoryUsagePolicy:    uint64(600000),
	}

	bdkscript.SetGlobalScriptConfig(bdkScriptConfig)
}

// checkScriptsWithGoBDK verifies script using GoBDK
func checkScriptsWithGoBDK(tv *TxValidator, tx *bt.Tx, blockHeight uint32) error {

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

		if errV := bdkscript.Verify(*in.UnlockingScript, *in.PreviousTxScript, true, uint(flags), tx.Bytes(), i, in.PreviousTxSatoshis); errV != nil {
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
			tv.logger.Warnf(errorLogMsg)
			return errors.NewTxInvalidError("Failed to verify script: %w", errV)
		}
	}

	return nil
}
