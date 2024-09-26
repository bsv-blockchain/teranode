//go:build bdk

package validator

import (
	"encoding/hex"
	"log"

	gobdk "github.com/bitcoin-sv/bdk/module/gobdk"
	bdkconfig "github.com/bitcoin-sv/bdk/module/gobdk/config"
	bdkscript "github.com/bitcoin-sv/bdk/module/gobdk/script"
	"github.com/bitcoin-sv/ubsv/errors"
	"github.com/libsv/go-bt/v2"
)

func init() {
	// Set the validator function to use the go-bdk script validation
	log.Println("Script Validation using GoBDK version : ", gobdk.BDK_VERSION_STRING())
	validatorFunc = checkScriptsWithGoBDK

	bdkScriptConfig := bdkconfig.ScriptConfig{
		MaxOpsPerScriptPolicy:        uint64(10000),
		MaxScriptNumLengthPolicy:     uint64(20000),
		MaxScriptSizePolicy:          uint64(30000),
		MaxPubKeysPerMultiSig:        uint64(40000),
		MaxStackMemoryUsageConsensus: uint64(500000),
		MaxStackMemoryUsagePolicy:    uint64(600000),
	}

	bdkscript.SetGlobalScriptConfig(bdkScriptConfig)
}

// checkScriptsWithGoBDK verifies script using GoBDK
func checkScriptsWithGoBDK(tv *TxValidator, tx *bt.Tx, blockHeight uint32) error {

	for i, in := range tx.Inputs {
		if in.PreviousTxScript == nil || in.UnlockingScript == nil {
			continue
		}
		flags, errF := bdkscript.ScriptVerificationFlags(*in.PreviousTxScript, true)
		if errF != nil {
			return errors.NewTxInvalidError("failed to calculate flags from prev locking script, flags : %v, error: %v", flags, errF)
		}

		if errV := bdkscript.Verify(*in.UnlockingScript, *in.PreviousTxScript, true, flags, tx.Bytes(), i, in.PreviousTxSatoshis); errV != nil {
			// Helpful logging to get full information to debug separately in GoBDK
			tv.logger.Warnf("Failed to verify script in go-bdk, error : \n%v\n\nInfo\n\n%+v\n\n", errV, struct {
				uScript     string
				lScript     string
				txBytes     string
				flags       uint
				input       int
				satoshis    uint64
				blockHeight uint32
			}{
				hex.EncodeToString(*in.UnlockingScript),
				hex.EncodeToString(*in.PreviousTxScript),
				tx.String(),
				flags,
				i,
				in.PreviousTxSatoshis,
				blockHeight,
			})
			return errors.NewTxInvalidError("Failed to verify script: %w", errV)
		}
	}

	return nil
}
