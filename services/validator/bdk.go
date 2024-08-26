//go:build bdk

package validator

import (
	"encoding/hex"
	"os"

	"github.com/bitcoin-sv/bdk/module/gobdk/config"
	goscript "github.com/bitcoin-sv/bdk/module/gobdk/script"
	"github.com/bitcoin-sv/ubsv/errors"
	"github.com/libsv/go-bt/v2"
)

func init() {
	// Set the validator function to use the go-bdk script validation
	validatorFunc = checkScriptsWithGoBDK
}

// main code
func checkScriptsWithGoBDK(tv *TxValidator, tx *bt.Tx, blockHeight uint32) error {
	// TEMP setenv for script config for go-bdk
	os.Setenv("GOBDK_SCRIPTCONFIG_MAXOPSPERSCRIPTPOLICY", "10000")
	os.Setenv("GOBDK_SCRIPTCONFIG_MAXSCRIPTNUMLENGTHPOLICY", "20000")
	os.Setenv("GOBDK_SCRIPTCONFIG_MAXSCRIPTSIZEPOLICY", "30000")
	os.Setenv("GOBDK_SCRIPTCONFIG_MAXPUBKEYSPERMULTISIG", "4000")
	os.Setenv("GOBDK_SCRIPTCONFIG_MAXSTACKMEMORYUSAGECONSENSUS", "500000")
	os.Setenv("GOBDK_SCRIPTCONFIG_MAXSTACKMEMORYUSAGEPOLICY", "600000")

	settings := config.LoadSetting(
		config.LoadScriptConfig(),
	)
	flags := uint(0)

	tv.logger.Infof("Golang module version : %+v", settings)

	for i, in := range tx.Inputs {
		if v := goscript.Verify(*in.UnlockingScript, *in.PreviousTxScript, true, flags, tx.Bytes(), i, in.PreviousTxSatoshis); v != 0 {
			tv.logger.Warnf("Invalid script in go-bdk: %d: %+v", v, struct {
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
			return errors.NewTxInvalidError("Invalid script: %d", v)
		}
	}

	return nil
}
