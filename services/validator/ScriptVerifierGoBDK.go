//go:build bdk

/*
Package validator implements Bitcoin SV transaction validation functionality.

This file implements the Go-BDK script verification functionality, providing
script validation using the Bitcoin Development Kit (BDK) implementation.
This verifier is only built when the 'bdk' build tag is specified.
*/
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

// bdkDebugVerification defines the structure for debug information during script verification
// Used for detailed logging when script verification fails
type bdkDebugVerification struct {
	UScript     string `mapstructure:"uScript" json:"uScript" validate:"required"`         // Unlocking script in hex
	LScript     string `mapstructure:"lScript" json:"lScript" validate:"required"`         // Locking script in hex
	TxBytes     string `mapstructure:"txBytes" json:"txBytes" validate:"required"`         // Full transaction in hex
	Flags       uint32 `mapstructure:"flags" json:"flags" validate:"required"`             // Script verification flags
	Input       int    `mapstructure:"input" json:"input" validate:"required"`             // Input index
	Satoshis    uint64 `mapstructure:"satoshis" json:"satoshis" validate:"required"`       // Amount in satoshis
	BlockHeight uint32 `mapstructure:"blockHeight" json:"blockHeight" validate:"required"` // Current block height
	Err         string `mapstructure:"err" json:"err" validate:"required"`                 // Error message if verification fails
}

// init registers the Go-BDK script verifier with the verification factory
// This is called automatically when the package is imported with the 'bdk' build tag
func init() {
	ScriptVerificationFactory[TxInterpreterGoBDK] = newScriptVerifierGoBDK

	log.Println("Registered scriptVerifierGoBDK")
}

// getBDKChainNameFromParams maps chain names from ubsv format to BDK format (bsv C++)
// Parameters:
//   - pa: Chain parameters containing the network name
//
// Returns:
//   - string: The BDK-compatible chain name
//
// Chain name mappings:
//   - mainnet  -> main
//   - testnet3 -> test
//   - regtest  -> regtest
//   - stn      -> stn
func getBDKChainNameFromParams(pa *chaincfg.Params) string {
	// ubsv : mainnet  testnet   regtest  stn
	// bdk  :    main      test  regtest  stn
	chainNameMap := map[string]string{
		"mainnet": "main",
		"stn":     "stn",
		"testnet": "test",
		"regtest": "regtest",
	}

	return chainNameMap[pa.Name]
}

// newScriptVerifierGoBDK creates a new Go-BDK script verifier instance
// Parameters:
//   - l: Logger instance for verification operations
//   - po: Policy settings for validation rules
//   - pa: Network parameters
//
// Returns:
//   - TxScriptInterpreter: The created script interpreter
func newScriptVerifierGoBDK(l ulogger.Logger, po *PolicySettings, pa *chaincfg.Params) TxScriptInterpreter {
	l.Infof("Use Script Verifier with GoBDK, version : %v", gobdk.BDK_VERSION_STRING())

	// Configure BDK script verification settings
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

// scriptVerifierGoBDK implements the TxScriptInterpreter interface using Go-BDK
type scriptVerifierGoBDK struct {
	logger ulogger.Logger   // Logger instance
	policy *PolicySettings  // Policy settings for validation
	params *chaincfg.Params // Network parameters
}

// Logger returns the verifier's logger instance
func (v *scriptVerifierGoBDK) Logger() ulogger.Logger {
	return v.logger
}

// Params returns the verifier's network parameters
func (v *scriptVerifierGoBDK) Params() *chaincfg.Params {
	return v.params
}

// PolicySettings returns the verifier's policy settings
func (v *scriptVerifierGoBDK) PolicySettings() *PolicySettings {
	return v.policy
}

// VerifyScript implements script verification using the Go-BDK library
// This method verifies all inputs of a transaction against their corresponding
// locking scripts using the BDK script verification engine.
//
// The verification process:
// 1. Iterates through all transaction inputs
// 2. Calculates appropriate script flags based on block height
// 3. Performs script verification using BDK's native implementation
// 4. Provides detailed error information for debugging
//
// Parameters:
//   - tx: The transaction containing scripts to verify
//   - blockHeight: Current block height for validation context
//
// Returns:
//   - error: Any script verification errors encountered
//
// Note: Empty scripts and special cases are handled with appropriate logging
func (v *scriptVerifierGoBDK) VerifyScript(tx *bt.Tx, blockHeight uint32) (err error) {
	for i, in := range tx.Inputs {
		// Skip empty scripts
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

		// Perform script verification
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
