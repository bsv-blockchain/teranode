/*
Package validator implements Bitcoin SV transaction validation functionality.

This file implements the Go-BDK script verification functionality, providing
script validation using the Bitcoin Development Kit (BDK) implementation.
This verifier is only built when the 'bdk' build tag is specified.
*/
package validator

import (
	"encoding/hex"
	"fmt"
	"log"

	gobdk "github.com/bitcoin-sv/bdk/module/gobdk"
	bdkconfig "github.com/bitcoin-sv/bdk/module/gobdk/config"
	bdkscript "github.com/bitcoin-sv/bdk/module/gobdk/script"
	"github.com/bitcoin-sv/teranode/chaincfg"
	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/settings"
	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/libsv/go-bt/v2"
)

// init registers the Go-BDK script verifier with the verification factory
// This is called automatically when the package is imported with the 'bdk' build tag
func init() {
	TxScriptInterpreterFactory[TxInterpreterGoBDK] = newScriptVerifierGoBDK

	log.Println("Registered scriptVerifierGoBDK")
}

// getBDKChainNameFromParams maps chain names from teranode format to BDK format (bsv C++)
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
	// teranode : mainnet  testnet   regtest  stn
	// bdk  :    main      test  regtest  stn
	chainNameMap := map[string]string{
		"mainnet":     "main",
		"stn":         "stn",
		"tstn":        "test",
		"teratestnet": "test",
		"testnet":     "test",
		"regtest":     "regtest",
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
func newScriptVerifierGoBDK(l ulogger.Logger, po *settings.PolicySettings, pa *chaincfg.Params) TxScriptInterpreter {
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
		GenesisActivationHeight:      pa.GenesisActivationHeight,
	}
	if err := bdkscript.SetGlobalScriptConfig(bdkScriptConfig); err != nil {
		l.Errorf("Failed to set global script config for GoBDK: %v", err)

		return nil
	}

	return &scriptVerifierGoBDK{
		logger: l,
		policy: po,
		params: pa,
		whiteList: map[string]struct{}{
			"7be4fa421844154ec4105894def768a8bcd80da25792947d585274ce38c07105": {}, // See https://github.com/bitcoin-sv/teranode/issues/1776
		},
	}
}

// scriptVerifierGoBDK implements the TxScriptInterpreter interface using Go-BDK
type scriptVerifierGoBDK struct {
	logger    ulogger.Logger
	policy    *settings.PolicySettings
	params    *chaincfg.Params
	whiteList map[string]struct{}
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
func (v *scriptVerifierGoBDK) VerifyScript(tx *bt.Tx, blockHeight uint32, consensus bool, utxoHeights []uint32) error {
	txID := tx.TxID()
	if _, has := v.whiteList[txID]; has {
		return nil
	}

	eTxBytes := tx.ExtendedBytes()

	_ = utxoHeights

	// TODO : use of VerifyExtendFull. The function is implemented, but for now it fails the tests
	//        as our tests doesn't have utxo heights slice in it yet.
	//        once we can have all the data for extended tx with utxo heights, we can pass to use this function
	// err := bdkscript.VerifyExtendFull(eTxBytes, utxoHeights, blockHeight-1, consensus)
	err := bdkscript.VerifyExtend(eTxBytes, blockHeight-1, consensus)
	if err != nil {
		errorLogMsg := fmt.Sprintf("Failed to verify script in go-bdk\n\nBlock Height : %v\n\nExtendTxHex:\n%v\n\nerror:\n%v\n\n", blockHeight, hex.EncodeToString(eTxBytes), err)
		v.logger.Warnf(errorLogMsg)

		return errors.NewTxInvalidError("Failed to verify script", err)
	}

	return nil
}
