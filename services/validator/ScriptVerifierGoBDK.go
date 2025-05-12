/*
Package validator implements Bitcoin SV transaction validation functionality.

This file implements the Go-BDK script verification functionality, providing
script validation using the Bitcoin Development Kit (BDK) implementation.
This verifier is only built when the 'bdk' build tag is specified.
*/
package validator

import (
	"fmt"
	"log"
	"strconv"
	"strings"

	gobdk "github.com/bitcoin-sv/bdk/module/gobdk"
	bdkscript "github.com/bitcoin-sv/bdk/module/gobdk/script"
	"github.com/bitcoin-sv/teranode/chaincfg"
	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/settings"
	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/bitcoin-sv/teranode/util"
	"github.com/libsv/go-bt/v2"
)

// init registers the Go-BDK script verifier with the verification factory
// This is called automatically when the package is imported with the 'bdk' build tag
func init() {
	TxScriptInterpreterFactory[TxInterpreterGoBDK] = newScriptVerifierGoBDK

	log.Println("Registered scriptVerifierGoBDK")
}

// uint2int helper to convert array of []uint32 to []int32
func uint2int(arr []uint32) ([]int32, error) {
	ret := make([]int32, len(arr))

	for idx, val := range arr {
		if valInt32, err := util.SafeUint32ToInt32(val); err == nil {
			ret[idx] = valInt32
		} else {
			return []int32{}, err
		}
	}

	return ret, nil
}

// getBDKChainNameFromParams maps chain names from teranode format to BDK format (bsv C++)
// Parameters:
//   - pa: Chain parameters containing the network name
//
// Returns:
//   - string: The BDK-compatible chain name
//
// Chain name mappings:
//   - mainnet     -> main
//   - testnet3    -> test
//   - regtest     -> regtest
//   - stn         -> stn
//   - teratestnet -> teratestnet
//   - tstn        -> tstn
func getBDKChainNameFromParams(l ulogger.Logger, pa *chaincfg.Params) string {
	// teranode : mainnet  testnet   regtest  stn
	// bdk  :    main      test  regtest  stn
	chainNameMap := map[string]string{
		"mainnet":     "main",
		"stn":         "stn",
		"tstn":        "tstn",
		"teratestnet": "teratestnet",
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

	network := getBDKChainNameFromParams(l, pa)
	se := bdkscript.NewScriptEngine(network)

	if se == nil {
		l.Fatalf("unable to create script engine for network %v", network)
	}

	// #nosec G115 -- blockHeight won't overflow
	if err := se.SetGenesisActivationHeight(int32(pa.GenesisActivationHeight)); err != nil {
		panic(err)
	}

	// #nosec G115 -- blockHeight won't overflow
	// For now as our chain params don't have chronicle height, we set that to just genesis+1
	// We are not sure if that's used yet, but we set it for safety as bdk might depend to this (bsv 1.2.0)
	if err := se.SetChronicleActivationHeight(int32(pa.GenesisActivationHeight + 1)); err != nil {
		panic(err)
	}

	if err := se.SetMaxOpsPerScriptPolicy(po.MaxOpsPerScriptPolicy); err != nil {
		panic(err)
	}

	if err := se.SetMaxScriptNumLengthPolicy(int64(po.MaxScriptNumLengthPolicy)); err != nil {
		panic(err)
	}

	if err := se.SetMaxScriptSizePolicy(int64(po.MaxScriptSizePolicy)); err != nil {
		panic(err)
	}

	if err := se.SetMaxPubKeysPerMultiSigPolicy(po.MaxPubKeysPerMultisigPolicy); err != nil {
		panic(err)
	}

	if err := se.SetMaxStackMemoryUsage(int64(po.MaxStackMemoryUsageConsensus), int64(po.MaxStackMemoryUsagePolicy)); err != nil {
		panic(err)
	}

	return &scriptVerifierGoBDK{
		logger: l,
		policy: po,
		params: pa,
		se:     se,
	}
}

// scriptVerifierGoBDK implements the TxScriptInterpreter interface using Go-BDK
type scriptVerifierGoBDK struct {
	logger ulogger.Logger
	policy *settings.PolicySettings
	params *chaincfg.Params
	se     *bdkscript.ScriptEngine
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
	eTxBytes := tx.ExtendedBytes()
	intUtxoHeights, errConv := uint2int(utxoHeights)

	if errConv != nil {
		return errors.NewInvalidArgumentError("failed conversion for utxo heights", errConv)
	}

	intBlockHeight, errConv := util.SafeUint32ToInt32(blockHeight)
	if errConv != nil {
		return errors.NewInvalidArgumentError("failed conversion for block height heights", errConv)
	}

	// #nosec G115 -- blockHeight won't overflow
	err := v.se.VerifyScript(eTxBytes, intUtxoHeights, intBlockHeight, consensus)
	if err != nil {
		// Get the information of all utxo heights
		var utxoHeighstStr []string
		for _, h := range utxoHeights {
			utxoHeighstStr = append(utxoHeighstStr, strconv.FormatUint(uint64(h), 10))
		}

		utxoInfoStr := strings.Join(utxoHeighstStr, "|")
		errorLogMsg := fmt.Sprintf("Failed to verify script in go-bdk\n\n TxID : %v\n\nBlock Height : %v\n\nUTXO Heights : %v\n\nerror:\n%v\n\n", tx.TxID(), blockHeight, utxoInfoStr, err)

		v.logger.Warnf(errorLogMsg)

		return errors.NewTxInvalidError("Failed to verify script", err)
	}

	return nil
}

func (v *scriptVerifierGoBDK) Interpreter() TxInterpreter {
	return TxInterpreterGoBDK
}
