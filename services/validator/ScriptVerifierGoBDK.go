/*
Package validator implements BSV Blockchain transaction validation functionality.

This file implements the Go-BDK script verification functionality, providing
script validation using the Bitcoin Development Kit (BDK) implementation.
This verifier is only built when the 'bdk' build tag is specified.
*/
package validator

import (
	"fmt"
	"strconv"
	"strings"

	gobdk "github.com/bitcoin-sv/bdk/module/gobdk"
	bdkscript "github.com/bitcoin-sv/bdk/module/gobdk/script"
	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-chaincfg"
	safeconversion "github.com/bsv-blockchain/go-safe-conversion"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/ulogger"
)

const (
	errMsgInvalidTx = "ScriptVerifierGoBDK fail to VerifyScript"
	errMsgPolicy    = "ScriptVerifierGoBDK fail to VerifyScript by policy settings"
)

// init registers the Go-BDK script verifier with the verification factory
// This is called automatically when the package is imported with the 'bdk' build tag
func init() {
	TxScriptInterpreterFactory[TxInterpreterGoBDK] = newScriptVerifierGoBDK
}

// uint2int safely converts uint32 slice to int32 slice, checking for overflow.
func uint2int(arr []uint32) ([]int32, error) {
	ret := make([]int32, len(arr))

	for idx, val := range arr {
		if valInt32, err := safeconversion.Uint32ToInt32(val); err == nil {
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
	if err := se.SetChronicleActivationHeight(int32(pa.ChronicleActivationHeight)); err != nil {
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

	intBlockHeight, errConv := safeconversion.Uint32ToInt32(blockHeight)
	if errConv != nil {
		return errors.NewInvalidArgumentError("failed conversion for block height heights", errConv)
	}

	// Validate utxo heights count matches transaction inputs before calling BDK,
	// which throws a C++ exception for mismatched counts.
	if len(intUtxoHeights) != len(tx.Inputs) {
		consensusErr := errors.NewTxConsensusError("inconsistent utxo heights: got %d, expected %d inputs", len(intUtxoHeights), len(tx.Inputs))
		return errors.NewTxInvalidError(errMsgInvalidTx, consensusErr)
	}

	// #nosec G115 -- blockHeight won't overflow
	errVerify := v.se.VerifyScript(eTxBytes, intUtxoHeights, intBlockHeight, consensus)
	if errVerify != nil {
		// Get the information of all utxo heights
		var utxoHeighstStr []string
		for _, h := range utxoHeights {
			utxoHeighstStr = append(utxoHeighstStr, strconv.FormatUint(uint64(h), 10))
		}

		utxoInfoStr := strings.Join(utxoHeighstStr, "|")
		errorLogMsg := fmt.Sprintf("%v \n\n TxID : %v\n\nBlock Height : %v\n\nUTXO Heights : %v\n\nerror:\n%v\n\n", errMsgInvalidTx, tx.TxID(), blockHeight, utxoInfoStr, errVerify)

		v.logger.Warnf("%s", errorLogMsg)

		errCode := errVerify.Code()
		policyRelatedError := (errCode == bdkscript.SCRIPT_ERR_OP_COUNT ||
			errCode == bdkscript.SCRIPT_ERR_SCRIPTNUM_OVERFLOW ||
			errCode == bdkscript.SCRIPT_ERR_SCRIPTNUM_MINENCODE ||
			errCode == bdkscript.SCRIPT_ERR_SCRIPT_SIZE ||
			errCode == bdkscript.SCRIPT_ERR_PUBKEY_COUNT ||
			errCode == bdkscript.SCRIPT_ERR_STACK_SIZE)

		if !consensus && policyRelatedError {
			// See https://github.com/bsv-blockchain/teranode/issues/2016
			policyErr := errors.NewTxPolicyError(errMsgPolicy, errVerify)
			return errors.NewTxInvalidError(errMsgInvalidTx, policyErr)
		}

		// The special case of policy with consensus == true for MaxStackMemoryUsageConsensus
		if consensus && errCode == bdkscript.SCRIPT_ERR_STACK_SIZE {
			policyErr := errors.NewTxPolicyError(errMsgPolicy, errVerify)
			return errors.NewTxInvalidError(errMsgInvalidTx, policyErr)
		}

		return errors.NewTxInvalidError(errMsgInvalidTx, errVerify)
	}

	return nil
}

// VerifyScriptBatch verifies scripts for multiple transactions using BDK's batch
// parallel verification. This submits all transactions as a single batch to the
// C++ side, which parallelizes across numThreads (0 = all cores), avoiding
// per-transaction CGO overhead.
func (v *scriptVerifierGoBDK) VerifyScriptBatch(txs []*bt.Tx, blockHeight uint32, consensus bool, utxoHeights [][]uint32, numThreads int) []error {
	n := len(txs)
	errs := make([]error, n)

	if n == 0 {
		return errs
	}

	if len(utxoHeights) != n {
		for i := range errs {
			errs[i] = errors.NewInvalidArgumentError("length of utxoHeights must match number of transactions")
		}

		return errs
	}

	intBlockHeight, errConv := safeconversion.Uint32ToInt32(blockHeight)
	if errConv != nil {
		convErr := errors.NewInvalidArgumentError("failed conversion for block height", errConv)
		for i := range errs {
			errs[i] = convErr
		}
		return errs
	}

	batch := bdkscript.NewVerifyBatch(n)

	for i, tx := range txs {
		if tx == nil {
			errs[i] = errors.NewTxInvalidError("transaction at index %d is nil", i)
			continue
		}

		intUtxoHeights, err := uint2int(utxoHeights[i])
		if err != nil {
			errs[i] = errors.NewInvalidArgumentError("failed conversion for utxo heights at index %d", i, err)
			continue
		}

		batch.Add(tx.ExtendedBytes(), intUtxoHeights, intBlockHeight, consensus, nil)
	}

	scriptErrors := v.se.VerifyScriptBatchParallel(batch, numThreads)

	// Map results back, accounting for nil transactions that were skipped
	batchIdx := 0
	for i := range txs {
		// Skip entries that already have errors (nil tx or conversion failure)
		if errs[i] != nil {
			continue
		}

		if batchIdx >= len(scriptErrors) {
			break
		}

		sErr := scriptErrors[batchIdx]
		batchIdx++

		if sErr == nil {
			continue
		}

		errCode := sErr.Code()
		if errCode == bdkscript.SCRIPT_ERR_OK {
			continue
		}

		policyRelatedError := (errCode == bdkscript.SCRIPT_ERR_OP_COUNT ||
			errCode == bdkscript.SCRIPT_ERR_SCRIPTNUM_OVERFLOW ||
			errCode == bdkscript.SCRIPT_ERR_SCRIPTNUM_MINENCODE ||
			errCode == bdkscript.SCRIPT_ERR_SCRIPT_SIZE ||
			errCode == bdkscript.SCRIPT_ERR_PUBKEY_COUNT ||
			errCode == bdkscript.SCRIPT_ERR_STACK_SIZE)

		if !consensus && policyRelatedError {
			policyErr := errors.NewTxPolicyError(errMsgPolicy, sErr)
			errs[i] = errors.NewTxInvalidError(errMsgInvalidTx, policyErr)
			continue
		}

		if consensus && errCode == bdkscript.SCRIPT_ERR_STACK_SIZE {
			policyErr := errors.NewTxPolicyError(errMsgPolicy, sErr)
			errs[i] = errors.NewTxInvalidError(errMsgInvalidTx, policyErr)
			continue
		}

		errs[i] = errors.NewTxInvalidError(errMsgInvalidTx, sErr)
	}

	return errs
}

func (v *scriptVerifierGoBDK) Interpreter() TxInterpreter {
	return TxInterpreterGoBDK
}
