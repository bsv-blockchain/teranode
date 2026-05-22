/*
Package validator implements BSV Blockchain transaction validation functionality.

This file implements the GoBDK transaction validation adapter.
*/
package validator

import (
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
	errMsgInvalidTx = "GoBDK fail to ValidateTransaction"
	errMsgPolicy    = "GoBDK fail to ValidateTransaction by policy settings"
)

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

// newScriptVerifierGoBDK creates a new GoBDK transaction validator adapter.
// Parameters:
//   - l: Logger instance for verification operations
//   - po: Policy settings for validation rules
//   - pa: Network parameters
//
// Returns:
//   - bdkValidator: The created GoBDK validation adapter
func newScriptVerifierGoBDK(l ulogger.Logger, po *settings.PolicySettings, pa *chaincfg.Params) bdkValidator {
	l.Infof("Use GoBDK transaction validator, version : %v", gobdk.BDK_VERSION_STRING())

	network := getBDKChainNameFromParams(l, pa)
	se := bdkscript.NewTxValidator(network)

	if se == nil {
		l.Fatalf("unable to create tx validator for network %v", network)
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

	if err := se.SetMaxTxSizePolicy(int64(po.MaxTxSizePolicy)); err != nil {
		panic(err)
	}

	if err := se.SetMaxSigOpsPostGenesisPolicy(po.GetMaxTxSigopsCountsPolicy()); err != nil {
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

	dataCarrierSize, err := safeconversion.Int64ToUint64(po.DataCarrierSize)
	if err != nil {
		panic(err)
	}

	se.SetDataCarrier(po.DataCarrier)
	se.SetDataCarrierSize(dataCarrierSize)
	se.SetAcceptNonStandardOutput(po.AcceptNonStdOutputs)
	se.SetRequireStandard(po.RequireStandard)
	se.SetPermitBareMultisig(po.PermitBareMultisig)

	return &scriptVerifierGoBDK{
		logger: l,
		policy: po,
		params: pa,
		se:     se,
	}
}

type bdkValidator interface {
	ValidateTransaction(tx *bt.Tx, blockHeight uint32, consensus bool, utxoHeights []uint32) error
}

type bdkNativeTxValidator interface {
	ValidateTransaction(extendedTX []byte, utxoHeights []int32, blockHeight int32, consensus bool) error
}

// scriptVerifierGoBDK adapts Teranode validation data to GoBDK.
type scriptVerifierGoBDK struct {
	logger ulogger.Logger
	policy *settings.PolicySettings
	params *chaincfg.Params
	se     bdkNativeTxValidator
}

func bdkBlockHeight(blockHeight uint32, consensus bool) (int32, error) {
	if consensus {
		return safeconversion.Uint32ToInt32(blockHeight)
	}

	if blockHeight == 0 {
		return 0, errors.NewInvalidArgumentError("policy validation block height cannot be zero")
	}

	return safeconversion.Uint32ToInt32(blockHeight - 1)
}

// ValidateTransaction runs BDK-side validation for tx.
func (v *scriptVerifierGoBDK) ValidateTransaction(tx *bt.Tx, blockHeight uint32, consensus bool, utxoHeights []uint32) error {
	if tx.IsCoinbase() {
		return errors.NewTxInvalidError("coinbase transactions are not supported")
	}

	eTxBytes := tx.ExtendedBytes()
	intUtxoHeights, errConv := uint2int(utxoHeights)

	if errConv != nil {
		return errors.NewInvalidArgumentError("failed conversion for utxo heights", errConv)
	}

	intBlockHeight, errConv := bdkBlockHeight(blockHeight, consensus)
	if errConv != nil {
		return errors.NewInvalidArgumentError("failed conversion for block height", errConv)
	}

	errVerify := v.se.ValidateTransaction(eTxBytes, intUtxoHeights, intBlockHeight, consensus)
	if errVerify != nil {
		// Get the information of all utxo heights
		var utxoHeighstStr []string
		for _, h := range utxoHeights {
			utxoHeighstStr = append(utxoHeighstStr, strconv.FormatUint(uint64(h), 10))
		}

		utxoInfoStr := strings.Join(utxoHeighstStr, "|")

		v.logger.Warnf("%s txID=%s blockHeight=%d utxoHeights=%s error=%v", errMsgInvalidTx, tx.TxID(), blockHeight, utxoInfoStr, errVerify)

		return v.mapBDKValidationError(errVerify, consensus)
	}

	return nil
}

func (v *scriptVerifierGoBDK) mapBDKValidationError(errVerify error, consensus bool) error {
	var dosErr bdkscript.DoSError
	if errors.As(errVerify, &dosErr) {
		switch dosErr.Code() {
		case bdkscript.DOS_ERR_NOT_STANDARD, bdkscript.DOS_ERR_SIGOPS_POLICY, bdkscript.DOS_ERR_NOT_FREE_CONSOLIDATION:
			policyErr := errors.NewTxPolicyError(errMsgPolicy, errVerify)
			return errors.NewTxInvalidError(errMsgInvalidTx, policyErr)
		default:
			if dosErr.Code() <= bdkscript.DOS_ERR_OK || dosErr.Code() >= bdkscript.DOS_ERR_COUNT {
				v.logger.Warnf("unknown BDK DoS error code=%d error=%v", dosErr.Code(), errVerify)
			}

			return errors.NewTxInvalidError(errMsgInvalidTx, errVerify)
		}
	}

	var scriptErr bdkscript.ScriptError
	if errors.As(errVerify, &scriptErr) {
		errCode := scriptErr.Code()
		if errCode == bdkscript.SCRIPT_ERR_CGO_EXCEPTION {
			return errors.NewProcessingError(errMsgInvalidTx, errVerify)
		}

		if (!consensus && bdkPolicyRelatedScriptError(errCode)) ||
			(consensus && errCode == bdkscript.SCRIPT_ERR_STACK_SIZE) {
			policyErr := errors.NewTxPolicyError(errMsgPolicy, errVerify)
			return errors.NewTxInvalidError(errMsgInvalidTx, policyErr)
		}
	}

	return errors.NewTxInvalidError(errMsgInvalidTx, errVerify)
}

func bdkPolicyRelatedScriptError(errCode bdkscript.ScriptErrorCode) bool {
	return errCode == bdkscript.SCRIPT_ERR_OP_COUNT ||
		errCode == bdkscript.SCRIPT_ERR_SCRIPTNUM_OVERFLOW ||
		errCode == bdkscript.SCRIPT_ERR_SCRIPTNUM_MINENCODE ||
		errCode == bdkscript.SCRIPT_ERR_SCRIPT_SIZE ||
		errCode == bdkscript.SCRIPT_ERR_PUBKEY_COUNT ||
		errCode == bdkscript.SCRIPT_ERR_STACK_SIZE
}
