//go:build bdk

package consensus

import (
	bdkscript "github.com/bitcoin-sv/bdk/module/gobdk/script"
	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-chaincfg"
	safeconversion "github.com/bsv-blockchain/go-safe-conversion"
	"github.com/bsv-blockchain/teranode/errors"
)

func (vi *ValidatorIntegration) verifyGoBDKScriptOnly(tx *bt.Tx, blockHeight uint32, utxoHeights []uint32) error {
	se := bdkscript.NewTxValidator(consensusBDKChainName(vi.params))
	if se == nil {
		return errors.NewProcessingError("unable to create go-bdk tx validator")
	}

	genesisHeight, err := safeconversion.Uint32ToInt32(vi.params.GenesisActivationHeight)
	if err != nil {
		return errors.NewInvalidArgumentError("failed conversion for genesis activation height", err)
	}

	if err := se.SetGenesisActivationHeight(genesisHeight); err != nil {
		return errors.NewProcessingError("failed to set BDK genesis activation height", err)
	}

	chronicleHeight, err := safeconversion.Uint32ToInt32(vi.params.ChronicleActivationHeight)
	if err != nil {
		return errors.NewInvalidArgumentError("failed conversion for chronicle activation height", err)
	}

	if err := se.SetChronicleActivationHeight(chronicleHeight); err != nil {
		return errors.NewProcessingError("failed to set BDK chronicle activation height", err)
	}

	if err := se.SetMaxOpsPerScriptPolicy(vi.policy.MaxOpsPerScriptPolicy); err != nil {
		return errors.NewProcessingError("failed to set BDK max ops per script policy", err)
	}

	if err := se.SetMaxScriptNumLengthPolicy(int64(vi.policy.MaxScriptNumLengthPolicy)); err != nil {
		return errors.NewProcessingError("failed to set BDK max script number length policy", err)
	}

	if err := se.SetMaxScriptSizePolicy(int64(vi.policy.MaxScriptSizePolicy)); err != nil {
		return errors.NewProcessingError("failed to set BDK max script size policy", err)
	}

	if err := se.SetMaxPubKeysPerMultiSigPolicy(vi.policy.MaxPubKeysPerMultisigPolicy); err != nil {
		return errors.NewProcessingError("failed to set BDK max pubkeys per multisig policy", err)
	}

	if err := se.SetMaxStackMemoryUsage(int64(vi.policy.MaxStackMemoryUsageConsensus), int64(vi.policy.MaxStackMemoryUsagePolicy)); err != nil {
		return errors.NewProcessingError("failed to set BDK max stack memory usage", err)
	}

	se.SetAcceptNonStandardOutput(vi.policy.AcceptNonStdOutputs)
	se.SetRequireStandard(vi.policy.RequireStandard)

	intUtxoHeights, err := consensusUint32sToInt32s(utxoHeights)
	if err != nil {
		return errors.NewInvalidArgumentError("failed conversion for utxo heights", err)
	}

	intBlockHeight, err := safeconversion.Uint32ToInt32(blockHeight)
	if err != nil {
		return errors.NewInvalidArgumentError("failed conversion for block height", err)
	}

	return se.VerifyScript(tx.ExtendedBytes(), intUtxoHeights, intBlockHeight, true)
}

func consensusBDKChainName(params *chaincfg.Params) string {
	chainNameMap := map[string]string{
		"mainnet":     "main",
		"stn":         "stn",
		"tstn":        "tstn",
		"teratestnet": "teratestnet",
		"testnet":     "test",
		"regtest":     "regtest",
	}

	return chainNameMap[params.Name]
}

func consensusUint32sToInt32s(values []uint32) ([]int32, error) {
	result := make([]int32, len(values))
	for i, value := range values {
		converted, err := safeconversion.Uint32ToInt32(value)
		if err != nil {
			return nil, err
		}

		result[i] = converted
	}

	return result, nil
}
