package validator

import (
	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/settings"
)

// CheckTransactionContextFree runs the checks on a transaction that need no parent data
// and no chain state: not a coinbase, bitcoin-sv's CheckTransactionCommon for the era of
// blockHeight (at least one input and one output, the consensus size limit, output values
// in the money range, and before Genesis the sigop limit), no null or duplicate prevout,
// and the policy size limit. It is for a transaction that cannot be fully validated yet;
// it never replaces full validation.
func CheckTransactionContextFree(tx *bt.Tx, tSettings *settings.Settings, blockHeight uint32) error {
	if tSettings == nil || tSettings.ChainCfgParams == nil {
		return errors.NewProcessingError("context-free check needs settings with chain params")
	}

	if tx.IsCoinbase() {
		return errors.NewTxInvalidError("coinbase transactions are not supported")
	}

	if reason := model.TxCommonRuleViolation(tx, blockHeight, tSettings.ChainCfgParams); reason != "" {
		return errors.NewTxInvalidError(reason)
	}

	type outpoint struct {
		hash  chainhash.Hash
		index uint32
	}

	seen := make(map[outpoint]struct{}, len(tx.Inputs))

	for _, input := range tx.Inputs {
		op := outpoint{hash: *input.PreviousTxIDChainHash(), index: input.PreviousTxOutIndex}

		if op.index == 0xffffffff && op.hash.IsEqual(&chainhash.Hash{}) {
			return errors.NewTxInvalidError("bad-txns-prevout-null")
		}

		if _, dup := seen[op]; dup {
			return errors.NewTxInvalidError("bad-txns-inputs-duplicate")
		}

		seen[op] = struct{}{}
	}

	if policy := tSettings.Policy; policy != nil && policy.MaxTxSizePolicy > 0 && tx.Size() > policy.MaxTxSizePolicy {
		return errors.NewTxPolicyError("tx-size")
	}

	return nil
}
