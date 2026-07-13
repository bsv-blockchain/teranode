package blockvalidation

import (
	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/utxo"
)

// shouldSkipUnspendableCreate reports whether a transaction should be skipped - not
// written to the UTXO store - during quick-validation catchup. It returns true only
// when the UTXO lock is being skipped for this block (lockUTXOs == false, i.e. the
// QuickValidateSkipUtxoLock optimization is active and there is therefore no
// post-AddBlock unlock pass), the SkipUnspendableTxStorageDuringCatchup setting is on,
// and the transaction has no spendable outputs. Gating on lockUTXOs == false is the
// safety invariant: skipping the create is only safe when the unlock pass is skipped
// too, otherwise unlockSubtreeTransactions would fail on the missing record.
// The caller still spends the transaction's inputs, so the UTXO set stays correct.
func shouldSkipUnspendableCreate(lockUTXOs bool, s *settings.Settings, tx *bt.Tx, blockHeight uint32) bool {
	if lockUTXOs || !s.BlockValidation.SkipUnspendableTxStorageDuringCatchup {
		return false
	}

	return utxo.HasNoSpendableOutputs(tx, tx.IsCoinbase(), blockHeight, s.ChainCfgParams.GenesisActivationHeight)
}
