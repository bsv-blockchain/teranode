package validator

import (
	"bytes"
	"math"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/util"
)

// This file implements the minminingtxfeebysize policy: a marginal, size-tiered
// minimum fee rate layered above BDK's MinMiningTxFee floor. BDK exposes only a
// single scalar fee rate (SetMinMiningTxFee), so the tier schedule is enforced
// here in Go, before BDK validation, and only in policy mode. The invariant is
// that this check only ever tightens fee policy: every tier rate is validated
// at startup to be at least the MinMiningTxFee floor, and an empty schedule
// (the default) makes the check a no-op, leaving behaviour identical to a node
// without the setting.
//
// BDK exempts free consolidation transactions from its fee floor
// (bitcoin-sv policy.cpp IsFreeConsolidationTxn). The same exemption is
// honoured here via isFreeConsolidationTxn so that enabling size tiers never
// rejects a consolidation BDK would have accepted for free.

// dustReturnScript is the exact dust-donation script svnode recognises in
// IsDustReturnScript: OP_FALSE OP_RETURN OP_PUSHDATA(4) 'dust'.
var dustReturnScript = []byte{0x00, 0x6a, 0x04, 'd', 'u', 's', 't'}

// minMiningTxFeeSatoshisPerKB converts the configured MinMiningTxFee from
// float BSV/kB to integer satoshis/kB, using the same math.Round conversion
// newScriptVerifierGoBDK pushes into BDK (IEEE-754 stores rates like
// 0.00000250 as 0.0000024999..., so truncation would drop a satoshi).
func minMiningTxFeeSatoshisPerKB(po *settings.PolicySettings) int64 {
	return int64(math.Round(po.MinMiningTxFee * 1e8))
}

// validateFeeSizeTiers rejects a tier schedule that would contradict the
// MinMiningTxFee floor BDK enforces. Called once from NewTxValidator; panics
// with a configuration error, matching how newScriptVerifierGoBDK reports
// invalid policy values at startup.
func validateFeeSizeTiers(po *settings.PolicySettings) {
	tiers := po.MinMiningTxFeeBySize
	if len(tiers) == 0 {
		return
	}

	floor := minMiningTxFeeSatoshisPerKB(po)
	for _, tier := range tiers {
		if tier.SatoshisPerKB < floor {
			panic(errors.NewConfigurationError("invalid minminingtxfeebysize: tier rate %d sat/kB at size %d is below the minminingtxfee floor of %d sat/kB", tier.SatoshisPerKB, tier.SizeBytes, floor))
		}
	}
}

// sizeTieredRequiredFee returns the minimum total fee in satoshis for a
// transaction of size bytes under the marginal tier schedule: bytes below the
// first threshold are priced at baseSatoshisPerKB, bytes between thresholds at
// the preceding tier's rate, and bytes beyond the last threshold at its rate.
// Band byte-rate products are accumulated in satoshi-thousandths and divided
// once at the end, so the result is monotone in size and never loses a satoshi
// to per-band truncation. tiers must be sorted ascending by SizeBytes, which
// parseFeeSizeTiers guarantees.
func sizeTieredRequiredFee(baseSatoshisPerKB int64, tiers []settings.FeeSizeTier, size uint64) uint64 {
	var thousandths uint64

	prevThreshold := uint64(0)
	prevRate := uint64(baseSatoshisPerKB)

	for _, tier := range tiers {
		if size <= tier.SizeBytes {
			break
		}

		thousandths += (tier.SizeBytes - prevThreshold) * prevRate
		prevThreshold = tier.SizeBytes
		prevRate = uint64(tier.SatoshisPerKB)
	}

	thousandths += (size - prevThreshold) * prevRate

	return thousandths / 1000
}

// checkSizeTieredFee enforces the minminingtxfeebysize policy for a single
// transaction. It is a no-op when the schedule is empty or the transaction
// does not cross the first size threshold, and callers must gate it on policy
// mode (it is never a consensus rule).
func (tv *TxValidator) checkSizeTieredFee(tx *bt.Tx, blockHeight uint32, utxoHeights []uint32) error {
	tiers := tv.settings.Policy.MinMiningTxFeeBySize
	if len(tiers) == 0 {
		return nil
	}

	size := uint64(tx.Size())
	if size <= tiers[0].SizeBytes {
		return nil
	}

	fee, err := util.GetFees(tx)
	if err != nil {
		// Outputs exceeding inputs is rejected as bad-txns-in-belowout by the
		// money-range checks (BDK, or the Go backstop on the skip-script
		// path); an unpayable fee is not this check's verdict to give.
		return nil
	}

	required := sizeTieredRequiredFee(minMiningTxFeeSatoshisPerKB(tv.settings.Policy), tiers, size)
	if fee >= required {
		return nil
	}

	if isFreeConsolidationTxn(tv.settings.Policy, tx, blockHeight, utxoHeights) {
		prometheusValidatorSizeTieredFeeConsolidationExemptions.Inc()

		return nil
	}

	prometheusValidatorSizeTieredFeeRejections.Inc()

	return errors.NewTxPolicyError("insufficient fee: %d satoshis paid, %d satoshis required for a %d byte transaction by minminingtxfeebysize", fee, required, size)
}

// isDustReturnTxn mirrors svnode's IsDustReturnTxn: a single zero-value output
// carrying the exact dust-return script. Such a donation to the miner relaxes
// the consolidation factor and confirmation requirements below.
func isDustReturnTxn(tx *bt.Tx) bool {
	if len(tx.Outputs) != 1 || tx.Outputs[0].Satoshis != 0 {
		return false
	}

	lockingScript := tx.Outputs[0].LockingScript
	if lockingScript == nil {
		return false
	}

	return bytes.Equal(*lockingScript, dustReturnScript)
}

// isStandardPrevoutScript mirrors svnode's IsStandardOutput classification for
// the consolidation-input standardness rule: the solvable standard templates.
// It is intentionally slightly more permissive than svnode (no data-carrier
// size or protocol-era checks): wrongly exempting a transaction only skips the
// size-tier surcharge while BDK's MinMiningTxFee floor still applies, whereas
// wrongly refusing the exemption would reject a consolidation BDK accepts for
// free.
func isStandardPrevoutScript(script *bscript.Script) bool {
	return script.IsP2PKH() || script.IsP2PK() || script.IsP2SH() || script.IsMultiSigOut() || script.IsData()
}

// isFreeConsolidationTxn reports whether tx qualifies as a free consolidation
// transaction under the rules BDK applies for its MinMiningTxFee exemption
// (bitcoin-sv policy.cpp IsFreeConsolidationTxn): it must reduce the UTXO set
// by the configured factor, spend only confirmed, size-bounded, standard
// inputs, and shrink the cumulated locking-script bytes by the same factor. A
// dust-return donation (isDustReturnTxn) relaxes the factor and confirmation
// requirements exactly as in svnode.
//
// Heights: utxoHeights carries the creation height of each input UTXO, with
// unconfirmedParentHeight as the unconfirmed sentinel. blockHeight is the
// candidate height (tip+1) in policy mode, so an input confirmed at height h
// has blockHeight-h confirmations, matching svnode's tipHeight+1-coinHeight.
// Inputs whose height or previous script is unavailable are treated as
// disqualifying: without them the rules cannot be checked, and not exempting
// is the containment direction (BDK's own floor is unaffected either way).
func isFreeConsolidationTxn(po *settings.PolicySettings, tx *bt.Tx, blockHeight uint32, utxoHeights []uint32) bool {
	// Factor zero disables free consolidations entirely, as in svnode.
	if po.MinConsolidationFactor <= 0 {
		return false
	}

	if tx.IsCoinbase() || len(tx.Inputs) == 0 {
		return false
	}

	if len(utxoHeights) != len(tx.Inputs) {
		return false
	}

	isDonation := isDustReturnTxn(tx)

	factor := uint64(po.MinConsolidationFactor)
	minConf := po.MinConfConsolidationInput

	if isDonation {
		factor = uint64(len(tx.Inputs))
		minConf = 0
	}

	// The consolidation transaction needs to reduce the count of UTXOs.
	if uint64(len(tx.Inputs)) < factor*uint64(len(tx.Outputs)) {
		return false
	}

	maxInputScriptSize := po.MaxConsolidationInputScriptSize
	stdInputOnly := !po.AcceptNonStdConsolidationInput

	sumInputLockingScriptBytes := uint64(0)

	for i, input := range tx.Inputs {
		height := utxoHeights[i]

		if minConf > 0 {
			if height == unconfirmedParentHeight {
				return false
			}

			// height 0 means the store did not record a height for this coin;
			// svnode skips the confirmation rule for such legacy coins.
			if height != 0 {
				if height >= blockHeight {
					return false
				}

				if blockHeight-height < uint32(minConf) { //nolint:gosec // minConf > 0 checked above
					return false
				}
			}
		}

		var unlockingScriptSize int
		if input.UnlockingScript != nil {
			unlockingScriptSize = len(*input.UnlockingScript)
		}

		if unlockingScriptSize > maxInputScriptSize {
			return false
		}

		if input.PreviousTxScript == nil {
			return false
		}

		if stdInputOnly && !isStandardPrevoutScript(input.PreviousTxScript) {
			return false
		}

		sumInputLockingScriptBytes += uint64(len(*input.PreviousTxScript))
	}

	sumOutputLockingScriptBytes := uint64(0)

	for _, output := range tx.Outputs {
		if output.LockingScript != nil {
			sumOutputLockingScriptBytes += uint64(len(*output.LockingScript))
		}
	}

	// Prevent consolidations that are not advantageous enough for miners.
	return sumInputLockingScriptBytes >= factor*sumOutputLockingScriptBytes
}
