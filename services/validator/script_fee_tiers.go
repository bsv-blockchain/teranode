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

// This file implements the per-script fee-tier policies minminingtxfeebyscriptsize
// and minminingtxfeebyscriptops: marginal minimum-fee surcharges for executed
// scripts that are large (the metric maxscriptsizepolicy caps) or op-dense (the
// counted-ops metric maxopsperscriptpolicy caps). Raising those caps admits such
// scripts; these settings let a miner price them instead of only capping them.
//
// BDK exposes only a single scalar fee rate (SetMinMiningTxFee), so the tier
// schedules are enforced here in Go, before BDK validation, and only in policy
// mode. The surcharges are additive on top of BDK's fee floor and every tier
// rate is non-negative, so this check only ever tightens fee policy; empty
// schedules (the default) make it a no-op, leaving behaviour identical to a
// node without the settings.
//
// BDK exempts free consolidation transactions from its fee floor
// (bitcoin-sv policy.cpp IsFreeConsolidationTxn). The same exemption is
// honoured here via isFreeConsolidationTxn so that enabling fee tiers never
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

// countScriptOps statically counts the operations BDK's EvalScript counts
// against maxopsperscriptpolicy: every opcode above OP_16; pushes are free.
// Bitcoin script has no loops, so the static count equals the executed count
// (svnode increments nOpCount for every fetched opcode, executed branch or
// not). A malformed push (length running past the end of the script) stops the
// count: such a script fails BDK's own parse, so undercounting is harmless.
func countScriptOps(script []byte) uint64 {
	var ops uint64

	for i := 0; i < len(script); {
		opcode := script[i]
		i++

		switch {
		case opcode <= 0x4b: // direct push of `opcode` bytes (OP_0 pushes none)
			i += int(opcode)
		case opcode == bscript.OpPUSHDATA1:
			if i >= len(script) {
				return ops
			}

			i += 1 + int(script[i])
		case opcode == bscript.OpPUSHDATA2:
			if i+1 >= len(script) {
				return ops
			}

			i += 2 + int(script[i]) + int(script[i+1])<<8
		case opcode == bscript.OpPUSHDATA4:
			if i+3 >= len(script) {
				return ops
			}

			i += 4 + int(script[i]) + int(script[i+1])<<8 + int(script[i+2])<<16 + int(script[i+3])<<24
		case opcode > bscript.Op16: // 0x60; OP_1NEGATE..OP_16 and OP_RESERVED are free
			ops++
		}
	}

	return ops
}

// lastPush returns the data of the final push in a push-parseable script, or
// nil if the script is empty, malformed, or ends in a non-push opcode. Used to
// extract a legacy P2SH redeem script from an unlocking script, mirroring how
// sigop counting treats P2SH spends.
func lastPush(script []byte) []byte {
	var last []byte

	for i := 0; i < len(script); {
		opcode := script[i]
		i++

		var size int

		switch {
		case opcode <= 0x4b:
			size = int(opcode)
		case opcode == bscript.OpPUSHDATA1:
			if i >= len(script) {
				return nil
			}

			size = int(script[i])
			i++
		case opcode == bscript.OpPUSHDATA2:
			if i+1 >= len(script) {
				return nil
			}

			size = int(script[i]) + int(script[i+1])<<8
			i += 2
		case opcode == bscript.OpPUSHDATA4:
			if i+3 >= len(script) {
				return nil
			}

			size = int(script[i]) + int(script[i+1])<<8 + int(script[i+2])<<16 + int(script[i+3])<<24
			i += 4
		default:
			// Not push-only; no unambiguous redeem script.
			return nil
		}

		if i+size > len(script) {
			return nil
		}

		last = script[i : i+size]
		i += size
	}

	return last
}

// executedScripts returns the scripts a miner executes to validate tx: each
// input's unlocking script, the locking script it spends, and, for a legacy
// P2SH spend, the redeem script. Output locking scripts of tx itself are not
// executed now; their cost falls on the future spender, who pays these tiers
// then. Missing extended data contributes nothing (BDK validation needs it
// anyway).
func executedScripts(tx *bt.Tx) [][]byte {
	scripts := make([][]byte, 0, 2*len(tx.Inputs))

	for _, input := range tx.Inputs {
		var unlocking []byte
		if input.UnlockingScript != nil {
			unlocking = *input.UnlockingScript
		}

		if len(unlocking) > 0 {
			scripts = append(scripts, unlocking)
		}

		if input.PreviousTxScript == nil {
			continue
		}

		scripts = append(scripts, *input.PreviousTxScript)

		if input.PreviousTxScript.IsP2SH() {
			if redeem := lastPush(unlocking); len(redeem) > 0 {
				scripts = append(scripts, redeem)
			}
		}
	}

	return scripts
}

// tierExcessThousandths prices a script metric value against a marginal tier
// schedule, in satoshi-thousandths: units between one threshold and the next
// are charged at that tier's rate, units beyond the last threshold at its
// rate, and units below the first threshold are free (the byte-rate floor
// covers them). tiers must be sorted ascending, which parseFeeTiers
// guarantees.
func tierExcessThousandths(tiers []settings.FeeTier, value uint64) uint64 {
	var thousandths uint64

	for i, tier := range tiers {
		if value <= tier.Threshold {
			break
		}

		upper := value
		if i+1 < len(tiers) && tiers[i+1].Threshold < value {
			upper = tiers[i+1].Threshold
		}

		thousandths += (upper - tier.Threshold) * uint64(tier.SatoshisPerK)
	}

	return thousandths
}

// checkScriptTieredFees enforces minminingtxfeebyscriptsize and
// minminingtxfeebyscriptops for a single transaction. The required fee is the
// minminingtxfee floor on the whole transaction plus the marginal per-script
// surcharges, accumulated in satoshi-thousandths and divided once so the
// result is monotone and never loses a satoshi to per-band truncation. It is a
// no-op when both schedules are empty, and callers must gate it on policy mode
// (it is never a consensus rule).
func (tv *TxValidator) checkScriptTieredFees(tx *bt.Tx, blockHeight uint32, utxoHeights []uint32) error {
	sizeTiers := tv.settings.Policy.MinMiningTxFeeByScriptSize
	opsTiers := tv.settings.Policy.MinMiningTxFeeByScriptOps

	if len(sizeTiers) == 0 && len(opsTiers) == 0 {
		return nil
	}

	var surchargeThousandths uint64

	for _, script := range executedScripts(tx) {
		if len(sizeTiers) > 0 {
			surchargeThousandths += tierExcessThousandths(sizeTiers, uint64(len(script)))
		}

		if len(opsTiers) > 0 {
			surchargeThousandths += tierExcessThousandths(opsTiers, countScriptOps(script))
		}
	}

	if surchargeThousandths == 0 {
		return nil
	}

	fee, err := util.GetFees(tx)
	if err != nil {
		// Outputs exceeding inputs is rejected as bad-txns-in-belowout by the
		// money-range checks (BDK, or the Go backstop on the skip-script
		// path); an unpayable fee is not this check's verdict to give.
		return nil
	}

	floorThousandths := uint64(tx.Size()) * uint64(minMiningTxFeeSatoshisPerKB(tv.settings.Policy)) //nolint:gosec // size and rate are non-negative
	required := (floorThousandths + surchargeThousandths) / 1000

	if fee >= required {
		return nil
	}

	if isFreeConsolidationTxn(tv.settings.Policy, tx, blockHeight, utxoHeights) {
		prometheusValidatorScriptTieredFeeConsolidationExemptions.Inc()

		return nil
	}

	prometheusValidatorScriptTieredFeeRejections.Inc()

	return errors.NewTxPolicyError("insufficient fee: %d satoshis paid, %d satoshis required by the per-script fee tiers (minminingtxfeebyscriptsize, minminingtxfeebyscriptops)", fee, required)
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
// fee-tier surcharge while BDK's MinMiningTxFee floor still applies, whereas
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
