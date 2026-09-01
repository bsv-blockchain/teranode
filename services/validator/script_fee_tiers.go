package validator

import (
	"bytes"
	"math"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/settings"
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
// The counted-ops metric matches svnode's executed op count exactly for every
// script that reaches BDK, given the coin's era (Genesis and Chronicle both
// change the grammar svnode walks with), with ONE documented exception:
// OP_CHECKMULTISIG. See countScriptOps. It is not, and cannot be, an exact copy
// of the executed count in the general case, because svnode charges
// OP_CHECKMULTISIG a key count read from the runtime stack; a static walk
// under-counts it. The metric is still monotone in script complexity and does
// its economic job.
//
// BDK exempts free consolidation transactions from its fee FLOOR (bitcoin-sv
// policy.cpp IsFreeConsolidationTxn). That exemption is honoured here for the
// floor term ONLY: the per-script surcharge is always due. The exemption exists
// to encourage cleanup of many small UTXOs, and at any threshold set near the
// policy caps these are meant to price, an ordinary consolidation's scripts sit
// far below the first tier and owe no surcharge at all, so declining to waive
// the surcharge costs honest consolidators nothing. (Thresholds are an operator
// choice: set one low enough, a few tens of bytes or a handful of ops, and even
// a plain P2PKH input is over it. That is the operator pricing ordinary spends,
// not a property of this exemption.) What the floor-only rule prevents is the
// shape that is adversarial by construction: a large-script output created
// cheaply, then "consolidated" to dodge the surcharge. See checkScriptTieredFees.

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

// satMulU64 multiplies two uint64 values, saturating at math.MaxUint64 instead
// of wrapping. A pathological but parse-valid fee-tier config could otherwise
// wrap the required fee down to a small value and silently weaken policy
// (PR review, raspi-user 1); saturating up rejects such a transaction (the safe,
// fail-closed direction) rather than under-charging it.
func satMulU64(a, b uint64) uint64 {
	if a == 0 || b == 0 {
		return 0
	}

	if a > math.MaxUint64/b {
		return math.MaxUint64
	}

	return a * b
}

// satAddU64 adds two uint64 values, saturating at math.MaxUint64. Same rationale
// as satMulU64: a wrap here would lower the required fee.
func satAddU64(a, b uint64) uint64 {
	if a > math.MaxUint64-b {
		return math.MaxUint64
	}

	return a + b
}

// opsCap returns the counted-ops ceiling BDK enforces (maxopsperscriptpolicy),
// or math.MaxUint64 when the policy is unlimited (0).
func opsCap(po *settings.PolicySettings) uint64 {
	if po.MaxOpsPerScriptPolicy <= 0 {
		return math.MaxUint64
	}

	return uint64(po.MaxOpsPerScriptPolicy)
}

// scriptSizeCap returns the script-size ceiling BDK enforces
// (maxscriptsizepolicy), or math.MaxUint64 when the policy is unlimited (0).
func scriptSizeCap(po *settings.PolicySettings) uint64 {
	if po.MaxScriptSizePolicy <= 0 {
		return math.MaxUint64
	}

	return uint64(po.MaxScriptSizePolicy)
}

// scriptNumMaxBytes is the default CScriptNum width svnode accepts for a numeric
// stack item, and so the widest push that can carry a multisig key count.
const scriptNumMaxBytes = 4

// A coin BDK evaluates under pre-Genesis rules is capped by svnode's fixed
// pre-Genesis limits (MAX_OPS_PER_SCRIPT_BEFORE_GENESIS and
// MAX_SCRIPT_SIZE_BEFORE_GENESIS), and the policy caps do not apply to it in
// either direction: a policy cap below these leaves such a script accepted, one
// above leaves it rejected. Measured in TestPreGenesisCapsDifferentialBDK.
const (
	maxOpsPerScriptBeforeGenesis = 500
	maxScriptSizeBeforeGenesis   = 10_000
)

// decodeScriptNum decodes a CScriptNum: little-endian magnitude with the sign in
// the top bit of the final byte. Callers bound the length to scriptNumMaxBytes.
func decodeScriptNum(b []byte) int64 {
	if len(b) == 0 {
		return 0
	}

	var value int64
	for i, c := range b {
		value |= int64(c) << (8 * i) //nolint:gosec // length bounded by scriptNumMaxBytes
	}

	if b[len(b)-1]&0x80 != 0 {
		value &^= int64(0x80) << (8 * (len(b) - 1))

		return -value
	}

	return value
}

// countScriptOps statically counts the operations BDK's EvalScript counts
// against maxopsperscriptpolicy: every opcode above OP_16; pushes are free.
// svnode increments its op counter for every FETCHED opcode above OP_16, before
// deciding whether the branch executes, so opcodes inside an unexecuted OP_IF
// branch count too; this linear walk matches that. Three svnode subtleties are
// mirrored explicitly:
//
//   - A top-level OP_RETURN (post-Genesis, i.e. one not nested inside an
//     OP_IF/OP_NOTIF) ends execution successfully. svnode counts up to and
//     including that OP_RETURN, then stops fetching, so opcodes after it are
//     never counted. IF-nesting depth is a static property, so this walk tracks
//     it and stops at a depth-zero OP_RETURN, reproducing the rule exactly.
//   - OP_CHECKMULTISIG and OP_CHECKMULTISIGVERIFY are charged their key count on
//     top of the opcode itself (nOpCount += nKeysCount), where the count is
//     popped from the stack. That addition happens only when the opcode actually
//     EXECUTES, and the count is knowable only when the immediately preceding
//     opcode is a literal push, which is then the stack top the opcode consumes.
//     Both conditions are static, so the key count is added exactly in that
//     case. This is the densest shape the ops tier exists to price (a bare
//     n-of-m multisig lock), which would otherwise be charged one operation.
//   - Anywhere the key count is not statically certain, the opcode counts as
//     one. That UNDER-counts, which is the containment direction: it can only
//     charge less than svnode, never reject a script svnode accepts cheaply.
//     The widest case is a multisig whose key count arrives on the stack from
//     the OTHER script of the pair, such as a bare OP_CHECKMULTISIG locking
//     script spent by an unlocking script that pushes the keys and the count.
//     Each script is counted on its own, as svnode counts them, so that count
//     is simply not present here and the opcode is charged as one. Such a spend
//     is under-priced; it is not mis-validated, and BDK still enforces
//     maxopsperscriptpolicy against the true count.
//   - OP_VERIF and OP_VERNOTIF depend on the coin's era. Before the Chronicle
//     upgrade an executed one is a fatal bad opcode and an unexecuted one is
//     skipped without opening a conditional. From Chronicle on they ARE
//     conditionals: svnode falls through into the OP_IF handler, so they open
//     a branch that OP_ENDIF closes and the IF-depth rules above apply to them.
//     postChronicle selects that grammar. It must follow the coin's height
//     exactly as BDK derives its flags (isBDKPostChronicleCoin), because each
//     grammar over-counts under the other era: the post-Chronicle grammar on a
//     pre-Chronicle coin keeps counting past an OP_RETURN that is really top
//     level, and the pre-Chronicle grammar on a post-Chronicle coin charges a
//     key count to a multisig inside an OP_VERIF branch that never runs. Both
//     were measured against BDK (TestVerifConditionalDifferentialBDK). The
//     pre-Chronicle grammar additionally stops charging key counts once it has
//     seen one of these opcodes, which makes it safe, never over-counting, on a
//     coin of either era; the only scripts that under-counts are pre-Chronicle
//     ones carrying the opcode in a branch that never runs and a multisig after
//     it.
//
// Execution is certain only at IF-depth zero AND before any nested OP_RETURN.
// Post-Genesis an OP_RETURN inside a conditional does not end the script; it
// puts svnode into a grammar-checking mode where the remaining opcodes are still
// fetched and counted but no longer executed, so a later OP_CHECKMULTISIG adds
// no key count even at depth zero. That was found against real BDK by
// TestCountScriptOpsFuzzDifferentialBDK, not derived from the source.
//
// That suspension is applied to any nested OP_RETURN, including one in a branch
// that never runs, where svnode would not suspend at all because it never
// executes the opcode. Whether a branch runs depends on runtime stack values, so
// the conservative reading is the only static one available, and it errs by
// under-counting a later multisig rather than over-counting it.
//
// Counting stops as soon as the running total exceeds opCap, returning
// capExceeded=true, so a caller can decline to price (and stop walking) a script
// BDK will reject with SCRIPT_ERR_OP_COUNT. A malformed push (length running
// past the end of the script) stops the count: such a script fails BDK's own
// parse, so undercounting is harmless.
func countScriptOps(script []byte, opCap uint64, postChronicle bool) (ops uint64, capExceeded bool) {
	var (
		ifDepth int
		// Set by an OP_RETURN inside a conditional: svnode keeps fetching and
		// counting from there but stops executing, so no further key count is
		// added.
		executionSuspended bool
		// Set by OP_VERIF or OP_VERNOTIF under the pre-Chronicle grammar, where
		// the walk cannot know whether svnode opened a conditional: no further
		// key count is charged.
		keyCountUncertain bool
		// The value of the immediately preceding literal push, when there was
		// one: the stack top an OP_CHECKMULTISIG would pop as its key count.
		lastPush    int64
		lastWasPush bool
	)

	for i := 0; i < len(script); {
		opcode := script[i]
		i++

		// Hot path first: the counted opcodes these tiers exist to price. Kept
		// ahead of the push cases so an op-dense script pays one comparison per
		// byte, not five (PR review P1-10).
		if opcode > bscript.Op16 {
			if opcode == bscript.OpRETURN {
				if ifDepth == 0 {
					// Top-level OP_RETURN: counted, then execution ends.
					ops++

					return ops, ops > opCap
				}

				// Nested OP_RETURN: counting continues, execution does not.
				executionSuspended = true
			}

			switch opcode {
			case bscript.OpIF, bscript.OpNOTIF:
				ifDepth++
			case bscript.OpVERIF, bscript.OpVERNOTIF:
				if postChronicle {
					ifDepth++
				} else {
					keyCountUncertain = true
				}
			case bscript.OpENDIF:
				if ifDepth > 0 {
					ifDepth--
				}
			}

			ops++

			// The multisig key count, when execution and the count are both
			// statically certain.
			if ifDepth == 0 && !executionSuspended && !keyCountUncertain && lastWasPush && lastPush > 0 &&
				(opcode == bscript.OpCHECKMULTISIG || opcode == bscript.OpCHECKMULTISIGVERIFY) {
				ops = satAddU64(ops, uint64(lastPush))
			}

			lastWasPush = false

			if ops > opCap {
				return ops, true
			}

			continue
		}

		// opcode <= OP_16: data pushes (free) and small constants (free).
		// Every branch below either sets both of these or continues the loop.
		// Push lengths are decoded as uint64 and bounded against the remaining
		// bytes BEFORE narrowing to int: assembling an OP_PUSHDATA4 length in a
		// signed int overflows negative on a 32-bit build, which would slice
		// backwards (a panic) and walk the index backwards (a hang).
		var dataStart, dataLen int

		var pushLen uint64

		switch {
		case opcode <= 0x4b: // direct push of `opcode` bytes (OP_0 pushes none)
			pushLen = uint64(opcode)
		case opcode == bscript.OpPUSHDATA1:
			if i >= len(script) {
				return ops, false
			}

			pushLen = uint64(script[i])
			i++
		case opcode == bscript.OpPUSHDATA2:
			if i+1 >= len(script) {
				return ops, false
			}

			pushLen = uint64(script[i]) | uint64(script[i+1])<<8
			i += 2
		case opcode == bscript.OpPUSHDATA4:
			if i+3 >= len(script) {
				return ops, false
			}

			pushLen = uint64(script[i]) | uint64(script[i+1])<<8 | uint64(script[i+2])<<16 | uint64(script[i+3])<<24
			i += 4
		case opcode >= bscript.OpONE && opcode <= bscript.Op16:
			// OP_1..OP_16 push the small integer 1..16.
			lastPush, lastWasPush = int64(opcode)-int64(bscript.OpONE)+1, true

			continue
		default:
			// OP_1NEGATE and OP_RESERVED: never a usable key count.
			lastWasPush = false

			continue
		}

		// A push running past the end of the script is malformed: BDK fails its
		// own parse, so stopping here can only under-count.
		if pushLen > uint64(len(script)-i) {
			return ops, false
		}

		dataStart, dataLen = i, int(pushLen)
		i += dataLen

		// A data push (including OP_0, which pushes an empty item worth zero).
		// Only a CScriptNum-width push can carry a key count; anything wider
		// leaves the preceding-push state unusable.
		if dataLen <= scriptNumMaxBytes {
			lastPush, lastWasPush = decodeScriptNum(script[dataStart:dataStart+dataLen]), true
		} else {
			lastWasPush = false
		}
	}

	return ops, false
}

// smallIntPushData holds the one-byte values OP_1..OP_16 push, and
// negOnePushData the value OP_1NEGATE pushes. Kept as package data so lastPush
// can return a slice of the pushed value without allocating.
var (
	smallIntPushData = func() [16][1]byte {
		var values [16][1]byte
		for i := range values {
			values[i][0] = byte(i + 1)
		}

		return values
	}()
	negOnePushData = [1]byte{0x81} // CScriptNum -1
)

// lastPush returns the data of the final push in a push-parseable script, or
// nil if the script is empty, malformed, or ends in a non-push opcode. Used to
// extract a legacy P2SH redeem script from an unlocking script, mirroring how
// sigop counting treats P2SH spends.
//
// The small-constant opcodes count as pushes here, as they do for svnode's
// push-only test: a scriptSig such as OP_1 <redeem script> is a valid P2SH
// spend, and treating OP_1 as a non-push would abandon the walk and leave the
// redeem script unpriced.
func lastPush(script []byte) []byte {
	var last []byte

	for i := 0; i < len(script); {
		opcode := script[i]
		i++

		switch {
		case opcode == bscript.Op1NEGATE:
			last = negOnePushData[:]

			continue
		case opcode >= bscript.OpONE && opcode <= bscript.Op16:
			last = smallIntPushData[opcode-bscript.OpONE][:]

			continue
		}

		// Decoded as uint64 and bounded before narrowing: an OP_PUSHDATA4 length
		// assembled in a signed int overflows negative on a 32-bit build, which
		// would slice backwards and panic.
		var pushLen uint64

		switch {
		case opcode <= 0x4b:
			pushLen = uint64(opcode)
		case opcode == bscript.OpPUSHDATA1:
			if i >= len(script) {
				return nil
			}

			pushLen = uint64(script[i])
			i++
		case opcode == bscript.OpPUSHDATA2:
			if i+1 >= len(script) {
				return nil
			}

			pushLen = uint64(script[i]) | uint64(script[i+1])<<8
			i += 2
		case opcode == bscript.OpPUSHDATA4:
			if i+3 >= len(script) {
				return nil
			}

			pushLen = uint64(script[i]) | uint64(script[i+1])<<8 | uint64(script[i+2])<<16 | uint64(script[i+3])<<24
			i += 4
		default:
			// Not push-only; no unambiguous redeem script.
			return nil
		}

		if pushLen > uint64(len(script)-i) {
			return nil
		}

		size := int(pushLen)
		last = script[i : i+size]
		i += size
	}

	return last
}

// mayCountMultiSig reports whether a script could contain an OP_CHECKMULTISIG or
// OP_CHECKMULTISIGVERIFY, the only opcodes that add more than one to svnode's op
// count. Counted ops otherwise never exceed the script length, since every
// counted opcode advances the walk by one byte, and that bound is what lets
// priceScript skip the walk entirely on short scripts. A multisig breaks the
// bound (a one-byte OP_CHECKMULTISIG can count its whole key count), so the
// shortcut has to be withdrawn whenever one of those bytes is present. A byte
// that only appears inside push data is a false positive here, which costs a
// walk and never a wrong price.
func mayCountMultiSig(script []byte) bool {
	return bytes.IndexByte(script, bscript.OpCHECKMULTISIG) >= 0 ||
		bytes.IndexByte(script, bscript.OpCHECKMULTISIGVERIFY) >= 0
}

// priceScript returns the marginal surcharge, in satoshi-thousandths, for a
// single executed script under both tier schedules. A script BDK will reject for
// exceeding a hard cap is priced at zero: pricing it would preempt BDK's specific
// rejection with a misleading insufficient-fee error the submitter would retry
// forever, and would walk the whole script for a value that never matters (PR
// review P1-9). opCap and sizeCap are the caps BDK applies to the coin's era:
// maxopsperscriptpolicy and maxscriptsizepolicy for a post-Genesis coin, the
// fixed pre-Genesis limits for a pre-Genesis one (checkScriptTieredFees). The ops walk is
// skipped when the whole script length is below the first ops threshold, since
// counted ops cannot exceed the length (PR review P1-10) unless a multisig adds
// its key count, which mayCountMultiSig checks for. postChronicle selects the
// conditional grammar of the coin's era (see countScriptOps).
func priceScript(sizeTiers, opsTiers []settings.FeeTier, opCap, sizeCap uint64, script []byte, postChronicle bool) uint64 {
	n := uint64(len(script))
	if n == 0 || n > sizeCap {
		return 0
	}

	var thousandths uint64

	if len(opsTiers) > 0 && (n > opsTiers[0].Threshold || mayCountMultiSig(script)) {
		ops, capExceeded := countScriptOps(script, opCap, postChronicle)
		if capExceeded {
			return 0
		}

		thousandths = satAddU64(thousandths, tierExcessThousandths(opsTiers, ops))
	}

	if len(sizeTiers) > 0 {
		thousandths = satAddU64(thousandths, tierExcessThousandths(sizeTiers, n))
	}

	return thousandths
}

// tierExcessThousandths prices a script metric value against a marginal tier
// schedule, in satoshi-thousandths: units between one threshold and the next
// are charged at that tier's rate, units beyond the last threshold at its
// rate, and units below the first threshold are free (the byte-rate floor
// covers them). tiers must be sorted ascending, which parseFeeTiers
// guarantees. Arithmetic saturates rather than wraps (see satMulU64).
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

		thousandths = satAddU64(thousandths, satMulU64(upper-tier.Threshold, uint64(tier.SatoshisPerK)))
	}

	return thousandths
}

// txFee returns fee = sum(inputs) - sum(outputs) for an extended transaction,
// and ok=false when input values are missing or outputs exceed inputs. It sums
// inline rather than calling util.GetFees, which formats the double-SHA256 txid
// into an error message this call site discards (PR review P1-14). Input and
// output totals are bounded by the money-range checks BDK (or the Go backstop)
// applies, so they cannot wrap before the transaction is rejected elsewhere.
func txFee(tx *bt.Tx) (fee uint64, ok bool) {
	var in, out uint64

	for _, input := range tx.Inputs {
		in += input.PreviousTxSatoshis
	}

	for _, output := range tx.Outputs {
		out += output.Satoshis
	}

	if out > in {
		return 0, false
	}

	return in - out, true
}

// checkScriptTieredFees enforces minminingtxfeebyscriptsize and
// minminingtxfeebyscriptops for a single transaction. The required fee is the
// minminingtxfee floor on the whole transaction plus the marginal per-script
// surcharges, accumulated in satoshi-thousandths and divided once so the result
// is monotone and never loses a satoshi to per-band truncation. It is a no-op
// when both schedules are empty, and callers must gate it on policy mode (it is
// never a consensus rule).
//
// The single final division truncates, so a total surcharge below one satoshi
// rounds to nothing. That is the rate doing what a rate does (999 bytes at
// 1 sat/kB is less than a satoshi) and it matches how the byte-rate floor
// behaves; an operator who wants a script priced at all sets a rate that clears
// a satoshi at the sizes they care about.
//
// The free-consolidation exemption waives the FLOOR term only; the surcharge is
// always due (see the file comment). Because BDK re-checks and re-exempts its
// own floor one call later, an imperfect exemption match here can at most drop
// the small byte-rate floor, never the surcharge.
func (tv *TxValidator) checkScriptTieredFees(tx *bt.Tx, blockHeight uint32, utxoHeights []uint32) error {
	sizeTiers := tv.settings.Policy.MinMiningTxFeeByScriptSize
	opsTiers := tv.settings.Policy.MinMiningTxFeeByScriptOps

	if len(sizeTiers) == 0 && len(opsTiers) == 0 {
		return nil
	}

	genesisHeight := tv.settings.ChainCfgParams.GenesisActivationHeight
	chronicleHeight := tv.settings.ChainCfgParams.ChronicleActivationHeight
	opCap := opsCap(tv.settings.Policy)
	sizeCap := scriptSizeCap(tv.settings.Policy)

	// Inline the executed-script walk rather than building a [][]byte: the slice
	// is consumed once and escapes to the heap as a return value, so it is pure
	// per-transaction garbage on a concurrent hot path (PR review P1-13).
	var surchargeThousandths uint64

	for idx, input := range tx.Inputs {
		// BDK evaluates both scripts of the pair under the coin's flags, so the
		// coin's era selects the conditional grammar countScriptOps walks with
		// and the caps a script is left unpriced beyond. A missing height falls
		// back to the policy caps and the pre-Chronicle grammar, which never
		// over-counts in either era.
		var preGenesis, postChronicle bool
		if idx < len(utxoHeights) {
			preGenesis = isBDKPreGenesisCoin(utxoHeights[idx], blockHeight, genesisHeight)
			postChronicle = isBDKPostChronicleCoin(utxoHeights[idx], blockHeight, chronicleHeight)
		}

		coinOpCap, coinSizeCap := opCap, sizeCap
		if preGenesis {
			coinOpCap, coinSizeCap = maxOpsPerScriptBeforeGenesis, maxScriptSizeBeforeGenesis
		}

		var unlocking []byte
		if input.UnlockingScript != nil {
			unlocking = *input.UnlockingScript
		}

		surchargeThousandths = satAddU64(surchargeThousandths, priceScript(sizeTiers, opsTiers, coinOpCap, coinSizeCap, unlocking, postChronicle))

		if input.PreviousTxScript == nil {
			continue
		}

		prevout := *input.PreviousTxScript
		surchargeThousandths = satAddU64(surchargeThousandths, priceScript(sizeTiers, opsTiers, coinOpCap, coinSizeCap, prevout, postChronicle))

		// A legacy P2SH spend also executes its redeem script; bill it as sigop
		// counting does. Gated on the coin's era: BDK's VerifyScript runs the
		// redeem EvalScript only when !(flags & SCRIPT_UTXO_AFTER_GENESIS), i.e.
		// the coin was created before Genesis (coinHeight < genesisHeight). A
		// coin created before Genesis but spent now still runs it. Post-Genesis
		// coins never do, so billing a "redeem script" there would over-charge a
		// script no BSV node executes (PR review P1-8).
		if input.PreviousTxScript.IsP2SH() && idx < len(utxoHeights) && isPreGenesisCoin(utxoHeights[idx], genesisHeight) {
			// A pre-Genesis coin is pre-Chronicle, so the redeem walks with
			// the pre-Chronicle grammar, under the pre-Genesis caps.
			if redeem := lastPush(unlocking); len(redeem) > 0 {
				surchargeThousandths = satAddU64(surchargeThousandths, priceScript(sizeTiers, opsTiers, coinOpCap, coinSizeCap, redeem, false))
			}
		}
	}

	if surchargeThousandths == 0 {
		return nil
	}

	fee, ok := txFee(tx)
	if !ok {
		// Outputs exceeding inputs is rejected as bad-txns-in-belowout by the
		// money-range checks (BDK, or the Go backstop on the skip-script
		// path); an unpayable fee is not this check's verdict to give.
		return nil
	}

	floorThousandths := satMulU64(uint64(tx.Size()), uint64(minMiningTxFeeSatoshisPerKB(tv.settings.Policy))) //nolint:gosec // size and rate are non-negative

	exempt := isFreeConsolidationTxn(tv.settings.Policy, tx, blockHeight, utxoHeights, genesisHeight)
	if exempt {
		// The exemption waives the floor only; the surcharge stands.
		floorThousandths = 0
	}

	required := satAddU64(floorThousandths, surchargeThousandths) / 1000

	if fee >= required {
		if exempt {
			prometheusValidatorScriptTieredFeeConsolidationExemptions.Inc()
		}

		return nil
	}

	prometheusValidatorScriptTieredFeeRejections.Inc()

	// Wrap as BDK's insufficient-fee rejection is wrapped (NewTxInvalidError over
	// a NewTxPolicyError) so the rejected-tx Kafka publish gate in Validator.go
	// (errors.Is(err, ErrTxInvalid)) matches this the same way it matches BDK's
	// own underpayment rejection (PR review P1-11). The wrapped ErrTxPolicy code
	// keeps this classified as a policy error too.
	policyErr := errors.NewTxPolicyError("insufficient fee: %d satoshis paid, %d required by the per-script fee tiers (minminingtxfeebyscriptsize, minminingtxfeebyscriptops)", fee, required)

	return errors.NewTxInvalidError(errMsgInvalidTx, policyErr)
}

// isPreGenesisCoin gates the billing of a P2SH redeem script, and only that: it
// reports whether a UTXO created at coinHeight predates Genesis and so still
// runs its redeem when spent. Height 0 means the store recorded no height; it
// is treated as not-pre-Genesis, which declines to bill the redeem. BDK itself
// reads 0 as pre-Genesis and does execute the redeem there (measured in
// TestP2SHRedeemEraDifferentialBDK), so declining is a deliberate under-count,
// not a mismatch that could over-charge; everything else that must track BDK's
// era uses isBDKPreGenesisCoin and isBDKPostChronicleCoin.
//
// The cost of that choice is bounded and small: a pre-Genesis P2SH redeem script
// arrives as a single push, and pre-Genesis pushes are capped at 520 bytes, so
// the most work an unbillable redeem can hide is about 520 bytes and 520
// operations. Against thresholds set anywhere near the policy caps these tiers
// price, that is nothing.
func isPreGenesisCoin(coinHeight, genesisHeight uint32) bool {
	return coinHeight != 0 && coinHeight < genesisHeight
}

// bdkCoinHeight is the height BDK sees for a coin spent in the block at
// blockHeight: an unconfirmed parent is placed at the candidate height
// (substituteUnconfirmedHeights), and every other height, including an
// unrecorded 0, is passed through as it is.
func bdkCoinHeight(coinHeight, blockHeight uint32) uint32 {
	if coinHeight == unconfirmedParentHeight {
		return blockHeight
	}

	return coinHeight
}

// isBDKPreGenesisCoin reports whether BDK evaluates the coin under pre-Genesis
// rules, exactly as BDK derives it: the height it sees is below the activation
// height. Unlike isPreGenesisCoin, an unrecorded 0 is pre-Genesis here, as it is
// to BDK (measured: the fixed pre-Genesis caps apply at height 0, and P2SH is
// standard there for the consolidation rule). This is the predicate for
// everything that has to follow BDK's flags rather than err on the side of
// charging less.
func isBDKPreGenesisCoin(coinHeight, blockHeight, genesisHeight uint32) bool {
	return bdkCoinHeight(coinHeight, blockHeight) < genesisHeight
}

// isBDKPostChronicleCoin reports whether BDK evaluates the coin under
// post-Chronicle rules: the height it sees is at or above the activation height
// (the activation height itself is post-Chronicle, measured in
// TestVerifConditionalDifferentialBDK). It selects the conditional grammar
// countScriptOps walks with, which has to follow BDK's flags in both
// directions.
func isBDKPostChronicleCoin(coinHeight, blockHeight, chronicleHeight uint32) bool {
	return bdkCoinHeight(coinHeight, blockHeight) >= chronicleHeight
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

// isStandardPrevoutScript classifies a prevout locking script against the
// standard templates svnode's IsStandardOutput accepts for the consolidation
// input rule, in the coin's era as BDK derives it (isBDKPreGenesisCoin: an
// unrecorded height is pre-Genesis, as it is to svnode's Solver, which was
// measured to exempt such a P2SH consolidation). It is panic-safe on arbitrary
// attacker-supplied scripts: go-bt's IsP2PK and IsMultiSigOut index into
// DecodeParts output without length checks and panic on crafted scripts
// (PR review P0-1), so the templates are matched here directly on the bytes.
//
// Era matters (PR review P1-5): post-Genesis, svnode's Solver maps P2SH to
// TX_NONSTANDARD, so P2SH is standard only for coins created before Genesis.
// The classification stays intentionally no stricter than svnode/BDK: because
// the exemption waives only the byte-rate floor (checkScriptTieredFees), an
// over-permissive verdict costs at most that floor, which BDK re-checks anyway,
// whereas an over-strict verdict would wrongly reject a consolidation BDK
// accepts for free.
func isStandardPrevoutScript(script *bscript.Script, preGenesis bool) bool {
	if script == nil {
		return false
	}

	b := []byte(*script)

	return isP2PKHScript(b) ||
		isP2PKScript(b) ||
		isMultiSigScript(b) ||
		isDataScript(b) ||
		(preGenesis && isP2SHScript(b))
}

// isP2PKHScript matches OP_DUP OP_HASH160 <20> OP_EQUALVERIFY OP_CHECKSIG.
func isP2PKHScript(b []byte) bool {
	return len(b) == 25 &&
		b[0] == bscript.OpDUP &&
		b[1] == bscript.OpHASH160 &&
		b[2] == bscript.OpDATA20 &&
		b[23] == bscript.OpEQUALVERIFY &&
		b[24] == bscript.OpCHECKSIG
}

// isP2SHScript matches OP_HASH160 <20> OP_EQUAL.
func isP2SHScript(b []byte) bool {
	return len(b) == 23 &&
		b[0] == bscript.OpHASH160 &&
		b[1] == bscript.OpDATA20 &&
		b[22] == bscript.OpEQUAL
}

// isP2PKScript matches a single compressed (33-byte) or uncompressed (65-byte)
// pubkey push followed by OP_CHECKSIG, with the pubkey version byte validated.
func isP2PKScript(b []byte) bool {
	if len(b) == 35 && b[0] == bscript.OpDATA33 && b[34] == bscript.OpCHECKSIG {
		return b[1] == 0x02 || b[1] == 0x03
	}

	if len(b) == 67 && b[0] == bscript.OpDATA65 && b[66] == bscript.OpCHECKSIG {
		return b[1] == 0x04 || b[1] == 0x06 || b[1] == 0x07
	}

	return false
}

// isDataScript matches a data carrier: a bare OP_RETURN (pre-Genesis form) or
// OP_FALSE OP_RETURN (post-Genesis form) prefix.
func isDataScript(b []byte) bool {
	return (len(b) >= 1 && b[0] == bscript.OpRETURN) ||
		(len(b) >= 2 && b[0] == bscript.OpFALSE && b[1] == bscript.OpRETURN)
}

// isMultiSigScript matches a bare multisig template OP_m <pubkey>... OP_n
// OP_CHECKMULTISIG with small-int m and n (1..16), m <= n, and n pubkey pushes
// of 33 or 65 bytes. It is a panic-safe manual parse (go-bt's IsMultiSigOut
// panics on crafted input; see PR review P0-1). Large post-Genesis multisigs
// whose counts are not small ints are not classified standard here; being
// slightly strict for that rare shape is safe (see isStandardPrevoutScript).
func isMultiSigScript(b []byte) bool {
	if len(b) < 3 || b[len(b)-1] != bscript.OpCHECKMULTISIG || !isSmallIntOp(b[0]) {
		return false
	}

	keys := 0

	for i := 1; i < len(b)-1; {
		op := b[i]

		if isSmallIntOp(op) {
			// This must be OP_n, immediately before OP_CHECKMULTISIG.
			if i != len(b)-2 {
				return false
			}

			m := smallIntVal(b[0])
			n := smallIntVal(op)

			return keys == n && m >= 1 && m <= n
		}

		var size int

		switch op {
		case bscript.OpDATA33:
			size = 33
		case bscript.OpDATA65:
			size = 65
		default:
			return false
		}

		if i+1+size > len(b)-1 {
			return false
		}

		i += 1 + size
		keys++
	}

	return false
}

// isSmallIntOp reports whether opcode is OP_0 or OP_1..OP_16.
func isSmallIntOp(opcode byte) bool {
	return opcode == bscript.OpZERO || (opcode >= bscript.OpONE && opcode <= bscript.Op16)
}

// smallIntVal returns the integer value of a small-int opcode (OP_0 -> 0,
// OP_1..OP_16 -> 1..16); it assumes isSmallIntOp(opcode).
func smallIntVal(opcode byte) int {
	if opcode == bscript.OpZERO {
		return 0
	}

	return int(opcode) - int(bscript.OpONE) + 1
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
// genesisHeight resolves each input's era for the standardness classification.
// Inputs whose height or previous script is unavailable are treated as
// disqualifying: without them the rules cannot be checked, and not exempting
// is the containment direction (BDK's own floor is unaffected either way).
//
// Zero-valued MaxConsolidationInputScriptSize and MinConfConsolidationInput are
// normalised to svnode's defaults (150 and 6): BDK's config.cpp rewrites 0 to
// those, while ScriptVerifierGoBDK pushes the raw settings, so reading them raw
// here would disagree with BDK (PR review P1-7).
func isFreeConsolidationTxn(po *settings.PolicySettings, tx *bt.Tx, blockHeight uint32, utxoHeights []uint32, genesisHeight uint32) bool {
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
	if minConf == 0 {
		minConf = 6 // BDK config.cpp rewrites 0 to the svnode default.
	}

	maxInputScriptSize := po.MaxConsolidationInputScriptSize
	if maxInputScriptSize == 0 {
		maxInputScriptSize = 150 // BDK config.cpp rewrites 0 to the svnode default.
	}

	if isDonation {
		factor = uint64(len(tx.Inputs))
		minConf = 0
	}

	// The consolidation transaction needs to reduce the count of UTXOs.
	if uint64(len(tx.Inputs)) < satMulU64(factor, uint64(len(tx.Outputs))) {
		return false
	}

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

		if stdInputOnly && !isStandardPrevoutScript(input.PreviousTxScript, isBDKPreGenesisCoin(height, blockHeight, genesisHeight)) {
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
	return sumInputLockingScriptBytes >= satMulU64(factor, sumOutputLockingScriptBytes)
}
