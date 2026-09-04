package protocol

import (
	"math/big"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-chaincfg"
)

// SCOPE OF THIS PORT — read before adding a caller.
//
// Only the DAA era is contextually validated. SVNode's GetNextWorkRequired
// (pow.cpp:95-117) selects between three difficulty regimes by the parent's
// height: the regtest no-retarget rule, the DAA (cw-144) at and above
// daaHeight, and GetNextEDAWorkRequired below it — which is itself the
// original 2016-block retarget plus the Emergency Difficulty Adjustment.
// THE EDA AND THE ORIGINAL RETARGET ARE NOT IMPLEMENTED HERE. A parent below
// daaHeight is reported as "no contextual answer" (validated == false) and its
// child is accepted on its own claimed target alone.
//
// That scope limit is deliberate, not an omission. Every mainnet height above
// the final chaincfg checkpoint is post-DAA, so the era this port covers is
// exactly the era a peer can still grind headers in. Headers below the final
// checkpoint are covered by two other gates: the headers-first round checks
// the hash at each checkpoint height, and checkIndexAgainstCheckpoint
// (headersync.go) refuses any new header forking below the last checkpoint the
// index holds. Neither of those needs a difficulty answer, so building the EDA
// would add consensus-critical arithmetic that changes no decision.
//
// WHAT IS AND IS NOT BOUNDED, precisely — do not read the paragraph above as
// "the index is now bounded everywhere".
//
//   - mainnet steady state (past the final checkpoint, 945000): fully bounded.
//     Every new header is post-DAA, so it costs the network's real difficulty.
//   - mainnet and STN during IBD: bounded BELOW the highest checkpoint the
//     index currently holds (the fence refuses those outright) and ABOVE
//     daaHeight (the DAA prices them). Between the two lies a transient band —
//     heights above the last checkpoint we have reached but below daaHeight —
//     that is priced by neither, and it shrinks to nothing as the sync passes
//     each checkpoint. A node holding no checkpoint yet has no lower bound at
//     all.
//   - ReduceMinDifficulty networks (testnet, teratestnet, tstn): NOT bounded by
//     difficulty. getNextCashWorkRequired's min-difficulty branch answers the
//     powLimit for any header whose timestamp is more than two target spacings
//     past its parent's, so those headers are cheap by design. The only bound
//     on them is the time-too-new cap in headersync.go acceptHeader, and that
//     bounds the DEPTH of such a chain (roughly six headers), not its WIDTH —
//     a peer may still put arbitrarily many cheap siblings on one parent.
//     SVNode carries the same exposure on the same networks; closing it is not
//     something this port can do without diverging from it.
//
// A reader who needs pre-DAA difficulty validation must port
// GetNextEDAWorkRequired (pow.cpp:22-93) and CalculateNextWorkRequired
// (pow.cpp:119-148) here, and must not assume this file already does it.
//
// SIBLING IMPLEMENTATION. Teranode already carries the DAA arithmetic in
// services/blockchain.ComputeTarget, which block assembly uses to pick the
// nBits of the block it is building. This file does not call it, on purpose:
// the protocol package holds no Teranode service dependency (spec §4.4), and
// that one reads a model.SuitableBlock the store produces, not a HeaderNode.
// The two agree ON EVERY REACHABLE INPUT — same work difference, same
// [72, 288] spacing clamp, same (2**256 - work) / work, same powLimit clip,
// and the same shallowest height they will answer for (that one's
// `blockHeight < DifficultyAdjustmentWindow+4` is this one's
// daaMinParentHeight, one height lower because it counts the block being built
// rather than its parent). A change to the arithmetic here belongs in both.
//
// They differ only in degenerate branches, in three places, none of which a
// shipped network can reach:
//
//   - a work sum that floors to zero: that one answers the last suitable
//     block's nBits, this one answers the powLimit;
//   - a clamped timespan of zero: that one guards it explicitly and answers the
//     last suitable block's nBits, this one has no guard. Both clamp to
//     [72, 288] spacings BEFORE dividing, so the guard is dead for any positive
//     target spacing;
//   - a chain params with a non-positive target spacing, which is the only way
//     to reach the previous branch: that one falls into its zero guard, this
//     one reports the header as unvalidated and accepts it on its own nBits.
//
// The difference is therefore a documentation matter rather than a consensus
// one, but a change that makes any of the three reachable is not.

// daaWindow is the 144-block adjustment interval of the DAA, the literal 144
// in pow.cpp GetNextCashWorkRequired's `int32_t nHeightFirst = nHeight - 144`.
const daaWindow int32 = 144

// daaMinParentHeight is the shallowest parent the DAA can be computed for.
// GetSuitableBlock asserts `pindex->GetHeight() >= 3` (pow.cpp:215) and runs
// on both ends of the window, so the first block of the window — the parent's
// ancestor at height-144 — must itself be at height 3 or more.
//
// SVNode instead asserts `nHeight >= params.DifficultyAdjustmentInterval()`
// (2016) at pow.cpp:275, which it can do because it validates every block in
// order from genesis and because daaHeight exceeds 2016 on every network. This
// port checks the arithmetic's real requirement, so the answer does not depend
// on a constant that is only incidentally larger.
const daaMinParentHeight = daaWindow + 3

// twoTo256 is 2**256, the value arith_uint256 cannot hold and works around
// with a complement. math/big has no such limit, so ComputeTarget's
// `(-work) / work` is written here as its exact meaning, (2**256 - work) /
// work.
func twoTo256() *big.Int {
	return new(big.Int).Lsh(big.NewInt(1), 256)
}

// compactFromTarget is arith_uint256::GetCompact (arith_uint256.cpp): pack a
// target into the 32-bit compact form nBits carries. It is the inverse of the
// SetCompact decoding model.NBit.CalculateTarget performs, and the only
// direction this file needs, because the DAA produces a target and the caller
// compares its compact form against the header's nBits.
//
// The negative-sign branch of the source is unreachable here: every target
// this file feeds it comes from a subtraction of chain work or from the
// powLimit, and both are positive. A non-positive input answers zero, the same
// value the source produces for a zero target.
func compactFromTarget(target *big.Int) uint32 {
	if target == nil || target.Sign() <= 0 {
		return 0
	}

	// int nSize = (bits() + 7) / 8;
	size := uint(target.BitLen()+7) / 8

	// The mantissa is at most three bytes wide in both branches below, so it
	// always fits a uint32.
	var mantissa uint32

	if size <= 3 {
		mantissa = uint32(target.Uint64() << (8 * (3 - size))) //nolint:gosec // a target under 2**24 shifted into three bytes stays under 2**24
	} else {
		mantissa = uint32(new(big.Int).Rsh(target, 8*(size-3)).Uint64()) //nolint:gosec // the shift leaves the top three bytes, under 2**24
	}

	// "The 0x00800000 bit denotes the sign. Thus, if it is already set, divide
	// the mantissa by 256 and increase the exponent."
	if mantissa&0x00800000 != 0 {
		mantissa >>= 8
		size++
	}

	// The source asserts nSize < 256. A target is at most 2**256, so size is
	// at most 33 after the carry; the guard keeps the shift below well defined
	// if a caller ever hands in something larger.
	if size > 0xff {
		return 0
	}

	return mantissa | uint32(size)<<24 //nolint:gosec // size is bounded to a byte just above
}

// targetSpacingSeconds is Consensus::Params::nPowTargetSpacing. go-chaincfg
// carries it as a time.Duration. It answers zero for params that do not
// declare one, which the callers treat as "no contextual answer" rather than
// dividing by zero.
func targetSpacingSeconds(params *chaincfg.Params) int64 {
	return int64(params.TargetTimePerBlock / time.Second)
}

// suitableBlock is pow.cpp GetSuitableBlock (pow.cpp:214-245): "To reduce the
// impact of timestamp manipulation, we select the block we are basing our
// computation on via a median of 3." It answers the median-by-timestamp of the
// blocks at height, height-1 and height-2 on the branch ending at tip.
//
// The source walks pprev twice from a CBlockIndex; walking by height from the
// same tip through GetAncestor is the same three blocks, because both ends sit
// on one branch. The source's `assert(pindex->GetHeight() >= 3)` becomes a
// failed lookup here: an ancestor below genesis does not exist, so ok is
// false and the caller skips the check instead of panicking.
//
// The comparison order is the source's sorting network, kept literally.
func suitableBlock(idx *HeaderIndex, tip chainhash.Hash, height int32) (HeaderNode, bool) {
	if height < 3 {
		return HeaderNode{}, false
	}

	var blocks [3]HeaderNode

	for i := range blocks {
		// blocks[2] is the block itself, blocks[1] its parent, blocks[0] its
		// grandparent.
		anc, ok := idx.Ancestor(tip, height-int32(2-i)) //nolint:gosec // i is a loop index over three elements
		if !ok {
			return HeaderNode{}, false
		}

		blocks[i] = anc
	}

	if blocks[0].Time > blocks[2].Time {
		blocks[0], blocks[2] = blocks[2], blocks[0]
	}

	if blocks[0].Time > blocks[1].Time {
		blocks[0], blocks[1] = blocks[1], blocks[0]
	}

	if blocks[1].Time > blocks[2].Time {
		blocks[1], blocks[2] = blocks[2], blocks[1]
	}

	// We should have our candidate in the middle now.
	return blocks[1], true
}

// computeTarget is pow.cpp ComputeTarget (pow.cpp:177-208): "Compute a target
// based on the work done between 2 blocks and the time required to produce
// that work."
//
// The bound on nActualTimespan is the source's difficulty-cliff guard, which
// holds the adjustment to a factor in [0.5, 2] by clamping the measured
// timespan to [72, 288] target spacings.
//
// The source's final `return (-work) / work` relies on arith_uint256 negation
// wrapping modulo 2**256, which is 2**256 - work; that is written out here.
// A work sum that floors to zero would divide by zero, which cannot happen for
// a real 144-block window but is answered with the powLimit rather than a
// panic.
func computeTarget(first, last HeaderNode, params *chaincfg.Params) *big.Int {
	spacing := targetSpacingSeconds(params)

	// arith_uint256 work = pindexLast->GetChainWork() -
	//                      pindexFirst->GetChainWork();
	// work *= params.nPowTargetSpacing;
	work := new(big.Int).Sub(chainWorkOf(last), chainWorkOf(first))
	work.Mul(work, big.NewInt(spacing))

	// In order to avoid difficulty cliffs, we bound the amplitude of the
	// adjustment we are going to do to a factor in [0.5, 2].
	actualTimespan := last.Time - first.Time
	if actualTimespan > 288*spacing {
		actualTimespan = 288 * spacing
	} else if actualTimespan < 72*spacing {
		actualTimespan = 72 * spacing
	}

	work.Div(work, big.NewInt(actualTimespan))

	if work.Sign() <= 0 {
		return new(big.Int).Set(params.PowLimit)
	}

	return new(big.Int).Div(new(big.Int).Sub(twoTo256(), work), work)
}

// getNextCashWorkRequired is pow.cpp GetNextCashWorkRequired (pow.cpp:256-297),
// the DAA: "Compute the next required proof of work using a weighted average
// of the estimated hashrate per block."
//
// validated is false when this port cannot answer — the parent's 147-block
// window is not fully in the index, or the params declare no target spacing.
// See the insufficient-context note on GetNextWorkRequired.
func getNextCashWorkRequired(idx *HeaderIndex, params *chaincfg.Params, parent HeaderNode, headerTime int64) (nBits uint32, validated bool) {
	spacing := targetSpacingSeconds(params)
	if spacing <= 0 {
		return 0, false
	}

	// Special difficulty rule for testnet: if the new block's timestamp is
	// more than 2 * 10 minutes then allow mining of a min-difficulty block.
	// The comparison is strict, so exactly two spacings stays on the DAA path.
	if params.ReduceMinDifficulty && headerTime > parent.Time+2*spacing {
		return compactFromTarget(params.PowLimit), true
	}

	// Compute the difficulty based on the full adjustment interval.
	height := parent.Height
	if height < daaMinParentHeight {
		return 0, false
	}

	// Get the last suitable block of the difficulty interval.
	last, ok := suitableBlock(idx, parent.Hash, height)
	if !ok {
		return 0, false
	}

	// Get the first suitable block of the difficulty interval.
	first, ok := suitableBlock(idx, parent.Hash, height-daaWindow)
	if !ok {
		return 0, false
	}

	// Compute the target based on time and work done during the interval.
	nextTarget := computeTarget(first, last, params)

	if nextTarget.Cmp(params.PowLimit) > 0 {
		return compactFromTarget(params.PowLimit), true
	}

	return compactFromTarget(nextTarget), true
}

// GetNextWorkRequired is pow.cpp GetNextWorkRequired (pow.cpp:95-117): the
// nBits a header building on parent must claim. headerTime is the candidate
// header's own timestamp, which only the min-difficulty branch reads.
//
// validated reports whether this port has an answer at all. It is false in
// three cases, and a false answer means the caller must accept the header on
// its own claimed target alone rather than treat it as wrong:
//
//   - the parent sits below daaHeight, in a historic difficulty era this port
//     deliberately does not implement (see the scope note at the top of this
//     file);
//   - the parent's 147-block DAA window is not fully in the index;
//   - the params carry no target spacing.
//
// INSUFFICIENT CONTEXT — the policy and why. SVNode always has the window,
// because it validates every block in order from genesis and asserts on the
// height. This index cannot assert: it is a rebuildable cache, and a peer
// chooses which headers arrive. The policy is to SKIP the check, not to refuse
// the header.
//
// The short-window case IS REACHABLE, on two of the six networks go-chaincfg
// defines. teratestnet and tstn both set DaaForkHeight 0 with
// NoDifficultyAdjustment false, and settings.conf configures both, so on those
// chains heights 0 to 146 reach the DAA and are skipped for a short window.
// (On mainnet 504031, testnet 1188697 and STN 2200 the daaHeight gate refuses
// first, and regtest takes the no-retarget branch before either; on those four
// the case is unreachable, because HeaderIndex is genesis-rooted — see its doc
// comment — so every parent at or above daaMinParentHeight has its full
// ancestor chain.)
//
// Skipping is preferred to refusing because of what the two buy. The whole
// exposure is 147 heights at the very start of a chain, once, and refusing
// would reject the honest early headers of a real teratestnet or tstn sync to
// gain a bound worth those 147 heights. Below-fence heights also stay covered
// by the checkpoint gates as soon as the node holds a checkpoint.
func GetNextWorkRequired(idx *HeaderIndex, params *chaincfg.Params, parent HeaderNode, headerTime int64) (nBits uint32, validated bool) {
	if idx == nil || params == nil || params.PowLimit == nil {
		return 0, false
	}

	// Special rule for regtest: we never retarget. This is tested before the
	// min-difficulty branch in the source, so regtest never reaches it.
	if params.NoDifficultyAdjustment {
		return parent.Bits, true
	}

	// pow.cpp: `if (pindexPrev->GetHeight() >= params.daaHeight)`. Below it the
	// source runs GetNextEDAWorkRequired, which this port does not carry.
	if int64(parent.Height) < int64(params.DaaForkHeight) {
		return 0, false
	}

	return getNextCashWorkRequired(idx, params, parent, headerTime)
}
