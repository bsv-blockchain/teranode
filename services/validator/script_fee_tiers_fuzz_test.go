package validator

import (
	"math/rand"
	"os"
	"strconv"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-chaincfg"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// This file fuzzes countScriptOps against real BDK.
//
// The named differential tests prove the divergences we already knew to look
// for. This one looks for the divergences we did not: it generates random
// scripts and pins BDK's executed op count for each, exactly, then requires our
// static count to match. A third divergence hiding in some opcode combination is
// precisely the failure that would price a legitimate script wrongly in
// production, so it is worth searching for rather than reasoning about.
//
// Pinning the count exactly costs two BDK calls, no bisection: BDK rejects with
// SCRIPT_ERR_OP_COUNT exactly when its count EXCEEDS the cap, so
//
//	accepted at cap N      => count <= N
//	rejected at cap N-1    => count >  N-1, i.e. count >= N
//
// and together those force count == N.

// requireBDKOpCount asserts that BDK's executed op count for lockingScript is
// exactly want, and reports whether the count could be pinned at all.
//
// maxopsperscriptpolicy = 0 means UNLIMITED, not "zero allowed", so the
// lower-bound probe only carries information when want >= 2. Scripts counted at
// 0 or 1 are therefore not pinnable by this method and are skipped; the corpus
// is generated so that the overwhelming majority are well above that.
func requireBDKOpCount(t *testing.T, params *chaincfg.Params, lockingScript []byte, want uint64, coinHeight, blockHeight uint32) (pinned bool) {
	t.Helper()

	if want < 2 {
		return false
	}

	// Upper bound: at a cap of `want`, the count must not be exceeded.
	atWant := bdkSpendVerdict(t, params, lockingScript, nil, int64(want), coinHeight, blockHeight) //nolint:gosec // bounded by the generator
	require.False(t, isOpCountRejection(atWant),
		"BDK counted MORE than %d ops for script %x (verdict %v)", want, lockingScript, atWant)

	// Lower bound: at a cap one below, the count must be exceeded.
	atWantMinusOne := bdkSpendVerdict(t, params, lockingScript, nil, int64(want-1), coinHeight, blockHeight) //nolint:gosec // bounded by the generator
	require.True(t, isOpCountRejection(atWantMinusOne),
		"BDK counted FEWER than %d ops for script %x (verdict %v)", want, lockingScript, atWantMinusOne)

	return true
}

// fuzzEra picks the coin era for one script. Half the corpus is a
// post-Chronicle coin and half a post-Genesis pre-Chronicle one, because the
// conditional grammar differs between them (countScriptOps) and each has to be
// fuzzed on a coin of its own era. The regtest parameters activate Genesis at
// 100 and Chronicle at 200, so the pre-Chronicle coin sits between the two and
// the post-Chronicle coin well past both.
func fuzzEra(t *testing.T, rng *rand.Rand, params *chaincfg.Params, blockHeight uint32) (coinHeight uint32, postChronicle bool) {
	t.Helper()

	genesis, chronicle := params.GenesisActivationHeight, params.ChronicleActivationHeight
	require.Greater(t, chronicle, genesis+1, "the test parameters must leave room for a post-Genesis pre-Chronicle coin")
	require.Greater(t, blockHeight, chronicle+1_000)

	if rng.Intn(2) == 0 {
		return blockHeight - 1_000, true
	}

	return genesis + (chronicle-genesis)/2, false
}

// randomScript builds a script from opcodes that cannot fail at runtime, so BDK
// evaluates it to completion and its op count is well defined. The grammar
// deliberately covers what the metric has to get right: OP_NOP runs (counted),
// data pushes of every width (free), small constants (free), balanced
// OP_IF/OP_ELSE/OP_ENDIF nesting including branches that do not execute (still
// counted), and OP_RETURN both nested (does not terminate) and at the top level
// (terminates, so the tail is never counted). On a post-Chronicle coin
// OP_VERNOTIF opens conditionals too: with a one-byte item on the stack (not a
// 4-byte transaction version) its condition is true, so the branch runs, which
// this generator's exactness relies on. OP_VERIF (false on such an item) is
// left to randomBranchyScript, since a nested OP_RETURN in a branch that never
// runs is a documented under-count rather than an exact match.
//
// It never emits OP_CHECKMULTISIG: that divergence is known, documented, and
// pinned by TestCheckMultiSigUnderCountDifferentialBDK.
func randomScript(rng *rand.Rand, postChronicle bool) []byte {
	script := make([]byte, 0, 256)
	depth := 0

	// Pushes must use their MINIMAL encoding or BDK rejects the script with
	// "Data push larger than necessary" before it finishes, which would make the
	// op count undefined. Minimal means: 0 bytes is OP_0, a single byte in 1..16
	// is OP_1..OP_16, 1..75 bytes is a direct push, 76..255 needs OP_PUSHDATA1,
	// and 256+ needs OP_PUSHDATA2. Payload bytes are 0xff so a one-byte push is
	// never a value that would itself demand a small-constant opcode.
	filler := func(n int) []byte {
		b := make([]byte, n)
		for i := range b {
			b[i] = 0xff
		}

		return b
	}

	emitPush := func() {
		switch rng.Intn(4) {
		case 0: // direct push, 1..75 bytes
			n := 1 + rng.Intn(75)
			script = append(script, byte(n))
			script = append(script, filler(n)...)

		case 1: // OP_PUSHDATA1, 76..255 bytes
			n := 76 + rng.Intn(180)
			script = append(script, bscript.OpPUSHDATA1, byte(n))
			script = append(script, filler(n)...)

		case 2: // OP_PUSHDATA2, 256..355 bytes
			n := 256 + rng.Intn(100)
			script = append(script, bscript.OpPUSHDATA2, byte(n&0xff), byte(n>>8))
			script = append(script, filler(n)...)

		default:
			// OP_0 and the small constants: free, and they leave a stack value.
			if rng.Intn(8) == 0 {
				script = append(script, bscript.OpZERO)
			} else {
				script = append(script, byte(int(bscript.OpONE)+rng.Intn(16)))
			}
		}
	}

	steps := 1 + rng.Intn(40)

	for i := 0; i < steps; i++ {
		switch rng.Intn(6) {
		case 0, 1: // a run of counted no-ops
			for n := rng.Intn(6); n >= 0; n-- {
				script = append(script, bscript.OpNOP)
			}

		case 2: // a free push
			emitPush()

		case 3: // open a conditional (needs a stack value to consume)
			if postChronicle && rng.Intn(3) == 0 {
				script = append(script, bscript.OpONE, bscript.OpVERNOTIF)
			} else {
				script = append(script, bscript.OpONE, bscript.OpIF)
			}

			depth++

		case 4: // close a conditional, sometimes with an ELSE arm
			if depth > 0 {
				if rng.Intn(2) == 0 {
					script = append(script, bscript.OpELSE, bscript.OpNOP)
				}

				script = append(script, bscript.OpENDIF)
				depth--
			}

		case 5: // an OP_RETURN, nested (keeps counting) or top level (stops)
			if depth > 0 || rng.Intn(6) == 0 {
				script = append(script, bscript.OpRETURN)
				// Give the terminator something to skip over.
				for n := rng.Intn(5); n >= 0; n-- {
					script = append(script, bscript.OpNOP)
				}
			}
		}
	}

	for ; depth > 0; depth-- {
		script = append(script, bscript.OpENDIF)
	}

	// Sometimes finish with a bare multisig, which exercises the key-count path.
	// It has to be last: OP_CHECKMULTISIG adds its key count and then fails on
	// the missing signatures, so BDK counts nothing after it, and only a script
	// that ends there keeps both counts comparable. It sits at depth zero, where
	// the key count is statically certain and must therefore match exactly.
	// Counts above 16 encode as a data push rather than a small constant, so the
	// range spans both, and OP_CHECKMULTISIGVERIFY is exercised alongside
	// OP_CHECKMULTISIG.
	if rng.Intn(3) == 0 {
		multisig := bareMultiSig(1 + rng.Intn(40))
		if rng.Intn(2) == 0 {
			multisig[len(multisig)-1] = bscript.OpCHECKMULTISIGVERIFY
		}

		return append(script, multisig...)
	}

	// Otherwise leave a true on the stack so a script without a top-level
	// OP_RETURN finishes cleanly rather than erroring on an empty stack.
	return append(script, bscript.OpONE)
}

// randomBranchyScript generates the shapes randomScript structurally cannot:
// conditions that are FALSE as well as true, OP_NOTIF, OP_RETURN inside a
// branch that never executes, and on a post-Chronicle coin OP_VERIF (false on a
// non-version item) as well as OP_VERNOTIF. A multisig only ever appears as the final
// construct, for the same reason as in randomScript: it fails on the missing
// signatures, so anything after it is never reached by BDK and the two counts
// would stop being comparable. Exact agreement is not claimed on these. svnode
// only suspends execution for a nested OP_RETURN it actually executes, while the
// static walk suspends for any of them, so a later multisig key count can be
// legitimately under-counted here. What must always hold is the safety
// invariant: never counting MORE than svnode, since only over-counting can
// price a legitimate script off the network.
func randomBranchyScript(rng *rand.Rand, postChronicle bool) []byte {
	script := make([]byte, 0, 128)
	depth := 0

	for i := 0; i < 1+rng.Intn(14); i++ {
		switch rng.Intn(5) {
		case 0:
			for n := rng.Intn(4); n >= 0; n-- {
				script = append(script, bscript.OpNOP)
			}

		case 1: // open a conditional, condition true or false, IF or NOTIF
			if rng.Intn(2) == 0 {
				script = append(script, bscript.OpZERO)
			} else {
				script = append(script, bscript.OpONE)
			}

			switch {
			case postChronicle && rng.Intn(3) == 0:
				if rng.Intn(2) == 0 {
					script = append(script, bscript.OpVERIF)
				} else {
					script = append(script, bscript.OpVERNOTIF)
				}
			case rng.Intn(2) == 0:
				script = append(script, bscript.OpIF)
			default:
				script = append(script, bscript.OpNOTIF)
			}

			depth++

		case 2: // an OP_RETURN inside a branch: may or may not be executed
			if depth > 0 {
				script = append(script, bscript.OpRETURN)
			}

		case 3: // a longer run of counted no-ops
			for n := rng.Intn(8); n >= 0; n-- {
				script = append(script, bscript.OpNOP)
			}

		case 4:
			if depth > 0 {
				if rng.Intn(2) == 0 {
					script = append(script, bscript.OpELSE, bscript.OpNOP)
				}

				script = append(script, bscript.OpENDIF)
				depth--
			}
		}
	}

	for ; depth > 0; depth-- {
		script = append(script, bscript.OpENDIF)
	}

	// A multisig at depth zero after the branches, where the key count is
	// statically certain unless a nested OP_RETURN suspended execution.
	if rng.Intn(2) == 0 {
		return append(script, bareMultiSig(1+rng.Intn(20))...)
	}

	return append(script, bscript.OpONE)
}

// TestCountScriptOpsNeverOverCountsFuzzBDK asserts the safety invariant on the
// branchy corpus: countScriptOps must never exceed BDK's executed count. Only
// over-counting can charge a fee svnode would not and strand a legitimate coin;
// under-counting costs the miner revenue and nothing else.
//
// Using the same oracle, BDK rejecting at a cap of ours-1 means BDK's count is
// at least ours, which is exactly "we did not over-count".
func TestCountScriptOpsNeverOverCountsFuzzBDK(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)
	params := tSettings.ChainCfgParams
	blockHeight := params.GenesisActivationHeight + 10_000

	rng := rand.New(rand.NewSource(20260901)) //nolint:gosec // deterministic corpus, not cryptographic

	scripts := 120
	if v := os.Getenv("FEE_TIER_FUZZ_SCRIPTS"); v != "" {
		n, err := strconv.Atoi(v)
		require.NoError(t, err)
		scripts = n
	}

	checked, checkedPreChronicle := 0, 0

	for i := 0; i < scripts; i++ {
		coinHeight, postChronicle := fuzzEra(t, rng, params, blockHeight)
		script := randomBranchyScript(rng, postChronicle)

		ours, capExceeded := countScriptOps(script, 1_000_000, postChronicle)
		require.False(t, capExceeded)

		if ours < 2 {
			continue
		}

		err := bdkSpendVerdict(t, params, script, nil, int64(ours-1), coinHeight, blockHeight) //nolint:gosec // bounded by the generator
		require.True(t, isOpCountRejection(err),
			"countScriptOps returned %d, more than BDK counted, for script %x on a coin at height %d (verdict %v)", ours, script, coinHeight, err)

		checked++

		if !postChronicle {
			checkedPreChronicle++
		}
	}

	require.Greater(t, checked, scripts/2, "too few scripts exercised the invariant")
	require.Greater(t, checkedPreChronicle, scripts/8, "too few pre-Chronicle scripts exercised the invariant")

	t.Logf("verified no over-count against BDK on %d of %d branchy scripts (%d on pre-Chronicle coins)", checked, scripts, checkedPreChronicle)
}

// TestCountScriptOpsFuzzDifferentialBDK searches for op-count divergences we did
// not anticipate, by pinning BDK's true count for randomly generated scripts and
// requiring countScriptOps to agree exactly.
func TestCountScriptOpsFuzzDifferentialBDK(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)
	params := tSettings.ChainCfgParams
	blockHeight := params.GenesisActivationHeight + 10_000

	// Fixed seed: a divergence must be reproducible, not a flake.
	rng := rand.New(rand.NewSource(20260901)) //nolint:gosec // deterministic corpus, not cryptographic

	// Kept small enough for CI. Raise it for a deep search, for example
	// FEE_TIER_FUZZ_SCRIPTS=5000 go test -run FuzzDifferentialBDK ./services/validator/
	scripts := 120
	if v := os.Getenv("FEE_TIER_FUZZ_SCRIPTS"); v != "" {
		n, err := strconv.Atoi(v)
		require.NoError(t, err, "FEE_TIER_FUZZ_SCRIPTS must be an integer")
		require.Positive(t, n)

		scripts = n
	}

	pinned, pinnedPreChronicle := 0, 0

	for i := 0; i < scripts; i++ {
		coinHeight, postChronicle := fuzzEra(t, rng, params, blockHeight)
		script := randomScript(rng, postChronicle)

		ours, capExceeded := countScriptOps(script, 1_000_000, postChronicle)
		require.False(t, capExceeded, "generator produced an over-cap script")

		if requireBDKOpCount(t, params, script, ours, coinHeight, blockHeight) {
			pinned++

			if !postChronicle {
				pinnedPreChronicle++
			}
		}
	}

	// Guard against the corpus degenerating into unpinnable scripts and the test
	// passing without actually comparing anything.
	require.Greater(t, pinned, scripts*3/4,
		"too few scripts had an exactly pinned op count; the corpus is not exercising the counter")
	require.Greater(t, pinnedPreChronicle, scripts/4, "too few pre-Chronicle scripts were pinned")

	t.Logf("exactly pinned BDK op count against countScriptOps for %d of %d random scripts (%d on pre-Chronicle coins)", pinned, scripts, pinnedPreChronicle)
}
