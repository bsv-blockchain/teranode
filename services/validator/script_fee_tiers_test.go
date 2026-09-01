package validator

import (
	"encoding/hex"
	"math"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	ec "github.com/bsv-blockchain/go-sdk/primitives/ec"
	sdkscript "github.com/bsv-blockchain/go-sdk/script"
	sdktx "github.com/bsv-blockchain/go-sdk/transaction"
	"github.com/bsv-blockchain/go-sdk/transaction/template/p2pkh"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

const (
	scriptTierTestPrevTxID    = "0000000000000000000000000000000000000000000000000000000000000001"
	scriptTierTestP2PKHScript = "76a914000000000000000000000000000000000000000088ac" // 25 bytes
	scriptTierTestBlockHeight = uint32(200_000)
	scriptTierTestDeepHeight  = uint32(190_000) // well past the 6-confirmation default
)

// repeatedOps returns a script of n counted opcodes (OP_NOP each).
func repeatedOps(n int) []byte {
	script := make([]byte, n)
	for i := range script {
		script[i] = bscript.OpNOP
	}

	return script
}

// bigPush returns a script that pushes n bytes of data via OP_PUSHDATA4: n+5
// bytes of script carrying zero counted ops.
func bigPush(n int) []byte {
	script := make([]byte, 0, n+5)
	script = append(script, bscript.OpPUSHDATA4,
		byte(n), byte(n>>8), byte(n>>16), byte(n>>24))

	return append(script, make([]byte, n)...)
}

// countOps is a test helper wrapping countScriptOps with an unlimited cap and
// the post-Chronicle grammar, the one every coin created on mainnet today is
// evaluated under. countOpsPreChronicle walks the earlier grammar.
func countOps(script []byte) uint64 {
	ops, _ := countScriptOps(script, math.MaxUint64, true)
	return ops
}

func countOpsPreChronicle(script []byte) uint64 {
	ops, _ := countScriptOps(script, math.MaxUint64, false)
	return ops
}

// newScriptTierTestTx builds an extended transaction with one input carrying
// feeSatoshis (fee = input satoshis: the single output is a zero-value
// OP_RETURN) whose unlocking script is unlockingScript.
func newScriptTierTestTx(t *testing.T, unlockingScript []byte, feeSatoshis uint64) *bt.Tx {
	t.Helper()

	tx := bt.NewTx()
	require.NoError(t, tx.From(scriptTierTestPrevTxID, 0, scriptTierTestP2PKHScript, feeSatoshis))

	unlocking := bscript.Script(unlockingScript)
	tx.Inputs[0].UnlockingScript = &unlocking

	require.NoError(t, tx.AddOpReturnOutput([]byte{0x01}))

	return tx
}

// newConsolidationTestTx builds an extended transaction shaped like a
// consolidation: numInputs P2PKH prevouts of 1000 satoshis each with
// unlockingSize-byte unlocking scripts, and numOutputs P2PKH outputs splitting
// the input value minus feeSatoshis. Scripts are not executed by these tests.
func newConsolidationTestTx(t *testing.T, numInputs, numOutputs, unlockingSize int, feeSatoshis uint64) *bt.Tx {
	t.Helper()

	tx := bt.NewTx()

	const perInput = uint64(1000)

	for i := 0; i < numInputs; i++ {
		require.NoError(t, tx.From(scriptTierTestPrevTxID, uint32(i), scriptTierTestP2PKHScript, perInput)) //nolint:gosec // test loop index

		unlocking := bscript.Script(make([]byte, unlockingSize))
		tx.Inputs[i].UnlockingScript = &unlocking
	}

	outScript, err := bscript.NewFromHexString(scriptTierTestP2PKHScript)
	require.NoError(t, err)

	total := perInput*uint64(numInputs) - feeSatoshis //nolint:gosec // test values
	for j := 0; j < numOutputs; j++ {
		tx.AddOutput(&bt.Output{Satoshis: total / uint64(numOutputs), LockingScript: outScript}) //nolint:gosec // test values
	}

	return tx
}

func deepHeights(n int) []uint32 {
	heights := make([]uint32, n)
	for i := range heights {
		heights[i] = scriptTierTestDeepHeight
	}

	return heights
}

// baseTestGenesis returns the Genesis activation height carried by the base test
// settings, so era-dependent classification stays in sync with those settings.
func baseTestGenesis(t *testing.T) uint32 {
	t.Helper()

	return test.CreateBaseTestSettings(t).ChainCfgParams.GenesisActivationHeight
}

func TestCountScriptOps(t *testing.T) {
	tests := []struct {
		name     string
		script   []byte
		expected uint64
	}{
		{name: "empty script", script: nil, expected: 0},
		{name: "pushes are free", script: []byte{0x03, 0xaa, 0xbb, 0xcc, 0x4b, 0x00}, expected: 0}, // truncated trailing push stops
		{name: "small constants are free", script: []byte{0x00, 0x4f, 0x51, 0x60}, expected: 0},    // OP_0, OP_1NEGATE, OP_1, OP_16
		{name: "opcodes above OP_16 count", script: []byte{bscript.OpNOP, bscript.OpDUP, bscript.OpHASH160}, expected: 3},
		{name: "p2pkh locking script", script: mustHex(t, scriptTierTestP2PKHScript), expected: 4}, // DUP HASH160 EQUALVERIFY CHECKSIG
		{name: "pushdata1 payload not counted", script: append([]byte{bscript.OpPUSHDATA1, 3, bscript.OpNOP, bscript.OpNOP, bscript.OpNOP}, bscript.OpNOP), expected: 1},
		{name: "pushdata2 payload not counted", script: append([]byte{bscript.OpPUSHDATA2, 2, 0, bscript.OpNOP, bscript.OpNOP}, bscript.OpNOP), expected: 1},
		{name: "pushdata4 payload not counted", script: append(bigPush(4), bscript.OpNOP), expected: 1},
		{name: "high bytes inside push data are free", script: bigPush(1000), expected: 0},
		{name: "truncated pushdata stops the count", script: []byte{bscript.OpNOP, bscript.OpPUSHDATA2, 0xff}, expected: 1},
		{name: "many ops", script: repeatedOps(1234), expected: 1234},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.expected, countOps(tt.script))
		})
	}
}

// TestCountScriptOpsOpReturn pins the OP_RETURN divergence fix (PR review P0-3):
// svnode counts a top-level OP_RETURN and then stops (post-Genesis it ends
// execution), so the data tail an attacker appends must not inflate the count.
// A nested OP_RETURN inside an OP_IF does NOT end execution, so counting
// continues there.
func TestCountScriptOpsOpReturn(t *testing.T) {
	t.Run("top-level OP_RETURN stops the count after itself", func(t *testing.T) {
		// OP_DUP OP_HASH160 <20> OP_EQUALVERIFY OP_CHECKSIG OP_RETURN <tail>
		script := append([]byte{}, mustHex(t, scriptTierTestP2PKHScript)...)
		script = append(script, bscript.OpRETURN)
		script = append(script, repeatedOps(100_000)...) // attacker-chosen tail

		// 4 P2PKH ops + the OP_RETURN = 5; svnode counts exactly this.
		require.Equal(t, uint64(5), countOps(script))
	})

	t.Run("bare OP_RETURN counts as one", func(t *testing.T) {
		require.Equal(t, uint64(1), countOps(append([]byte{bscript.OpRETURN}, repeatedOps(1_000)...)))
	})

	t.Run("OP_RETURN nested in OP_IF does not stop the count", func(t *testing.T) {
		// OP_IF OP_RETURN OP_ENDIF OP_NOP: IF(1) RETURN(1, does not terminate)
		// ENDIF(1) NOP(1) = 4 counted ops.
		script := []byte{bscript.OpIF, bscript.OpRETURN, bscript.OpENDIF, bscript.OpNOP}
		require.Equal(t, uint64(4), countOps(script))
	})

	t.Run("count stops at the op cap", func(t *testing.T) {
		ops, capExceeded := countScriptOps(repeatedOps(1_000), 100, true)
		require.True(t, capExceeded)
		require.Equal(t, uint64(101), ops) // stopped one past the cap
	})

	t.Run("OP_CHECKMULTISIG carries its key count when that is certain", func(t *testing.T) {
		// svnode charges nKeysCount on top of the opcode, but only when the
		// opcode executes. The key count is added when it is statically certain
		// and omitted otherwise; the omission under-counts, which is the safe
		// direction. Each case here is pinned against real BDK in
		// TestCheckMultiSigKeyCountDifferentialBDK.

		// No preceding push: the key count is unknown, so just the opcode.
		require.Equal(t, uint64(1), countOps([]byte{bscript.OpCHECKMULTISIG}))

		// A small-constant key count immediately before it, at depth zero.
		require.Equal(t, uint64(4), countOps([]byte{bscript.Op3, bscript.OpCHECKMULTISIG}))
		require.Equal(t, uint64(4), countOps([]byte{bscript.Op3, bscript.OpCHECKMULTISIGVERIFY}))

		// A number push wider than a small constant (20 keys).
		require.Equal(t, uint64(21), countOps([]byte{0x01, 20, bscript.OpCHECKMULTISIG}))

		// A non-push in between makes the stack top unknown.
		require.Equal(t, uint64(2), countOps([]byte{bscript.Op3, bscript.OpNOP, bscript.OpCHECKMULTISIG}))

		// Inside a conditional the opcode may not execute, so no key count.
		require.Equal(t, uint64(3), countOps([]byte{bscript.OpONE, bscript.OpIF, bscript.Op3, bscript.OpCHECKMULTISIG, bscript.OpENDIF}))

		// After a nested OP_RETURN svnode stops executing while still counting,
		// so a later multisig adds no key count even at depth zero.
		require.Equal(t, uint64(4), countOps([]byte{
			bscript.OpONE, bscript.OpIF, bscript.OpRETURN, bscript.OpENDIF,
			bscript.Op3, bscript.OpCHECKMULTISIG,
		}))
	})
}

// TestCountScriptOpsVerifGrammar pins the era-dependent handling of OP_VERIF and
// OP_VERNOTIF in the walker itself. From the Chronicle upgrade on they are
// conditionals; before it an executed one fails the script and an unexecuted
// one opens nothing. Every shape here is measured against real BDK in
// TestVerifConditionalDifferentialBDK.
func TestCountScriptOpsVerifGrammar(t *testing.T) {
	t.Run("a multisig in an OP_VERIF branch that never runs adds no key count", func(t *testing.T) {
		// OP_0 OP_VERIF OP_16 OP_CHECKMULTISIG OP_ENDIF OP_1. Post-Chronicle the
		// condition (not a 4-byte version) is false, so the multisig never
		// executes: VERIF, CHECKMULTISIG, ENDIF, no keys.
		script := []byte{bscript.OpZERO, bscript.OpVERIF, bscript.Op16, bscript.OpCHECKMULTISIG, bscript.OpENDIF, bscript.OpONE}
		require.Equal(t, uint64(3), countOps(script))

		// The pre-Chronicle grammar cannot know the branch exists, but it stops
		// charging key counts after the opcode, so it does not over-count.
		require.Equal(t, uint64(3), countOpsPreChronicle(script))
	})

	t.Run("pre-Chronicle an unexecuted OP_VERIF opens nothing", func(t *testing.T) {
		// OP_1 OP_0 OP_IF OP_VERIF OP_ENDIF OP_RETURN <100 NOPs>: the OP_RETURN
		// is top level and the tail is never counted. The post-Chronicle grammar
		// would count it, which is why the coin's era selects the grammar.
		script := append([]byte{bscript.OpONE, bscript.OpZERO, bscript.OpIF, bscript.OpVERIF, bscript.OpENDIF, bscript.OpRETURN}, repeatedOps(100)...)
		require.Equal(t, uint64(4), countOpsPreChronicle(script))
		require.Equal(t, uint64(104), countOps(script))
	})

	t.Run("post-Chronicle an OP_RETURN inside an OP_VERIF branch is nested", func(t *testing.T) {
		// OP_0 OP_VERIF OP_RETURN OP_ENDIF <100 NOPs> OP_1: counting continues
		// through the tail. The pre-Chronicle grammar stops at 2, an under-count.
		script := append([]byte{bscript.OpZERO, bscript.OpVERIF, bscript.OpRETURN, bscript.OpENDIF}, repeatedOps(100)...)
		script = append(script, bscript.OpONE)
		require.Equal(t, uint64(103), countOps(script))
		require.Equal(t, uint64(2), countOpsPreChronicle(script))
	})

	t.Run("OP_VERNOTIF opens a branch and OP_ELSE flips it", func(t *testing.T) {
		// OP_0 OP_VERNOTIF OP_NOP OP_ELSE OP_16 OP_CHECKMULTISIG OP_ENDIF OP_1:
		// the VERNOTIF arm runs, the ELSE arm with the multisig does not.
		script := []byte{bscript.OpZERO, bscript.OpVERNOTIF, bscript.OpNOP, bscript.OpELSE, bscript.Op16, bscript.OpCHECKMULTISIG, bscript.OpENDIF, bscript.OpONE}
		require.Equal(t, uint64(5), countOps(script))
		require.Equal(t, uint64(5), countOpsPreChronicle(script))
	})

	t.Run("a multisig after the branch closes is exact only in the post-Chronicle grammar", func(t *testing.T) {
		// OP_0 OP_VERIF OP_ENDIF OP_3 OP_CHECKMULTISIG: the multisig executes at
		// depth zero, so its three keys are charged post-Chronicle, while the
		// pre-Chronicle grammar, having seen an OP_VERIF, charges none.
		script := []byte{bscript.OpZERO, bscript.OpVERIF, bscript.OpENDIF, bscript.Op3, bscript.OpCHECKMULTISIG}
		require.Equal(t, uint64(6), countOps(script))
		require.Equal(t, uint64(3), countOpsPreChronicle(script))
	})
}

func TestIsPostChronicleCoin(t *testing.T) {
	const chronicle, tip = uint32(1_000), uint32(5_000)

	require.False(t, isPostChronicleCoin(chronicle-1, tip, chronicle))
	require.True(t, isPostChronicleCoin(chronicle, tip, chronicle), "the activation height itself is post-Chronicle")
	require.True(t, isPostChronicleCoin(chronicle+1, tip, chronicle))
	require.False(t, isPostChronicleCoin(0, tip, chronicle), "an unrecorded height compares as BDK compares it")
	require.True(t, isPostChronicleCoin(unconfirmedParentHeight, tip, chronicle), "an unconfirmed parent takes the candidate height")
	require.False(t, isPostChronicleCoin(unconfirmedParentHeight, chronicle-1, chronicle))
}

func mustHex(t *testing.T, s string) []byte {
	t.Helper()

	b, err := hex.DecodeString(s)
	require.NoError(t, err)

	return b
}

func TestLastPush(t *testing.T) {
	t.Run("returns the final pushed data", func(t *testing.T) {
		script := []byte{0x01, 0xaa, 0x02, 0xbb, 0xcc}
		require.Equal(t, []byte{0xbb, 0xcc}, lastPush(script))
	})

	t.Run("nil for non-push opcodes", func(t *testing.T) {
		require.Nil(t, lastPush([]byte{0x01, 0xaa, bscript.OpNOP}))
	})

	t.Run("nil for truncated push", func(t *testing.T) {
		require.Nil(t, lastPush([]byte{0x05, 0xaa}))
	})

	t.Run("nil for empty script", func(t *testing.T) {
		require.Nil(t, lastPush(nil))
	})

	t.Run("small constants count as pushes", func(t *testing.T) {
		// A scriptSig such as OP_1 <redeem script> is a valid push-only P2SH
		// spend. Treating OP_1 as a non-push abandoned the walk and left the
		// redeem script unpriced entirely.
		require.Equal(t, []byte{0xbb, 0xcc}, lastPush([]byte{bscript.OpONE, 0x02, 0xbb, 0xcc}))

		// A trailing small constant is itself the final push.
		require.Equal(t, []byte{0x05}, lastPush([]byte{0x02, 0xbb, 0xcc, bscript.Op5}))
		require.Equal(t, []byte{0x81}, lastPush([]byte{bscript.Op1NEGATE}))
		require.Equal(t, []byte{0x10}, lastPush([]byte{bscript.Op16}))
	})
}

// TestIsStandardPrevoutScriptPanicSafety pins the PR-review P0-1 fix: the
// classifier must never panic on an attacker-supplied prevout script. The two
// named scripts crash go-bt's IsP2PK / IsMultiSigOut (which this used to call);
// the table then fuzzes a range of truncated and empty pushes.
func TestIsStandardPrevoutScriptPanicSafety(t *testing.T) {
	genesis := baseTestGenesis(t)

	scripts := [][]byte{
		{0x01, 0xAA, 0x4C, 0x00}, // panicked IsP2PK: DecodeParts yields an empty part
		{0x4C, 0x00, 0xAE, 0xAE}, // panicked IsMultiSigOut: leading empty part
		nil,
		{},
		{0x00},
		{0x4c},             // OP_PUSHDATA1 with no length byte
		{0x4d, 0x01},       // OP_PUSHDATA2 truncated
		{0x4e, 0x01, 0x00}, // OP_PUSHDATA4 truncated
		{0xae},             // bare OP_CHECKMULTISIG
		{0x51, 0xae},       // OP_1 OP_CHECKMULTISIG, no keys
		{0x6a},             // bare OP_RETURN
		{0x00, 0x6a},       // OP_FALSE OP_RETURN
	}

	for _, s := range scripts {
		script := bscript.Script(s)
		require.NotPanics(t, func() {
			// Exercise both eras; the result value is irrelevant, only that it
			// does not panic.
			_ = isStandardPrevoutScript(&script, 50, genesis)        // pre-Genesis coin
			_ = isStandardPrevoutScript(&script, 1_000_000, genesis) // post-Genesis coin
		}, "isStandardPrevoutScript panicked on %x", s)
	}
}

// TestIsStandardPrevoutScriptTemplates checks the standard-template classifier
// accepts the real templates and rejects junk, and that P2SH is era-gated.
func TestIsStandardPrevoutScriptTemplates(t *testing.T) {
	genesis := uint32(620_538)

	p2pkh := bscript.Script(mustHex(t, scriptTierTestP2PKHScript))
	require.True(t, isStandardPrevoutScript(&p2pkh, 1_000_000, genesis))

	p2sh := bscript.Script(mustHex(t, "a914000000000000000000000000000000000000000087"))
	require.True(t, isStandardPrevoutScript(&p2sh, 100, genesis), "pre-Genesis P2SH is standard")
	require.False(t, isStandardPrevoutScript(&p2sh, 700_000, genesis), "post-Genesis P2SH is not standard")

	// Compressed-pubkey P2PK.
	p2pk := make([]byte, 0, 35)
	p2pk = append(p2pk, bscript.OpDATA33, 0x02)
	p2pk = append(p2pk, make([]byte, 32)...)
	p2pk = append(p2pk, bscript.OpCHECKSIG)
	p2pkScript := bscript.Script(p2pk)
	require.True(t, isStandardPrevoutScript(&p2pkScript, 1_000_000, genesis))

	// Bare 1-of-1 multisig: OP_1 <33-byte key> OP_1 OP_CHECKMULTISIG.
	ms := make([]byte, 0, 37)
	ms = append(ms, bscript.OpONE, bscript.OpDATA33, 0x02)
	ms = append(ms, make([]byte, 32)...)
	ms = append(ms, bscript.OpONE, bscript.OpCHECKMULTISIG)
	msScript := bscript.Script(ms)
	require.True(t, isStandardPrevoutScript(&msScript, 1_000_000, genesis))

	// Data carrier.
	data := bscript.Script([]byte{bscript.OpFALSE, bscript.OpRETURN, 0x01, 0xaa})
	require.True(t, isStandardPrevoutScript(&data, 1_000_000, genesis))

	// Junk.
	junk := bscript.Script(repeatedOps(25))
	require.False(t, isStandardPrevoutScript(&junk, 1_000_000, genesis))
}

func TestTierExcessThousandths(t *testing.T) {
	oneTier := []settings.FeeTier{{Threshold: 1_000, SatoshisPerK: 10}}
	twoTiers := []settings.FeeTier{
		{Threshold: 1_000, SatoshisPerK: 10},
		{Threshold: 2_000, SatoshisPerK: 50},
	}

	tests := []struct {
		name     string
		tiers    []settings.FeeTier
		value    uint64
		expected uint64
	}{
		{name: "below the threshold is free", tiers: oneTier, value: 999, expected: 0},
		{name: "at the threshold is free", tiers: oneTier, value: 1_000, expected: 0},
		{name: "one unit over", tiers: oneTier, value: 1_001, expected: 10},
		{name: "five hundred units over", tiers: oneTier, value: 1_500, expected: 5_000},
		{name: "spanning two tiers", tiers: twoTiers, value: 2_500, expected: 10*1_000 + 50*500},
		{name: "inside the first band with a second tier configured", tiers: twoTiers, value: 1_500, expected: 5_000},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.expected, tierExcessThousandths(tt.tiers, tt.value))
		})
	}

	t.Run("saturates instead of wrapping", func(t *testing.T) {
		// A huge threshold-to-value span at the maximum rate would wrap uint64;
		// saturating keeps the required fee high (fail-closed), never low.
		tiers := []settings.FeeTier{{Threshold: 1, SatoshisPerK: math.MaxInt64}}
		require.Equal(t, uint64(math.MaxUint64), tierExcessThousandths(tiers, math.MaxUint64/2))
	})
}

func TestSatArithmetic(t *testing.T) {
	require.Equal(t, uint64(math.MaxUint64), satMulU64(math.MaxUint64, 2))
	require.Equal(t, uint64(6), satMulU64(2, 3))
	require.Equal(t, uint64(0), satMulU64(0, math.MaxUint64))
	require.Equal(t, uint64(math.MaxUint64), satAddU64(math.MaxUint64, 1))
	require.Equal(t, uint64(5), satAddU64(2, 3))
}

func TestCheckScriptTieredFees(t *testing.T) {
	// Thresholds are set low so tests stay small: script bytes beyond 1000
	// pay 1000 sat/kB, counted ops beyond 100 pay 1000 sat/kOps. The byte
	// floor is 0 so required fees come from the tiers alone.
	newTieredValidator := func(t *testing.T) *TxValidator {
		t.Helper()

		tSettings := test.CreateBaseTestSettings(t)
		tSettings.Policy.MinMiningTxFee = 0
		tSettings.Policy.MinMiningTxFeeByScriptSize = []settings.FeeTier{{Threshold: 1_000, SatoshisPerK: 1_000}}
		tSettings.Policy.MinMiningTxFeeByScriptOps = []settings.FeeTier{{Threshold: 100, SatoshisPerK: 1_000}}

		tv := NewTxValidator(ulogger.TestLogger{}, tSettings)
		tv.bdk = noopBDKValidator{}

		return tv
	}

	t.Run("small scripts pass with zero fee", func(t *testing.T) {
		tv := newTieredValidator(t)
		tx := newScriptTierTestTx(t, repeatedOps(50), 0) // 50 ops, 50 bytes: under both thresholds

		require.NoError(t, tv.ValidateTransaction(tx, scriptTierTestBlockHeight, deepHeights(1), NewDefaultOptions()))
	})

	t.Run("op-dense script pays the ops tier", func(t *testing.T) {
		tv := newTieredValidator(t)

		// 600 ops: 500 beyond the threshold at 1000 sat/kOps = 500 satoshis.
		underpaying := newScriptTierTestTx(t, repeatedOps(600), 499)
		err := tv.ValidateTransaction(underpaying, scriptTierTestBlockHeight, deepHeights(1), NewDefaultOptions())
		require.Error(t, err)
		require.True(t, errors.Is(err, errors.ErrTxPolicy), "expected a policy error, got %v", err)

		exact := newScriptTierTestTx(t, repeatedOps(600), 500)
		require.NoError(t, tv.ValidateTransaction(exact, scriptTierTestBlockHeight, deepHeights(1), NewDefaultOptions()))
	})

	t.Run("large data-only script pays the size tier, not the ops tier", func(t *testing.T) {
		tv := newTieredValidator(t)

		// A 2005-byte push-only script has zero counted ops; 1005 bytes
		// beyond the 1000-byte threshold at 1000 sat/kB = 1005 satoshis.
		underpaying := newScriptTierTestTx(t, bigPush(2_000), 1_004)
		err := tv.ValidateTransaction(underpaying, scriptTierTestBlockHeight, deepHeights(1), NewDefaultOptions())
		require.Error(t, err)
		require.True(t, errors.Is(err, errors.ErrTxPolicy), "expected a policy error, got %v", err)

		exact := newScriptTierTestTx(t, bigPush(2_000), 1_005)
		require.NoError(t, tv.ValidateTransaction(exact, scriptTierTestBlockHeight, deepHeights(1), NewDefaultOptions()))
	})

	t.Run("size and ops surcharges add up", func(t *testing.T) {
		tv := newTieredValidator(t)

		// 1600 OP_NOPs: 600 bytes beyond the size threshold (600 sat) and
		// 1500 ops beyond the ops threshold (1500 sat) = 2100 satoshis.
		underpaying := newScriptTierTestTx(t, repeatedOps(1_600), 2_099)
		err := tv.ValidateTransaction(underpaying, scriptTierTestBlockHeight, deepHeights(1), NewDefaultOptions())
		require.Error(t, err)

		exact := newScriptTierTestTx(t, repeatedOps(1_600), 2_100)
		require.NoError(t, tv.ValidateTransaction(exact, scriptTierTestBlockHeight, deepHeights(1), NewDefaultOptions()))
	})

	t.Run("the spent locking script counts as an executed script", func(t *testing.T) {
		tv := newTieredValidator(t)

		// The prevout locking script is 150 OP_NOPs: 50 ops beyond the
		// threshold = 50 satoshis, even with an empty unlocking script.
		tx := bt.NewTx()
		require.NoError(t, tx.From(scriptTierTestPrevTxID, 0, hex.EncodeToString(repeatedOps(150)), 0))
		require.NoError(t, tx.AddOpReturnOutput([]byte{0x01}))

		err := tv.ValidateTransaction(tx, scriptTierTestBlockHeight, deepHeights(1), NewDefaultOptions())
		require.Error(t, err)
		require.True(t, errors.Is(err, errors.ErrTxPolicy), "expected a policy error, got %v", err)
	})

	t.Run("a pre-Genesis P2SH redeem script counts as an executed script", func(t *testing.T) {
		tv := newTieredValidator(t)

		// P2SH prevout; the unlocking script pushes a 150-op redeem script
		// (via OP_PUSHDATA2, so the unlocking script itself has 0 counted
		// ops and is only 153 bytes: under both thresholds on its own).
		redeem := repeatedOps(150)
		unlocking := append([]byte{bscript.OpPUSHDATA2, 150, 0}, redeem...)

		p2shPrevout := "a914000000000000000000000000000000000000000087"

		tx := bt.NewTx()
		require.NoError(t, tx.From(scriptTierTestPrevTxID, 0, p2shPrevout, 0))

		unlockingScript := bscript.Script(unlocking)
		tx.Inputs[0].UnlockingScript = &unlockingScript

		require.NoError(t, tx.AddOpReturnOutput([]byte{0x01}))

		// Coin created before Genesis (height 50 < test genesis 100): the redeem
		// is executed and billed.
		err := tv.ValidateTransaction(tx, scriptTierTestBlockHeight, []uint32{50}, NewDefaultOptions())
		require.Error(t, err)
		require.True(t, errors.Is(err, errors.ErrTxPolicy), "expected a policy error, got %v", err)
	})

	t.Run("a P2SH redeem behind a small constant is still priced", func(t *testing.T) {
		// OP_1 <redeem> is a valid push-only P2SH scriptSig. Abandoning the
		// walk at the OP_1 left the redeem script, and any amount of work in
		// it, entirely unpriced.
		tv := newTieredValidator(t)

		redeem := repeatedOps(150)
		unlocking := append([]byte{bscript.OpONE, bscript.OpPUSHDATA2, 150, 0}, redeem...)

		tx := bt.NewTx()
		require.NoError(t, tx.From(scriptTierTestPrevTxID, 0, "a914000000000000000000000000000000000000000087", 0))

		unlockingScript := bscript.Script(unlocking)
		tx.Inputs[0].UnlockingScript = &unlockingScript

		require.NoError(t, tx.AddOpReturnOutput([]byte{0x01}))

		err := tv.ValidateTransaction(tx, scriptTierTestBlockHeight, []uint32{50}, NewDefaultOptions())
		require.Error(t, err, "the redeem script must be priced through a leading small constant")
		require.True(t, errors.Is(err, errors.ErrTxPolicy), "expected a policy error, got %v", err)
	})

	t.Run("a post-Genesis P2SH redeem script is not double-counted", func(t *testing.T) {
		tv := newTieredValidator(t)

		// Same shape as above, but the coin was created post-Genesis. No BSV
		// node runs the P2SH redeem there, so it must not be billed: the
		// unlocking (153 bytes) and prevout (23 bytes) are both under the
		// thresholds, so the surcharge is zero and the tx passes (PR review P1-8).
		redeem := repeatedOps(150)
		unlocking := append([]byte{bscript.OpPUSHDATA2, 150, 0}, redeem...)

		tx := bt.NewTx()
		require.NoError(t, tx.From(scriptTierTestPrevTxID, 0, "a914000000000000000000000000000000000000000087", 0))

		unlockingScript := bscript.Script(unlocking)
		tx.Inputs[0].UnlockingScript = &unlockingScript

		require.NoError(t, tx.AddOpReturnOutput([]byte{0x01}))

		require.NoError(t, tv.ValidateTransaction(tx, scriptTierTestBlockHeight, deepHeights(1), NewDefaultOptions()))
	})

	t.Run("skip policy checks bypasses the tiers", func(t *testing.T) {
		tv := newTieredValidator(t)
		tx := newScriptTierTestTx(t, repeatedOps(600), 0)

		require.NoError(t, tv.ValidateTransaction(tx, scriptTierTestBlockHeight, deepHeights(1), ProcessOptions(WithSkipPolicyChecks(true))))
	})

	t.Run("empty schedules are a no-op", func(t *testing.T) {
		tSettings := test.CreateBaseTestSettings(t)
		tSettings.Policy.MinMiningTxFee = 0

		tv := NewTxValidator(ulogger.TestLogger{}, tSettings)
		tv.bdk = noopBDKValidator{}

		tx := newScriptTierTestTx(t, repeatedOps(5_000), 0)

		require.NoError(t, tv.ValidateTransaction(tx, scriptTierTestBlockHeight, deepHeights(1), NewDefaultOptions()))
	})

	t.Run("the minminingtxfee floor joins the required fee", func(t *testing.T) {
		tSettings := test.CreateBaseTestSettings(t)
		tSettings.Policy.MinMiningTxFee = 0.00001 // 1000 sat/kB
		tSettings.Policy.MinMiningTxFeeByScriptOps = []settings.FeeTier{{Threshold: 100, SatoshisPerK: 1_000}}

		tv := NewTxValidator(ulogger.TestLogger{}, tSettings)
		tv.bdk = noopBDKValidator{}

		tx := newScriptTierTestTx(t, repeatedOps(600), 0)
		size := uint64(tx.Size()) //nolint:gosec // test size
		required := (size*1_000 + 500*1_000) / 1_000

		underpaying := newScriptTierTestTx(t, repeatedOps(600), required-1)
		err := tv.ValidateTransaction(underpaying, scriptTierTestBlockHeight, deepHeights(1), NewDefaultOptions())
		require.Error(t, err)

		exact := newScriptTierTestTx(t, repeatedOps(600), required)
		require.NoError(t, tv.ValidateTransaction(exact, scriptTierTestBlockHeight, deepHeights(1), NewDefaultOptions()))
	})

	t.Run("tier rejection is classified ErrTxInvalid for the rejected-tx topic", func(t *testing.T) {
		// PR review P1-11: the rejected-tx Kafka publish gate matches
		// errors.Is(err, ErrTxInvalid); a bare policy error would be dropped.
		tv := newTieredValidator(t)
		tx := newScriptTierTestTx(t, repeatedOps(600), 0)

		err := tv.ValidateTransaction(tx, scriptTierTestBlockHeight, deepHeights(1), NewDefaultOptions())
		require.Error(t, err)
		require.True(t, errors.Is(err, errors.ErrTxInvalid), "must be ErrTxInvalid so it reaches the rejected-tx topic")
		require.True(t, errors.Is(err, errors.ErrTxPolicy), "and still classified as a policy error")
	})

	t.Run("a short script carrying a multisig key count is still priced", func(t *testing.T) {
		// The ops walk is skipped when a script is shorter than the first ops
		// threshold, because counted ops cannot exceed the script length. A
		// multisig breaks that bound: this three-byte locking script counts 18
		// ops, one for the opcode plus a key count of 17. Skipping it would hand
		// out the key count for free, which is the whole point of the ops tier.
		tSettings := test.CreateBaseTestSettings(t)
		tSettings.Policy.MinMiningTxFee = 0
		tSettings.Policy.MinMiningTxFeeByScriptOps = []settings.FeeTier{{Threshold: 10, SatoshisPerK: 1_000_000}}

		tv := NewTxValidator(ulogger.TestLogger{}, tSettings)
		tv.bdk = noopBDKValidator{}

		prevout := []byte{0x01, 17, bscript.OpCHECKMULTISIG} // <push 17> OP_CHECKMULTISIG
		require.Len(t, prevout, 3)
		require.Equal(t, uint64(18), countOps(prevout))

		tx := bt.NewTx()
		require.NoError(t, tx.From(scriptTierTestPrevTxID, 0, hex.EncodeToString(prevout), 0))
		require.NoError(t, tx.AddOpReturnOutput([]byte{0x01}))

		err := tv.ValidateTransaction(tx, scriptTierTestBlockHeight, deepHeights(1), NewDefaultOptions())
		require.Error(t, err, "a multisig key count must be priced even below the length threshold")
		require.True(t, errors.Is(err, errors.ErrTxPolicy), "expected a policy error, got %v", err)
	})

	t.Run("the coin's era selects the conditional grammar", func(t *testing.T) {
		// OP_0 OP_VERIF OP_RETURN OP_ENDIF <300 NOPs> OP_1 as the prevout.
		// Post-Chronicle the OP_RETURN is nested, so the 300 opcodes after it
		// count: 303 ops, 203 beyond the threshold, 203 satoshis. With the
		// pre-Chronicle grammar the OP_RETURN reads as top level and the tail
		// is free. The regtest test parameters activate Chronicle at 200.
		tv := newTieredValidator(t)
		chronicle := tv.settings.ChainCfgParams.ChronicleActivationHeight

		prevout := append([]byte{bscript.OpZERO, bscript.OpVERIF, bscript.OpRETURN, bscript.OpENDIF}, repeatedOps(300)...)
		prevout = append(prevout, bscript.OpONE)

		build := func(t *testing.T, fee uint64) *bt.Tx {
			t.Helper()

			tx := bt.NewTx()
			require.NoError(t, tx.From(scriptTierTestPrevTxID, 0, hex.EncodeToString(prevout), fee))
			require.NoError(t, tx.AddOpReturnOutput([]byte{0x01}))

			return tx
		}

		err := tv.ValidateTransaction(build(t, 202), scriptTierTestBlockHeight, []uint32{chronicle}, NewDefaultOptions())
		require.Error(t, err, "a post-Chronicle coin must be walked with the post-Chronicle grammar")
		require.True(t, errors.Is(err, errors.ErrTxPolicy), "expected a policy error, got %v", err)

		require.NoError(t, tv.ValidateTransaction(build(t, 203), scriptTierTestBlockHeight, []uint32{chronicle}, NewDefaultOptions()))

		// One block earlier the coin is pre-Chronicle: the walk stops at the
		// OP_RETURN and there is no surcharge. (BDK then rejects the executed
		// OP_VERIF itself, which is not this check's verdict to give.)
		require.NoError(t, tv.ValidateTransaction(build(t, 0), scriptTierTestBlockHeight, []uint32{chronicle - 1}, NewDefaultOptions()))
	})

	t.Run("a malformed oversized push neither panics nor hangs", func(t *testing.T) {
		// An OP_PUSHDATA4 length assembled in a signed int overflows negative on
		// a 32-bit build, which would slice backwards and walk the index
		// backwards. Lengths are decoded as uint64 and bounded first.
		require.NotPanics(t, func() {
			ops, capExceeded := countScriptOps([]byte{bscript.OpNOP, bscript.OpPUSHDATA4, 0xff, 0xff, 0xff, 0xff, 0xaa}, math.MaxUint64, true)
			require.Equal(t, uint64(1), ops, "the OP_NOP counts, then the malformed push stops the walk")
			require.False(t, capExceeded)

			require.Nil(t, lastPush([]byte{bscript.OpPUSHDATA4, 0xff, 0xff, 0xff, 0xff, 0xaa}))
			require.Nil(t, lastPush([]byte{bscript.OpPUSHDATA2, 0xff, 0xff, 0xaa}))
		})
	})

	t.Run("a script over the op cap is left to BDK, not rejected for fee", func(t *testing.T) {
		// PR review P1-9: a script beyond maxopsperscriptpolicy must not be
		// priced (BDK rejects it with SCRIPT_ERR_OP_COUNT); pricing it would
		// mislabel the rejection as insufficient-fee.
		tSettings := test.CreateBaseTestSettings(t)
		tSettings.Policy.MinMiningTxFee = 0
		tSettings.Policy.MaxOpsPerScriptPolicy = 500
		tSettings.Policy.MinMiningTxFeeByScriptOps = []settings.FeeTier{{Threshold: 100, SatoshisPerK: 1_000}}

		tv := NewTxValidator(ulogger.TestLogger{}, tSettings)
		tv.bdk = noopBDKValidator{}

		// 1000 ops, over the 500 cap: no surcharge, so the zero-fee tx passes
		// the tier check (BDK, here a no-op, would otherwise reject it).
		tx := newScriptTierTestTx(t, repeatedOps(1_000), 0)
		require.NoError(t, tv.ValidateTransaction(tx, scriptTierTestBlockHeight, deepHeights(1), NewDefaultOptions()))
	})
}

// TestScriptTieredFeeSurchargeAlwaysDue pins the PR-review P1-4 fix: the
// free-consolidation exemption waives the byte-rate floor only, never the
// per-script surcharge, so the "create a large-script output cheaply then
// consolidate it for free" evasion no longer works.
func TestScriptTieredFeeSurchargeAlwaysDue(t *testing.T) {
	newValidator := func(t *testing.T, floorBSVPerKB float64) *TxValidator {
		t.Helper()

		tSettings := test.CreateBaseTestSettings(t)
		tSettings.Policy.MinMiningTxFee = floorBSVPerKB
		tSettings.Policy.MinMiningTxFeeByScriptSize = []settings.FeeTier{{Threshold: 10, SatoshisPerK: 1_000}}

		tv := NewTxValidator(ulogger.TestLogger{}, tSettings)
		tv.bdk = noopBDKValidator{}

		return tv
	}

	t.Run("dust-return donation cannot buy off the surcharge", func(t *testing.T) {
		tv := newValidator(t, 0)

		// The evasion: spend a coin whose locking script is OP_RETURN + 5000
		// bytes (standard as a data carrier, so it passes the standard-input
		// rule) with a single dust-return donation output, which relaxes the
		// consolidation factor and confirmations. Old behaviour: fully exempt.
		// New behaviour: the ~5000-byte size surcharge is still owed.
		prevout := append([]byte{bscript.OpRETURN}, make([]byte, 5_000)...)

		tx := bt.NewTx()
		require.NoError(t, tx.From(scriptTierTestPrevTxID, 0, hex.EncodeToString(prevout), 1_000))

		donation := bscript.Script(dustReturnScript)
		tx.AddOutput(&bt.Output{Satoshis: 0, LockingScript: &donation})

		require.True(t, isDustReturnTxn(tx), "test fixture must be a dust-return donation")

		err := tv.ValidateTransaction(tx, scriptTierTestBlockHeight, deepHeights(1), NewDefaultOptions())
		require.Error(t, err, "the surcharge is due despite the donation exemption")
		require.True(t, errors.Is(err, errors.ErrTxPolicy))
	})

	t.Run("the exemption waives the floor but not the surcharge", func(t *testing.T) {
		// A genuine 20-in consolidation whose 107-byte scripts exceed the tiny
		// 10-byte threshold: the surcharge is owed, but the floor is waived.
		// Paying exactly the surcharge passes; one satoshi short fails. Surcharge
		// (size tier {10:1000}): 20 * ((107-10) + (25-10)) = 20 * 112 = 2240 sat
		// (20 unlocking scripts of 107 bytes, 20 prevouts of 25; outputs are not
		// priced). With a 0.001 BSV/kB floor the tx cannot also pay the floor.
		tv := newValidator(t, 0.001)

		const surcharge = uint64(2_240) // fee param IS the fee (see newConsolidationTestTx)

		exact := newConsolidationTestTx(t, 20, 1, 107, surcharge)
		require.NoError(t, tv.ValidateTransaction(exact, scriptTierTestBlockHeight, deepHeights(20), NewDefaultOptions()),
			"a free consolidation paying the surcharge passes even though it cannot pay the floor")

		short := newConsolidationTestTx(t, 20, 1, 107, surcharge-1)
		require.Error(t, tv.ValidateTransaction(short, scriptTierTestBlockHeight, deepHeights(20), NewDefaultOptions()),
			"one satoshi short of the surcharge still fails")
	})

	t.Run("a genuine small-script consolidation stays free", func(t *testing.T) {
		// Scripts under the threshold produce no surcharge, so the tx returns
		// before the floor is even considered: the honest UTXO sweep is
		// unaffected by the tiers, even with a floor it does not pay.
		tSettings := test.CreateBaseTestSettings(t)
		tSettings.Policy.MinMiningTxFee = 0.001
		tSettings.Policy.MinMiningTxFeeByScriptSize = []settings.FeeTier{{Threshold: 1_000, SatoshisPerK: 1_000}}
		tv := NewTxValidator(ulogger.TestLogger{}, tSettings)
		tv.bdk = noopBDKValidator{}

		tx := newConsolidationTestTx(t, 20, 1, 107, 0) // zero fee, all scripts under 1000 bytes
		require.NoError(t, tv.ValidateTransaction(tx, scriptTierTestBlockHeight, deepHeights(20), NewDefaultOptions()))
	})
}

func TestIsFreeConsolidationTxn(t *testing.T) {
	basePolicy := func(t *testing.T) *settings.PolicySettings {
		t.Helper()

		// CreateBaseTestSettings carries the svnode defaults: factor 20,
		// 6 confirmations, 150-byte input scripts, standard inputs only.
		return test.CreateBaseTestSettings(t).Policy
	}

	genesis := baseTestGenesis(t)

	t.Run("qualifying consolidation", func(t *testing.T) {
		tx := newConsolidationTestTx(t, 20, 1, 107, 0)
		require.True(t, isFreeConsolidationTxn(basePolicy(t), tx, scriptTierTestBlockHeight, deepHeights(20), genesis))
	})

	t.Run("too few inputs for the factor", func(t *testing.T) {
		tx := newConsolidationTestTx(t, 19, 1, 107, 0)
		require.False(t, isFreeConsolidationTxn(basePolicy(t), tx, scriptTierTestBlockHeight, deepHeights(19), genesis))
	})

	t.Run("factor zero disables free consolidations", func(t *testing.T) {
		po := basePolicy(t)
		po.MinConsolidationFactor = 0

		tx := newConsolidationTestTx(t, 20, 1, 107, 0)
		require.False(t, isFreeConsolidationTxn(po, tx, scriptTierTestBlockHeight, deepHeights(20), genesis))
	})

	t.Run("unconfirmed input disqualifies", func(t *testing.T) {
		tx := newConsolidationTestTx(t, 20, 1, 107, 0)

		heights := deepHeights(20)
		heights[7] = unconfirmedParentHeight

		require.False(t, isFreeConsolidationTxn(basePolicy(t), tx, scriptTierTestBlockHeight, heights, genesis))
	})

	t.Run("shallowly confirmed input disqualifies", func(t *testing.T) {
		tx := newConsolidationTestTx(t, 20, 1, 107, 0)

		heights := deepHeights(20)
		heights[7] = scriptTierTestBlockHeight - 3 // 3 confirmations, need 6

		require.False(t, isFreeConsolidationTxn(basePolicy(t), tx, scriptTierTestBlockHeight, heights, genesis))
	})

	t.Run("height zero skips the confirmation rule", func(t *testing.T) {
		// svnode skips the check for coins whose height was never recorded.
		tx := newConsolidationTestTx(t, 20, 1, 107, 0)

		heights := deepHeights(20)
		heights[7] = 0

		require.True(t, isFreeConsolidationTxn(basePolicy(t), tx, scriptTierTestBlockHeight, heights, genesis))
	})

	t.Run("oversized unlocking script disqualifies", func(t *testing.T) {
		tx := newConsolidationTestTx(t, 20, 1, 151, 0)
		require.False(t, isFreeConsolidationTxn(basePolicy(t), tx, scriptTierTestBlockHeight, deepHeights(20), genesis))
	})

	t.Run("zero max-input-script-size normalises to 150", func(t *testing.T) {
		// PR review P1-7: BDK rewrites 0 to 150, so a 107-byte script must still
		// qualify (a raw 0 limit would reject it).
		po := basePolicy(t)
		po.MaxConsolidationInputScriptSize = 0

		tx := newConsolidationTestTx(t, 20, 1, 107, 0)
		require.True(t, isFreeConsolidationTxn(po, tx, scriptTierTestBlockHeight, deepHeights(20), genesis))
	})

	t.Run("non-standard prevout script disqualifies unless accepted", func(t *testing.T) {
		po := basePolicy(t)

		tx := newConsolidationTestTx(t, 20, 1, 107, 0)

		// Same 25-byte length as the P2PKH prevouts so only standardness
		// changes, not the locking-script shrinkage ratio.
		nonStandard := bscript.Script(repeatedOps(25))
		tx.Inputs[3].PreviousTxScript = &nonStandard

		require.False(t, isFreeConsolidationTxn(po, tx, scriptTierTestBlockHeight, deepHeights(20), genesis))

		po.AcceptNonStdConsolidationInput = true
		require.True(t, isFreeConsolidationTxn(po, tx, scriptTierTestBlockHeight, deepHeights(20), genesis))
	})

	t.Run("insufficient locking-script shrinkage disqualifies", func(t *testing.T) {
		tx := newConsolidationTestTx(t, 20, 1, 107, 0)

		// Grow the single output script beyond sumIn/factor: 20 inputs of 25
		// bytes give 500; a 26-byte output script needs 520.
		bigOutput := bscript.Script(make([]byte, 26))
		tx.Outputs[0].LockingScript = &bigOutput

		require.False(t, isFreeConsolidationTxn(basePolicy(t), tx, scriptTierTestBlockHeight, deepHeights(20), genesis))
	})

	t.Run("dust-return donation relaxes factor and confirmations", func(t *testing.T) {
		tx := bt.NewTx()
		for i := 0; i < 3; i++ {
			require.NoError(t, tx.From(scriptTierTestPrevTxID, uint32(i), scriptTierTestP2PKHScript, 1000)) //nolint:gosec // test loop index
		}

		donation := bscript.Script(dustReturnScript)
		tx.AddOutput(&bt.Output{Satoshis: 0, LockingScript: &donation})

		heights := []uint32{unconfirmedParentHeight, unconfirmedParentHeight, unconfirmedParentHeight}
		require.True(t, isFreeConsolidationTxn(basePolicy(t), tx, scriptTierTestBlockHeight, heights, genesis))
	})

	t.Run("single-output zero-value tx without the dust script is not a donation", func(t *testing.T) {
		tx := bt.NewTx()
		for i := 0; i < 3; i++ {
			require.NoError(t, tx.From(scriptTierTestPrevTxID, uint32(i), scriptTierTestP2PKHScript, 1000)) //nolint:gosec // test loop index
		}

		notDust := bscript.Script([]byte{0x00, 0x6a, 0x04, 'd', 'u', 's', 'x'})
		tx.AddOutput(&bt.Output{Satoshis: 0, LockingScript: &notDust})

		// Falls through to the normal path: 3 inputs < factor 20.
		require.False(t, isFreeConsolidationTxn(basePolicy(t), tx, scriptTierTestBlockHeight, deepHeights(3), genesis))
	})

	t.Run("mismatched utxoHeights length disqualifies", func(t *testing.T) {
		tx := newConsolidationTestTx(t, 20, 1, 107, 0)
		require.False(t, isFreeConsolidationTxn(basePolicy(t), tx, scriptTierTestBlockHeight, deepHeights(19), genesis))
	})
}

// TestFreeConsolidationDifferentialBDK pins isFreeConsolidationTxn against
// BDK's own free-consolidation exemption: with the fee floor set punishingly
// high, a zero-fee transaction passes BDK policy validation if and only if BDK
// classifies it as a free consolidation. The Go predicate must agree on both
// sides, or enabling the per-script fee tiers would either reject
// consolidations BDK mines for free or exempt transactions BDK does not.
func TestFreeConsolidationDifferentialBDK(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)
	po := tSettings.Policy
	po.MinMiningTxFee = 0.01 // 1,000,000 sat/kB: any non-exempt zero-fee tx must fail

	genesisHeight := tSettings.ChainCfgParams.GenesisActivationHeight

	verifier := newScriptVerifierGoBDK(ulogger.TestLogger{}, po, tSettings.ChainCfgParams)

	blockHeight := genesisHeight + 10_000
	prevTxID := "aa00000000000000000000000000000000000000000000000000000000000001"

	priv, err := ec.NewPrivateKey()
	require.NoError(t, err)

	addr, err := sdkscript.NewAddressFromPublicKey(priv.PubKey(), true)
	require.NoError(t, err)

	lock, err := p2pkh.Lock(addr)
	require.NoError(t, err)
	lockHex := hex.EncodeToString(*lock)

	unlockTemplate, err := p2pkh.Unlock(priv, nil)
	require.NoError(t, err)

	// buildSignedTx signs numInputs P2PKH spends of 1000 satoshis each with
	// go-sdk, then rebuilds the identical transaction as a bt.Tx (the txid
	// equality pins the conversion).
	buildSignedTx := func(t *testing.T, numInputs, numOutputs int) *bt.Tx {
		t.Helper()

		const perInput = uint64(1000)

		stx := sdktx.NewTransaction()
		for i := 0; i < numInputs; i++ {
			require.NoError(t, stx.AddInputFrom(prevTxID, uint32(i), lockHex, perInput, unlockTemplate)) //nolint:gosec // test loop index
		}

		total := perInput * uint64(numInputs) //nolint:gosec // test values
		for j := 0; j < numOutputs; j++ {
			stx.AddOutput(&sdktx.TransactionOutput{LockingScript: lock, Satoshis: total / uint64(numOutputs)}) //nolint:gosec // test values
		}

		require.NoError(t, stx.Sign())

		btTx := bt.NewTx()
		for i := 0; i < numInputs; i++ {
			require.NoError(t, btTx.From(prevTxID, uint32(i), lockHex, perInput)) //nolint:gosec // test loop index
			btTx.Inputs[i].UnlockingScript = bscript.NewFromBytes(*stx.Inputs[i].UnlockingScript)
		}

		outScript, err := bscript.NewFromHexString(lockHex)
		require.NoError(t, err)

		for j := 0; j < numOutputs; j++ {
			btTx.AddOutput(&bt.Output{Satoshis: total / uint64(numOutputs), LockingScript: outScript}) //nolint:gosec // test values
		}

		require.Equal(t, stx.TxID().String(), btTx.TxID(), "go-sdk and go-bt disagree on the assembled transaction")

		return btTx
	}

	heights := make([]uint32, 20)
	for i := range heights {
		heights[i] = blockHeight - 1_000
	}

	t.Run("BDK exempts the free consolidation and the Go predicate agrees", func(t *testing.T) {
		tx := buildSignedTx(t, 20, 1)

		require.NoError(t, verifier.ValidateTransaction(tx, blockHeight, false, heights),
			"BDK must accept a zero-fee free consolidation despite the fee floor")
		require.True(t, isFreeConsolidationTxn(po, tx, blockHeight, heights, genesisHeight))
	})

	t.Run("BDK charges the split transaction and the Go predicate agrees", func(t *testing.T) {
		tx := buildSignedTx(t, 20, 2)

		err := verifier.ValidateTransaction(tx, blockHeight, false, heights)
		require.Error(t, err, "a zero-fee non-consolidation must fail the raised fee floor")
		require.True(t, errors.Is(err, errors.ErrTxPolicy), "expected a policy error, got %v", err)
		require.False(t, isFreeConsolidationTxn(po, tx, blockHeight, heights, genesisHeight))
	})
}

// benchTieredValidator builds a validator with both tier schedules enabled and
// realistic caps, for the per-transaction-path benchmarks the PR review asks for.
func benchTieredValidator(b *testing.B, enabled bool) *TxValidator {
	b.Helper()

	tSettings := test.CreateBaseTestSettings(b)
	tSettings.Policy.MinMiningTxFee = 0
	if enabled {
		tSettings.Policy.MinMiningTxFeeByScriptSize = []settings.FeeTier{{Threshold: 500_000, SatoshisPerK: 10}}
		tSettings.Policy.MinMiningTxFeeByScriptOps = []settings.FeeTier{{Threshold: 1_000_000, SatoshisPerK: 10}}
	}

	tv := NewTxValidator(ulogger.TestLogger{}, tSettings)
	tv.bdk = noopBDKValidator{}

	return tv
}

// benchConsolidationTx builds a numInputs-in, 1-out P2PKH consolidation without
// a testing.T (usable from benchmarks).
func benchConsolidationTx(b *testing.B, numInputs int) *bt.Tx {
	b.Helper()

	tx := bt.NewTx()
	for i := 0; i < numInputs; i++ {
		require.NoError(b, tx.From(scriptTierTestPrevTxID, uint32(i), scriptTierTestP2PKHScript, 1000)) //nolint:gosec // bench index
		unlocking := bscript.Script(make([]byte, 107))
		tx.Inputs[i].UnlockingScript = &unlocking
	}

	outScript, err := bscript.NewFromHexString(scriptTierTestP2PKHScript)
	require.NoError(b, err)
	tx.AddOutput(&bt.Output{Satoshis: 500, LockingScript: outScript})

	return tx
}

func BenchmarkCheckScriptTieredFees_Disabled(b *testing.B) {
	tv := benchTieredValidator(b, false)
	tx := benchConsolidationTx(b, 20)
	heights := deepHeights(20)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_ = tv.checkScriptTieredFees(tx, scriptTierTestBlockHeight, heights)
	}
}

func BenchmarkCheckScriptTieredFees_Enabled(b *testing.B) {
	for _, n := range []int{2, 20, 100} {
		b.Run(sizeName(n), func(b *testing.B) {
			tv := benchTieredValidator(b, true)
			tx := benchConsolidationTx(b, n)
			heights := deepHeights(n)

			b.ReportAllocs()
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				_ = tv.checkScriptTieredFees(tx, scriptTierTestBlockHeight, heights)
			}
		})
	}
}

func BenchmarkCountScriptOps(b *testing.B) {
	cases := []struct {
		name   string
		script []byte
	}{
		{"p2pkh", mustHexB(b, scriptTierTestP2PKHScript)},
		{"ops_1k", repeatedOps(1_000)},
		{"push_1k", bigPush(1_000)},
		{"adversarial_10mb_ops", repeatedOps(10 << 20)},
	}

	for _, c := range cases {
		b.Run(c.name, func(b *testing.B) {
			b.SetBytes(int64(len(c.script)))
			b.ReportAllocs()
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				_, _ = countScriptOps(c.script, math.MaxUint64, true)
			}
		})
	}
}

func sizeName(n int) string {
	switch n {
	case 2:
		return "2in"
	case 20:
		return "20in"
	case 100:
		return "100in"
	default:
		return "nin"
	}
}

func mustHexB(b *testing.B, s string) []byte {
	b.Helper()

	out, err := hex.DecodeString(s)
	require.NoError(b, err)

	return out
}
