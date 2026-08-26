package validator

import (
	"encoding/hex"
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
			require.Equal(t, tt.expected, countScriptOps(tt.script))
		})
	}
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

	t.Run("a legacy P2SH redeem script counts as an executed script", func(t *testing.T) {
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

		err := tv.ValidateTransaction(tx, scriptTierTestBlockHeight, deepHeights(1), NewDefaultOptions())
		require.Error(t, err)
		require.True(t, errors.Is(err, errors.ErrTxPolicy), "expected a policy error, got %v", err)
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

	t.Run("free consolidation is exempt from the tiers", func(t *testing.T) {
		tSettings := test.CreateBaseTestSettings(t)
		tSettings.Policy.MinMiningTxFee = 0
		// Every 107-byte unlocking script is over a 10-byte size threshold.
		tSettings.Policy.MinMiningTxFeeByScriptSize = []settings.FeeTier{{Threshold: 10, SatoshisPerK: 1_000}}

		tv := NewTxValidator(ulogger.TestLogger{}, tSettings)
		tv.bdk = noopBDKValidator{}

		tx := newConsolidationTestTx(t, 20, 1, 107, 0)

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

	t.Run("qualifying consolidation", func(t *testing.T) {
		tx := newConsolidationTestTx(t, 20, 1, 107, 0)
		require.True(t, isFreeConsolidationTxn(basePolicy(t), tx, scriptTierTestBlockHeight, deepHeights(20)))
	})

	t.Run("too few inputs for the factor", func(t *testing.T) {
		tx := newConsolidationTestTx(t, 19, 1, 107, 0)
		require.False(t, isFreeConsolidationTxn(basePolicy(t), tx, scriptTierTestBlockHeight, deepHeights(19)))
	})

	t.Run("factor zero disables free consolidations", func(t *testing.T) {
		po := basePolicy(t)
		po.MinConsolidationFactor = 0

		tx := newConsolidationTestTx(t, 20, 1, 107, 0)
		require.False(t, isFreeConsolidationTxn(po, tx, scriptTierTestBlockHeight, deepHeights(20)))
	})

	t.Run("unconfirmed input disqualifies", func(t *testing.T) {
		tx := newConsolidationTestTx(t, 20, 1, 107, 0)

		heights := deepHeights(20)
		heights[7] = unconfirmedParentHeight

		require.False(t, isFreeConsolidationTxn(basePolicy(t), tx, scriptTierTestBlockHeight, heights))
	})

	t.Run("shallowly confirmed input disqualifies", func(t *testing.T) {
		tx := newConsolidationTestTx(t, 20, 1, 107, 0)

		heights := deepHeights(20)
		heights[7] = scriptTierTestBlockHeight - 3 // 3 confirmations, need 6

		require.False(t, isFreeConsolidationTxn(basePolicy(t), tx, scriptTierTestBlockHeight, heights))
	})

	t.Run("height zero skips the confirmation rule", func(t *testing.T) {
		// svnode skips the check for coins whose height was never recorded.
		tx := newConsolidationTestTx(t, 20, 1, 107, 0)

		heights := deepHeights(20)
		heights[7] = 0

		require.True(t, isFreeConsolidationTxn(basePolicy(t), tx, scriptTierTestBlockHeight, heights))
	})

	t.Run("oversized unlocking script disqualifies", func(t *testing.T) {
		tx := newConsolidationTestTx(t, 20, 1, 151, 0)
		require.False(t, isFreeConsolidationTxn(basePolicy(t), tx, scriptTierTestBlockHeight, deepHeights(20)))
	})

	t.Run("non-standard prevout script disqualifies unless accepted", func(t *testing.T) {
		po := basePolicy(t)

		tx := newConsolidationTestTx(t, 20, 1, 107, 0)

		// Same 25-byte length as the P2PKH prevouts so only standardness
		// changes, not the locking-script shrinkage ratio.
		nonStandard := bscript.Script(repeatedOps(25))
		tx.Inputs[3].PreviousTxScript = &nonStandard

		require.False(t, isFreeConsolidationTxn(po, tx, scriptTierTestBlockHeight, deepHeights(20)))

		po.AcceptNonStdConsolidationInput = true
		require.True(t, isFreeConsolidationTxn(po, tx, scriptTierTestBlockHeight, deepHeights(20)))
	})

	t.Run("insufficient locking-script shrinkage disqualifies", func(t *testing.T) {
		tx := newConsolidationTestTx(t, 20, 1, 107, 0)

		// Grow the single output script beyond sumIn/factor: 20 inputs of 25
		// bytes give 500; a 26-byte output script needs 520.
		bigOutput := bscript.Script(make([]byte, 26))
		tx.Outputs[0].LockingScript = &bigOutput

		require.False(t, isFreeConsolidationTxn(basePolicy(t), tx, scriptTierTestBlockHeight, deepHeights(20)))
	})

	t.Run("dust-return donation relaxes factor and confirmations", func(t *testing.T) {
		tx := bt.NewTx()
		for i := 0; i < 3; i++ {
			require.NoError(t, tx.From(scriptTierTestPrevTxID, uint32(i), scriptTierTestP2PKHScript, 1000)) //nolint:gosec // test loop index
		}

		donation := bscript.Script(dustReturnScript)
		tx.AddOutput(&bt.Output{Satoshis: 0, LockingScript: &donation})

		heights := []uint32{unconfirmedParentHeight, unconfirmedParentHeight, unconfirmedParentHeight}
		require.True(t, isFreeConsolidationTxn(basePolicy(t), tx, scriptTierTestBlockHeight, heights))
	})

	t.Run("single-output zero-value tx without the dust script is not a donation", func(t *testing.T) {
		tx := bt.NewTx()
		for i := 0; i < 3; i++ {
			require.NoError(t, tx.From(scriptTierTestPrevTxID, uint32(i), scriptTierTestP2PKHScript, 1000)) //nolint:gosec // test loop index
		}

		notDust := bscript.Script([]byte{0x00, 0x6a, 0x04, 'd', 'u', 's', 'x'})
		tx.AddOutput(&bt.Output{Satoshis: 0, LockingScript: &notDust})

		// Falls through to the normal path: 3 inputs < factor 20.
		require.False(t, isFreeConsolidationTxn(basePolicy(t), tx, scriptTierTestBlockHeight, deepHeights(3)))
	})

	t.Run("mismatched utxoHeights length disqualifies", func(t *testing.T) {
		tx := newConsolidationTestTx(t, 20, 1, 107, 0)
		require.False(t, isFreeConsolidationTxn(basePolicy(t), tx, scriptTierTestBlockHeight, deepHeights(19)))
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

	verifier := newScriptVerifierGoBDK(ulogger.TestLogger{}, po, tSettings.ChainCfgParams)

	blockHeight := tSettings.ChainCfgParams.GenesisActivationHeight + 10_000
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
		require.True(t, isFreeConsolidationTxn(po, tx, blockHeight, heights))
	})

	t.Run("BDK charges the split transaction and the Go predicate agrees", func(t *testing.T) {
		tx := buildSignedTx(t, 20, 2)

		err := verifier.ValidateTransaction(tx, blockHeight, false, heights)
		require.Error(t, err, "a zero-fee non-consolidation must fail the raised fee floor")
		require.True(t, errors.Is(err, errors.ErrTxPolicy), "expected a policy error, got %v", err)
		require.False(t, isFreeConsolidationTxn(po, tx, blockHeight, heights))
	})
}
