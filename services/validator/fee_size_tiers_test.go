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
	sizeTierTestPrevTxID    = "0000000000000000000000000000000000000000000000000000000000000001"
	sizeTierTestP2PKHScript = "76a914000000000000000000000000000000000000000088ac" // 25 bytes
	sizeTierTestBlockHeight = uint32(200_000)
	sizeTierTestDeepHeight  = uint32(190_000) // well past the 6-confirmation default
)

// newSizeTierTestTx builds an extended transaction with one P2PKH-shaped input
// carrying feeSatoshis and one zero-value OP_RETURN output of payloadSize, so
// tests control both tx.Size() and the fee exactly (fee = input satoshis).
func newSizeTierTestTx(t *testing.T, payloadSize int, feeSatoshis uint64) *bt.Tx {
	t.Helper()

	tx := bt.NewTx()
	require.NoError(t, tx.From(sizeTierTestPrevTxID, 0, sizeTierTestP2PKHScript, feeSatoshis))
	require.NoError(t, tx.AddOpReturnOutput(make([]byte, payloadSize)))

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
		require.NoError(t, tx.From(sizeTierTestPrevTxID, uint32(i), sizeTierTestP2PKHScript, perInput)) //nolint:gosec // test loop index

		unlocking := bscript.Script(make([]byte, unlockingSize))
		tx.Inputs[i].UnlockingScript = &unlocking
	}

	outScript, err := bscript.NewFromHexString(sizeTierTestP2PKHScript)
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
		heights[i] = sizeTierTestDeepHeight
	}

	return heights
}

func TestSizeTieredRequiredFee(t *testing.T) {
	oneTier := []settings.FeeSizeTier{{SizeBytes: 1_000_000, SatoshisPerKB: 10}}
	twoTiers := []settings.FeeSizeTier{
		{SizeBytes: 1_000_000, SatoshisPerKB: 10},
		{SizeBytes: 2_000_000, SatoshisPerKB: 50},
	}

	tests := []struct {
		name     string
		base     int64
		tiers    []settings.FeeSizeTier
		size     uint64
		expected uint64
	}{
		{name: "at the threshold only the base rate applies", base: 1, tiers: oneTier, size: 1_000_000, expected: 1_000},
		{name: "one byte over the threshold rounds down", base: 1, tiers: oneTier, size: 1_000_001, expected: 1_000},
		{name: "half a megabyte over the threshold", base: 1, tiers: oneTier, size: 1_500_000, expected: 6_000},
		{name: "spanning two tiers", base: 1, tiers: twoTiers, size: 2_500_000, expected: 36_000},
		{name: "zero base rate charges only tier bytes", base: 0, tiers: oneTier, size: 1_500_000, expected: 5_000},
		{name: "sub-satoshi requirement rounds to zero", base: 0, tiers: []settings.FeeSizeTier{{SizeBytes: 100, SatoshisPerKB: 5}}, size: 150, expected: 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.expected, sizeTieredRequiredFee(tt.base, tt.tiers, tt.size))
		})
	}
}

func TestValidateFeeSizeTiers(t *testing.T) {
	t.Run("empty schedule is valid", func(t *testing.T) {
		po := &settings.PolicySettings{MinMiningTxFee: 0.00000500}
		require.NotPanics(t, func() { validateFeeSizeTiers(po) })
	})

	t.Run("tier rates at or above the floor are valid", func(t *testing.T) {
		po := &settings.PolicySettings{
			MinMiningTxFee:       0.00000500, // 500 sat/kB
			MinMiningTxFeeBySize: []settings.FeeSizeTier{{SizeBytes: 1_000_000, SatoshisPerKB: 500}},
		}
		require.NotPanics(t, func() { validateFeeSizeTiers(po) })
	})

	t.Run("tier rate below the floor panics", func(t *testing.T) {
		po := &settings.PolicySettings{
			MinMiningTxFee:       0.00000500, // 500 sat/kB
			MinMiningTxFeeBySize: []settings.FeeSizeTier{{SizeBytes: 1_000_000, SatoshisPerKB: 400}},
		}
		require.Panics(t, func() { validateFeeSizeTiers(po) })
	})

	t.Run("NewTxValidator rejects a schedule below the floor", func(t *testing.T) {
		tSettings := test.CreateBaseTestSettings(t)
		tSettings.Policy.MinMiningTxFee = 0.00000500
		tSettings.Policy.MinMiningTxFeeBySize = []settings.FeeSizeTier{{SizeBytes: 1_000_000, SatoshisPerKB: 400}}

		require.Panics(t, func() { NewTxValidator(ulogger.TestLogger{}, tSettings) })
	})
}

func TestCheckSizeTieredFee(t *testing.T) {
	newTieredValidator := func(t *testing.T) *TxValidator {
		t.Helper()

		tSettings := test.CreateBaseTestSettings(t)
		tSettings.Policy.MinMiningTxFee = 0.00000001 // 1 sat/kB floor
		tSettings.Policy.MinMiningTxFeeBySize = []settings.FeeSizeTier{{SizeBytes: 1_000, SatoshisPerKB: 1_000}}

		tv := NewTxValidator(ulogger.TestLogger{}, tSettings)
		tv.bdk = noopBDKValidator{}

		return tv
	}

	t.Run("below the threshold zero fee passes", func(t *testing.T) {
		tv := newTieredValidator(t)
		tx := newSizeTierTestTx(t, 500, 0)
		require.LessOrEqual(t, uint64(tx.Size()), uint64(1_000)) //nolint:gosec // test size

		err := tv.ValidateTransaction(tx, sizeTierTestBlockHeight, deepHeights(1), NewDefaultOptions())
		require.NoError(t, err)
	})

	t.Run("above the threshold underpaying is rejected as a policy error", func(t *testing.T) {
		tv := newTieredValidator(t)

		tx := newSizeTierTestTx(t, 2_000, 0)
		size := uint64(tx.Size()) //nolint:gosec // test size
		require.Greater(t, size, uint64(1_000))

		required := sizeTieredRequiredFee(1, tv.settings.Policy.MinMiningTxFeeBySize, size)
		require.Positive(t, required)

		underpaying := newSizeTierTestTx(t, 2_000, required-1)

		err := tv.ValidateTransaction(underpaying, sizeTierTestBlockHeight, deepHeights(1), NewDefaultOptions())
		require.Error(t, err)
		require.True(t, errors.Is(err, errors.ErrTxPolicy), "expected a policy error, got %v", err)
	})

	t.Run("above the threshold the exact marginal fee passes", func(t *testing.T) {
		tv := newTieredValidator(t)

		tx := newSizeTierTestTx(t, 2_000, 0)
		required := sizeTieredRequiredFee(1, tv.settings.Policy.MinMiningTxFeeBySize, uint64(tx.Size())) //nolint:gosec // test size

		exact := newSizeTierTestTx(t, 2_000, required)

		err := tv.ValidateTransaction(exact, sizeTierTestBlockHeight, deepHeights(1), NewDefaultOptions())
		require.NoError(t, err)
	})

	t.Run("skip policy checks bypasses the tier schedule", func(t *testing.T) {
		tv := newTieredValidator(t)
		tx := newSizeTierTestTx(t, 2_000, 0)

		err := tv.ValidateTransaction(tx, sizeTierTestBlockHeight, deepHeights(1), ProcessOptions(WithSkipPolicyChecks(true)))
		require.NoError(t, err)
	})

	t.Run("empty schedule is a no-op", func(t *testing.T) {
		tSettings := test.CreateBaseTestSettings(t)
		tSettings.Policy.MinMiningTxFee = 0.00000001
		tSettings.Policy.MinMiningTxFeeBySize = nil

		tv := NewTxValidator(ulogger.TestLogger{}, tSettings)
		tv.bdk = noopBDKValidator{}

		tx := newSizeTierTestTx(t, 2_000, 0)

		err := tv.ValidateTransaction(tx, sizeTierTestBlockHeight, deepHeights(1), NewDefaultOptions())
		require.NoError(t, err)
	})

	t.Run("free consolidation is exempt from the tier schedule", func(t *testing.T) {
		tv := newTieredValidator(t)

		tx := newConsolidationTestTx(t, 20, 1, 107, 0)
		require.Greater(t, uint64(tx.Size()), uint64(1_000)) //nolint:gosec // test size

		err := tv.ValidateTransaction(tx, sizeTierTestBlockHeight, deepHeights(20), NewDefaultOptions())
		require.NoError(t, err)
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
		require.True(t, isFreeConsolidationTxn(basePolicy(t), tx, sizeTierTestBlockHeight, deepHeights(20)))
	})

	t.Run("too few inputs for the factor", func(t *testing.T) {
		tx := newConsolidationTestTx(t, 19, 1, 107, 0)
		require.False(t, isFreeConsolidationTxn(basePolicy(t), tx, sizeTierTestBlockHeight, deepHeights(19)))
	})

	t.Run("factor zero disables free consolidations", func(t *testing.T) {
		po := basePolicy(t)
		po.MinConsolidationFactor = 0

		tx := newConsolidationTestTx(t, 20, 1, 107, 0)
		require.False(t, isFreeConsolidationTxn(po, tx, sizeTierTestBlockHeight, deepHeights(20)))
	})

	t.Run("unconfirmed input disqualifies", func(t *testing.T) {
		tx := newConsolidationTestTx(t, 20, 1, 107, 0)

		heights := deepHeights(20)
		heights[7] = unconfirmedParentHeight

		require.False(t, isFreeConsolidationTxn(basePolicy(t), tx, sizeTierTestBlockHeight, heights))
	})

	t.Run("shallowly confirmed input disqualifies", func(t *testing.T) {
		tx := newConsolidationTestTx(t, 20, 1, 107, 0)

		heights := deepHeights(20)
		heights[7] = sizeTierTestBlockHeight - 3 // 3 confirmations, need 6

		require.False(t, isFreeConsolidationTxn(basePolicy(t), tx, sizeTierTestBlockHeight, heights))
	})

	t.Run("height zero skips the confirmation rule", func(t *testing.T) {
		// svnode skips the check for coins whose height was never recorded.
		tx := newConsolidationTestTx(t, 20, 1, 107, 0)

		heights := deepHeights(20)
		heights[7] = 0

		require.True(t, isFreeConsolidationTxn(basePolicy(t), tx, sizeTierTestBlockHeight, heights))
	})

	t.Run("oversized unlocking script disqualifies", func(t *testing.T) {
		tx := newConsolidationTestTx(t, 20, 1, 151, 0)
		require.False(t, isFreeConsolidationTxn(basePolicy(t), tx, sizeTierTestBlockHeight, deepHeights(20)))
	})

	t.Run("non-standard prevout script disqualifies unless accepted", func(t *testing.T) {
		po := basePolicy(t)

		tx := newConsolidationTestTx(t, 20, 1, 107, 0)

		// Same 25-byte length as the P2PKH prevouts so only standardness
		// changes, not the locking-script shrinkage ratio.
		nonStandardBytes := make([]byte, 25)
		for i := range nonStandardBytes {
			nonStandardBytes[i] = bscript.OpNOP
		}

		nonStandard := bscript.Script(nonStandardBytes)
		tx.Inputs[3].PreviousTxScript = &nonStandard

		require.False(t, isFreeConsolidationTxn(po, tx, sizeTierTestBlockHeight, deepHeights(20)))

		po.AcceptNonStdConsolidationInput = true
		require.True(t, isFreeConsolidationTxn(po, tx, sizeTierTestBlockHeight, deepHeights(20)))
	})

	t.Run("insufficient locking-script shrinkage disqualifies", func(t *testing.T) {
		tx := newConsolidationTestTx(t, 20, 1, 107, 0)

		// Grow the single output script beyond sumIn/factor: 20 inputs of 25
		// bytes give 500; a 26-byte output script needs 520.
		bigOutput := bscript.Script(make([]byte, 26))
		tx.Outputs[0].LockingScript = &bigOutput

		require.False(t, isFreeConsolidationTxn(basePolicy(t), tx, sizeTierTestBlockHeight, deepHeights(20)))
	})

	t.Run("dust-return donation relaxes factor and confirmations", func(t *testing.T) {
		tx := bt.NewTx()
		for i := 0; i < 3; i++ {
			require.NoError(t, tx.From(sizeTierTestPrevTxID, uint32(i), sizeTierTestP2PKHScript, 1000)) //nolint:gosec // test loop index
		}

		donation := bscript.Script(dustReturnScript)
		tx.AddOutput(&bt.Output{Satoshis: 0, LockingScript: &donation})

		heights := []uint32{unconfirmedParentHeight, unconfirmedParentHeight, unconfirmedParentHeight}
		require.True(t, isFreeConsolidationTxn(basePolicy(t), tx, sizeTierTestBlockHeight, heights))
	})

	t.Run("single-output zero-value tx without the dust script is not a donation", func(t *testing.T) {
		tx := bt.NewTx()
		for i := 0; i < 3; i++ {
			require.NoError(t, tx.From(sizeTierTestPrevTxID, uint32(i), sizeTierTestP2PKHScript, 1000)) //nolint:gosec // test loop index
		}

		notDust := bscript.Script([]byte{0x00, 0x6a, 0x04, 'd', 'u', 's', 'x'})
		tx.AddOutput(&bt.Output{Satoshis: 0, LockingScript: &notDust})

		// Falls through to the normal path: 3 inputs < factor 20.
		require.False(t, isFreeConsolidationTxn(basePolicy(t), tx, sizeTierTestBlockHeight, deepHeights(3)))
	})

	t.Run("mismatched utxoHeights length disqualifies", func(t *testing.T) {
		tx := newConsolidationTestTx(t, 20, 1, 107, 0)
		require.False(t, isFreeConsolidationTxn(basePolicy(t), tx, sizeTierTestBlockHeight, deepHeights(19)))
	})
}

// TestFreeConsolidationDifferentialBDK pins isFreeConsolidationTxn against
// BDK's own free-consolidation exemption: with the fee floor set punishingly
// high, a zero-fee transaction passes BDK policy validation if and only if BDK
// classifies it as a free consolidation. The Go predicate must agree on both
// sides, or enabling minminingtxfeebysize would either reject consolidations
// BDK mines for free or exempt transactions BDK does not.
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
