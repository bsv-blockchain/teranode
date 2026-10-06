package validator

import (
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-chaincfg"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// batchEquivalenceInputs returns a mix of transactions whose single-call verdicts
// differ: a valid mainnet transaction, the same transaction with a corrupted
// unlocking script, a coinbase, and the valid transaction again.
func batchEquivalenceInputs(t *testing.T) ([]*bt.Tx, []uint32, [][]uint32) {
	t.Helper()

	valid := aTx.Clone()

	corrupted := aTx.Clone()
	script := *corrupted.Inputs[0].UnlockingScript
	script[len(script)-2] ^= 0xff
	corrupted.Inputs[0].UnlockingScript = &script

	coinbase, err := bt.NewTxFromString(model.CoinbaseHex)
	require.NoError(t, err)

	txs := []*bt.Tx{valid, corrupted, coinbase, aTx.Clone()}
	heights := []uint32{110300, 110300, 110300, 110300}
	utxoHeights := [][]uint32{{631924, 631924}, {631924, 631924}, {}, {631924, 631924}}

	return txs, heights, utxoHeights
}

// TestScriptVerifierGoBDK_BatchMatchesSingle pins the contract of the batched BDK
// path: for every transaction and every parallelism, the batch verdict must be
// the verdict the single-transaction call returns.
func TestScriptVerifierGoBDK_BatchMatchesSingle(t *testing.T) {
	params, err := chaincfg.GetChainParams("mainnet")
	require.NoError(t, err)

	verifier := newScriptVerifierGoBDK(ulogger.TestLogger{}, settings.NewPolicySettings(), params).(*scriptVerifierGoBDK)

	txs, heights, utxoHeights := batchEquivalenceInputs(t)

	single := make([]error, len(txs))
	for i, tx := range txs {
		single[i] = verifier.ValidateTransaction(tx, heights[i], true, utxoHeights[i])
	}

	require.NoError(t, single[0])
	require.Error(t, single[1])
	require.Error(t, single[2])
	require.NoError(t, single[3])

	for _, parallelism := range []int{0, 1, 2, 3, 16} {
		batch := verifier.ValidateTransactionBatch(txs, heights, true, utxoHeights, parallelism)
		require.Len(t, batch, len(txs))

		for i := range txs {
			if single[i] == nil {
				assert.NoError(t, batch[i], "parallelism=%d tx=%d", parallelism, i)
				continue
			}

			require.Error(t, batch[i], "parallelism=%d tx=%d", parallelism, i)
			assert.Equal(t, single[i].Error(), batch[i].Error(), "parallelism=%d tx=%d", parallelism, i)
		}
	}
}

func TestScriptVerifierGoBDK_BatchRejectsMismatchedLengths(t *testing.T) {
	params, err := chaincfg.GetChainParams("mainnet")
	require.NoError(t, err)

	verifier := newScriptVerifierGoBDK(ulogger.TestLogger{}, settings.NewPolicySettings(), params).(*scriptVerifierGoBDK)

	errs := verifier.ValidateTransactionBatch([]*bt.Tx{aTx, aTx}, []uint32{1}, true, [][]uint32{{1, 1}, {1, 1}}, 0)
	require.Len(t, errs, 2)
	assert.Error(t, errs[0])
	assert.Error(t, errs[1])

	assert.Empty(t, verifier.ValidateTransactionBatch(nil, nil, true, nil, 0))
}

// TestTxValidator_BatchMatchesSingle covers the Teranode-owned checks in front of
// BDK: the batch must apply them per transaction exactly as ValidateTransaction does.
func TestTxValidator_BatchMatchesSingle(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)

	params, err := chaincfg.GetChainParams("mainnet")
	require.NoError(t, err)

	tSettings.ChainCfgParams = params
	tSettings.Validator.ScriptBatchThreads = 2

	txValidator := NewTxValidator(ulogger.TestLogger{}, tSettings)

	txs, heights, utxoHeights := batchEquivalenceInputs(t)

	for _, opts := range []*Options{
		NewDefaultOptions(),
		ProcessOptions(WithSkipPolicyChecks(true)),
	} {
		batch := txValidator.ValidateTransactionBatch(txs, heights, utxoHeights, opts)
		require.Len(t, batch, len(txs))

		for i, tx := range txs {
			single := txValidator.ValidateTransaction(tx, heights[i], utxoHeights[i], opts)
			if single == nil {
				assert.NoError(t, batch[i], "tx=%d", i)
				continue
			}

			require.Error(t, batch[i], "tx=%d", i)
			assert.Equal(t, single.Error(), batch[i].Error(), "tx=%d", i)
		}
	}
}

// TestTxValidator_BatchFallsBackForNonBatchingEngine checks that an engine without
// a batch entry point is still called once per transaction that needs BDK.
func TestTxValidator_BatchFallsBackForNonBatchingEngine(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)
	counter := &countingBDKValidator{}
	txValidator := &TxValidator{
		logger:   ulogger.TestLogger{},
		settings: tSettings,
		bdk:      counter,
	}

	coinbase, err := bt.NewTxFromString(model.CoinbaseHex)
	require.NoError(t, err)

	errs := txValidator.ValidateTransactionBatch(
		[]*bt.Tx{aTx, coinbase, aTx},
		[]uint32{1000, 1000, 1000},
		[][]uint32{{1, 1}, nil, {1, 1}},
		NewDefaultOptions(),
	)

	require.Len(t, errs, 3)
	assert.NoError(t, errs[0])
	assert.Error(t, errs[1])
	assert.NoError(t, errs[2])
	assert.Equal(t, 2, counter.calls)
}
