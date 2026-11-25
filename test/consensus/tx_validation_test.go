package consensus

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"path/filepath"
	"strconv"
	"testing"

	bdkscript "github.com/bitcoin-sv/bdk/module/gobdk/script"
	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-chaincfg"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	validatorservice "github.com/bsv-blockchain/teranode/services/validator"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/stretchr/testify/require"
)

// txVectorDeviations lists the tx_valid.json and tx_invalid.json vectors on which
// Teranode's verdict differs from bitcoin-sv's, keyed "<file>:<index>", with the reason.
// The test asserts the deviating verdict for these, so a change in either direction is
// noticed, and fails when a listed vector agrees with bitcoin-sv again.
var txVectorDeviations = map[string]string{}

// TestTransactionValidation runs every vector in tx_valid.json and tx_invalid.json the way
// bitcoin-sv's transaction_tests.cpp does:
//
//   - The serialized transaction must parse, and serialize back to the same bytes.
//   - The context-free checks (bitcoin-sv's CheckRegularTransaction, Teranode's
//     CheckTransactionContextFree) run with pre-Genesis limits.
//   - For each flag set, every input's unlocking script is verified against its prevout's
//     locking script and amount (0 when the vector gives none), through gobdk with exactly
//     those flags.
//
// A valid vector must pass the checks and every input under every flag set. An invalid
// vector must fail the context-free checks, or fail an input under every flag set.
func TestTransactionValidation(t *testing.T) {
	t.Run("tx_valid.json", func(t *testing.T) {
		runTxVectors(t, "tx_valid.json", true)
	})

	t.Run("tx_invalid.json", func(t *testing.T) {
		runTxVectors(t, "tx_invalid.json", false)
	})
}

func runTxVectors(t *testing.T, file string, shouldBeValid bool) {
	tests := LoadTxTests(t, filepath.Join(GetTestDataPath(), file))
	require.NotEmpty(t, tests)

	tSettings := settings.NewSettings()
	tSettings.ChainCfgParams = &chaincfg.MainNetParams
	preGenesisHeight := tSettings.ChainCfgParams.GenesisActivationHeight - 1

	engine := bdkscript.NewTxValidator("main")
	require.NotNil(t, engine)

	for _, test := range tests {
		key := file + ":" + strconv.Itoa(test.Index)

		t.Run(fmt.Sprintf("%03d", test.Index), func(t *testing.T) {
			if test.Comment != "" {
				t.Log(test.Comment)
			}

			want := shouldBeValid
			if reason, deviates := txVectorDeviations[key]; deviates {
				t.Logf("known deviation from bitcoin-sv: %s", reason)
				want = !shouldBeValid
			}

			tx, err := txFromVector(t, test)
			if err != nil {
				// Teranode never accepts a transaction go-bt cannot parse.
				require.False(t, want, "%v", err)
				return
			}

			if err := contextFreeChecks(tx, tSettings, preGenesisHeight); err != nil {
				require.False(t, want, "context-free checks rejected a transaction bitcoin-sv accepts: %v", err)
				return
			}

			for _, set := range test.FlagSets {
				flags, err := ParseScriptFlags(set)
				require.NoError(t, err, "bad test")

				err = verifyInputs(engine, tx, flags)
				if want {
					require.NoError(t, err, "flags %s", set)
				} else {
					require.Error(t, err, "flags %s: every input verifies, but bitcoin-sv rejects the transaction", set)
				}
			}
		})
	}

}

// contextFreeChecks is bitcoin-sv's CheckCoinbase or CheckRegularTransaction: for a
// coinbase, the common transaction rules and the scriptSig length bound that block
// validation applies; for any other transaction, the validator's context-free checks.
func contextFreeChecks(tx *bt.Tx, tSettings *settings.Settings, height uint32) error {
	if !tx.IsCoinbase() {
		return validatorservice.CheckTransactionContextFree(tx, tSettings, height)
	}

	if reason := model.TxCommonRuleViolation(tx, height, tSettings.ChainCfgParams); reason != "" {
		return errors.NewTxInvalidError(reason)
	}

	if !model.CoinbaseScriptSigLengthInBounds(tx, tSettings.ChainCfgParams) {
		return errors.NewTxInvalidError("bad-cb-length")
	}

	return nil
}

// txFromVector parses the vector's transaction, checks it serializes back to the same
// bytes, and extends each input with its prevout's locking script and amount, looked up
// by outpoint as bitcoin-sv does. It returns an error only when go-bt cannot parse the
// transaction; a malformed vector fails the test.
func txFromVector(t *testing.T, test TxTest) (*bt.Tx, error) {
	t.Helper()

	raw, err := hex.DecodeString(test.RawTx)
	require.NoError(t, err, "bad test")

	tx, err := bt.NewTxFromBytes(raw)
	if err != nil {
		return nil, errors.NewTxInvalidError("go-bt cannot parse the transaction", err)
	}

	require.True(t, bytes.Equal(raw, tx.Bytes()), "go-bt does not serialize the transaction back to the same bytes")

	type outpoint struct {
		txid string
		vout uint32
	}

	prevouts := make(map[outpoint]TxTestInput, len(test.Inputs))

	for _, in := range test.Inputs {
		prevouts[outpoint{in.TxID, in.Vout}] = in
	}

	for i, in := range tx.Inputs {
		prev, ok := prevouts[outpoint{in.PreviousTxIDStr(), in.PreviousTxOutIndex}]
		require.True(t, ok, "bad test: no prevout for input %d", i)

		script, err := ParseScriptBitcoinSV(prev.ScriptPubKey)
		require.NoError(t, err, "bad test: prevout script of input %d", i)

		s := bscript.Script(script)
		in.PreviousTxScript = &s
		in.PreviousTxSatoshis = uint64(prev.Amount) //nolint:gosec // vector amounts are non-negative
	}

	return tx, nil
}

// verifyInputs verifies every input's script with the given flags, in consensus mode.
func verifyInputs(engine *bdkscript.TxValidator, tx *bt.Tx, flags uint32) error {
	heights := make([]int32, len(tx.Inputs))
	customFlags := make([]uint32, len(tx.Inputs))

	for i := range customFlags {
		customFlags[i] = flags
	}

	if err := engine.VerifyScriptWithCustomFlags(tx.ExtendedBytes(), heights, 0, true, customFlags); err != nil {
		return errors.NewTxInvalidError("script verification failed", err)
	}

	return nil
}

// TestSingleTransactionWithValidators tests a specific transaction with all validators
func TestSingleTransactionWithValidators(t *testing.T) {
	// Create a simple transaction test
	test := TxTest{
		Inputs: []TxTestInput{
			{
				TxID:         "0000000000000000000000000000000000000000000000000000000000000000",
				Vout:         0,
				ScriptPubKey: "0x51", // OP_1
			},
		},
		// Simple transaction that spends the above output
		// This is a minimal transaction for testing
		RawTx:    "0100000001000000000000000000000000000000000000000000000000000000000000000000000000025100ffffffff0100e1f505000000001976a914389ffce9cd9ae88dcc0631e88a821ffdbe9bfe2688ac00000000",
		FlagSets: []string{"P2SH"},
	}

	ctx := NewTxTestContext()
	err := ctx.LoadFromTxTest(test, true)
	if err != nil {
		t.Fatalf("Failed to load test transaction: %v", err)
	}

	// Test with each validator individually
	// Only use GoBDK validator - go-bt and go-sdk disabled
	validators := []ValidatorType{ValidatorGoBDK}

	for _, validatorType := range validators {
		t.Run(string(validatorType), func(t *testing.T) {
			result := ctx.ValidateWithValidator(validatorType)
			t.Logf("%s validation result: Valid=%v, Error=%v", validatorType, result.Valid, result.Error)
		})
	}

	// Test with all validators
	t.Run("AllValidators", func(t *testing.T) {
		result := ctx.Validate()
		t.Logf("Combined validation result: Valid=%v, Details=%s", result.Valid, result.Details)
	})
}
