package consensus

import (
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	bdkscript "github.com/bitcoin-sv/bdk/module/gobdk/script"
	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/stretchr/testify/require"
)

// scriptVectorDeviations lists the script_tests.json vectors on which Teranode's result
// differs from bitcoin-sv's, keyed by the entry's index, with the result Teranode gives
// and why. The test asserts that result, so a change in either direction is noticed.
var scriptVectorDeviations = map[int]struct {
	got    string
	reason string
}{}

// bitcoinSVScriptErrors maps gobdk's script error codes to the names bitcoin-sv's
// script_tests.cpp uses in script_tests.json (its script_errors table). Some names differ
// from the enum, for example OPERAND_SIZE for SCRIPT_ERR_INVALID_OPERAND_SIZE.
var bitcoinSVScriptErrors = map[bdkscript.ScriptErrorCode]string{
	bdkscript.SCRIPT_ERR_UNKNOWN_ERROR:              "UNKNOWN_ERROR",
	bdkscript.SCRIPT_ERR_EVAL_FALSE:                 "EVAL_FALSE",
	bdkscript.SCRIPT_ERR_OP_RETURN:                  "OP_RETURN",
	bdkscript.SCRIPT_ERR_SCRIPT_SIZE:                "SCRIPT_SIZE",
	bdkscript.SCRIPT_ERR_PUSH_SIZE:                  "PUSH_SIZE",
	bdkscript.SCRIPT_ERR_OP_COUNT:                   "OP_COUNT",
	bdkscript.SCRIPT_ERR_STACK_SIZE:                 "STACK_SIZE",
	bdkscript.SCRIPT_ERR_SIG_COUNT:                  "SIG_COUNT",
	bdkscript.SCRIPT_ERR_PUBKEY_COUNT:               "PUBKEY_COUNT",
	bdkscript.SCRIPT_ERR_INVALID_OPERAND_SIZE:       "OPERAND_SIZE",
	bdkscript.SCRIPT_ERR_INVALID_NUMBER_RANGE:       "INVALID_NUMBER_RANGE",
	bdkscript.SCRIPT_ERR_INVALID_SPLIT_RANGE:        "SPLIT_RANGE",
	bdkscript.SCRIPT_ERR_SCRIPTNUM_OVERFLOW:         "SCRIPTNUM_OVERFLOW",
	bdkscript.SCRIPT_ERR_SCRIPTNUM_MINENCODE:        "SCRIPTNUM_MINENCODE",
	bdkscript.SCRIPT_ERR_VERIFY:                     "VERIFY",
	bdkscript.SCRIPT_ERR_EQUALVERIFY:                "EQUALVERIFY",
	bdkscript.SCRIPT_ERR_CHECKMULTISIGVERIFY:        "CHECKMULTISIGVERIFY",
	bdkscript.SCRIPT_ERR_CHECKSIGVERIFY:             "CHECKSIGVERIFY",
	bdkscript.SCRIPT_ERR_NUMEQUALVERIFY:             "NUMEQUALVERIFY",
	bdkscript.SCRIPT_ERR_BAD_OPCODE:                 "BAD_OPCODE",
	bdkscript.SCRIPT_ERR_DISABLED_OPCODE:            "DISABLED_OPCODE",
	bdkscript.SCRIPT_ERR_INVALID_STACK_OPERATION:    "INVALID_STACK_OPERATION",
	bdkscript.SCRIPT_ERR_INVALID_ALTSTACK_OPERATION: "INVALID_ALTSTACK_OPERATION",
	bdkscript.SCRIPT_ERR_UNBALANCED_CONDITIONAL:     "UNBALANCED_CONDITIONAL",
	bdkscript.SCRIPT_ERR_DIV_BY_ZERO:                "DIV_BY_ZERO",
	bdkscript.SCRIPT_ERR_MOD_BY_ZERO:                "MOD_BY_ZERO",
	bdkscript.SCRIPT_ERR_NEGATIVE_LOCKTIME:          "NEGATIVE_LOCKTIME",
	bdkscript.SCRIPT_ERR_UNSATISFIED_LOCKTIME:       "UNSATISFIED_LOCKTIME",
	bdkscript.SCRIPT_ERR_SIG_HASHTYPE:               "SIG_HASHTYPE",
	bdkscript.SCRIPT_ERR_SIG_DER:                    "SIG_DER",
	bdkscript.SCRIPT_ERR_MINIMALDATA:                "MINIMALDATA",
	bdkscript.SCRIPT_ERR_SIG_PUSHONLY:               "SIG_PUSHONLY",
	bdkscript.SCRIPT_ERR_SIG_HIGH_S:                 "SIG_HIGH_S",
	bdkscript.SCRIPT_ERR_SIG_NULLDUMMY:              "SIG_NULLDUMMY",
	bdkscript.SCRIPT_ERR_PUBKEYTYPE:                 "PUBKEYTYPE",
	bdkscript.SCRIPT_ERR_CLEANSTACK:                 "CLEANSTACK",
	bdkscript.SCRIPT_ERR_MINIMALIF:                  "MINIMALIF",
	bdkscript.SCRIPT_ERR_SIG_NULLFAIL:               "NULLFAIL",
	bdkscript.SCRIPT_ERR_DISCOURAGE_UPGRADABLE_NOPS: "DISCOURAGE_UPGRADABLE_NOPS",
	bdkscript.SCRIPT_ERR_NONCOMPRESSED_PUBKEY:       "NONCOMPRESSED_PUBKEY",
	bdkscript.SCRIPT_ERR_ILLEGAL_FORKID:             "ILLEGAL_FORKID",
	bdkscript.SCRIPT_ERR_MUST_USE_FORKID:            "MISSING_FORKID",
}

// TestScriptVectors runs every vector in script_tests.json as bitcoin-sv's
// script_tests.cpp (script_json_test and DoTest) does: a crediting transaction pays the
// amount (0 unless given) to scriptPubKey, a spending transaction of the vector's version
// spends it with scriptSig, and gobdk verifies that input in consensus mode with exactly the vector's
// flags (CLEANSTACK adds P2SH). The script error must be the one the vector names, by
// bitcoin-sv's names, or OK.
func TestScriptVectors(t *testing.T) {
	tests := LoadScriptTests(t, filepath.Join(GetTestDataPath(), "script_tests.json"))
	require.NotEmpty(t, tests)

	engine := bdkscript.NewTxValidator("main")
	require.NotNil(t, engine)

	for _, test := range tests {
		t.Run(fmt.Sprintf("%04d", test.Index), func(t *testing.T) {
			if test.Comment != "" {
				t.Log(test.Comment)
			}

			scriptSig, err := ParseScriptBitcoinSV(test.ScriptSig)
			require.NoError(t, err, "bad test: scriptSig")

			scriptPubKey, err := ParseScriptBitcoinSV(test.ScriptPubKey)
			require.NoError(t, err, "bad test: scriptPubKey")

			flags, err := ParseScriptFlags(strings.Join(test.Flags, ","))
			require.NoError(t, err, "bad test: flags")

			if flags&SCRIPT_VERIFY_CLEANSTACK != 0 {
				flags |= SCRIPT_VERIFY_P2SH
			}

			want := test.Expected
			if d, deviates := scriptVectorDeviations[test.Index]; deviates {
				t.Logf("known deviation from bitcoin-sv (%s expected there): %s", test.Expected, d.reason)
				want = d.got
			}

			got := scriptErrorName(verifyScriptVector(t, engine, scriptSig, scriptPubKey, flags, test.Amount, test.TxVersion))
			require.Equal(t, want, got, "scriptSig %q, scriptPubKey %q, flags %q", test.ScriptSig, test.ScriptPubKey, strings.Join(test.Flags, ","))
		})
	}
}

// verifyScriptVector builds bitcoin-sv's crediting and spending transactions
// (BuildCreditingTransaction, BuildSpendingTransaction) and verifies the spend's input.
func verifyScriptVector(t *testing.T, engine *bdkscript.TxValidator, scriptSig, scriptPubKey []byte, flags uint32, amount uint64, version int32) error {
	t.Helper()

	credit := bt.NewTx()
	credit.Version, credit.LockTime = 1, 0

	coinbaseIn := &bt.Input{
		PreviousTxOutIndex: 0xffffffff,
		SequenceNumber:     0xffffffff,
		UnlockingScript:    bscript.NewFromBytes([]byte{bscript.Op0, bscript.Op0}), // CScript() << CScriptNum(0) << CScriptNum(0)
	}
	require.NoError(t, coinbaseIn.PreviousTxIDAdd(&chainhash.Hash{}))
	credit.Inputs = append(credit.Inputs, coinbaseIn)
	credit.AddOutput(&bt.Output{Satoshis: amount, LockingScript: bscript.NewFromBytes(scriptPubKey)})

	spend := bt.NewTx()
	spend.Version, spend.LockTime = uint32(version), 0 //nolint:gosec // bitcoin-sv's int32 nVersion, serialized as its four bytes

	in := &bt.Input{
		PreviousTxOutIndex: 0,
		SequenceNumber:     0xffffffff,
		UnlockingScript:    bscript.NewFromBytes(scriptSig),
		PreviousTxScript:   bscript.NewFromBytes(scriptPubKey),
		PreviousTxSatoshis: amount,
	}
	require.NoError(t, in.PreviousTxIDAdd(credit.TxIDChainHash()))
	spend.Inputs = append(spend.Inputs, in)
	spend.AddOutput(&bt.Output{Satoshis: amount, LockingScript: &bscript.Script{}})

	return engine.VerifyScriptWithCustomFlags(spend.ExtendedBytes(), []int32{0}, 0, true, []uint32{flags})
}

// scriptErrorName is the bitcoin-sv name of a gobdk verification result: OK for nil, the
// script error's name, or the error text for anything that is not a script error.
func scriptErrorName(err error) string {
	if err == nil {
		return "OK"
	}

	var scriptErr bdkscript.ScriptError
	if errors.As(err, &scriptErr) {
		if name, ok := bitcoinSVScriptErrors[scriptErr.Code()]; ok {
			return name
		}

		return fmt.Sprintf("unmapped gobdk script error %d (%v)", scriptErr.Code(), err)
	}

	return "not a script error: " + err.Error()
}
