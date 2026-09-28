package txscript

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	sdkscript "github.com/bsv-blockchain/go-sdk/script"
	"github.com/bsv-blockchain/go-sdk/transaction"
	sighash "github.com/bsv-blockchain/go-sdk/transaction/sighash"
	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/services/legacy/bsvec"
	"github.com/stretchr/testify/require"
)

// forkIDTestAmt is the amount of the output spent by input 0 of the test
// transaction, committed to by the BIP143 digest.
const forkIDTestAmt int64 = 100_000_000

// forkIDTestKey returns a fixed key pair so the tests are deterministic.
func forkIDTestKey() (*bsvec.PrivateKey, *bsvec.PublicKey) {
	return bsvec.PrivKeyFromBytes(bsvec.S256(), bytes.Repeat([]byte{0x2a}, 32))
}

// newForkIDTestTx returns a transaction with two inputs and two outputs, so
// that every sighash type commits to something different.
func newForkIDTestTx() *wire.MsgTx {
	tx := wire.NewMsgTx(1)

	in0 := wire.NewTxIn(wire.NewOutPoint(&chainhash.Hash{0x01, 0x02, 0x03}, 1), nil)
	in0.Sequence = 0xfffffffe
	tx.AddTxIn(in0)

	in1 := wire.NewTxIn(wire.NewOutPoint(&chainhash.Hash{0x04, 0x05, 0x06}, 7), nil)
	in1.Sequence = 0xffffffff
	tx.AddTxIn(in1)

	tx.AddTxOut(wire.NewTxOut(40_000_000, []byte{OP_TRUE}))
	tx.AddTxOut(wire.NewTxOut(50_000_000, []byte{OP_2}))

	return tx
}

// mustScript returns the script built by b.
func mustScript(t *testing.T, b *ScriptBuilder) []byte {
	t.Helper()

	script, err := b.Script()
	require.NoError(t, err)

	return script
}

// signOver signs input 0 of tx over scriptCode with the given hash type and
// digest, and returns the signature with its hash type byte appended.  It
// does not go through RawTxInSignature, which always sets SIGHASH_FORKID.
func signOver(t *testing.T, tx *wire.MsgTx, scriptCode []byte, hashType SigHashType, useBip143 bool) []byte {
	t.Helper()

	parsed, err := ParseScript(scriptCode)
	require.NoError(t, err)

	hash, err := calcSignatureHash(parsed, NewTxSigHashes(tx), hashType, tx, 0, forkIDTestAmt, useBip143)
	require.NoError(t, err)

	priv, _ := forkIDTestKey()

	sig, err := priv.Sign(hash)
	require.NoError(t, err)

	return append(sig.Serialize(), byte(hashType))
}

// runEngine executes scriptSig as input 0 of tx against the locking script
// OP_VERIFY OP_TRUE, so a false result of the scriptSig surfaces as ErrVerify.
func runEngine(t *testing.T, tx *wire.MsgTx, scriptSig []byte, flags ScriptFlags) error {
	t.Helper()

	pkScript := mustScript(t, NewScriptBuilder().AddOp(OP_VERIFY).AddOp(OP_TRUE))

	tx.TxIn[0].SignatureScript = scriptSig

	vm, err := NewEngine(pkScript, tx, 0, flags, nil, nil, forkIDTestAmt)
	require.NoError(t, err)

	return vm.Execute()
}

// TestBip143SigHashSkipsFindAndDelete checks that the signature is removed from
// the script code only for the legacy digest.  Each signature is made over the
// script code with the signature push removed, then executed from a scriptSig
// that contains that push.  The BIP143 digest commits to the script code
// unchanged, so there the signature must not verify.
func TestBip143SigHashSkipsFindAndDelete(t *testing.T) {
	_, pub := forkIDTestKey()
	pk := pub.SerializeCompressed()

	legacyFlags := ScriptVerifyStrictEncoding
	bip143Flags := ScriptVerifyStrictEncoding | ScriptVerifyBip143SigHash

	tests := []struct {
		name      string
		flags     ScriptFlags
		hashType  SigHashType
		multiSig  bool
		wantValid bool
	}{
		{name: "checksig/legacy", flags: legacyFlags, hashType: SigHashAll, wantValid: true},
		{name: "checksig/bip143", flags: bip143Flags, hashType: SigHashAll | SigHashForkID, wantValid: false},
		{name: "checkmultisig/legacy", flags: legacyFlags, hashType: SigHashAll, multiSig: true, wantValid: true},
		{name: "checkmultisig/bip143", flags: bip143Flags, hashType: SigHashAll | SigHashForkID, multiSig: true, wantValid: false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tx := newForkIDTestTx()
			useBip143 := tc.flags&ScriptVerifyBip143SigHash != 0

			var signedCode, scriptSig []byte

			if tc.multiSig {
				signedCode = mustScript(t, NewScriptBuilder().AddOp(OP_0).AddOp(OP_1).
					AddData(pk).AddOp(OP_1).AddOp(OP_CHECKMULTISIG))
				sig := signOver(t, tx, signedCode, tc.hashType, useBip143)
				scriptSig = mustScript(t, NewScriptBuilder().AddOp(OP_0).AddData(sig).AddOp(OP_1).
					AddData(pk).AddOp(OP_1).AddOp(OP_CHECKMULTISIG))
			} else {
				signedCode = mustScript(t, NewScriptBuilder().AddData(pk).AddOp(OP_CHECKSIG))
				sig := signOver(t, tx, signedCode, tc.hashType, useBip143)
				scriptSig = mustScript(t, NewScriptBuilder().AddData(sig).AddData(pk).AddOp(OP_CHECKSIG))
			}

			err := runEngine(t, tx, scriptSig, tc.flags)
			if tc.wantValid {
				require.NoError(t, err)
				return
			}

			require.Error(t, err)
			require.True(t, IsErrorCode(err, ErrVerify), "want ErrVerify, got %v", err)
		})
	}
}

// TestCleanupScriptCodeGate pins the removal predicate of cleanupScriptCode to
// the one of CleanupScriptCode in bitcoin-sv: the removal is skipped only when
// the fork id flag is set AND the signature's hash type carries SIGHASH_FORKID.
func TestCleanupScriptCodeGate(t *testing.T) {
	sigF := append(bytes.Repeat([]byte{0xaa}, 9), byte(SigHashAll|SigHashForkID))
	sigL := append(bytes.Repeat([]byte{0xbb}, 9), byte(SigHashAll))

	script := mustScript(t, NewScriptBuilder().AddData(sigF).AddData(sigL).AddOp(OP_CHECKSIG))
	withoutF := mustScript(t, NewScriptBuilder().AddData(sigL).AddOp(OP_CHECKSIG))
	withoutL := mustScript(t, NewScriptBuilder().AddData(sigF).AddOp(OP_CHECKSIG))

	parsed, err := ParseScript(script)
	require.NoError(t, err)

	emptyRemoved, err := UnparseScript(removeOpcodeByData(parsed, nil))
	require.NoError(t, err)

	tests := []struct {
		name  string
		flags ScriptFlags
		sig   []byte
		want  []byte
	}{
		{name: "no-forkid-flag/forkid-sig", flags: 0, sig: sigF, want: withoutF},
		{name: "no-forkid-flag/legacy-sig", flags: 0, sig: sigL, want: withoutL},
		{name: "forkid-flag/forkid-sig", flags: ScriptVerifyBip143SigHash, sig: sigF, want: script},
		// A flag-only gate would keep the push here; bitcoin-sv removes it.
		{name: "forkid-flag/legacy-sig", flags: ScriptVerifyBip143SigHash, sig: sigL, want: withoutL},
		// An empty signature has hash type 0, so it takes the removal path.
		{name: "forkid-flag/empty-sig", flags: ScriptVerifyBip143SigHash, sig: nil, want: emptyRemoved},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			vm := &Engine{flags: tc.flags}

			got, err := UnparseScript(vm.cleanupScriptCode(parsed, tc.sig))
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}
}

// TestCalcBip143SignatureHashRetainsCodeSeparators checks that the BIP143
// digest commits to the script code as given, OP_CODESEPARATORs included, as
// SignatureHashBIP143 in bitcoin-sv does.  go-sdk serves as an independent
// implementation of the digest.
func TestCalcBip143SignatureHashRetainsCodeSeparators(t *testing.T) {
	scripts := []struct {
		name       string
		ops        []byte
		separators int
	}{
		{name: "no-separator", ops: []byte{OP_1, OP_2, OP_CHECKSIG}},
		{name: "one-separator", ops: []byte{OP_1, OP_CODESEPARATOR, OP_2, OP_CHECKSIG}, separators: 1},
		{name: "two-separators", ops: []byte{OP_1, OP_CODESEPARATOR, OP_2, OP_CODESEPARATOR, OP_CHECKSIG}, separators: 2},
		{name: "leading-and-trailing", ops: []byte{OP_CODESEPARATOR, OP_1, OP_CHECKSIG, OP_CODESEPARATOR}, separators: 2},
	}

	hashTypes := []SigHashType{
		SigHashAll | SigHashForkID,
		SigHashNone | SigHashForkID,
		SigHashSingle | SigHashForkID,
		SigHashAll | SigHashAnyOneCanPay | SigHashForkID,
	}

	for _, sc := range scripts {
		for _, ht := range hashTypes {
			t.Run(fmt.Sprintf("%s/0x%02x", sc.name, uint32(ht)), func(t *testing.T) {
				tx := newForkIDTestTx()
				scriptBytes := mustScript(t, NewScriptBuilder().AddOps(sc.ops))

				got, err := CalcSignatureHash(scriptBytes, NewTxSigHashes(tx), ht, tx, 0, forkIDTestAmt, true)
				require.NoError(t, err)

				var buf bytes.Buffer
				require.NoError(t, tx.Serialize(&buf))

				sdkTx, err := transaction.NewTransactionFromBytes(buf.Bytes())
				require.NoError(t, err)

				sdkTx.Inputs[0].SetSourceTxOutput(&transaction.TransactionOutput{
					Satoshis:      uint64(forkIDTestAmt),
					LockingScript: sdkscript.NewFromBytes(scriptBytes),
				})

				want, err := sdkTx.CalcInputSignatureHash(0, sighash.Flag(ht))
				require.NoError(t, err)
				require.Equal(t, want, got)

				if sc.separators == 0 {
					return
				}

				parsed, err := ParseScript(scriptBytes)
				require.NoError(t, err)

				stripped, err := calcSignatureHash(removeOpcode(parsed, OP_CODESEPARATOR), NewTxSigHashes(tx),
					ht, tx, 0, forkIDTestAmt, true)
				require.NoError(t, err)
				require.NotEqual(t, got, stripped)
			})
		}
	}
}
