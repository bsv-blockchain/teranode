package consensus

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"path/filepath"
	"slices"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/sighash"
	"github.com/bsv-blockchain/go-chaincfg"
	crypto "github.com/bsv-blockchain/go-sdk/primitives/hash"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/stretchr/testify/require"
)

// sigHashOne is the original algorithm's result for SIGHASH_SINGLE on an input with no
// matching output: uint256 one, little-endian. It is not a hash of a preimage.
var sigHashOne = append([]byte{1}, make([]byte, 31)...)

// sigHashOneHex is sigHashOne in bitcoin-sv's uint256 hex, as the vectors write it.
const sigHashOneHex = "0000000000000000000000000000000000000000000000000000000000000001"

// TestSigHashVectors checks go-bt's signature hashes against every vector in
// sighash.json, as bitcoin-sv's sighash_tests.cpp does: the transaction must parse,
// serialize back to the same bytes and pass the context-free checks; then SignatureHash
// (amount 0; BIP143 when the hash type has FORKID) and SignatureHashOriginal (the
// original algorithm always) must give the vector's hashes.
//
// Teranode verifies signatures in gobdk, not go-bt, so this covers go-bt's signing path
// and its preimage serialization, which a go-bt bump could change.
//
// Known deviation: bitcoin-sv's original algorithm drops every OP_CODESEPARATOR from the
// script code (SerializeScriptCode); go-bt's CalcInputPreimageLegacy writes the script code
// as given. The test checks the original algorithm on the script code with the separators
// removed as bitcoin-sv removes them, and asserts that go-bt alone still differs whenever
// the script code has one, so a go-bt change in either direction is noticed. Teranode
// verifies signatures in gobdk, which follows bitcoin-sv; the deviation only affects
// go-bt's signing of non-FORKID inputs whose script code has an OP_CODESEPARATOR.
//
// The vectors use arbitrary 32-bit hash types. bitcoin-sv takes every decision from the
// low byte (base type & 0x1f, ANYONECANPAY 0x80, FORKID 0x40) and writes the full value
// only as the preimage's last four bytes. go-bt's sighash.Flag is one byte, as the hash
// type of a real signature is, so the test passes the low byte to go-bt and writes the
// full value into the preimage's last four bytes itself.
func TestSigHashVectors(t *testing.T) {
	tests := LoadSigHashTests(t, filepath.Join(GetTestDataPath(), "sighash.json"))
	require.NotEmpty(t, tests)

	tSettings := settings.NewSettings()
	tSettings.ChainCfgParams = &chaincfg.MainNetParams
	preGenesisHeight := tSettings.ChainCfgParams.GenesisActivationHeight - 1

	for _, test := range tests {
		t.Run(fmt.Sprintf("%04d", test.Index), func(t *testing.T) {
			raw, err := hex.DecodeString(test.RawTx)
			require.NoError(t, err, "bad test")

			tx, err := bt.NewTxFromBytes(raw)
			require.NoError(t, err, "go-bt cannot parse the transaction")
			require.True(t, bytes.Equal(raw, tx.Bytes()), "go-bt does not serialize the transaction back to the same bytes")
			require.NoError(t, contextFreeChecks(tx, tSettings, preGenesisHeight))

			scriptCode, err := hex.DecodeString(test.Script)
			require.NoError(t, err, "bad test")

			require.Less(t, test.InputIndex, len(tx.Inputs), "bad test")

			hashType := uint32(test.HashType) //nolint:gosec // a 32-bit hash type, negative ones included, as bitcoin-sv reads it
			stripped := withoutCodeSeparators(scriptCode)

			original := sigHashHex(t, tx, test.InputIndex, stripped, hashType, false)
			require.Equal(t, test.OriginalHash, original, "SignatureHashOriginal")

			regular := original
			if hashType&uint32(sighash.ForkID) != 0 {
				regular = sigHashHex(t, tx, test.InputIndex, scriptCode, hashType, true)
			}

			require.Equal(t, test.RegularHash, regular, "SignatureHash")

			// SIGHASH_SINGLE with no matching output gives the constant one, which never
			// hashes the script code, so there is no deviation to see.
			if !bytes.Equal(stripped, scriptCode) && test.OriginalHash != sigHashOneHex {
				require.NotEqual(t, test.OriginalHash, sigHashHex(t, tx, test.InputIndex, scriptCode, hashType, false),
					"go-bt's legacy preimage now drops OP_CODESEPARATOR like bitcoin-sv: remove the known deviation from this test")
			}
		})
	}
}

// sigHashHex is go-bt's signature hash for input i with the given script code and amount
// 0, in bitcoin-sv's uint256 hex (byte-reversed), with BIP143 or the original algorithm.
func sigHashHex(t *testing.T, tx *bt.Tx, i int, scriptCode []byte, hashType uint32, bip143 bool) string {
	t.Helper()

	s := bscript.Script(scriptCode)
	tx.Inputs[i].PreviousTxScript = &s
	tx.Inputs[i].PreviousTxSatoshis = 0

	flag := sighash.Flag(hashType & 0xff)

	var (
		preimage []byte
		err      error
	)

	if bip143 {
		preimage, err = tx.CalcInputPreimage(uint32(i), flag) //nolint:gosec // input index from a vector
	} else {
		preimage, err = tx.CalcInputPreimageLegacy(uint32(i), flag) //nolint:gosec // input index from a vector
	}

	require.NoError(t, err)

	digest := sigHashOne
	if !bytes.Equal(preimage, sigHashOne) {
		require.GreaterOrEqual(t, len(preimage), 4)
		binary.LittleEndian.PutUint32(preimage[len(preimage)-4:], hashType)
		digest = crypto.Sha256d(preimage)
	}

	reversed := slices.Clone(digest)
	slices.Reverse(reversed)

	return hex.EncodeToString(reversed)
}

// withoutCodeSeparators is the script code bitcoin-sv's original algorithm hashes
// (CTransactionSignatureSerializer::SerializeScriptCode): the bytes between
// OP_CODESEPARATORs, walking the script with GetOp. When GetOp stops at a truncated push,
// bitcoin-sv writes up to where it stopped, so the same happens here.
func withoutCodeSeparators(script []byte) []byte {
	var (
		out   []byte
		begin int
		pc    int
	)

	for {
		next, op, ok := scriptOp(script, pc)
		pc = next

		if !ok {
			break
		}

		if op == bscript.OpCODESEPARATOR {
			out = append(out, script[begin:pc-1]...)
			begin = pc
		}
	}

	if begin != len(script) {
		out = append(out, script[begin:pc]...)
	}

	return out
}

// scriptOp is bitcoin-sv's GetScriptOp: it reads the opcode at pc and skips its push data,
// returning where it stopped. On a truncated push it reports false, having moved past the
// opcode and any length bytes it could read, but not the data.
func scriptOp(script []byte, pc int) (next int, op byte, ok bool) {
	if pc >= len(script) {
		return pc, 0, false
	}

	op = script[pc]
	pc++

	if op > bscript.OpPUSHDATA4 {
		return pc, op, true
	}

	var size int

	switch op {
	case bscript.OpPUSHDATA1:
		if len(script)-pc < 1 {
			return pc, op, false
		}

		size = int(script[pc])
		pc++
	case bscript.OpPUSHDATA2:
		if len(script)-pc < 2 {
			return pc, op, false
		}

		size = int(binary.LittleEndian.Uint16(script[pc:]))
		pc += 2
	case bscript.OpPUSHDATA4:
		if len(script)-pc < 4 {
			return pc, op, false
		}

		size = int(binary.LittleEndian.Uint32(script[pc:]))
		pc += 4
	default:
		size = int(op)
	}

	if len(script)-pc < size {
		return pc, op, false
	}

	return pc + size, op, true
}

// TestSigHashSingleWithoutOutputIgnoresCodeSeparators covers the case TestSigHashVectors
// leaves out of its OP_CODESEPARATOR deviation check: SIGHASH_SINGLE on an input with no
// matching output gives the constant one whatever the script code, so stripping the
// separators changes nothing. No vector in sighash.json has this combination.
func TestSigHashSingleWithoutOutputIgnoresCodeSeparators(t *testing.T) {
	tx := bt.NewTx()
	require.NoError(t, tx.From("a6bb1b6a2cd4d8cf8ff3c1ea6af4a2a6dfcd5ae7f9b6ce6e4cb9e0bdb2ab0f6c", 0, "51", 0))
	require.NoError(t, tx.From("a6bb1b6a2cd4d8cf8ff3c1ea6af4a2a6dfcd5ae7f9b6ce6e4cb9e0bdb2ab0f6c", 1, "51", 0))
	require.NoError(t, tx.PayToAddress("1NRoySJ9Lvby6DuE2UQYnyT67AASwNZxGb", 1))

	scriptCode := []byte{bscript.Op1, bscript.OpCODESEPARATOR, bscript.Op1}
	hashType := uint32(sighash.Single)

	require.Equal(t, sigHashOneHex, sigHashHex(t, tx, 1, scriptCode, hashType, false))
	require.Equal(t, sigHashOneHex, sigHashHex(t, tx, 1, withoutCodeSeparators(scriptCode), hashType, false))
}
