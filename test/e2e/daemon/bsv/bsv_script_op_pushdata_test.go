package bsv

import (
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/teranode/test/utils/wirepeer"
	"github.com/stretchr/testify/require"
)

// Port of bitcoin-sv's bsv-script-op_pushdata.py.
//
// Upstream checks that the three OP_PUSHDATA encodings round-trip: a locking
// script pushes a large byte array, asks the interpreter for its size with
// OP_SIZE, and compares that against the length it expects. If the push were
// encoded or decoded wrongly, OP_SIZE would disagree and OP_EQUALVERIFY would
// fail the script. Each case then spends the output it created, so the script is
// exercised as a real unlocking, not merely accepted into a transaction.
//
// THE SCRIPT IS SELF-CONTAINED, which is why this port needs no signing. It ends
// in OP_TRUE and consumes only what it pushed, but OP_2DROP/OP_DROP need two more
// items on the stack than the script itself supplies, so the unlocking script has
// to provide them. Upstream gets them incidentally, from the signature and public
// key its signed spend pushes. This port pushes two OP_1s, which is the same
// stack shape without a signature:
//
//	[1, 1]                      unlocking script
//	[1, 1, data]                the big push
//	[1, 1, data, len]           OP_SIZE
//	[1, 1, data, len, expected] the expected-length push
//	[1, 1, data]                OP_EQUALVERIFY consumes len and expected
//	[1]                         OP_2DROP
//	[]                          OP_DROP
//	[1]                         OP_TRUE
//
// GENESIS MUST BE ACTIVE, and upstream says so by setting
// -genesisactivationheight=100 and mining 120 blocks. Before Genesis a single
// script element may not exceed 520 bytes, so the OP_PUSHDATA2 and OP_PUSHDATA4
// cases are not merely large, they are illegal. Under SETTINGS_CONTEXT=test
// Genesis activates at 10000, so this port moves it down rather than mining to it.
const (
	// pushData1Size is upstream's 0xff - the largest push OP_PUSHDATA1 can carry.
	pushData1Size = 0xff

	// pushData2Size is upstream's 0xffff - the largest OP_PUSHDATA2 can carry, so
	// one more byte forces OP_PUSHDATA4.
	pushData2Size = 0xffff

	// pushData4Size is this port's OP_PUSHDATA4 case, and it is NOT upstream's.
	//
	// Upstream uses 0x3b110000 - about 991 MB - and to do that it lifts every
	// policy limit it can: -maxtxsizepolicy=0, -maxscriptsizepolicy=0,
	// -maxstackmemoryusagepolicy=0, -maxmempool=3GB. Teranode's defaults are
	// maxscriptsizepolicy 500000 and maxtxsizepolicy 10485760, so that size is far
	// out of reach, and a ~1 GB transaction is not something an in-process
	// TestDaemon can carry regardless of policy.
	//
	// What upstream's magnitude buys is a test of the SIZE LIMITS at that scale.
	// What the three cases have in common, and what the script's name is about, is
	// the ENCODING: which push opcode is chosen and whether OP_SIZE agrees with it.
	// 0x20000 is above OP_PUSHDATA2's ceiling, so it exercises the OP_PUSHDATA4
	// path on exactly the same code, and sits inside Teranode's default policy so
	// the case measures the encoding rather than a policy refusal. The magnitude
	// half is recorded as a waived assertion in registry.yaml.
	pushData4Size = 0x20000

	// pushDataActivation is where Genesis activates for this port. The pool's
	// funding lands within the first few blocks, so 1 puts every transaction here
	// on the post-Genesis side.
	pushDataActivation = 1

	// pushDataFee is deducted from each spend so the transaction pays its way.
	pushDataFee = uint64(5000)
)

func TestBSVScriptOpPushData(t *testing.T) {
	td := wirepeer.NewLegacyDaemon(t, wirepeer.WithGenesisActivationHeight(pushDataActivation))
	defer td.Stop(t)

	require.EqualValues(t, pushDataActivation, td.Settings.ChainCfgParams.GenesisActivationHeight,
		"Genesis must be active or the two larger pushes exceed the pre-Genesis "+
			"520-byte script element limit and the port measures that instead")

	// Recorded as assertions because the two larger cases depend on them: if either
	// default tightened, those cases would be refused on policy and the port would
	// be measuring a limit rather than an encoding.
	require.GreaterOrEqual(t, int(td.Settings.Policy.MaxScriptSizePolicy), pushData4Size+64,
		"maxscriptsizepolicy must leave room for the largest push this port makes")

	pool := newOutputPool(t, td, 3)

	for _, tc := range []struct {
		name string
		size int
	}{
		{"OP_PUSHDATA1", pushData1Size},
		{"OP_PUSHDATA2", pushData2Size},
		{"OP_PUSHDATA4", pushData4Size},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fundingTx, vout := pool.take(t)

			// Upstream's tx0: the output carrying the big push.
			locking := sizeCheckingScript(t, tc.size)
			tx0 := spendToScriptNoSig(t, fundingTx, vout, locking)

			require.NoError(t, td.PropagationClient.ProcessTransaction(td.Ctx, tx0),
				"a transaction whose output carries a %d-byte push should be accepted", tc.size)
			td.WaitForBlockAssemblyToProcessTx(t, tx0.TxID())

			// Upstream's tx1: spending it, which is what actually runs the script.
			tx1 := bt.NewTx()

			require.NoError(t, tx1.FromUTXOs(&bt.UTXO{
				TxIDHash:      tx0.TxIDChainHash(),
				Vout:          0,
				LockingScript: tx0.Outputs[0].LockingScript,
				Satoshis:      tx0.Outputs[0].Satoshis,
			}), "add an input spending the big-push output")

			// The two stack items OP_2DROP and OP_DROP need; see the file comment.
			tx1.Inputs[0].UnlockingScript = bscript.NewFromBytes(
				[]byte{bscript.Op1, bscript.Op1})

			tx1.AddOutput(&bt.Output{
				Satoshis:      tx0.Outputs[0].Satoshis - pushDataFee,
				LockingScript: anyoneCanSpendScript(),
			})

			require.NoError(t, td.PropagationClient.ProcessTransaction(td.Ctx, tx1),
				"spending the %d-byte push should succeed - OP_SIZE must agree with the "+
					"push encoding or OP_EQUALVERIFY fails the script", tc.size)
			td.WaitForBlockAssemblyToProcessTx(t, tx1.TxID())
		})
	}
}

// sizeCheckingScript builds upstream's scriptPubKey: push n bytes, ask OP_SIZE
// for the length, push the length it should be, and require the two to match.
func sizeCheckingScript(t *testing.T, n int) *bscript.Script {
	t.Helper()

	out := make([]byte, 0, n+32)
	out = append(out, pushData(bytes42(n))...)
	out = append(out, bscript.OpSIZE)
	out = append(out, pushData(scriptNum(int64(n)))...)
	out = append(out, bscript.OpEQUALVERIFY, bscript.Op2DROP, bscript.OpDROP, bscript.OpTRUE)

	return bscript.NewFromBytes(out)
}

// bytes42 is upstream's bytearray([42] * n).
func bytes42(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = 42
	}

	return b
}

// pushData prefixes data with the push opcode Bitcoin's encoding rules select for
// its length - which is the mechanism this whole script is about, so the port
// chooses it explicitly rather than trusting a builder.
func pushData(data []byte) []byte {
	n := len(data)

	switch {
	case n < int(bscript.OpPUSHDATA1):
		return append([]byte{byte(n)}, data...)
	case n <= 0xff:
		return append([]byte{bscript.OpPUSHDATA1, byte(n)}, data...)
	case n <= 0xffff:
		return append([]byte{bscript.OpPUSHDATA2, byte(n), byte(n >> 8)}, data...)
	default:
		return append([]byte{bscript.OpPUSHDATA4,
			byte(n), byte(n >> 8), byte(n >> 16), byte(n >> 24)}, data...)
	}
}

// scriptNum encodes n the way OP_SIZE pushes it: minimal little-endian, with an
// extra zero byte when the top bit would otherwise read as a sign. Upstream
// hand-writes the same values and comments each one "extra byte for sign bit".
func scriptNum(n int64) []byte {
	if n == 0 {
		return nil
	}

	out := make([]byte, 0, 8)
	for v := n; v > 0; v >>= 8 {
		out = append(out, byte(v&0xff))
	}

	if out[len(out)-1]&0x80 != 0 {
		out = append(out, 0)
	}

	return out
}

// spendToScriptNoSig spends an anyone-can-spend output into a single output
// carrying the given locking script, with an empty unlocking script.
func spendToScriptNoSig(t *testing.T, parent *bt.Tx, vout uint32, locking *bscript.Script) *bt.Tx {
	t.Helper()

	tx := bt.NewTx()

	require.NoError(t, tx.FromUTXOs(&bt.UTXO{
		TxIDHash:      parent.TxIDChainHash(),
		Vout:          vout,
		LockingScript: parent.Outputs[vout].LockingScript,
		Satoshis:      parent.Outputs[vout].Satoshis,
	}), "add input spending %s:%d", parent.TxID(), vout)

	tx.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{})

	tx.AddOutput(&bt.Output{
		Satoshis:      parent.Outputs[vout].Satoshis - pushDataFee,
		LockingScript: locking,
	})

	return tx
}
