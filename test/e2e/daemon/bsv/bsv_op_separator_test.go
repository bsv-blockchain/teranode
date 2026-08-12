package bsv

import (
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/sighash"
	bec "github.com/bsv-blockchain/go-sdk/primitives/ec"
	"github.com/bsv-blockchain/teranode/daemon"
	"github.com/bsv-blockchain/teranode/test/utils/transactions"
	"github.com/bsv-blockchain/teranode/test/utils/wirepeer"
	"github.com/stretchr/testify/require"
)

// opSeparatorSignings is upstream's n_sgnings: how many public keys the locking
// script demands, and so how many OP_CODESEPARATORs minus one it contains. Kept at
// upstream's 5, since the whole question is whether each signature is checked
// against a different subscript and one or two would make a weaker case.
const opSeparatorSignings = 5

// TestBSVOpSeparator ports bsv-op-separator.py, which builds a locking script of
// the form
//
//	<pk1> OP_CHECKSIGVERIFY OP_CODESEPARATOR <pk2> OP_CHECKSIGVERIFY OP_CODESEPARATOR ... <pkN> OP_CHECKSIG
//
// and then spends it with N signatures. The point is what OP_CODESEPARATOR does to
// the sighash: each OP_CHECKSIG(VERIFY) signs not the whole script but the subscript
// running from the most recently executed OP_CODESEPARATOR to the end. So the N
// signatures are over N different scriptCodes, and a node that ignored
// OP_CODESEPARATOR - or applied it at the wrong offset - would reject the spend.
//
// Upstream asserts only that the spend reaches the mempool. That is the whole test:
// if the sighashes were computed over the wrong subscripts the signatures would not
// verify and nothing would arrive. This port asserts the same thing against block
// assembly, and adds the negative control upstream has no need for, since a passing
// spend is only evidence about OP_CODESEPARATOR if signing the WRONG subscript fails.
//
// Signing is done by hand. go-bt's signing helpers use the input's PreviousTxScript
// as the scriptCode, which is exactly the right hook: the port swaps in each
// subscript in turn, takes the sighash, and restores the real locking script
// afterwards. That is the same approach test/consensus/test_builder.go takes in
// PushSeparatorSigs.
func TestBSVOpSeparator(t *testing.T) {
	td := wirepeer.NewLegacyDaemon(t)
	defer td.Stop(t)

	keys := separatorKeys(t, opSeparatorSignings)
	segments := separatorScriptSegments(t, keys)
	locking := concatScripts(segments...)

	t.Logf("locking script (%d keys, %d bytes): %x", len(keys), len(*locking), *locking)

	sepTx := fundSeparatorOutput(t, td, locking)

	t.Run("a spend signed over the correct subscripts is accepted", func(t *testing.T) {
		// Upstream: spend_separator_tx, then wait for the mempool to hold one entry.
		spend := spendSeparatorOutput(t, sepTx, keys, separatorSubscripts(segments))

		require.NoError(t, td.PropagationClient.ProcessTransaction(td.Ctx, spend),
			"a spend whose signatures follow OP_CODESEPARATOR should be accepted")

		td.WaitForBlockAssemblyToProcessTx(t, spend.TxID())
		require.Contains(t, tryRawMempool(td), spend.TxID(),
			"the accepted spend should be queued for mining")
	})

	t.Run("a spend signed over the whole script every time is refused", func(t *testing.T) {
		// No upstream counterpart, and the reason the subtest above means anything.
		// Every signature is taken over the full locking script, which is what a
		// node that ignored OP_CODESEPARATOR would expect. Only the first signature
		// is then correct, so the second OP_CHECKSIGVERIFY must fail.
		wrong := make([]*bscript.Script, len(keys))
		for i := range wrong {
			wrong[i] = locking
		}

		spend := spendSeparatorOutput(t, sepTx, keys, wrong)

		require.Error(t, td.PropagationClient.ProcessTransaction(td.Ctx, spend),
			"CONTROL: signing every input over the whole script must fail, or the test above proves "+
				"nothing about OP_CODESEPARATOR")

		require.NotContains(t, tryRawMempool(td), spend.TxID(),
			"the refused spend must not be queued for mining")
	})
}

// separatorKeys builds n deterministic keys, mirroring upstream's
// set_secretbytes(b"x" * (i + 1)) so the two files describe the same scripts.
func separatorKeys(t *testing.T, n int) []*bec.PrivateKey {
	t.Helper()

	keys := make([]*bec.PrivateKey, n)

	for i := range keys {
		secret := make([]byte, i+1)
		for j := range secret {
			secret[j] = 'x'
		}

		key, _ := bec.PrivateKeyFromBytes(secret)
		require.NotNil(t, key, "derive key %d", i)

		keys[i] = key
	}

	return keys
}

// separatorScriptSegments returns the locking script in the pieces the subscripts
// are cut from: each of the first n-1 keys contributes
// "<pk> OP_CHECKSIGVERIFY OP_CODESEPARATOR", and the last contributes
// "<pk> OP_CHECKSIG".
//
// Returned as segments rather than one script because the subscripts are exactly
// its suffixes, and building them here is both simpler and less error-prone than
// parsing the assembled script back apart to find the separators.
func separatorScriptSegments(t *testing.T, keys []*bec.PrivateKey) []*bscript.Script {
	t.Helper()

	segments := make([]*bscript.Script, len(keys))

	for i, key := range keys {
		seg := &bscript.Script{}

		require.NoError(t, seg.AppendPushData(key.PubKey().Compressed()), "push pubkey %d", i)

		if i == len(keys)-1 {
			require.NoError(t, seg.AppendOpcodes(bscript.OpCHECKSIG), "append OP_CHECKSIG")
		} else {
			require.NoError(t, seg.AppendOpcodes(bscript.OpCHECKSIGVERIFY, bscript.OpCODESEPARATOR),
				"append OP_CHECKSIGVERIFY OP_CODESEPARATOR")
		}

		segments[i] = seg
	}

	return segments
}

// separatorSubscripts returns the scriptCode each signature must be taken over:
// subscript i is the locking script from just after the i-th OP_CODESEPARATOR to
// the end, with subscript 0 being the whole script because no separator has been
// executed when the first OP_CHECKSIGVERIFY runs.
func separatorSubscripts(segments []*bscript.Script) []*bscript.Script {
	subscripts := make([]*bscript.Script, len(segments))
	for i := range segments {
		subscripts[i] = concatScripts(segments[i:]...)
	}

	return subscripts
}

// concatScripts joins scripts end to end.
func concatScripts(parts ...*bscript.Script) *bscript.Script {
	size := 0
	for _, p := range parts {
		size += len(*p)
	}

	out := make([]byte, 0, size)
	for _, p := range parts {
		out = append(out, *p...)
	}

	return bscript.NewFromBytes(out)
}

// fundSeparatorOutput pays a spendable coinbase into the separator script and mines
// it, returning the transaction that created the output.
func fundSeparatorOutput(t *testing.T, td *daemon.TestDaemon, locking *bscript.Script) *bt.Tx {
	t.Helper()

	coinbaseTx := td.MineToMaturityAndGetSpendableCoinbaseTx(t, td.Ctx)

	tx := td.CreateTransactionWithOptions(t,
		transactions.WithInput(coinbaseTx, 0),
		transactions.WithOutput(1e6, locking),
	)

	require.NoError(t, td.PropagationClient.ProcessTransaction(td.Ctx, tx),
		"the transaction creating the OP_CODESEPARATOR output should be accepted")
	td.WaitForBlockAssemblyToProcessTx(t, tx.TxID())

	// Upstream mines here too, ten blocks of it. One is enough - nothing about this
	// depends on depth, only on the output being confirmed.
	td.MineAndWait(t, 1)

	return tx
}

// spendSeparatorOutput builds a transaction spending sepTx's output, signing once
// per key over the matching entry in scriptCodes.
//
// scriptCodes is a parameter rather than derived so the test can pass deliberately
// wrong ones; with the correct subscripts this is upstream's spend_separator_tx.
func spendSeparatorOutput(t *testing.T, sepTx *bt.Tx, keys []*bec.PrivateKey, scriptCodes []*bscript.Script) *bt.Tx {
	t.Helper()

	require.Len(t, scriptCodes, len(keys), "one scriptCode per key")

	tx := bt.NewTx()

	require.NoError(t, tx.FromUTXOs(&bt.UTXO{
		TxIDHash:      sepTx.TxIDChainHash(),
		Vout:          0,
		LockingScript: sepTx.Outputs[0].LockingScript,
		Satoshis:      sepTx.Outputs[0].Satoshis,
	}), "add input spending the separator output")

	tx.AddOutput(&bt.Output{
		Satoshis:      sepTx.Outputs[0].Satoshis - 2000,
		LockingScript: anyoneCanSpendScript(),
	})

	const shf = sighash.AllForkID

	signatures := make([][]byte, len(keys))

	for i, key := range keys {
		// The hook: CalcInputSignatureHash takes its scriptCode from
		// PreviousTxScript, so pointing that at the subscript is what makes each
		// signature cover a different part of the script.
		tx.Inputs[0].PreviousTxScript = scriptCodes[i]

		sh, err := tx.CalcInputSignatureHash(0, shf)
		require.NoError(t, err, "sighash for key %d", i)

		sig, err := key.Sign(sh)
		require.NoError(t, err, "sign for key %d", i)

		signatures[i] = append(sig.Serialize(), byte(shf))
	}

	// Put the real locking script back, or the transaction carries a scriptCode
	// that was only ever a signing device.
	tx.Inputs[0].PreviousTxScript = sepTx.Outputs[0].LockingScript

	// Upstream: CScript(reversed(sign_list)). The first OP_CHECKSIGVERIFY consumes
	// the top of the stack, so the first key's signature has to be pushed last.
	unlocking := &bscript.Script{}

	for i := len(signatures) - 1; i >= 0; i-- {
		require.NoError(t, unlocking.AppendPushData(signatures[i]), "push signature %d", i)
	}

	tx.Inputs[0].UnlockingScript = unlocking

	return tx
}
