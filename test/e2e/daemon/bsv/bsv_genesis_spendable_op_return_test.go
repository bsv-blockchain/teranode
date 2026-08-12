package bsv

import (
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/teranode/daemon"
	"github.com/bsv-blockchain/teranode/test/utils/transactions"
	"github.com/bsv-blockchain/teranode/test/utils/wirepeer"
	"github.com/stretchr/testify/require"
)

// spendableOpReturnActivation is where the port puts the Genesis fork for the
// post-Genesis half. Upstream uses 10; anything at or below the height the funded
// output is spent at will do, and 2 keeps the chain short. See
// wirepeer.WithGenesisActivationHeight for why moving it is safe.
const spendableOpReturnActivation = 2

// opTrueOpReturnScript is upstream's OP_TRUE_OP_RETURN_SCRIPT:
// CScript([OP_TRUE, OP_RETURN, b"xxx"]), which serialises to 516a03787878.
//
// The leading OP_TRUE is the whole point. A locking script that STARTS with
// OP_RETURN is an ordinary data output and unspendable in every era; this one puts
// a satisfied condition in front of the OP_RETURN, so what happens depends entirely
// on what OP_RETURN means - a hard failure before Genesis, and a clean end of script
// after it.
func opTrueOpReturnScript() *bscript.Script {
	return bscript.NewFromBytes([]byte{bscript.OpTRUE, bscript.OpRETURN, 0x03, 'x', 'x', 'x'})
}

// TestBSVGenesisSpendableOpReturn ports bsv-genesis-spendable-op-return.py.
//
// Upstream pays a coinbase into an OP_TRUE OP_RETURN output with Genesis active and
// then checks three things through the REST interface: the transaction is in the
// mempool, it is the only thing there, and getutxos reports the output's
// scriptPubKey verbatim. Teranode has no mempool and no /rest endpoints, so the
// second and third are waived - see the registry entry.
//
// What replaces them is better aimed at what the script is called: the port spends
// the output. Upstream never does, so this is beyond its assertions, and it is the
// only way available here to show the node stored something usable rather than just
// something present.
//
// It is asserted on both sides of the fork, which upstream also does not do. One
// side alone would show that the output is spendable without showing that Genesis is
// what makes it so - and the pre-Genesis half is the half that could silently
// regress into accepting a spend it must refuse. Measured: refused below the fork,
// accepted at or above it.
func TestBSVGenesisSpendableOpReturn(t *testing.T) {
	t.Run("after Genesis the output is created and can be spent", func(t *testing.T) {
		td := wirepeer.NewLegacyDaemon(t, wirepeer.WithGenesisActivationHeight(spendableOpReturnActivation))
		defer td.Stop(t)

		require.EqualValues(t, spendableOpReturnActivation, td.Settings.ChainCfgParams.GenesisActivationHeight,
			"the Genesis override must have taken, or this subtest tests the wrong era")

		dataTx := payToOpTrueOpReturn(t, td)

		// Upstream: assert the transaction is in the mempool. Teranode has none, so
		// the equivalent is block assembly's pending set - see tryRawMempool.
		require.Contains(t, tryRawMempool(td), dataTx.TxID(),
			"the transaction paying to an OP_TRUE OP_RETURN output should be queued for mining")

		td.MineAndWait(t, 1)

		// Beyond upstream, and the point of the script's name: post-Genesis the
		// OP_RETURN ends the script with OP_TRUE's result still standing, so the
		// output is spendable with no unlocking script at all.
		spend := spendOpReturnOutput(t, dataTx)

		require.NoError(t, td.PropagationClient.ProcessTransaction(td.Ctx, spend),
			"after Genesis an OP_TRUE OP_RETURN output should be spendable")

		td.WaitForBlockAssemblyToProcessTx(t, spend.TxID())
		require.Contains(t, tryRawMempool(td), spend.TxID(),
			"the spending transaction should be queued for mining")
	})

	t.Run("before Genesis the same output cannot be spent", func(t *testing.T) {
		// Default settings leave Genesis at 10000 under this context, so a chain a
		// few blocks long is comfortably below the fork.
		td := wirepeer.NewLegacyDaemon(t)
		defer td.Stop(t)

		require.Greater(t, td.Settings.ChainCfgParams.GenesisActivationHeight, uint32(100),
			"this subtest needs Genesis to be far above the heights it mines")

		// Creating the output is accepted in both eras - measured. Upstream needs
		// -acceptnonstdtxn=1 for this, which suggests bitcoin-sv would refuse it as
		// non-standard by default; Teranode accepts it either way, so no contrast is
		// asserted here. The contrast that does exist is in spending it.
		dataTx := payToOpTrueOpReturn(t, td)

		td.MineAndWait(t, 1)

		spend := spendOpReturnOutput(t, dataTx)

		err := td.PropagationClient.ProcessTransaction(td.Ctx, spend)
		require.Error(t, err,
			"before Genesis OP_RETURN must make the output unspendable")

		t.Logf("pre-Genesis spend refused with: %v", err)

		require.NotContains(t, tryRawMempool(td), spend.TxID(),
			"a refused spend must not be queued for mining")
	})
}

// payToOpTrueOpReturn funds an OP_TRUE OP_RETURN output from a spendable coinbase
// and returns the transaction that created it, having waited for the node to take it.
//
// Upstream mines its own coinbase to a key it holds and spends that directly.
// Teranode coinbases are P2PKH, so this uses the daemon's own spendable coinbase and
// pays it onward, which is the funding pattern the other Genesis port uses.
func payToOpTrueOpReturn(t *testing.T, td *daemon.TestDaemon) *bt.Tx {
	t.Helper()

	coinbaseTx := td.MineToMaturityAndGetSpendableCoinbaseTx(t, td.Ctx)

	tx := td.CreateTransactionWithOptions(t,
		transactions.WithInput(coinbaseTx, 0),
		transactions.WithOutput(1e6, opTrueOpReturnScript()),
	)

	require.NoError(t, td.PropagationClient.ProcessTransaction(td.Ctx, tx),
		"paying into an OP_TRUE OP_RETURN output should be accepted")
	td.WaitForBlockAssemblyToProcessTx(t, tx.TxID())

	return tx
}

// spendOpReturnOutput builds a transaction spending dataTx's OP_RETURN output with
// an empty unlocking script.
//
// Empty on purpose: the locking script satisfies itself post-Genesis, so anything
// pushed here would only obscure which script did the work. Built by hand because
// td.CreateTransactionWithOptions signs its inputs, and a signature is exactly what
// this must not carry.
func spendOpReturnOutput(t *testing.T, dataTx *bt.Tx) *bt.Tx {
	t.Helper()

	tx := bt.NewTx()

	require.NoError(t, tx.FromUTXOs(&bt.UTXO{
		TxIDHash:      dataTx.TxIDChainHash(),
		Vout:          0,
		LockingScript: dataTx.Outputs[0].LockingScript,
		Satoshis:      dataTx.Outputs[0].Satoshis,
	}), "add input spending the OP_RETURN output")

	tx.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{})

	tx.AddOutput(&bt.Output{
		Satoshis:      dataTx.Outputs[0].Satoshis - 1000,
		LockingScript: anyoneCanSpendScript(),
	})

	return tx
}
