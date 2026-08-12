package bsv

import (
	"bytes"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/daemon"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/test/utils/transactions"
	"github.com/bsv-blockchain/teranode/test/utils/wirepeer"
	"github.com/stretchr/testify/require"
)

// opNotIfUnlockingScript is upstream's b'\x64': a single OP_NOTIF as the whole
// unlocking script. It fails because OP_NOTIF pops a condition from an empty
// stack, which is what upstream expects code 16 and
// mandatory-script-verify-flag-failed for.
func opNotIfUnlockingScript() *bscript.Script {
	return bscript.NewFromBytes([]byte{0x64})
}

// TestBSVInvalidTxRequest ports invalidtxrequest.py, which spends an
// anyone-can-spend coinbase with a deliberately failing unlocking script and
// expects the node to reject it with code 16 and the reason
// mandatory-script-verify-flag-failed.
//
// The code half ports exactly: Teranode answers with a wire reject carrying
// RejectInvalid, which is code 16, naming the transaction. The reason half does
// not - every rejected transaction gets the fixed string "rejected". See the
// opaque-tx-reject-reason gap, which is the transaction twin of
// opaque-block-reject-reason and, unlike it, loses the detail twice over.
//
// Two adaptations, both from harness facts rather than choice:
//
//   - Upstream mines its own anyone-can-spend coinbase over P2P and then 100
//     blocks to mature it. Teranode's coinbases are P2PKH and CoinbaseMaturity is
//     1 under this settings context, so the port takes a spendable coinbase from
//     the daemon and pays it into an OP_TRUE output, exactly as the
//     bsv-genesis-pushonly.py port does. The 100 maturity blocks have no purpose
//     here.
//   - Upstream reads the rejection through ComparisonTestFramework, which compares
//     a node's P2P responses. This sends the transaction twice: once through the
//     propagation client, where the returned error is checked, and once over the
//     wire, where the reject message is. They are different outputs of the same
//     verdict and the port asserts both, because only one of them is what a real
//     peer would see and only the other carries a usable error type.
func TestBSVInvalidTxRequest(t *testing.T) {
	td := wirepeer.NewLegacyDaemon(t)
	defer td.Stop(t)

	// Two OP_TRUE outputs: one for each of the two submission paths below, so the
	// second is not a double spend of the first and each verdict stands alone.
	coinbaseTx := td.MineToMaturityAndGetSpendableCoinbaseTx(t, td.Ctx)

	fundingTx := td.CreateTransactionWithOptions(t,
		transactions.WithInput(coinbaseTx, 0),
		transactions.WithOutput(1e6, anyoneCanSpendScript()),
		transactions.WithOutput(1e6, anyoneCanSpendScript()),
	)

	require.NoError(t, td.PropagationClient.ProcessTransaction(td.Ctx, fundingTx),
		"the funding transaction should be accepted")
	td.WaitForBlockAssemblyToProcessTx(t, fundingTx.TxID())
	td.MineAndWait(t, 1)

	t.Run("the transaction is refused, as a typed validation error", func(t *testing.T) {
		tx := spendWithOpNotIf(t, fundingTx, 0)

		err := td.PropagationClient.ProcessTransaction(td.Ctx, tx)
		require.Error(t, err, "a transaction whose script fails must not be accepted")

		// Upstream asserts reject code 16, which is REJECT_INVALID. On this path the
		// equivalent is the error's own type, and it is the only part of the verdict
		// that is machine-readable here - see the subtest below for what a peer gets.
		require.True(t, errors.Is(err, errors.ErrTxInvalid),
			"expected a TX_INVALID error, got %v", err)

		t.Logf("propagation client reported: %q", err.Error())
	})

	t.Run("a wire peer is sent a reject naming the transaction", func(t *testing.T) {
		p := wirepeer.Connect(t, td)
		defer p.Close()

		tx := spendWithOpNotIf(t, fundingTx, 1)
		p.Send(t, asWireTx(t, tx))

		reject := p.WaitForReject(t, 30*time.Second)

		// Upstream: RejectResult(16, ...). This half is reproduced exactly.
		require.Equal(t, wire.CmdTx, reject.Cmd, "the reject should be about a transaction")
		require.Equal(t, wire.RejectInvalid, reject.Code,
			"upstream expects code 16, which is RejectInvalid")
		require.Equal(t, tx.TxID(), reject.Hash.String(),
			"the reject should name the transaction that was refused")

		t.Logf("wire reject: cmd=%q code=%v reason=%q", reject.Cmd, reject.Code, reject.Reason)

		// Upstream keeps using this connection afterwards, so the peer must survive
		// having sent an invalid transaction. Worth pinning: a node that banned for
		// this would break every later assertion in upstream's sequence.
		p.AssertStillConnected(t, 2*time.Second,
			"sending an invalid transaction must not cost the peer its connection")
	})

	// Neither submission may have been accepted. Checked after both, because the
	// reject says what the node told us and this says what the node did.
	requireNotInBlockAssembly(t, td, spendWithOpNotIf(t, fundingTx, 0).TxID())
	requireNotInBlockAssembly(t, td, spendWithOpNotIf(t, fundingTx, 1).TxID())
}

// spendWithOpNotIf builds upstream's failing transaction: fundingTx's vout spent
// with OP_NOTIF as the unlocking script.
//
// Built by hand rather than through td.CreateTransactionWithOptions for the same
// reason spendWithOpAdd is: that helper signs every input, and the point here is an
// unlocking script that is not a signature. Deterministic in its output, so calling
// it twice for the same vout yields the same txid - which is what lets the checks at
// the end of the test name transactions built earlier.
func spendWithOpNotIf(t *testing.T, fundingTx *bt.Tx, vout uint32) *bt.Tx {
	t.Helper()

	tx := bt.NewTx()

	require.NoError(t, tx.FromUTXOs(&bt.UTXO{
		TxIDHash:      fundingTx.TxIDChainHash(),
		Vout:          vout,
		LockingScript: fundingTx.Outputs[vout].LockingScript,
		Satoshis:      fundingTx.Outputs[vout].Satoshis,
	}), "add input spending funding output %d", vout)

	tx.Inputs[0].UnlockingScript = opNotIfUnlockingScript()

	// A fee of 1000 satoshis, so the transaction is refused for its script rather
	// than for paying nothing.
	tx.AddOutput(&bt.Output{
		Satoshis:      fundingTx.Outputs[vout].Satoshis - 1000,
		LockingScript: anyoneCanSpendScript(),
	})

	return tx
}

// asWireTx re-encodes a go-bt transaction as the wire message a peer would send.
func asWireTx(t *testing.T, tx *bt.Tx) *wire.MsgTx {
	t.Helper()

	msg := wire.NewMsgTx(1)
	require.NoError(t, msg.Bsvdecode(bytes.NewReader(tx.Bytes()), wire.ProtocolVersion, wire.BaseEncoding),
		"re-encode transaction %s as a wire message", tx.TxID())

	return msg
}

// requireNotInBlockAssembly asserts the node is not holding the transaction for
// mining. Teranode has no mempool; getrawmempool reports block assembly's pending
// hashes, which is the closest equivalent - see tryRawMempool.
func requireNotInBlockAssembly(t *testing.T, td *daemon.TestDaemon, txid string) {
	t.Helper()

	require.NotContains(t, tryRawMempool(td), txid,
		"a transaction whose script failed must not be queued for mining")
}
