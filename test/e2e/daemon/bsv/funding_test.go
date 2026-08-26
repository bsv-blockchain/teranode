package bsv

import (
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/teranode/daemon"
	"github.com/bsv-blockchain/teranode/test/utils/transactions"
	"github.com/stretchr/testify/require"
)

// outputPool stands in for bitcoin-sv's listunspent.
//
// WHY THIS EXISTS, and what it is not. Many upstream scripts need "several
// spendable outputs" and get them by asking the node's wallet: listunspent
// returns whatever the wallet holds, and the script makes one transaction per
// entry. Teranode declines every wallet RPC by design - getnewaddress,
// sendtoaddress, listunspent and signrawtransaction all route to handleAskWallet
// in services/rpc/Server.go, because key custody lives outside the node - so
// there is nothing to ask.
//
// What those scripts actually need is not a wallet. It is a set of outputs the
// test can spend, tracked so no two spends collide. That is bookkeeping, and it
// is all this type does: fund N outputs once, then hand them out one at a time.
//
// This is the whole of what the `funding-shim` prerequisite turned out to be.
// Signing was the part that looked hard and is not: transactions.WithInput takes
// a per-input private key, so an output paid to a freshly generated address can
// be spent with that address's own key. TestBSVGetData proves that end to end
// against a real P2PKH output. So a port needs this pool, and does not need a
// wallet.
//
// NOT USABLE UNDER STANDARD-ONLY POLICY, established while porting
// dustrelayfee.py. OP_TRUE is itself a non-standard output script, so with
// RequireStandard=true and AcceptNonStdOutputs=false the pool's own funding
// transaction is refused with TX_POLICY (39) before any port assertion runs. A
// port that needs upstream's -acceptnonstdtxn=0 has to fund with P2PKH instead -
// see TestBSVDustRelayFee for that shape.
//
// The outputs are anyone-can-spend. That is deliberate: it keeps the spends
// signature-free, which matters when a port is counting sigops or measuring
// message sizes and does not want a signature's bytes in the arithmetic. A port
// that specifically needs keyed spending should build them with
// WithP2PKHOutputs and spend with WithInput(tx, vout, key) instead - see
// TestBSVGetData for that shape.
type outputPool struct {
	tx   *bt.Tx
	next uint32
}

// newOutputPool funds n anyone-can-spend outputs in one transaction, confirms
// them, and returns a pool that hands them out.
//
// One transaction rather than n is deliberate: it costs one propagation and one
// block regardless of n, where funding each output separately would cost n of
// each and dominate the runtime of any port using more than a handful.
func newOutputPool(t *testing.T, td *daemon.TestDaemon, n int) *outputPool {
	t.Helper()

	require.Positive(t, n, "a pool needs at least one output")

	coinbaseTx := td.MineToMaturityAndGetSpendableCoinbaseTx(t, td.Ctx)

	// The coinbase carries the full subsidy; leave headroom for the fee rather
	// than dividing it exactly, so a port does not have to reason about whether
	// the funding transaction itself can pay its way.
	perOutput := (coinbaseTx.Outputs[0].Satoshis - 1e6) / uint64(n)

	opts := make([]transactions.TxOption, 0, n+1)
	opts = append(opts, transactions.WithInput(coinbaseTx, 0))

	for range n {
		opts = append(opts, transactions.WithOutput(perOutput, anyoneCanSpendScript()))
	}

	tx := td.CreateTransactionWithOptions(t, opts...)

	require.NoError(t, td.PropagationClient.ProcessTransaction(td.Ctx, tx),
		"the pool's funding transaction should be accepted")
	td.WaitForBlockAssemblyToProcessTx(t, tx.TxID())
	td.MineAndWait(t, 1)

	return &outputPool{tx: tx}
}

// take hands out the next unused output, as one entry of upstream's listunspent.
// It fails rather than wrapping around, so a port that outgrows its pool says so
// instead of silently double-spending an output it already used.
func (p *outputPool) take(t *testing.T) (*bt.Tx, uint32) {
	t.Helper()

	require.Less(t, int(p.next), len(p.tx.Outputs),
		"output pool exhausted after %d outputs; size the pool to the port", p.next)

	vout := p.next
	p.next++

	return p.tx, vout
}

// remaining reports how many outputs are still unspent.
func (p *outputPool) remaining() int {
	return len(p.tx.Outputs) - int(p.next)
}
