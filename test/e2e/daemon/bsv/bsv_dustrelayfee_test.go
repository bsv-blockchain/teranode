package bsv

import (
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/unlocker"
	bec "github.com/bsv-blockchain/go-sdk/primitives/ec"
	"github.com/bsv-blockchain/teranode/daemon"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/test/utils/transactions"
	"github.com/bsv-blockchain/teranode/test/utils/wirepeer"
	"github.com/stretchr/testify/require"
)

// Port of bitcoin-sv's dustrelayfee.py.
//
// Upstream verifies that an output below the dust threshold is refused and one
// at the threshold is accepted, with the node restarted under -acceptnonstdtxn=0
// so standardness policy is in force. Its threshold is one satoshi, so in
// practice the test is: a ZERO-satoshi spendable output is dust and refused; a
// one-satoshi output is not and is accepted.
//
// POLICY MUST BE SET, and this is the whole reason the port has two halves.
// Upstream's -acceptnonstdtxn=0 corresponds to Teranode's RequireStandard=true
// with AcceptNonStdOutputs=false. Teranode's DEFAULTS are the opposite - the
// permissive BSV posture, where "non-standard" is a misnomer for perfectly valid
// scripts - and under those defaults a zero-satoshi output is accepted. That is
// not a divergence, it is a different configuration, and the port asserts both so
// the distinction is on the record rather than implied:
//
//	default (permissive):  0 satoshis accepted
//	standard-only:         0 satoshis refused, 1 satoshi accepted  <- upstream's case
//
// A MEASUREMENT THAT CORRECTED A WRONG GUESS, recorded because the wrong guess
// was reasonable. services/validator/Validator.go declares
// DustLimit = uint64(1) with a comment describing exactly this rule - and the
// constant is referenced NOWHERE else in the tree. That is the same shape as the
// legacy-whitelist-inert and tx-validation-timeouts-inert defects, so it looked
// like a third instance of a rule that is documented and never enforced.
//
// It is not. Measured both ways: under standard-only policy the zero-satoshi
// output is refused with TX_POLICY (39). The rule is enforced inside GoBDK, and
// the Go constant is merely unused. Worth knowing before anyone "fixes" the dead
// constant by wiring it up and double-enforces the rule.
//
// THE POOL CANNOT FUND THIS TEST, which is why it builds its own funding.
// outputPool hands out anyone-can-spend outputs, and OP_TRUE is itself
// non-standard: under RequireStandard the pool's own funding transaction is
// refused with the same TX_POLICY (39). Any port that needs standard-only policy
// has to fund with P2PKH, as this one does.
func TestBSVDustRelayFee(t *testing.T) {
	t.Run("standard-only refuses a dust output", func(t *testing.T) {
		td, key, fund := dustFixture(t, true)
		defer td.Stop(t)

		// Upstream: outputs[addr] = amount_is_dust; assert_raises_rpc_error(-26, "64: dust", ...)
		err := td.PropagationClient.ProcessTransaction(td.Ctx, dustSpend(t, td, key, fund, 0, 0))
		require.Error(t, err, "a zero-satoshi output is dust and must be refused under "+
			"standard-only policy, as upstream asserts with -acceptnonstdtxn=0")
		require.Contains(t, err.Error(), "TX_POLICY",
			"the refusal should be a policy refusal")

		// Upstream: outputs[addr] = amount_is_not_dust; txid = sendrawtransaction(...)
		require.NoError(t,
			td.PropagationClient.ProcessTransaction(td.Ctx, dustSpend(t, td, key, fund, 1, 1)),
			"a one-satoshi output meets the dust threshold and must be accepted")
	})

	// Not an upstream case. It records that the refusal above is policy rather than
	// consensus, and pins Teranode's default posture: if this ever starts failing,
	// the BSV defaults have changed and the subtest above is no longer testing a
	// deliberate policy choice.
	t.Run("bsv default policy accepts the same output", func(t *testing.T) {
		td, key, fund := dustFixture(t, false)
		defer td.Stop(t)

		require.NoError(t,
			td.PropagationClient.ProcessTransaction(td.Ctx, dustSpend(t, td, key, fund, 0, 0)),
			"under Teranode's permissive defaults a zero-satoshi output is accepted; "+
				"the refusal in the other subtest is a policy choice, not consensus")
	})
}

// dustFixture starts a daemon, optionally under upstream's standard-only policy,
// and funds two P2PKH outputs to a key the test owns.
func dustFixture(t *testing.T, standardOnly bool) (*daemon.TestDaemon, *bec.PrivateKey, *bt.Tx) {
	t.Helper()

	td := wirepeer.NewLegacyDaemon(t, func(s *settings.Settings) {
		if standardOnly {
			s.Policy.RequireStandard = true
			s.Policy.AcceptNonStdOutputs = false
		}
	})

	require.Equal(t, standardOnly, td.Settings.Policy.RequireStandard,
		"the standardness override should have reached the daemon; without it this "+
			"measures the permissive default and the dust case asserts nothing")

	key, err := bec.NewPrivateKey()
	require.NoError(t, err, "generate the key the fixture's outputs pay to")

	coinbaseTx := td.MineToMaturityAndGetSpendableCoinbaseTx(t, td.Ctx)

	fund := td.CreateTransactionWithOptions(t,
		transactions.WithInput(coinbaseTx, 0),
		transactions.WithP2PKHOutputs(2, 10e7, key.PubKey()),
	)

	require.NoError(t, td.PropagationClient.ProcessTransaction(td.Ctx, fund),
		"the P2PKH funding transaction should be accepted under either policy")
	td.WaitForBlockAssemblyToProcessTx(t, fund.TxID())
	td.MineAndWait(t, 1)

	return td, key, fund
}

// dustSpend spends one funded output into a P2PKH output of exactly `amount`
// satoshis, with the remainder going to a second P2PKH output.
//
// Built by hand rather than through CreateTransactionWithOptions because the
// point of the test is the exact value of one output, and a change-calculating
// builder would not leave it at zero.
func dustSpend(t *testing.T, td *daemon.TestDaemon, key *bec.PrivateKey, fund *bt.Tx,
	vout uint32, amount uint64) *bt.Tx {
	t.Helper()

	p2pkh, err := bscript.NewP2PKHFromPubKeyBytes(key.PubKey().Compressed())
	require.NoError(t, err, "build the P2PKH script the outputs pay to")

	tx := bt.NewTx()

	require.NoError(t, tx.FromUTXOs(&bt.UTXO{
		TxIDHash:      fund.TxIDChainHash(),
		Vout:          vout,
		LockingScript: fund.Outputs[vout].LockingScript,
		Satoshis:      fund.Outputs[vout].Satoshis,
	}), "add input spending the funded output")

	tx.AddOutput(&bt.Output{Satoshis: amount, LockingScript: p2pkh})
	tx.AddOutput(&bt.Output{
		Satoshis:      fund.Outputs[vout].Satoshis - amount - dustSpendFee,
		LockingScript: p2pkh,
	})

	require.NoError(t, tx.FillAllInputs(td.Ctx, &unlocker.Getter{PrivateKey: key}),
		"sign the spend with the key the funding paid to")

	return tx
}

// dustSpendFee is deducted so the spend pays its way.
const dustSpendFee = uint64(5000)
