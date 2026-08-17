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

const (
	// chroniclePushOnlyGenesis and chroniclePushOnlyChronicle put the two forks a
	// few blocks apart so one short chain covers three eras. Upstream uses 103 and
	// 106, three apart, because it needs 101 blocks of coinbase maturity first;
	// CoinbaseMaturity is 1 here, so the same three-block gap sits much lower.
	//
	// Genesis must come first, and the port asserts that rather than trusting these
	// two constants to stay in the right order.
	chroniclePushOnlyGenesis   = 6
	chroniclePushOnlyChronicle = 9

	// chronicleTxVersion is the transaction version Chronicle requires before it
	// will accept a non-push-only unlocking script. Version 1 stays refused, which
	// is the half of the rule that makes it a gate rather than a repeal.
	chronicleTxVersion = 2
)

// nonPushOnlyUnlockingScript is upstream's CScript([1, 1, OP_ADD, OP_DROP]) used as
// a scriptSig: it computes something and discards it, so it leaves the stack as it
// found it and the OP_TRUE locking script still succeeds. Its only offence is
// containing operators at all, which is what SIGPUSHONLY forbids.
func nonPushOnlyUnlockingScript() *bscript.Script {
	return bscript.NewFromBytes([]byte{
		bscript.OpTRUE, bscript.OpTRUE, bscript.OpADD, bscript.OpDROP,
	})
}

// TestBSVGenesisChroniclePushOnlyNonStd ports
// bsv-genesis-chronicle-pushonly-nonstd.py, which walks a transaction with a
// non-push-only unlocking script through three consensus eras and checks the
// verdict changes twice:
//
//   - before Genesis it is accepted;
//   - from Genesis it is refused, because SIGPUSHONLY becomes mandatory;
//   - from Chronicle it is accepted again, but only at transaction version 2.
//
// Teranode gets all four verdicts right, including the version gate. That is worth
// having as a test rather than only as a note, because Chronicle is ahead of the
// network rather than behind it - go-chaincfg marks every ChronicleActivationHeight
// "temporary and subject to change" - so this is coverage of a rule that has not
// been exercised in production yet.
//
// Upstream runs the whole sequence twice, against a node with -acceptnonstdtxn=0
// and one with -acceptnonstdtxn=1, because the pre-Genesis verdict differs between
// them: policy refuses it in the first, mandatory verification allows it in the
// second. Teranode has no such switch and behaves as the second, so only that pass
// is reproduced - see the registry entry.
//
// Overlap with TestBSVGenesisPushOnly is deliberate. That port covers the Genesis
// boundary from the block-validation side and carries the readmission tripwire for
// the validated-tx-not-rechecked-across-activation gap; this one covers the same
// boundary from the transaction-submission side and then goes on to Chronicle,
// which nothing else reaches.
func TestBSVGenesisChroniclePushOnlyNonStd(t *testing.T) {
	require.Less(t, chroniclePushOnlyGenesis, chroniclePushOnlyChronicle,
		"Chronicle sits above Genesis; these constants describe a chain that cannot exist")

	td := wirepeer.NewLegacyDaemon(t,
		wirepeer.WithGenesisActivationHeight(chroniclePushOnlyGenesis),
		wirepeer.WithChronicleActivationHeight(chroniclePushOnlyChronicle),
	)
	defer td.Stop(t)

	require.EqualValues(t, chroniclePushOnlyGenesis, td.Settings.ChainCfgParams.GenesisActivationHeight,
		"the Genesis override must have taken")
	require.EqualValues(t, chroniclePushOnlyChronicle, td.Settings.ChainCfgParams.ChronicleActivationHeight,
		"the Chronicle override must have taken")

	// One funding transaction with an output per probe. Each spend must have its
	// own output or the later ones would be refused as double spends and the eras
	// would look stricter than they are.
	funding := fundAnyoneCanSpendOutputs(t, td, 4)

	t.Run("before Genesis a non-push-only unlocking script is accepted", func(t *testing.T) {
		requireValidationHeightBelow(t, td, chroniclePushOnlyGenesis)

		spend := spendNonPushOnly(t, funding, 0, 1)

		require.NoError(t, td.PropagationClient.ProcessTransaction(td.Ctx, spend),
			"before Genesis SIGPUSHONLY is not mandatory, so this should be accepted")
	})

	t.Run("from Genesis it is refused", func(t *testing.T) {
		mineUntilValidationHeight(t, td, chroniclePushOnlyGenesis)

		spend := spendNonPushOnly(t, funding, 1, 1)

		require.Error(t, td.PropagationClient.ProcessTransaction(td.Ctx, spend),
			"from Genesis SIGPUSHONLY is mandatory, so a non-push-only unlocking script must be refused")
	})

	t.Run("from Chronicle it is accepted again at version 2", func(t *testing.T) {
		mineUntilValidationHeight(t, td, chroniclePushOnlyChronicle)

		spend := spendNonPushOnly(t, funding, 2, chronicleTxVersion)

		require.NoError(t, td.PropagationClient.ProcessTransaction(td.Ctx, spend),
			"from Chronicle a version %d transaction may carry a non-push-only unlocking script",
			chronicleTxVersion)

		td.WaitForBlockAssemblyToProcessTx(t, spend.TxID())
		require.Contains(t, tryRawMempool(td), spend.TxID(),
			"the accepted transaction should be queued for mining")
	})

	t.Run("from Chronicle version 1 is still refused", func(t *testing.T) {
		// The half that makes Chronicle a gate rather than a repeal, and the half a
		// node could most easily get wrong by relaxing SIGPUSHONLY for everything.
		spend := spendNonPushOnly(t, funding, 3, 1)

		require.Error(t, td.PropagationClient.ProcessTransaction(td.Ctx, spend),
			"Chronicle relaxes SIGPUSHONLY only for version %d; version 1 must still be refused",
			chronicleTxVersion)

		require.NotContains(t, tryRawMempool(td), spend.TxID(),
			"a refused transaction must not be queued for mining")
	})
}

// fundAnyoneCanSpendOutputs pays a spendable coinbase into n OP_TRUE outputs and
// mines the result, returning the funding transaction.
func fundAnyoneCanSpendOutputs(t *testing.T, td *daemon.TestDaemon, n int) *bt.Tx {
	t.Helper()

	coinbaseTx := td.MineToMaturityAndGetSpendableCoinbaseTx(t, td.Ctx)

	opts := make([]transactions.TxOption, 0, n+1)
	opts = append(opts, transactions.WithInput(coinbaseTx, 0))

	for range n {
		opts = append(opts, transactions.WithOutput(1e5, anyoneCanSpendScript()))
	}

	tx := td.CreateTransactionWithOptions(t, opts...)

	require.NoError(t, td.PropagationClient.ProcessTransaction(td.Ctx, tx),
		"the funding transaction should be accepted")
	td.WaitForBlockAssemblyToProcessTx(t, tx.TxID())
	td.MineAndWait(t, 1)

	return tx
}

// spendNonPushOnly builds a transaction spending funding's vout with upstream's
// non-push-only unlocking script, at the given transaction version.
//
// Built by hand because td.CreateTransactionWithOptions signs its inputs, and the
// point is an unlocking script that is not a signature - the same reason
// spendWithOpAdd exists in the other Genesis port.
func spendNonPushOnly(t *testing.T, funding *bt.Tx, vout uint32, version uint32) *bt.Tx {
	t.Helper()

	tx := bt.NewTx()

	require.NoError(t, tx.FromUTXOs(&bt.UTXO{
		TxIDHash:      funding.TxIDChainHash(),
		Vout:          vout,
		LockingScript: funding.Outputs[vout].LockingScript,
		Satoshis:      funding.Outputs[vout].Satoshis,
	}), "add input spending funding output %d", vout)

	tx.Inputs[0].UnlockingScript = nonPushOnlyUnlockingScript()
	tx.AddOutput(&bt.Output{
		Satoshis:      funding.Outputs[vout].Satoshis - 1000,
		LockingScript: anyoneCanSpendScript(),
	})

	tx.Version = version

	return tx
}

// requireValidationHeightBelow asserts the chain is still short enough that the next
// block - the height a submitted transaction is validated against - falls below an
// activation height.
//
// The distinction matters and upstream is explicit about it: its comments read "tip
// is on height 101 (nHeight=102, pre-genesis)". A test that compared the tip rather
// than the tip plus one would be off by exactly one block at every boundary.
func requireValidationHeightBelow(t *testing.T, td *daemon.TestDaemon, activation uint32) {
	t.Helper()

	require.Less(t, bestHeight(t, td)+1, activation,
		"this subtest needs the next block to fall below the activation height")
}

// mineUntilValidationHeight mines until a submitted transaction would be validated
// at or above activation.
func mineUntilValidationHeight(t *testing.T, td *daemon.TestDaemon, activation uint32) {
	t.Helper()

	for bestHeight(t, td)+1 < activation {
		td.MineAndWait(t, 1)
	}

	require.GreaterOrEqual(t, bestHeight(t, td)+1, activation,
		"the chain should now validate transactions at or above the activation height")
}
