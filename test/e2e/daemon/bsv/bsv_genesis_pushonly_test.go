package bsv

import (
	"encoding/json"
	"slices"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/teranode/daemon"
	"github.com/bsv-blockchain/teranode/test/utils/transactions"
	"github.com/bsv-blockchain/teranode/test/utils/wirepeer"
	"github.com/stretchr/testify/require"
)

// genesisPushOnlyActivation is the height at which Genesis activates for this
// port.
//
// Upstream passes -genesisactivationheight=104 and mines 102 blocks to sit just
// below it. The number here is small on purpose: the TestDaemon's regtest copy
// sets CoinbaseMaturity to 1, so the whole fixture needs three blocks, and the
// fork can sit at 5 while one short chain still covers both sides of it. See
// wirepeer.WithGenesisActivationHeight for why moving the height is safe.
const genesisPushOnlyActivation = 5

// opAddUnlockingScript is upstream's CScript([1, 1, OP_ADD]): a script that
// succeeds but is not push-only. OP_ADD leaving 2 on the stack is what keeps it
// acceptable before Genesis; SIGPUSHONLY becoming mandatory is what makes it
// unacceptable after.
func opAddUnlockingScript() *bscript.Script {
	return bscript.NewFromBytes([]byte{bscript.OpTRUE, bscript.OpTRUE, bscript.OpADD})
}

// anyoneCanSpendScript is upstream's CScript([OP_TRUE]).
func anyoneCanSpendScript() *bscript.Script {
	return bscript.NewFromBytes([]byte{bscript.OpTRUE})
}

// spendWithOpAdd builds a transaction spending fundingTx's vout with a
// non-push-only unlocking script.
//
// It cannot go through td.CreateTransactionWithOptions, which signs every input:
// the point of the transaction is an unlocking script that is not a signature
// push, so the input is filled in by hand.
func spendWithOpAdd(t *testing.T, fundingTx *bt.Tx, vout uint32, amount uint64) *bt.Tx {
	t.Helper()

	tx := bt.NewTx()

	require.NoError(t, tx.FromUTXOs(&bt.UTXO{
		TxIDHash:      fundingTx.TxIDChainHash(),
		Vout:          vout,
		LockingScript: fundingTx.Outputs[vout].LockingScript,
		Satoshis:      fundingTx.Outputs[vout].Satoshis,
	}), "add input spending funding output %d", vout)

	tx.Inputs[0].UnlockingScript = opAddUnlockingScript()

	tx.AddOutput(&bt.Output{Satoshis: amount, LockingScript: anyoneCanSpendScript()})

	return tx
}

// tryRawMempool reads getrawmempool, returning nil on any failure. See
// tryBestBlockHash for why polling conditions must not assert.
//
// Teranode has no mempool; the RPC reports block assembly's pending transaction
// hashes, and the list always carries an all-ff sentinel for the coinbase
// placeholder node, so callers must look for a specific hash rather than count.
func tryRawMempool(td *daemon.TestDaemon) []string {
	resp, err := td.CallRPC(td.Ctx, "getrawmempool", nil)
	if err != nil {
		return nil
	}

	var envelope struct {
		Result []string `json:"result"`
	}

	if json.Unmarshal([]byte(resp), &envelope) != nil {
		return nil
	}

	return envelope.Result
}

// TestBSVGenesisPushOnly is the Teranode port of bitcoin-sv's
// bsv-genesis-pushonly.py.
//
// Upstream checks that SCRIPT_VERIFY_SIGPUSHONLY becomes mandatory at Genesis by
// sending two whole blocks over P2P, one either side of the activation height,
// each spending an output with CScript([1, 1, OP_ADD]) as its unlocking script.
// The first is accepted, the second rejected.
//
// This port submits the blocks through td.BlockValidationClient.ProcessBlock
// rather than over the wire. That is the same code path a block arriving from a
// peer takes, and it reaches it with the transaction unvalidated: CreateTestBlock
// stores the subtree as FileTypeSubtreeToCheck with full transaction data, so
// block validation validates the transaction itself. Going through the validator
// instead - the propagation path - would not reproduce upstream at all, because
// upstream's whole point is that these transactions never pass individual
// checks.
//
// Two differences from upstream are recorded in registry.yaml, not papered over:
// the rejection reason string, and what getrawmempool reports after
// invalidateblock. Both are asserted in the form Teranode actually exhibits.
//
// Reproduced from upstream:
//   - the initial chain leaves the tip below the activation height
//   - a block below the activation height whose transaction has a non-push-only
//     unlocking script is accepted, and becomes the tip
//   - an equivalent block at the activation height is rejected
//   - the tip is unchanged by the rejection, and is still the accepted block
//   - invalidating the accepted block returns the tip to its parent
func TestBSVGenesisPushOnly(t *testing.T) {
	td := wirepeer.NewLegacyDaemon(t, wirepeer.WithGenesisActivationHeight(genesisPushOnlyActivation))
	defer td.Stop(t)

	require.EqualValues(t, 1, td.Settings.ChainCfgParams.CoinbaseMaturity,
		"the fixture's block layout assumes the TestDaemon's CoinbaseMaturity of 1")
	require.EqualValues(t, genesisPushOnlyActivation, td.Settings.ChainCfgParams.GenesisActivationHeight,
		"the override should have moved the Genesis activation height")

	coinbaseTx := td.MineToMaturityAndGetSpendableCoinbaseTx(t, td.Ctx)

	// Two anyone-can-spend outputs, one for the block below the fork and one for
	// the block at it - upstream spends two separate outputs so the second block
	// is not a double spend of the first. Teranode coinbases are P2PKH, so
	// upstream's OP_TRUE coinbases are not available and this funding
	// transaction stands in for them.
	fundingTx := td.CreateTransactionWithOptions(t,
		transactions.WithInput(coinbaseTx, 0),
		transactions.WithOutput(1e6, anyoneCanSpendScript()),
		transactions.WithOutput(1e6, anyoneCanSpendScript()),
	)

	require.NoError(t, td.PropagationClient.ProcessTransaction(td.Ctx, fundingTx),
		"funding transaction should be accepted")
	td.WaitForBlockAssemblyToProcessTx(t, fundingTx.TxID())

	fundingBlock := td.MineAndWait(t, 1)

	// Upstream: assert_equal(getblock(getbestblockhash())['height'], 102), two
	// below its activation height of 104. The same relationship holds here: the
	// funding block must leave exactly one pre-Genesis height free for the block
	// that has to be accepted.
	require.EqualValues(t, genesisPushOnlyActivation-2, fundingBlock.Height,
		"funding tx %s should confirm two blocks below the activation height", fundingTx.TxID())

	// Below the fork: not push-only, and accepted.
	acceptedTx := spendWithOpAdd(t, fundingTx, 0, 100000)
	_, acceptedBlock := td.CreateTestBlock(t, fundingBlock, 0xadd0, acceptedTx)

	require.EqualValues(t, genesisPushOnlyActivation-1, acceptedBlock.Height,
		"the accepted block must sit below the activation height")
	require.NoError(t,
		td.BlockValidationClient.ProcessBlock(td.Ctx, acceptedBlock, acceptedBlock.Height, "", "legacy", 0),
		"a non-push-only unlocking script should be accepted below the Genesis activation height")

	acceptedHash := acceptedBlock.Header.Hash().String()

	require.Eventually(t, func() bool {
		return tryBestBlockHash(td) == acceptedHash
	}, 30*time.Second, 250*time.Millisecond,
		"the pre-Genesis block should become the tip; tip is %s at height %d",
		bestBlockHash(t, td), bestHeight(t, td))
	require.EqualValues(t, genesisPushOnlyActivation-1, bestHeight(t, td),
		"the tip should be the pre-Genesis block's height")

	// At the fork: the same shape of transaction, rejected. Upstream expects
	// RejectResult(16, b'blk-bad-inputs'); Teranode names the rule instead, which
	// is a stronger statement about why the block was refused. See the waiver in
	// registry.yaml.
	rejectedTx := spendWithOpAdd(t, fundingTx, 1, 100000)
	_, rejectedBlock := td.CreateTestBlock(t, acceptedBlock, 0xadd1, rejectedTx)

	require.EqualValues(t, genesisPushOnlyActivation, rejectedBlock.Height,
		"the rejected block must sit at the activation height")

	err := td.BlockValidationClient.ProcessBlock(td.Ctx, rejectedBlock, rejectedBlock.Height, "", "legacy", 0)
	require.Error(t, err,
		"a non-push-only unlocking script should be rejected at the Genesis activation height")
	require.ErrorContains(t, err, "Only non-push operators allowed in signatures",
		"the block should be refused for the push-only rule specifically, not for some unrelated defect")
	require.ErrorContains(t, err, rejectedTx.TxID(),
		"the rejection should name the offending transaction")

	// Upstream: the tip is still 103, and still blk_accepted.
	require.Equal(t, acceptedHash, bestBlockHash(t, td),
		"the rejected block must not have displaced the tip")
	require.EqualValues(t, genesisPushOnlyActivation-1, bestHeight(t, td),
		"the tip height must be unchanged by the rejection")

	// Upstream: invalidateblock on the accepted block puts the tip back on 102.
	_, err = td.CallRPC(td.Ctx, "invalidateblock", []any{acceptedHash})
	require.NoError(t, err, "invalidateblock %s", acceptedHash)

	fundingHash := fundingBlock.Header.Hash().String()

	require.Eventually(t, func() bool {
		return tryBestBlockHash(td) == fundingHash
	}, 30*time.Second, 250*time.Millisecond,
		"invalidating the accepted block should return the tip to its parent; tip is %s at height %d",
		bestBlockHash(t, td), bestHeight(t, td))
	require.EqualValues(t, genesisPushOnlyActivation-2, bestHeight(t, td),
		"the tip should be back at the funding block's height")

	// Upstream's last assertion is that the accepted block's transaction is not
	// in the mempool, because bitcoin-sv re-checks each returned transaction
	// against mempool policy and push-only is a policy rule. Teranode has no
	// mempool and no policy layer, so this asserts what it does instead - a
	// tripwire, not an endorsement.
	//
	// Diagnosing this waiver is what turned up the
	// validated-tx-not-rechecked-across-activation defect: readmission is only the
	// trigger, and the substance is that a validation verdict stored against a txid
	// carries no record of the activation era it was reached under, so it is never
	// revisited when the rules change. See that gap before weakening anything here.
	t.Run("readmission_tripwire", func(t *testing.T) {
		require.Eventually(t, func() bool {
			return slices.Contains(tryRawMempool(td), acceptedTx.TxID())
		}, 30*time.Second, 250*time.Millisecond,
			"Teranode currently returns the invalidated block's transactions to block assembly "+
				"without re-checking them. If this now fails, Teranode has gained the "+
				"re-validation upstream relies on, and this port should assert upstream's "+
				"assertion - that %s is absent - instead. getrawmempool: %v",
			acceptedTx.TxID(), tryRawMempool(td))
	})
}
