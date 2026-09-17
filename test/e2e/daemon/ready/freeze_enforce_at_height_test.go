package smoke

import (
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/daemon"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/test"
	"github.com/bsv-blockchain/teranode/test/utils/transactions"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/stretchr/testify/require"
)

// freezeSpend builds the utxo.Spend identifying output vout of tx, carrying the
// enforceAtHeight window [freezeFrom, freezeUntil) the alert system would have supplied.
func freezeSpend(t *testing.T, tx *bt.Tx, vout uint32, freezeFrom, freezeUntil uint32) *utxo.Spend {
	t.Helper()

	utxoHash, err := util.UTXOHashFromOutput(tx.TxIDChainHash(), tx.Outputs[vout], vout)
	require.NoError(t, err)

	return &utxo.Spend{
		TxID:        tx.TxIDChainHash(),
		Vout:        vout,
		UTXOHash:    utxoHash,
		FreezeFrom:  freezeFrom,
		FreezeUntil: freezeUntil,
	}
}

// waitForTxMined blocks until the node's UTXO store has stamped tx with the block that
// mined it. Accepting a block and stamping its transactions are separate steps, and a
// spend of an unstamped parent is rejected as a floater — which would look like a freeze
// disagreement in a test that is not actually about one.
func waitForTxMined(t *testing.T, td *daemon.TestDaemon, tx *bt.Tx) {
	t.Helper()

	require.Eventually(t, func() bool {
		txMeta, err := td.UtxoStore.Get(td.Ctx, tx.TxIDChainHash(), fields.BlockIDs)

		return err == nil && txMeta != nil && len(txMeta.BlockIDs) > 0
	}, 90*time.Second, 200*time.Millisecond,
		"transaction %s was never stamped as mined on %s", tx.TxIDChainHash().String(), td.Settings.ClientName)
}

// waitForBestBlock blocks until the node's best block is the given hash.
func waitForBestBlock(t *testing.T, td *daemon.TestDaemon, hash *chainhash.Hash, context string) {
	t.Helper()

	require.Eventually(t, func() bool {
		best, _, err := td.BlockchainClient.GetBestBlockHeader(td.Ctx)

		return err == nil && best.Hash().Equal(*hash)
	}, 30*time.Second, 200*time.Millisecond, "%s: best block never became %s on %s", context, hash.String(), td.Settings.ClientName)
}

// requireRecordedInvalid asserts that a block was rejected with a clean block-invalid
// verdict AND that the node actually recorded the verdict, rather than treating the
// rejection as transient and queuing the block for another attempt.
//
// The distinction is the second half of issue #1422: a frozen spend used to surface as a
// processing error, which block validation classifies as infrastructure trouble, so the
// rejecting node re-fetched and re-validated the same block forever instead of failing it
// once. A test that only asserts require.Error cannot tell the two apart.
func requireRecordedInvalid(t *testing.T, td *daemon.TestDaemon, block *model.Block, err error) {
	t.Helper()

	require.Error(t, err, "a block spending a consensus-frozen utxo must be rejected")

	// The verdict, not merely "an error". Block validation persists a block as invalid
	// only on ErrBlockInvalid; anything else it reads as infrastructure trouble and
	// re-queues, which is the endless re-fetch/re-validate loop issue #1422 describes.
	// Inner ErrProcessing links from intermediate wrapping are fine — what matters is
	// that the outermost classification is a verdict and that the node acted on it,
	// which the GetLastNInvalidBlocks poll below proves.
	require.ErrorIs(t, err, errors.ErrBlockInvalid,
		"the rejection must be a clean block-invalid verdict, not a retryable error: %v", err)
	require.ErrorIs(t, err, errors.ErrTxInvalid,
		"the verdict must name the transaction-level cause so it survives classification: %v", err)
	require.ErrorIs(t, err, errors.ErrUtxoConsensusFrozen,
		"the cause must be the height-anchored consensus code, never the policy/maturity one: %v", err)

	require.Eventually(t, func() bool {
		invalidBlocks, listErr := td.BlockchainClient.GetLastNInvalidBlocks(td.Ctx, 20)
		if listErr != nil {
			return false
		}

		for _, invalid := range invalidBlocks {
			header, headerErr := model.NewBlockHeaderFromBytes(invalid.BlockHeader)
			if headerErr != nil {
				continue
			}

			if header.Hash().Equal(*block.Hash()) {
				return true
			}
		}

		return false
	}, 20*time.Second, 200*time.Millisecond,
		"the rejected block must be recorded as invalid so it is never re-fetched")
}

// TestFreezeEnforceAtHeightBoundary walks a single node across the boundaries of a
// freeze's enforceAtHeight window and asserts that a block is rejected for exactly the
// heights inside it — and that the rejection is a clean, recorded block-invalid verdict.
//
// Before issue #1422 a freeze was applied the instant the alert was processed, with no
// height attached, so the first case below (a block BELOW the freeze's start height)
// would be rejected here while a node that had not yet seen the alert accepted it.
func TestFreezeEnforceAtHeightBoundary(t *testing.T) {
	SharedTestLock.Lock()
	defer SharedTestLock.Unlock()

	const coinbaseMaturity = 2

	td := daemon.NewTestDaemon(t, daemon.TestOptions{
		EnableRPC:       true,
		EnableValidator: true,
		SettingsOverrideFunc: test.ComposeSettings(
			test.SystemTestSettings(),
			func(s *settings.Settings) {
				s.ChainCfgParams.CoinbaseMaturity = coinbaseMaturity
			},
		),
	})
	defer td.Stop(t)

	require.NoError(t, td.BlockchainClient.Run(td.Ctx, "test"))

	_, err := td.CallRPC(td.Ctx, "generate", []interface{}{coinbaseMaturity + 1})
	require.NoError(t, err)

	block1, err := td.BlockchainClient.GetBlockByHeight(td.Ctx, 1)
	require.NoError(t, err)

	// One parent transaction with four spendable outputs. Each case consumes a different
	// one, so a case never depends on whether an earlier case spent the coin.
	parentTx, err := td.CreateParentTransactionWithNOutputs(t, block1.CoinbaseTx, 4)
	require.NoError(t, err)

	require.NoError(t, td.PropagationClient.ProcessTransaction(td.Ctx, parentTx))
	td.WaitForBlockAssemblyToProcessTx(t, parentTx.TxIDChainHash().String())

	tip := td.MineAndWait(t, 1)

	// The window opens two blocks above the current tip, so the "below the window" case
	// below is genuinely a block the freeze does not yet cover.
	windowStart := tip.Height + 2
	windowStop := windowStart + 2

	frozen := []*utxo.Spend{
		freezeSpend(t, parentTx, 0, windowStart, windowStop),
		freezeSpend(t, parentTx, 1, windowStart, windowStop),
		freezeSpend(t, parentTx, 2, windowStart, windowStop),
	}
	require.NoError(t, td.UtxoStore.FreezeUTXOs(td.Ctx, frozen, td.Settings))

	// The fourth coin's policy freeze expires with its window (policyExpiresWithConsensus).
	expiring := freezeSpend(t, parentTx, 3, windowStart, windowStop)
	expiring.FreezePolicyExpires = true
	require.NoError(t, td.UtxoStore.FreezeUTXOs(td.Ctx, []*utxo.Spend{expiring}, td.Settings))

	// The policy tier bites immediately and at every height: a frozen coin never reaches
	// this node's mempool or its block templates, however far off the consensus window is.
	policyRejected := td.CreateTransactionWithOptions(t,
		transactions.WithInput(parentTx, 0),
		transactions.WithP2PKHOutputs(1, 1000),
	)
	require.Error(t, td.PropagationClient.ProcessTransaction(td.Ctx, policyRejected),
		"a frozen coin must not be accepted into the mempool, even below the freeze's start height")

	// submitBlockSpending mines up to targetHeight-1, then hands block validation a block
	// at targetHeight that spends the given output of parentTx.
	submitBlockSpending := func(t *testing.T, vout uint32, targetHeight uint32) (*model.Block, error) {
		t.Helper()

		best, _, bestErr := td.BlockchainClient.GetBestBlockHeader(td.Ctx)
		require.NoError(t, bestErr)

		current, currentErr := td.BlockchainClient.GetBlock(td.Ctx, best.Hash())
		require.NoError(t, currentErr)

		for current.Height < targetHeight-1 {
			current = td.MineAndWait(t, 1)
		}

		require.Equal(t, targetHeight-1, current.Height, "the chain must be one block below the target height")

		spendingTx := td.CreateTransactionWithOptions(t,
			transactions.WithInput(parentTx, vout),
			transactions.WithP2PKHOutputs(1, 1000),
		)

		_, block := td.CreateTestBlock(t, current, targetHeight*1000+vout, spendingTx)
		require.Equal(t, targetHeight, block.Height)

		return block, td.BlockValidation.ValidateBlock(td.Ctx, block, "legacy", true)
	}

	// A block BELOW the window: the freeze exists on this node but does not cover this
	// height, so the block is valid — and must be accepted, or this node has split from
	// every node that has not yet seen the alert.
	_, err = submitBlockSpending(t, 0, windowStart-1)
	require.NoError(t, err, "a block below the freeze's start height must be accepted")

	// A block INSIDE the window: a consensus violation every node derives identically.
	blockInside, err := submitBlockSpending(t, 1, windowStart)
	requireRecordedInvalid(t, td, blockInside, err)

	// A block AT the stop height: the window is half-open, so enforcement is over.
	_, err = submitBlockSpending(t, 2, windowStop)
	require.NoError(t, err, "a block at the freeze's stop height must be accepted again")

	// The policy tier outlives the window unless the alert said otherwise: coin 3's did,
	// so it may enter the mempool now; coin 2's did not, and its spend is still rejected
	// there even though a block spending it was just accepted.
	require.Eventually(t, func() bool {
		return td.UtxoStore.GetBlockHeight() >= windowStop-1
	}, 20*time.Second, 100*time.Millisecond, "the utxo store must have caught up to the tip")

	expiredSpend := td.CreateTransactionWithOptions(t,
		transactions.WithInput(parentTx, 3),
		transactions.WithP2PKHOutputs(1, 1000),
	)
	require.NoError(t, td.PropagationClient.ProcessTransaction(td.Ctx, expiredSpend),
		"a policy freeze that expires with consensus must lift once the window has ended")
}

// TestFreezeSameBlockParentChain covers the pair block validation cannot delegate to the
// store: a parent and its child both already validated by this node, then an alert on
// the parent's output, then a block inside the window carrying BOTH. Subtree validation
// blesses both on existence without re-spending the child, and the parent is in the
// same block so the chain check skips it — the freeze is judged only by the block-level
// parent check, which must therefore see same-block parents too (issue #1422).
func TestFreezeSameBlockParentChain(t *testing.T) {
	SharedTestLock.Lock()
	defer SharedTestLock.Unlock()

	const coinbaseMaturity = 2

	td := daemon.NewTestDaemon(t, daemon.TestOptions{
		EnableRPC:       true,
		EnableValidator: true,
		SettingsOverrideFunc: test.ComposeSettings(
			test.SystemTestSettings(),
			func(s *settings.Settings) {
				s.ChainCfgParams.CoinbaseMaturity = coinbaseMaturity
			},
		),
	})
	defer td.Stop(t)

	require.NoError(t, td.BlockchainClient.Run(td.Ctx, "test"))

	_, err := td.CallRPC(td.Ctx, "generate", []interface{}{coinbaseMaturity + 1})
	require.NoError(t, err)

	block1, err := td.BlockchainClient.GetBlockByHeight(td.Ctx, 1)
	require.NoError(t, err)

	// The pair: the parent spends the coinbase, the child spends parent:0. Both are
	// validated and sit in this node's store before any alert exists.
	parentTx, err := td.CreateParentTransactionWithNOutputs(t, block1.CoinbaseTx, 2)
	require.NoError(t, err)
	require.NoError(t, td.PropagationClient.ProcessTransaction(td.Ctx, parentTx))
	td.WaitForBlockAssemblyToProcessTx(t, parentTx.TxIDChainHash().String())

	childTx := td.CreateTransactionWithOptions(t,
		transactions.WithInput(parentTx, 0),
		transactions.WithP2PKHOutputs(1, 1000),
	)
	require.NoError(t, td.PropagationClient.ProcessTransaction(td.Ctx, childTx))
	td.WaitForBlockAssemblyToProcessTx(t, childTx.TxIDChainHash().String())

	best, _, err := td.BlockchainClient.GetBestBlockHeader(td.Ctx)
	require.NoError(t, err)
	tip, err := td.BlockchainClient.GetBlock(td.Ctx, best.Hash())
	require.NoError(t, err)

	// The alert arrives after the child was validated: parent:0 is already spent, so only
	// the consensus record is written.
	windowStart := tip.Height + 2
	windowStop := windowStart + 2
	require.NoError(t, td.UtxoStore.FreezeUTXOs(td.Ctx,
		[]*utxo.Spend{freezeSpend(t, parentTx, 0, windowStart, windowStop)}, td.Settings))

	// Below the window the pair is valid, in one block, everywhere.
	_, blockBelow := td.CreateTestBlock(t, tip, 93001, parentTx, childTx)
	require.Equal(t, windowStart-1, blockBelow.Height)
	require.NoError(t, td.BlockValidation.ValidateBlock(td.Ctx, blockBelow, "legacy", true),
		"a block below the window carrying the parent and its child must be accepted")

	// A fork: an empty sibling of blockBelow, then a block at windowStart re-mining the
	// same pair. Both transactions are known, so nothing re-spends the child; the verdict
	// rests on the block-level check seeing the parent's record through the in-block edge.
	_, forkBase := td.CreateTestBlock(t, tip, 93002)
	require.NoError(t, td.BlockValidation.ValidateBlock(td.Ctx, forkBase, "legacy", true), "an empty fork block is valid")

	_, forkInside := td.CreateTestBlock(t, forkBase, 93003, parentTx, childTx)
	require.Equal(t, windowStart, forkInside.Height)

	err = td.BlockValidation.ValidateBlock(td.Ctx, forkInside, "legacy", true)
	requireRecordedInvalid(t, td, forkInside, err)
}

// TestFreezeAlertTimingKeepsFleetInAgreement is the multi-node reproduction from issue
// #1422: two honest nodes receive the same freeze alert on opposite sides of a block.
//
// The chain up to the block under test is synced over P2P so both nodes reach it the way
// they would in production. The blocks under test are then handed to each node's block
// validation directly, so every verdict is observed synchronously and the test cannot
// pass by racing. They have to be hand-built: no honest node will put a spend of a frozen
// coin in its own template, which is exactly why the policy tier is not enough on its own.
func TestFreezeAlertTimingKeepsFleetInAgreement(t *testing.T) {
	SharedTestLock.Lock()
	defer SharedTestLock.Unlock()

	const coinbaseMaturity = 2

	createNode := func(t *testing.T, nodeNumber int) *daemon.TestDaemon {
		return daemon.NewTestDaemon(t, daemon.TestOptions{
			EnableRPC:         true,
			EnableP2P:         true,
			EnableValidator:   true,
			SkipRemoveDataDir: nodeNumber > 1,
			PreserveDataDir:   true,
			SettingsOverrideFunc: func(s *settings.Settings) {
				test.MultiNodeSettings(nodeNumber)(s)
				s.ChainCfgParams.CoinbaseMaturity = coinbaseMaturity
				s.P2P.PeerCacheDir = t.TempDir()
				s.P2P.SyncCoordinatorPeriodicEvaluationInterval = time.Second
			},
			FSMState: blockchain.FSMStateRUNNING,
		})
	}

	nodeA := createNode(t, 1)
	defer nodeA.Stop(t)

	nodeB := createNode(t, 2)
	defer nodeB.Stop(t)

	requireSameTip := func(t *testing.T, context string) {
		t.Helper()

		headerA, metaA, errA := nodeA.BlockchainClient.GetBestBlockHeader(nodeA.Ctx)
		require.NoError(t, errA)

		headerB, metaB, errB := nodeB.BlockchainClient.GetBestBlockHeader(nodeB.Ctx)
		require.NoError(t, errB)

		require.Equal(t, metaA.Height, metaB.Height, "the two nodes disagree on chain height (%s)", context)
		require.Equal(t, headerA.Hash().String(), headerB.Hash().String(),
			"the two nodes disagree on the best block (%s)", context)
	}

	// syncB pulls nodeB up to nodeA's current tip. InjectPeer snapshots nodeA's best
	// header at call time, so it has to be re-issued after nodeA mines rather than once
	// up front.
	syncB := func(t *testing.T, hash *chainhash.Hash) {
		t.Helper()

		nodeB.InjectPeer(t, nodeA)
		nodeB.WaitForBlockhash(t, hash, 60*time.Second)
	}

	coinbaseTx := nodeA.MineToMaturityAndGetSpendableCoinbaseTx(t, nodeA.Ctx)

	bestA, _, err := nodeA.BlockchainClient.GetBestBlockHeader(nodeA.Ctx)
	require.NoError(t, err)

	syncB(t, bestA.Hash())
	requireSameTip(t, "after the initial sync")

	// Two coins, so the below-window and inside-window cases are independent.
	parentTx := nodeA.CreateTransactionWithOptions(t,
		transactions.WithInput(coinbaseTx, 0),
		transactions.WithP2PKHOutputs(2, (coinbaseTx.Outputs[0].Satoshis-1000)/2),
	)

	require.NoError(t, nodeA.PropagationClient.ProcessTransaction(nodeA.Ctx, parentTx))
	nodeA.WaitForBlockAssemblyToProcessTx(t, parentTx.TxIDChainHash().String())

	parentBlock := nodeA.MineAndWait(t, 1)
	syncB(t, parentBlock.Hash())
	requireSameTip(t, "after the parent transaction was mined")

	// Accepting a block and stamping its transactions as mined are separate steps. Both
	// nodes must have the stamp before a later block may spend parentTx, or the spend is
	// rejected as a floater and the test would report a freeze disagreement that is really
	// a missing-parent one.
	waitForTxMined(t, nodeA, parentTx)
	waitForTxMined(t, nodeB, parentTx)

	windowStart := parentBlock.Height + 2
	windowStop := windowStart + 10

	// The alert reaches nodeA now and nodeB only later — the gossip skew the issue is
	// about. Under the old immediate freeze this asymmetry alone made the two nodes
	// disagree about the very next block.
	freezes := []*utxo.Spend{
		freezeSpend(t, parentTx, 0, windowStart, windowStop),
		freezeSpend(t, parentTx, 1, windowStart, windowStop),
	}
	require.NoError(t, nodeA.UtxoStore.FreezeUTXOs(nodeA.Ctx, freezes, nodeA.Settings))

	// A block BELOW the window spending one of the frozen coins. nodeA holds the freeze,
	// nodeB does not, and they must still agree: the window does not cover this height, so
	// the block is valid for both. This is the case that split the fleet before #1422.
	spendBelow := nodeA.CreateTransactionWithOptions(t,
		transactions.WithInput(parentTx, 0),
		transactions.WithP2PKHOutputs(1, 1000),
	)

	_, blockBelow := nodeA.CreateTestBlock(t, parentBlock, 90001, spendBelow)
	require.Equal(t, windowStart-1, blockBelow.Height)

	require.NoError(t, nodeA.BlockValidation.ValidateBlock(nodeA.Ctx, blockBelow, "legacy", true),
		"nodeA holds the freeze but the block is below its start height, so it must accept")
	require.NoError(t, nodeB.BlockValidation.ValidateBlock(nodeB.Ctx, blockBelow, nodeA.AssetURL, true),
		"nodeB has not seen the freeze and must reach the same verdict as nodeA")

	nodeA.WaitForBlockHeight(t, blockBelow, 30*time.Second)
	nodeB.WaitForBlockHeight(t, blockBelow, 30*time.Second)
	requireSameTip(t, "after a block below the freeze window")

	// Now the alert reaches nodeB too — late, which is the whole point. The first coin is
	// already spent there, by the block both nodes just accepted; the freeze is recorded
	// on it regardless, because the consensus record is a property of the outpoint, not
	// of what this node currently records about the coin. That is what lets nodeB judge
	// the fork below exactly as nodeA does.
	require.NoError(t, nodeB.UtxoStore.FreezeUTXOs(nodeB.Ctx, freezes, nodeB.Settings),
		"a late alert must be recorded even on a coin this node already has spent")

	// A block INSIDE the window spending the other frozen coin. Both nodes must now reject
	// it, cleanly, and neither may be left re-validating it.
	spendInside := nodeA.CreateTransactionWithOptions(t,
		transactions.WithInput(parentTx, 1),
		transactions.WithP2PKHOutputs(1, 1000),
	)

	_, blockInside := nodeA.CreateTestBlock(t, blockBelow, 90002, spendInside)
	require.Equal(t, windowStart, blockInside.Height)

	errA := nodeA.BlockValidation.ValidateBlock(nodeA.Ctx, blockInside, "legacy", true)
	requireRecordedInvalid(t, nodeA, blockInside, errA)

	errB := nodeB.BlockValidation.ValidateBlock(nodeB.Ctx, blockInside, nodeA.AssetURL, true)
	requireRecordedInvalid(t, nodeB, blockInside, errB)

	requireSameTip(t, "after both nodes rejected a block inside the freeze window")

	// The adversarial case: a FORK carrying the below-window spend at a height inside the
	// window. Both nodes already record that coin as spent by this very transaction (from
	// blockBelow), so the store's idempotent "already spent by this tx" path would accept
	// it if the consensus check did not run first. A node that had never seen blockBelow
	// would reject it — so both nodes must reject it, or the fleet splits along who saw
	// blockBelow. forkBase is an empty sibling of blockBelow; forkInside builds on it.
	_, forkBase := nodeA.CreateTestBlock(t, parentBlock, 90003)
	require.Equal(t, windowStart-1, forkBase.Height)

	require.NoError(t, nodeA.BlockValidation.ValidateBlock(nodeA.Ctx, forkBase, "legacy", true), "an empty fork block is valid")
	require.NoError(t, nodeB.BlockValidation.ValidateBlock(nodeB.Ctx, forkBase, nodeA.AssetURL, true), "an empty fork block is valid")

	_, forkInside := nodeA.CreateTestBlock(t, forkBase, 90004, spendBelow)
	require.Equal(t, windowStart, forkInside.Height)

	errA = nodeA.BlockValidation.ValidateBlock(nodeA.Ctx, forkInside, "legacy", true)
	requireRecordedInvalid(t, nodeA, forkInside, errA)

	errB = nodeB.BlockValidation.ValidateBlock(nodeB.Ctx, forkInside, nodeA.AssetURL, true)
	requireRecordedInvalid(t, nodeB, forkInside, errB)

	requireSameTip(t, "after both nodes rejected a fork re-mining the below-window spend inside the window")
}

// TestFreezeReorgRejectsReminedSpend is the other adversarial shape: the block that spent
// the coin below the window is reorged out, and the same spend then turns up at a height
// inside the window. Teranode does not unspend on reorg — the disconnected block's
// transactions simply become unmined again — so the store still records the coin as
// spent by that transaction, and the verdict must not depend on that.
func TestFreezeReorgRejectsReminedSpend(t *testing.T) {
	SharedTestLock.Lock()
	defer SharedTestLock.Unlock()

	const coinbaseMaturity = 2

	td := daemon.NewTestDaemon(t, daemon.TestOptions{
		EnableRPC:       true,
		EnableValidator: true,
		SettingsOverrideFunc: test.ComposeSettings(
			test.SystemTestSettings(),
			func(s *settings.Settings) {
				s.ChainCfgParams.CoinbaseMaturity = coinbaseMaturity
			},
		),
	})
	defer td.Stop(t)

	require.NoError(t, td.BlockchainClient.Run(td.Ctx, "test"))

	_, err := td.CallRPC(td.Ctx, "generate", []interface{}{coinbaseMaturity + 1})
	require.NoError(t, err)

	block1, err := td.BlockchainClient.GetBlockByHeight(td.Ctx, 1)
	require.NoError(t, err)

	parentTx, err := td.CreateParentTransactionWithNOutputs(t, block1.CoinbaseTx, 2)
	require.NoError(t, err)

	require.NoError(t, td.PropagationClient.ProcessTransaction(td.Ctx, parentTx))
	td.WaitForBlockAssemblyToProcessTx(t, parentTx.TxIDChainHash().String())

	parentBlock := td.MineAndWait(t, 1)
	waitForTxMined(t, td, parentTx)

	windowStart := parentBlock.Height + 2
	windowStop := windowStart + 2

	require.NoError(t, td.UtxoStore.FreezeUTXOs(td.Ctx, []*utxo.Spend{
		freezeSpend(t, parentTx, 0, windowStart, windowStop),
		freezeSpend(t, parentTx, 1, windowStart, windowStop),
	}, td.Settings))

	// The coin is spent below the window, validly.
	spendBelow := td.CreateTransactionWithOptions(t,
		transactions.WithInput(parentTx, 0),
		transactions.WithP2PKHOutputs(1, 1000),
	)

	_, blockBelow := td.CreateTestBlock(t, parentBlock, 91001, spendBelow)
	require.Equal(t, windowStart-1, blockBelow.Height)
	require.NoError(t, td.BlockValidation.ValidateBlock(td.Ctx, blockBelow, "legacy", true))
	waitForBestBlock(t, td, blockBelow.Hash(), "after the below-window spend")

	// A competing two-block chain from the same parent, carrying nothing, reorgs it out.
	_, competing1 := td.CreateTestBlock(t, parentBlock, 91002)
	require.NoError(t, td.BlockValidation.ValidateBlock(td.Ctx, competing1, "legacy", true), "a sibling of the tip is a valid fork block")

	_, competing2 := td.CreateTestBlock(t, competing1, 91003)
	require.Equal(t, windowStart, competing2.Height)
	require.NoError(t, td.BlockValidation.ValidateBlock(td.Ctx, competing2, "legacy", true), "the longer fork must be accepted")
	waitForBestBlock(t, td, competing2.Hash(), "after the reorg")

	// The spend is back in this node's own template — Teranode re-admits a disconnected
	// block's transactions without re-validation. Reported, not asserted: keeping it out
	// belongs to block assembly and is tracked separately from #1422.
	if hashes, listErr := td.BlockAssemblyClient.GetTransactionHashes(td.Ctx); listErr == nil {
		for _, h := range hashes {
			if h == spendBelow.TxIDChainHash().String() {
				t.Logf("NOTE: the reorged-out spend %s is back in block assembly at height %d, inside the freeze window", h, windowStart+1)
			}
		}
	}

	// The same spend re-mined at a height inside the window. The store still records the
	// coin as spent by exactly this transaction, so without the consensus check running
	// ahead of the idempotent path this would be accepted here and rejected on every node
	// that never saw blockBelow.
	_, remined := td.CreateTestBlock(t, competing2, 91004, spendBelow)
	require.Equal(t, windowStart+1, remined.Height)
	requireRecordedInvalid(t, td, remined, td.BlockValidation.ValidateBlock(td.Ctx, remined, "legacy", true))

	// And the coin that was never spent is, of course, still frozen.
	spendOther := td.CreateTransactionWithOptions(t,
		transactions.WithInput(parentTx, 1),
		transactions.WithP2PKHOutputs(1, 1000),
	)

	_, insideOther := td.CreateTestBlock(t, competing2, 91005, spendOther)
	requireRecordedInvalid(t, td, insideOther, td.BlockValidation.ValidateBlock(td.Ctx, insideOther, "legacy", true))

	// Past the window, both spends are valid again.
	_, filler := td.CreateTestBlock(t, competing2, 91006)
	require.NoError(t, td.BlockValidation.ValidateBlock(td.Ctx, filler, "legacy", true))
	waitForBestBlock(t, td, filler.Hash(), "after the filler block")

	_, atStop := td.CreateTestBlock(t, filler, 91007, spendOther)
	require.Equal(t, windowStop, atStop.Height)
	require.NoError(t, td.BlockValidation.ValidateBlock(td.Ctx, atStop, "legacy", true),
		"a block at the window's end spending a frozen coin must be accepted again")
}
