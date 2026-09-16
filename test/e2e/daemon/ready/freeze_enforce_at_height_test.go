package smoke

import (
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
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

	// One parent transaction with three spendable outputs. Each boundary case consumes a
	// different one, so a case never depends on whether an earlier case spent the coin.
	parentTx, err := td.CreateParentTransactionWithNOutputs(t, block1.CoinbaseTx, 3)
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
	syncB := func(t *testing.T, block *model.Block) {
		t.Helper()

		nodeB.InjectPeer(t, nodeA)
		nodeB.WaitForBlockhash(t, block.Hash(), 60*time.Second)
	}

	coinbaseTx := nodeA.MineToMaturityAndGetSpendableCoinbaseTx(t, nodeA.Ctx)

	bestA, _, err := nodeA.BlockchainClient.GetBestBlockHeader(nodeA.Ctx)
	require.NoError(t, err)

	nodeB.InjectPeer(t, nodeA)
	nodeB.WaitForBlockhash(t, bestA.Hash(), 60*time.Second)
	requireSameTip(t, "after the initial sync")

	// Two coins, so the below-window and inside-window cases are independent.
	parentTx := nodeA.CreateTransactionWithOptions(t,
		transactions.WithInput(coinbaseTx, 0),
		transactions.WithP2PKHOutputs(2, (coinbaseTx.Outputs[0].Satoshis-1000)/2),
	)

	require.NoError(t, nodeA.PropagationClient.ProcessTransaction(nodeA.Ctx, parentTx))
	nodeA.WaitForBlockAssemblyToProcessTx(t, parentTx.TxIDChainHash().String())

	parentBlock := nodeA.MineAndWait(t, 1)
	syncB(t, parentBlock)
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

	// Now the alert reaches nodeB too — late, which is the whole point.
	//
	// The first coin has already been spent, by the block above that both nodes accepted
	// because it sits below the window, so freezing it now legitimately fails as
	// already-spent. That is the price of height-anchoring and it is the right price: a
	// coin can genuinely be spent before its window opens, identically on every node,
	// instead of on whichever nodes happened to hear the alert last.
	require.Error(t, nodeB.UtxoStore.FreezeUTXOs(nodeB.Ctx, freezes[:1], nodeB.Settings),
		"the coin spent below the window is gone on nodeB, exactly as it is on nodeA")
	require.NoError(t, nodeB.UtxoStore.FreezeUTXOs(nodeB.Ctx, freezes[1:], nodeB.Settings))

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
}
