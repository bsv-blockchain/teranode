package blockassembly

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/services/blockassembly/subtreeprocessor"
	"github.com/bsv-blockchain/teranode/settings"
	blockchainoptions "github.com/bsv-blockchain/teranode/stores/blockchain/options"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	utxosql "github.com/bsv-blockchain/teranode/stores/utxo/sql"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

func replayPoolTx(parent *chainhash.Hash, amount uint64) *bt.Tx {
	tx := bt.NewTx()
	input := &bt.Input{PreviousTxOutIndex: 0, PreviousTxSatoshis: amount + 1, SequenceNumber: 0xffffffff, UnlockingScript: bscript.NewFromBytes([]byte{0x51})}
	_ = input.PreviousTxIDAdd(parent)
	tx.Inputs = []*bt.Input{input}
	tx.Outputs = []*bt.Output{{Satoshis: amount, LockingScript: bscript.NewFromBytes([]byte{0x51})}}
	return tx
}

func TestReplayBestTipChanges_ResetRecoversParentBeforeChild(t *testing.T) {
	t.Run("normal", func(t *testing.T) { testReplayResetPool(t, false, false, testCoinbaseMaturity) })
	t.Run("retry_after_target_finalized", func(t *testing.T) { testReplayResetPool(t, true, false, testCoinbaseMaturity) })
	// The reload keeps failing until the replay gives the history up. Candidates are built
	// again from the partial pool, and the reload is still retried on the next passes until
	// it brings the child back.
	t.Run("reload_retried_after_history_given_up", func(t *testing.T) { testReplayResetPool(t, true, true, testCoinbaseMaturity) })
	// With maturity 1 the move from A1 to B2 would take A1 back although A3 builds on it,
	// and a spend of A1's coinbase could already be confirmed. The replay restamps B
	// where it lies instead, and the parent still reaches the pool ahead of its child.
	t.Run("restamped_below_coinbase_maturity", func(t *testing.T) { testReplayResetPool(t, false, false, 1) })
}

func testReplayResetPool(t *testing.T, lateFailure, giveUp bool, coinbaseMaturity uint16) {
	initPrometheusMetrics()
	items := setupBlockAssemblyTest(t, func(s *settings.Settings) {
		s.GlobalBlockHeightRetention = 2
		s.UtxoStore.BlockHeightRetentionAdjustment = 0
		s.BlockAssembly.StoreTxInpointsForSubtreeMeta = true
	}, withCoinbaseMaturity(coinbaseMaturity))
	ba := items.blockAssembler
	ba.subtreeProcessor.Stop(context.Background())
	stp, err := subtreeprocessor.NewSubtreeProcessor(t.Context(), ulogger.TestLogger{}, ba.settings, items.blobStore, items.blockchainClient, items.utxoStore, items.newSubtreeChan)
	require.NoError(t, err)
	ba.subtreeProcessor = stp
	stp.Start(t.Context())
	genesis := genesisHeader(t, items)
	a1 := buildChain(genesis, 1, 19000)[0]
	addBlockWithMinedSet(t.Context(), t, items, a1)
	startReplayAt(t, items, a1, 1)
	stp.InitCurrentBlockHeader(a1)
	require.NoError(t, items.utxoStore.SetBlockHeight(3))
	funding := replayPoolTx(&chainhash.Hash{}, 100)
	parent := replayPoolTx(funding.TxIDChainHash(), 99)
	child := replayPoolTx(parent.TxIDChainHash(), 98)
	for _, tx := range []*bt.Tx{funding, parent, child} {
		_, err = items.utxoStore.(*utxosql.Store).Create(t.Context(), tx, 1)
		require.NoError(t, err)
	}
	require.NoError(t, items.utxoStore.MarkTransactionsOnLongestChain(t.Context(), []chainhash.Hash{*funding.TxIDChainHash(), *parent.TxIDChainHash()}, true))
	tree, err := subtree.NewTreeByLeafCount(4)
	require.NoError(t, err)
	require.NoError(t, tree.AddCoinbaseNode())
	require.NoError(t, tree.AddNode(*parent.TxIDChainHash(), 1, uint64(len(parent.Bytes()))))
	bytes, err := tree.Serialize()
	require.NoError(t, err)
	require.NoError(t, items.blobStore.Set(t.Context(), tree.RootHash()[:], fileformat.FileTypeSubtree, bytes))
	b := buildChain(genesis, 2, 20000)
	coinbase, err := bt.NewTxFromString("02000000010000000000000000000000000000000000000000000000000000000000000000ffffffff03510101ffffffff0100f2052a01000000232103656065e6886ca1e947de3471c9e723673ab6ba34724476417fa9fcef8bafa604ac00000000")
	require.NoError(t, err)
	require.NoError(t, items.blockchainClient.AddBlock(t.Context(), &model.Block{Header: b[0], CoinbaseTx: coinbase, TransactionCount: 2, Subtrees: []*chainhash.Hash{tree.RootHash()}}, "", blockchainoptions.WithMinedSet(true)))
	addBlockWithMinedSet(t.Context(), t, items, b[1])
	_, genMeta, err := items.blockchainClient.GetBlockHeader(t.Context(), genesis.Hash())
	require.NoError(t, err)
	_, bMeta, err := items.blockchainClient.GetBlockHeader(t.Context(), b[0].Hash())
	require.NoError(t, err)
	_, err = items.utxoStore.SetMinedMulti(t.Context(), []*chainhash.Hash{funding.TxIDChainHash()}, utxo.MinedBlockInfo{BlockID: genMeta.ID, BlockHeight: 0})
	require.NoError(t, err)
	_, err = items.utxoStore.SetMinedMulti(t.Context(), []*chainhash.Hash{parent.TxIDChainHash()}, utxo.MinedBlockInfo{BlockID: bMeta.ID, BlockHeight: 1})
	require.NoError(t, err)

	a := buildChain(a1, 2, 21000)
	for _, h := range a {
		addBlockWithMinedSet(t.Context(), t, items, h)
	}
	if lateFailure {
		failing := &iteratorDownStore{Store: items.utxoStore}
		failing.iteratorDown.Store(true)
		ba.utxoStore = failing
		pos := ba.bestTipPos.Load()
		_, err = ba.replayBestTipChanges(t.Context())
		require.Error(t, err)
		requireBlockAssemblyTip(t, items, b[1])
		// One pass short of giving the history up: the retries keep the position.
		for i := 1; i < maxBestTipReplayFailures; i++ {
			require.False(t, ba.followBestTipChanges(t.Context()))
			require.Equal(t, pos.Bytes(), ba.bestTipPos.Load().Bytes(), "finalized target is not a successful reload")
		}
		if giveUp {
			require.True(t, ba.followBestTipChanges(t.Context()), "the last failure gives the history up")
			require.False(t, ba.bestTipResetReplayPending.Load(), "candidates are built again")
			require.True(t, ba.bestTipResetIncomplete.Load(), "the reload is still owed")
			// A retry that fails again keeps candidates open and the reload owed.
			ba.followBestTipChanges(t.Context())
			require.False(t, ba.bestTipResetReplayPending.Load(), "a failed retry does not hold candidates empty again")
			require.True(t, ba.bestTipResetIncomplete.Load(), "the reload is still owed")
		}
		failing.iteratorDown.Store(false)
	}
	if giveUp {
		ba.processNewBlockAnnouncement(t.Context())
		require.False(t, ba.bestTipResetIncomplete.Load(), "the retried reload finished")
	} else {
		_, err = ba.replayBestTipChanges(t.Context())
		require.NoError(t, err)
	}
	requireBlockAssemblyTip(t, items, a[1])
	current := ba.subtreeProcessor.GetCurrentSubtree()
	var hashes []chainhash.Hash
	for _, node := range current.Nodes {
		if !node.Hash.IsEqual(subtree.CoinbasePlaceholderHash) {
			hashes = append(hashes, node.Hash)
		}
	}
	require.Equal(t, []chainhash.Hash{*parent.TxIDChainHash(), *child.TxIDChainHash()}, hashes, "reset must recover block-only parent before its child")
	data, err := items.utxoStore.Get(t.Context(), parent.TxIDChainHash())
	require.NoError(t, err)
	require.NotZero(t, data.UnminedSince, "orphaned parent must survive restart/pruning")
}

// TestBranchOnlyTxs checks which transactions of a stale branch a restamp marks: one that a block
// of the best chain also contains stays mined, one only the stale branch contains is marked, and
// one the UTXO store has pruned is left out.
func TestBranchOnlyTxs(t *testing.T) {
	initPrometheusMetrics()
	items := setupBlockAssemblyTest(t)
	genesis := genesisHeader(t, items)
	a := buildChain(genesis, 2, 32000)
	b := buildChain(genesis, 1, 33000)
	for _, h := range append(a, b...) {
		addBlockWithMinedSet(t.Context(), t, items, h)
	}
	_, aMeta, err := items.blockchainClient.GetBlockHeader(t.Context(), a[0].Hash())
	require.NoError(t, err)
	_, bMeta, err := items.blockchainClient.GetBlockHeader(t.Context(), b[0].Hash())
	require.NoError(t, err)

	both := replayPoolTx(&chainhash.Hash{}, 100)
	staleOnly := replayPoolTx(&chainhash.Hash{}, 99)
	pruned := replayPoolTx(&chainhash.Hash{}, 98)
	for _, tx := range []*bt.Tx{both, staleOnly} {
		_, err = items.utxoStore.(*utxosql.Store).Create(t.Context(), tx, 1)
		require.NoError(t, err)
	}
	_, err = items.utxoStore.SetMinedMulti(t.Context(), []*chainhash.Hash{both.TxIDChainHash(), staleOnly.TxIDChainHash()}, utxo.MinedBlockInfo{BlockID: bMeta.ID, BlockHeight: 1})
	require.NoError(t, err)
	_, err = items.utxoStore.SetMinedMulti(t.Context(), []*chainhash.Hash{both.TxIDChainHash()}, utxo.MinedBlockInfo{BlockID: aMeta.ID, BlockHeight: 1})
	require.NoError(t, err)

	got, err := items.blockAssembler.branchOnlyTxs(t.Context(),
		[]chainhash.Hash{*both.TxIDChainHash(), *staleOnly.TxIDChainHash(), *pruned.TxIDChainHash()},
		map[uint32]struct{}{bMeta.ID: {}}, a[1])
	require.NoError(t, err)
	require.Equal(t, []chainhash.Hash{*staleOnly.TxIDChainHash()}, got)
}

// TestReplayBestTipChanges_LowWaterReplaysShortBranch is the case the best-tip order alone misses.
// Block assembly is on X2. X2 is invalidated, so the best tip drops to A1, and S2, carrying a
// transaction T no other block has, extends A1 and becomes the best tip. Branch Y then overtakes
// S2 from A1, and X2 is revalidated, all before block assembly looks. S2 sorts below X2, so a
// read of the blocks better than block assembly's position never returns it. The invalidation
// recorded A1 in the low-water mark, and the replay from there moves to S2 and off it again, so T
// ends in the pool.
func TestReplayBestTipChanges_LowWaterReplaysShortBranch(t *testing.T) {
	initPrometheusMetrics()
	ctx := t.Context()
	// The default retention keeps every move here an in-memory reorg. A reset would reload
	// the pool from the UTXO store, which finds T without the replay, so it would not show
	// whether the replay visited S2.
	items := setupBlockAssemblyTest(t, func(s *settings.Settings) {
		s.BlockAssembly.StoreTxInpointsForSubtreeMeta = true
	}, withCoinbaseMaturity(testCoinbaseMaturity))
	ba := items.blockAssembler
	ba.subtreeProcessor.Stop(context.Background())
	stp, err := subtreeprocessor.NewSubtreeProcessor(ctx, ulogger.TestLogger{}, ba.settings, items.blobStore, items.blockchainClient, items.utxoStore, items.newSubtreeChan)
	require.NoError(t, err)
	ba.subtreeProcessor = stp
	stp.Start(ctx)

	genesis := genesisHeader(t, items)
	a1 := buildChain(genesis, 1, 40000)[0]
	addBlockWithMinedSet(ctx, t, items, a1)
	x2 := buildChain(a1, 1, 41000)[0]
	addBlockWithMinedSet(ctx, t, items, x2)
	startReplayAt(t, items, x2, 2)
	stp.InitCurrentBlockHeader(x2)
	require.NoError(t, items.utxoStore.SetBlockHeight(3))

	// T spends a confirmed transaction, and only S2 contains it.
	funding := replayPoolTx(&chainhash.Hash{}, 100)
	branchOnly := replayPoolTx(funding.TxIDChainHash(), 99)
	for _, tx := range []*bt.Tx{funding, branchOnly} {
		_, err = items.utxoStore.(*utxosql.Store).Create(ctx, tx, 1)
		require.NoError(t, err)
	}
	require.NoError(t, items.utxoStore.MarkTransactionsOnLongestChain(ctx, []chainhash.Hash{*funding.TxIDChainHash()}, true))

	tree, err := subtree.NewTreeByLeafCount(2)
	require.NoError(t, err)
	require.NoError(t, tree.AddCoinbaseNode())
	require.NoError(t, tree.AddNode(*branchOnly.TxIDChainHash(), 1, uint64(len(branchOnly.Bytes()))))
	treeBytes, err := tree.Serialize()
	require.NoError(t, err)
	require.NoError(t, items.blobStore.Set(ctx, tree.RootHash()[:], fileformat.FileTypeSubtree, treeBytes))

	// The tip drops: X2 is invalidated, and A1 is the best again.
	_, err = items.blockchainClient.InvalidateBlock(ctx, x2.Hash())
	require.NoError(t, err)

	// S2 extends A1 and becomes the best tip.
	s2 := buildChain(a1, 1, 42000)[0]
	coinbase, err := bt.NewTxFromString("02000000010000000000000000000000000000000000000000000000000000000000000000ffffffff03510101ffffffff0100f2052a01000000232103656065e6886ca1e947de3471c9e723673ab6ba34724476417fa9fcef8bafa604ac00000000")
	require.NoError(t, err)
	require.NoError(t, items.blockchainClient.AddBlock(ctx, &model.Block{Header: s2, CoinbaseTx: coinbase, TransactionCount: 2, Subtrees: []*chainhash.Hash{tree.RootHash()}}, "", blockchainoptions.WithMinedSet(true)))
	best, _, err := items.blockchainClient.GetBestBlockHeader(ctx)
	require.NoError(t, err)
	require.Equal(t, s2.Hash().String(), best.Hash().String(), "S2 was the best tip")

	_, genMeta, err := items.blockchainClient.GetBlockHeader(ctx, genesis.Hash())
	require.NoError(t, err)
	_, s2Meta, err := items.blockchainClient.GetBlockHeader(ctx, s2.Hash())
	require.NoError(t, err)
	_, err = items.utxoStore.SetMinedMulti(ctx, []*chainhash.Hash{funding.TxIDChainHash()}, utxo.MinedBlockInfo{BlockID: genMeta.ID, BlockHeight: 0})
	require.NoError(t, err)
	_, err = items.utxoStore.SetMinedMulti(ctx, []*chainhash.Hash{branchOnly.TxIDChainHash()}, utxo.MinedBlockInfo{BlockID: s2Meta.ID, BlockHeight: 2})
	require.NoError(t, err)

	// Branch Y overtakes S2 from A1.
	y := buildChain(a1, 2, 43000)
	for _, h := range y {
		addBlockWithMinedSet(ctx, t, items, h)
	}

	// X2 is revalidated and processed again by block validation; Y3 stays the best tip.
	require.NoError(t, items.blockchainClient.RevalidateBlock(ctx, x2.Hash()))
	require.NoError(t, items.blockchainClient.SetBlockMinedSet(ctx, x2.Hash()))
	best, _, err = items.blockchainClient.GetBestBlockHeader(ctx)
	require.NoError(t, err)
	require.Equal(t, y[1].Hash().String(), best.Hash().String())

	// Nothing that sorts better than X2 is S2.
	betterThanX2, _, err := items.blockchainClient.BlocksBetterThan(ctx, bestTipKeyOf(t, items, x2), nil, 0, 100)
	require.NoError(t, err)
	require.NotContains(t, headerHashes(betterThanX2...), s2.Hash().String())

	_, err = ba.replayBestTipChanges(ctx)
	require.NoError(t, err)
	requireBlockAssemblyTip(t, items, y[1])
	requireReplayPosition(t, items, y[1])
	requireNoLowWater(t, items)

	current := ba.subtreeProcessor.GetCurrentSubtree()
	inPool := make([]string, 0, len(current.Nodes))
	for _, node := range current.Nodes {
		if !node.Hash.IsEqual(subtree.CoinbasePlaceholderHash) {
			inPool = append(inPool, node.Hash.String())
		}
	}
	require.Contains(t, inPool, branchOnly.TxIDChainHash().String(), "the transaction only the short branch S2 had must be in the pool")
}

// TestReplayBestTipChanges_DroppedTipNeverAppliedReloadsItsTransactions is the case the low-water
// mark's key alone misses. Block assembly is on A1. X extends A1, carries a transaction P no other
// block has, and becomes the best tip; X is then invalidated, so the best tip drops back to A1,
// all before block assembly looks. The replay reads only valid blocks and the mark's key is A1,
// where block assembly already is, so no move ever touches X. The mark names X as the tip that
// dropped, and block assembly never applied it, so the replay resets and the reload finds P.
func TestReplayBestTipChanges_DroppedTipNeverAppliedReloadsItsTransactions(t *testing.T) {
	initPrometheusMetrics()
	ctx := t.Context()
	// The default retention keeps every move here an in-memory reorg, so the only reset is
	// the one the dropped tip asks for: nothing else reloads P from the UTXO store.
	items := setupBlockAssemblyTest(t, func(s *settings.Settings) {
		s.BlockAssembly.StoreTxInpointsForSubtreeMeta = true
	}, withCoinbaseMaturity(testCoinbaseMaturity))
	ba := items.blockAssembler
	ba.subtreeProcessor.Stop(context.Background())
	stp, err := subtreeprocessor.NewSubtreeProcessor(ctx, ulogger.TestLogger{}, ba.settings, items.blobStore, items.blockchainClient, items.utxoStore, items.newSubtreeChan)
	require.NoError(t, err)
	ba.subtreeProcessor = stp
	stp.Start(ctx)

	genesis := genesisHeader(t, items)
	a1 := buildChain(genesis, 1, 50000)[0]
	addBlockWithMinedSet(ctx, t, items, a1)
	startReplayAt(t, items, a1, 1)
	stp.InitCurrentBlockHeader(a1)
	require.NoError(t, items.utxoStore.SetBlockHeight(2))

	// P spends a confirmed transaction, and only X contains it.
	funding := replayPoolTx(&chainhash.Hash{}, 100)
	blockOnly := replayPoolTx(funding.TxIDChainHash(), 99)
	for _, tx := range []*bt.Tx{funding, blockOnly} {
		_, err = items.utxoStore.(*utxosql.Store).Create(ctx, tx, 1)
		require.NoError(t, err)
	}
	require.NoError(t, items.utxoStore.MarkTransactionsOnLongestChain(ctx, []chainhash.Hash{*funding.TxIDChainHash(), *blockOnly.TxIDChainHash()}, true))

	tree, err := subtree.NewTreeByLeafCount(2)
	require.NoError(t, err)
	require.NoError(t, tree.AddCoinbaseNode())
	require.NoError(t, tree.AddNode(*blockOnly.TxIDChainHash(), 1, uint64(len(blockOnly.Bytes()))))
	treeBytes, err := tree.Serialize()
	require.NoError(t, err)
	require.NoError(t, items.blobStore.Set(ctx, tree.RootHash()[:], fileformat.FileTypeSubtree, treeBytes))

	x := buildChain(a1, 1, 51000)[0]
	coinbase, err := bt.NewTxFromString("02000000010000000000000000000000000000000000000000000000000000000000000000ffffffff03510101ffffffff0100f2052a01000000232103656065e6886ca1e947de3471c9e723673ab6ba34724476417fa9fcef8bafa604ac00000000")
	require.NoError(t, err)
	require.NoError(t, items.blockchainClient.AddBlock(ctx, &model.Block{Header: x, CoinbaseTx: coinbase, TransactionCount: 2, Subtrees: []*chainhash.Hash{tree.RootHash()}}, "", blockchainoptions.WithMinedSet(true)))
	best, _, err := items.blockchainClient.GetBestBlockHeader(ctx)
	require.NoError(t, err)
	require.Equal(t, x.Hash().String(), best.Hash().String(), "X was the best tip")

	_, genMeta, err := items.blockchainClient.GetBlockHeader(ctx, genesis.Hash())
	require.NoError(t, err)
	_, xMeta, err := items.blockchainClient.GetBlockHeader(ctx, x.Hash())
	require.NoError(t, err)
	_, err = items.utxoStore.SetMinedMulti(ctx, []*chainhash.Hash{funding.TxIDChainHash()}, utxo.MinedBlockInfo{BlockID: genMeta.ID, BlockHeight: 0, OnLongestChain: true})
	require.NoError(t, err)
	_, err = items.utxoStore.SetMinedMulti(ctx, []*chainhash.Hash{blockOnly.TxIDChainHash()}, utxo.MinedBlockInfo{BlockID: xMeta.ID, BlockHeight: 2, OnLongestChain: true})
	require.NoError(t, err)

	// X is invalidated, and block validation processes the invalidation: P loses X's block id
	// and is stamped unmined, and X's mined_set is set again.
	_, err = items.blockchainClient.InvalidateBlock(ctx, x.Hash())
	require.NoError(t, err)
	_, err = items.utxoStore.SetMinedMulti(ctx, []*chainhash.Hash{blockOnly.TxIDChainHash()}, utxo.MinedBlockInfo{BlockID: xMeta.ID, BlockHeight: 2, UnsetMined: true})
	require.NoError(t, err)
	require.NoError(t, items.blockchainClient.SetBlockMinedSet(ctx, x.Hash()))

	best, _, err = items.blockchainClient.GetBestBlockHeader(ctx)
	require.NoError(t, err)
	require.Equal(t, a1.Hash().String(), best.Hash().String(), "the best tip dropped back to block assembly's tip")

	markData, err := items.blockchainClient.GetState(ctx, model.BestTipLowWaterStateKey)
	require.NoError(t, err)
	mark, err := model.NewBestTipLowWaterFromBytes(markData)
	require.NoError(t, err)
	require.Equal(t, a1.Hash().String(), mark.Key.Hash.String())
	require.Equal(t, []chainhash.Hash{*x.Hash()}, mark.DroppedTips)

	require.False(t, poolContains(ba, blockOnly.TxIDChainHash()), "nothing but the replay puts P in the pool")

	_, err = ba.replayBestTipChanges(ctx)
	require.NoError(t, err)
	requireBlockAssemblyTip(t, items, a1)
	requireReplayPosition(t, items, a1)
	requireNoLowWater(t, items)
	require.False(t, ba.bestTipResetReplayPending.Load())

	saved, err := items.blockchainClient.GetState(ctx, BestTipPositionStateKey)
	require.NoError(t, err)
	require.Equal(t, bestTipKeyOf(t, items, a1).Bytes(), saved, "the position is saved before the mark is cleared")

	require.True(t, poolContains(ba, blockOnly.TxIDChainHash()), "the transaction only the dropped tip X had must be in the pool")
}

// poolContains reports whether the current subtree of block assembly holds hash.
func poolContains(ba *BlockAssembler, hash *chainhash.Hash) bool {
	for _, node := range ba.subtreeProcessor.GetCurrentSubtree().Nodes {
		if node.Hash.IsEqual(hash) {
			return true
		}
	}

	return false
}
