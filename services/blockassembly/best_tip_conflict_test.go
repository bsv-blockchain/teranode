package blockassembly

import (
	"context"
	"net/url"
	"testing"
	"time"

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
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	utxosql "github.com/bsv-blockchain/teranode/stores/utxo/sql"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

// Exercise reset conflict swaps against PostgreSQL as well as the SQLite pool fixture.
func TestResetToHistoricalTip_RestoresConflictingSpender(t *testing.T) {
	for _, commonWinner := range []bool{false, true} {
		name := "orphaned_winner"
		if commonWinner {
			name = "winner_on_both_branches"
		}
		t.Run(name, func(t *testing.T) { testResetHistoricalConflict(t, commonWinner, false) })
	}
	// A reset is the path for reorgs as deep as the UTXO retention, so the pruner may
	// already have deleted the winner while the subtree still lists it as conflicting.
	// The reset must skip it rather than fail on its missing record for ever.
	t.Run("pruned_winner", func(t *testing.T) { testResetHistoricalConflict(t, false, true) })
}

func testResetHistoricalConflict(t *testing.T, commonWinner, pruneWinner bool) {
	initPrometheusMetrics()
	items := setupBlockAssemblyTest(t, func(s *settings.Settings) { s.BlockAssembly.OnRestartValidateParentChain = false })
	ba := items.blockAssembler
	ba.subtreeProcessor.Stop(context.Background())

	// The pruned-winner case runs on the SQLite fixture, so it needs no container.
	store, _ := items.utxoStore.(*utxosql.Store)
	if !pruneWinner {
		if testing.Short() {
			t.Skip("requires PostgreSQL testcontainer")
		}
		pg, err := postgres.Run(t.Context(), "postgres:16-alpine", postgres.WithDatabase("replay"), postgres.WithUsername("replay"), postgres.WithPassword("replay"), testcontainers.WithWaitStrategy(wait.ForLog("database system is ready to accept connections").WithOccurrence(2).WithStartupTimeout(time.Minute)))
		test.SkipIfContainerUnavailable(t, err)
		t.Cleanup(func() { require.NoError(t, pg.Terminate(context.Background())) })
		conn, err := pg.ConnectionString(t.Context(), "sslmode=disable")
		require.NoError(t, err)
		dbURL, err := url.Parse(conn)
		require.NoError(t, err)
		store, err = utxosql.New(t.Context(), ulogger.TestLogger{}, ba.settings, dbURL)
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, store.Close(context.Background())) })
		items.utxoStore = store
		ba.utxoStore = store
	}
	require.NotNil(t, store)
	stp, err := subtreeprocessor.NewSubtreeProcessor(t.Context(), ulogger.TestLogger{}, ba.settings, items.blobStore, items.blockchainClient, store, items.newSubtreeChan)
	require.NoError(t, err)
	ba.subtreeProcessor = stp
	stp.Start(t.Context())
	genesis := genesisHeader(t, items)
	a1 := buildChain(genesis, 1, 22000)[0]
	addBlockWithMinedSet(t.Context(), t, items, a1)
	ba.setBestBlockHeader(a1, 1)
	stp.InitCurrentBlockHeader(a1)
	require.NoError(t, store.SetBlockHeight(3))
	funding := replayPoolTx(&chainhash.Hash{}, 100)
	loser := replayPoolTx(funding.TxIDChainHash(), 99)
	child := replayPoolTx(loser.TxIDChainHash(), 98)
	child.Inputs[0].PreviousTxScript = bscript.NewFromBytes([]byte{0x51})
	winner := replayPoolTx(funding.TxIDChainHash(), 98)
	loser.Inputs[0].PreviousTxScript = bscript.NewFromBytes([]byte{0x51})
	winner.Inputs[0].PreviousTxScript = bscript.NewFromBytes([]byte{0x51})
	winner.Inputs[0].PreviousTxSatoshis = 100
	for _, tx := range []*bt.Tx{funding, loser, child} {
		_, _, err = store.SpendAndCreate(t.Context(), tx, 0, utxo.WithCreateOnly())
		require.NoError(t, err)
	}
	_, _, err = store.SpendAndCreate(t.Context(), winner, 1, utxo.WithCreateOnly(), utxo.WithConflicting(true))
	require.NoError(t, err)
	_, _, err = store.SpendAndCreate(t.Context(), loser, 1, utxo.WithSpendOnly())
	require.NoError(t, err)
	_, _, err = store.SpendAndCreate(t.Context(), child, 1, utxo.WithSpendOnly())
	require.NoError(t, err)
	require.NoError(t, store.MarkTransactionsOnLongestChain(t.Context(), []chainhash.Hash{*funding.TxIDChainHash()}, true))
	tree, err := subtree.NewTreeByLeafCount(4)
	require.NoError(t, err)
	require.NoError(t, tree.AddCoinbaseNode())
	require.NoError(t, tree.AddNode(*winner.TxIDChainHash(), 2, uint64(len(winner.Bytes()))))
	tree.ConflictingNodes = []chainhash.Hash{*winner.TxIDChainHash()}
	data, err := tree.Serialize()
	require.NoError(t, err)
	require.NoError(t, items.blobStore.Set(t.Context(), tree.RootHash()[:], fileformat.FileTypeSubtree, data))
	b := buildChain(genesis, 2, 23000)
	coinbase, err := bt.NewTxFromString("02000000010000000000000000000000000000000000000000000000000000000000000000ffffffff03510101ffffffff0100f2052a01000000232103656065e6886ca1e947de3471c9e723673ab6ba34724476417fa9fcef8bafa604ac00000000")
	require.NoError(t, err)
	require.NoError(t, items.blockchainClient.AddBlock(t.Context(), &model.Block{Header: b[0], CoinbaseTx: coinbase, TransactionCount: 2, Subtrees: []*chainhash.Hash{tree.RootHash()}}, "", blockchainoptions.WithMinedSet(true)))
	addBlockWithMinedSet(t.Context(), t, items, b[1])
	a := buildChain(a1, 2, 24000)
	for i, h := range a {
		if commonWinner && i == 0 {
			require.NoError(t, items.blockchainClient.AddBlock(t.Context(), &model.Block{Header: h, CoinbaseTx: coinbase, TransactionCount: 2, Subtrees: []*chainhash.Hash{tree.RootHash()}}, "", blockchainoptions.WithMinedSet(true)))
		} else {
			addBlockWithMinedSet(t.Context(), t, items, h)
		}
	}
	require.NoError(t, ba.resetToBlockAndRecord(t.Context(), b[1], 2, true))
	assertSpender := func(want *bt.Tx, winnerConflicting, loserConflicting bool) {
		t.Helper()
		p, e := store.Get(t.Context(), funding.TxIDChainHash(), fields.Utxos)
		require.NoError(t, e)
		require.NotNil(t, p.SpendingDatas[0])
		require.Equal(t, want.TxIDChainHash(), p.SpendingDatas[0].TxID)
		w, e := store.Get(t.Context(), winner.TxIDChainHash(), fields.Conflicting)
		require.NoError(t, e)
		require.Equal(t, winnerConflicting, w.Conflicting)
		l, e := store.Get(t.Context(), loser.TxIDChainHash(), fields.Conflicting)
		require.NoError(t, e)
		require.Equal(t, loserConflicting, l.Conflicting)
		c, e := store.Get(t.Context(), child.TxIDChainHash(), fields.Conflicting)
		require.NoError(t, e)
		// ProcessConflicting unspent this descendant's input, so its edge is
		// detached before ReverseProcessConflicting runs. Reconstructing that
		// graph is a separate existing limitation; keep the child excluded.
		require.True(t, c.Conflicting, "detached descendant must remain safely excluded")
	}
	assertSpender(winner, false, true)
	if pruneWinner {
		require.NoError(t, store.Delete(t.Context(), winner.TxIDChainHash()))
		require.NoError(t, ba.resetToBlockAndRecord(t.Context(), a[1], 3, true), "a pruned winner has nothing to reverse")
		requireBlockAssemblyTip(t, items, a[1])
		return
	}
	require.NoError(t, ba.resetToBlockAndRecord(t.Context(), a[1], 3, true))
	if commonWinner {
		assertSpender(winner, false, true)
	} else {
		assertSpender(loser, true, false)
	}
}

// setupConflictResolvedAtX puts block assembly on X, at height 2 above A1, where X's subtree
// resolved a double spend: winner and loser both spend funding, and winner is mined in X. The
// returned check fails once winner no longer holds funding's output. Coinbase maturity is 100, so
// moving X back stays below maturity and the coinbase rule never restamps.
func setupConflictResolvedAtX(t *testing.T) (items *baTestItems, a1, x *model.BlockHeader, assertWinnerKeepsInput func()) {
	t.Helper()
	initPrometheusMetrics()
	items = setupBlockAssemblyTest(t, func(s *settings.Settings) { s.BlockAssembly.OnRestartValidateParentChain = false }, withCoinbaseMaturity(testCoinbaseMaturity))
	ba := items.blockAssembler
	ba.subtreeProcessor.Stop(context.Background())
	store, ok := items.utxoStore.(*utxosql.Store)
	require.True(t, ok)
	stp, err := subtreeprocessor.NewSubtreeProcessor(t.Context(), ulogger.TestLogger{}, ba.settings, items.blobStore, items.blockchainClient, store, items.newSubtreeChan)
	require.NoError(t, err)
	ba.subtreeProcessor = stp
	stp.Start(t.Context())

	genesis := genesisHeader(t, items)
	a1 = buildChain(genesis, 1, 25000)[0]
	addBlockWithMinedSet(t.Context(), t, items, a1)
	ba.setBestBlockHeader(a1, 1)
	stp.InitCurrentBlockHeader(a1)
	require.NoError(t, store.SetBlockHeight(3))

	// A non-zero parent keeps funding from reading as a coinbase, which maturity 100 would lock.
	funding := replayPoolTx(&chainhash.Hash{1}, 100)
	loser := replayPoolTx(funding.TxIDChainHash(), 99)
	winner := replayPoolTx(funding.TxIDChainHash(), 98)
	loser.Inputs[0].PreviousTxScript = bscript.NewFromBytes([]byte{0x51})
	winner.Inputs[0].PreviousTxScript = bscript.NewFromBytes([]byte{0x51})
	winner.Inputs[0].PreviousTxSatoshis = 100
	for _, tx := range []*bt.Tx{funding, loser} {
		_, _, err = store.SpendAndCreate(t.Context(), tx, 0, utxo.WithCreateOnly())
		require.NoError(t, err)
	}
	_, _, err = store.SpendAndCreate(t.Context(), winner, 1, utxo.WithCreateOnly(), utxo.WithConflicting(true))
	require.NoError(t, err)
	_, _, err = store.SpendAndCreate(t.Context(), loser, 1, utxo.WithSpendOnly())
	require.NoError(t, err)
	require.NoError(t, store.MarkTransactionsOnLongestChain(t.Context(), []chainhash.Hash{*funding.TxIDChainHash()}, true))

	tree, err := subtree.NewTreeByLeafCount(4)
	require.NoError(t, err)
	require.NoError(t, tree.AddCoinbaseNode())
	require.NoError(t, tree.AddNode(*winner.TxIDChainHash(), 2, uint64(len(winner.Bytes()))))
	tree.ConflictingNodes = []chainhash.Hash{*winner.TxIDChainHash()}
	data, err := tree.Serialize()
	require.NoError(t, err)
	require.NoError(t, items.blobStore.Set(t.Context(), tree.RootHash()[:], fileformat.FileTypeSubtree, data))

	coinbase, err := bt.NewTxFromString("02000000010000000000000000000000000000000000000000000000000000000000000000ffffffff03510101ffffffff0100f2052a01000000232103656065e6886ca1e947de3471c9e723673ab6ba34724476417fa9fcef8bafa604ac00000000")
	require.NoError(t, err)
	x = buildChain(a1, 1, 26000)[0]
	require.NoError(t, items.blockchainClient.AddBlock(t.Context(), &model.Block{Header: x, CoinbaseTx: coinbase, TransactionCount: 2, Subtrees: []*chainhash.Hash{tree.RootHash()}}, "", blockchainoptions.WithMinedSet(true)))
	_, xMeta, err := items.blockchainClient.GetBlockHeader(t.Context(), x.Hash())
	require.NoError(t, err)
	// Block validation stamps the winner with X's id, which is what the guard reads.
	_, err = store.SetMinedMulti(t.Context(), []*chainhash.Hash{winner.TxIDChainHash()}, utxo.MinedBlockInfo{BlockID: xMeta.ID, BlockHeight: 2})
	require.NoError(t, err)

	require.NoError(t, ba.resetToBlockAndRecord(t.Context(), x, 2, true))
	requireBlockAssemblyTip(t, items, x)

	assertWinnerKeepsInput = func() {
		t.Helper()
		parent, e := store.Get(t.Context(), funding.TxIDChainHash(), fields.Utxos)
		require.NoError(t, e)
		require.NotNil(t, parent.SpendingDatas[0])
		require.Equal(t, winner.TxIDChainHash(), parent.SpendingDatas[0].TxID)
		w, e := store.Get(t.Context(), winner.TxIDChainHash(), fields.Conflicting)
		require.NoError(t, e)
		require.False(t, w.Conflicting)
		l, e := store.Get(t.Context(), loser.TxIDChainHash(), fields.Conflicting)
		require.NoError(t, e)
		require.True(t, l.Conflicting)
	}
	assertWinnerKeepsInput()

	return items, a1, x, assertWinnerKeepsInput
}

// TestReplayBestTipChanges_PastChangeOverResolvedConflictOnBestChain replays a past change whose
// move back would reverse a conflict swap in a block of the current best chain. Block assembly is
// on X, which resolved a double spend. The best tip went to Y2 on a sibling branch and then to X3,
// which builds on X. Replaying Y2 would move X back, and the current-chain guard refuses to demote
// X's winner, so the replay would fail on every pass. The replay restamps the Y branch instead,
// and X keeps its swap.
func TestReplayBestTipChanges_PastChangeOverResolvedConflictOnBestChain(t *testing.T) {
	items, a1, x, assertWinnerKeepsInput := setupConflictResolvedAtX(t)
	ba := items.blockAssembler
	startReplayAt(t, items, x, 2)

	// Y2 sorts better than X, then X3 better than Y2. X2 ties Y2 on work and loses on id, and
	// the replay skips it because X3 builds on it.
	y := buildChain(a1, 2, 27000)
	for _, h := range y {
		addBlockWithMinedSet(t.Context(), t, items, h)
	}
	xs := buildChain(x, 2, 28000)
	for _, h := range xs {
		addBlockWithMinedSet(t.Context(), t, items, h)
	}

	_, err := ba.replayBestTipChanges(t.Context())
	require.NoError(t, err, "a move back over X would be refused by the current-chain guard")
	requireBlockAssemblyTip(t, items, xs[1])
	assertWinnerKeepsInput()
}

// TestReplayBestTipChanges_PastChangeOverResolvedConflictTipLost is the same race with three
// branches. Block assembly is on X2, a child of X. The best tip went to Y3 on a branch from A1 and
// then to Z3, another child of X, so X2 lost too. Replaying Y3 would move back X2, off the best
// chain, and then X, on it, and the guard refuses to demote X's winner. The replay restamps the Y
// branch, then reorgs from X2 to Z3.
func TestReplayBestTipChanges_PastChangeOverResolvedConflictTipLost(t *testing.T) {
	items, a1, x, assertWinnerKeepsInput := setupConflictResolvedAtX(t)
	ba := items.blockAssembler

	x2 := buildChain(x, 1, 29000)[0]
	addBlockWithMinedSet(t.Context(), t, items, x2)
	require.NoError(t, ba.resetToBlockAndRecord(t.Context(), x2, 3, true))
	requireBlockAssemblyTip(t, items, x2)
	startReplayAt(t, items, x2, 3)

	// Z is stored before Y, so at equal work Z's blocks have the lower ids and sort better. The
	// read is Y3, then Z2 and Z3: Y3 is replayed while block assembly is still on X2.
	z := buildChain(x, 3, 30000)
	for _, h := range z {
		addBlockWithMinedSet(t.Context(), t, items, h)
	}
	// Y1 carries yOnly, a transaction no other block has, spending yFunding. Block validation
	// stamped it mined when Y was the best chain.
	store, ok := items.utxoStore.(*utxosql.Store)
	require.True(t, ok)
	yFunding := replayPoolTx(&chainhash.Hash{2}, 100)
	yOnly := replayPoolTx(yFunding.TxIDChainHash(), 99)
	yOnly.Inputs[0].PreviousTxScript = bscript.NewFromBytes([]byte{0x51})
	_, _, err := store.SpendAndCreate(t.Context(), yFunding, 0, utxo.WithCreateOnly())
	require.NoError(t, err)
	_, _, err = store.SpendAndCreate(t.Context(), yOnly, 2)
	require.NoError(t, err)
	require.NoError(t, store.MarkTransactionsOnLongestChain(t.Context(), []chainhash.Hash{*yFunding.TxIDChainHash(), *yOnly.TxIDChainHash()}, true))
	tree, err := subtree.NewTreeByLeafCount(4)
	require.NoError(t, err)
	require.NoError(t, tree.AddCoinbaseNode())
	require.NoError(t, tree.AddNode(*yOnly.TxIDChainHash(), 1, uint64(len(yOnly.Bytes()))))
	data, err := tree.Serialize()
	require.NoError(t, err)
	require.NoError(t, items.blobStore.Set(t.Context(), tree.RootHash()[:], fileformat.FileTypeSubtree, data))
	coinbase, err := bt.NewTxFromString("02000000010000000000000000000000000000000000000000000000000000000000000000ffffffff03510101ffffffff0100f2052a01000000232103656065e6886ca1e947de3471c9e723673ab6ba34724476417fa9fcef8bafa604ac00000000")
	require.NoError(t, err)

	y := buildChain(a1, 3, 31000)
	require.NoError(t, items.blockchainClient.AddBlock(t.Context(), &model.Block{Header: y[0], CoinbaseTx: coinbase, TransactionCount: 2, Subtrees: []*chainhash.Hash{tree.RootHash()}}, "", blockchainoptions.WithMinedSet(true)))
	for _, h := range y[1:] {
		addBlockWithMinedSet(t.Context(), t, items, h)
	}
	_, y1Meta, err := items.blockchainClient.GetBlockHeader(t.Context(), y[0].Hash())
	require.NoError(t, err)
	_, err = items.utxoStore.SetMinedMulti(t.Context(), []*chainhash.Hash{yOnly.TxIDChainHash()}, utxo.MinedBlockInfo{BlockID: y1Meta.ID, BlockHeight: 2})
	require.NoError(t, err)

	_, err = ba.replayBestTipChanges(t.Context())
	require.NoError(t, err, "a move back over X would be refused by the current-chain guard")
	requireBlockAssemblyTip(t, items, z[2])
	assertWinnerKeepsInput()

	pool := ba.subtreeProcessor.GetTransactionHashes(t.Context())
	require.Contains(t, pool, *yOnly.TxIDChainHash(), "Y's block-only transaction reaches the pool")
	meta, err := items.utxoStore.Get(t.Context(), yOnly.TxIDChainHash(), fields.UnminedSince)
	require.NoError(t, err)
	require.NotZero(t, meta.UnminedSince, "Y's block-only transaction is marked unmined")
}
