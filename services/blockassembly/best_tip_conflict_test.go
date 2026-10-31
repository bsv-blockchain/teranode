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
