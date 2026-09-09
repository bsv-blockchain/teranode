package subtreevalidation

import (
	"context"
	"net/url"
	"sync/atomic"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/bsv-blockchain/teranode/stores/utxo/sql"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// countingUtxoStore wraps a real utxo.Store and counts per-transaction reads. It
// overrides GetConflictingChildren to run the package-level walk against itself,
// so the walk's per-node reads are observed by the counter instead of going to
// the underlying store directly.
//
// Both Get and GetMeta are counted: blessing issues one GetMeta per member of
// the counter-conflicting set, so a counter watching Get alone would be blind to
// a regression that reintroduced the O(N^2) blow-up on that side.
type countingUtxoStore struct {
	utxo.Store
	gets atomic.Int64
}

func (c *countingUtxoStore) Get(ctx context.Context, hash *chainhash.Hash, f ...fields.FieldName) (*meta.Data, error) {
	c.gets.Add(1)

	return c.Store.Get(ctx, hash, f...)
}

func (c *countingUtxoStore) GetMeta(ctx context.Context, hash *chainhash.Hash, data *meta.Data) error {
	c.gets.Add(1)

	return c.Store.GetMeta(ctx, hash, data)
}

func (c *countingUtxoStore) GetConflictingChildren(ctx context.Context, txHash chainhash.Hash) ([]chainhash.Hash, error) {
	return utxo.GetConflictingChildren(ctx, c, txHash)
}

// spendingChild builds a minimal extended child tx spending parent:vout, with the
// empty unlocking script the SQL store's NOT NULL constraint requires.
func spendingChild(t *testing.T, parent *bt.Tx, vout uint32) *bt.Tx {
	t.Helper()

	child := bt.NewTx()
	require.NoError(t, child.From(parent.TxIDChainHash().String(), vout, parent.Outputs[vout].LockingScript.String(), parent.Outputs[vout].Satoshis))
	child.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{})
	require.NoError(t, child.AddP2PKHOutputFromScript(parent.Outputs[vout].LockingScript, parent.Outputs[vout].Satoshis))

	return child
}

// buildLoserChain seeds the store with the shape of the mainnet 960,828 wedge: a
// conflicting (mined-winner) tx1 whose counter-conflicting loser heads a linear
// self-spend chain of the given length.
func buildLoserChain(ctx context.Context, t *testing.T, utxoStore utxo.Store, chainLength int) {
	t.Helper()

	_, err := utxoStore.Create(ctx, parentTx1, 122)
	require.NoError(t, err)

	// The loser: spends the same parent output as tx1 (the winner) but is a
	// distinct tx, accepted into the store first.
	loser := tx1.Clone()
	loser.Version = 2

	_, err = utxoStore.Spend(ctx, loser, 122)
	require.NoError(t, err)

	_, err = utxoStore.Create(ctx, loser, 122)
	require.NoError(t, err)

	// Extend the loser with a linear chain of descendants, each spending the
	// previous tx's output 0 — the store records every spend in SpendingDatas.
	prev := loser

	for i := 0; i < chainLength; i++ {
		child := spendingChild(t, prev, 0)

		_, err = utxoStore.Spend(ctx, child, 122)
		require.NoError(t, err)

		_, err = utxoStore.Create(ctx, child, 122)
		require.NoError(t, err)

		prev = child
	}

	// The winner: mined double-spend of the loser, flagged conflicting locally.
	_, err = utxoStore.Create(ctx, tx1, 123, utxo.WithConflicting(true))
	require.NoError(t, err)
}

// Test_checkCounterConflictingOnCurrentChain_scaling reproduces the shape of the
// mainnet 960,828 wedge (issue 1391): a conflicting (mined-winner) tx whose
// counter-conflicting (loser) tx heads a long linear self-spend chain. Blessing
// the winner must read each chain member O(1) times; the pre-fix code re-walked
// the full descendant cone once per descendant, i.e. O(K^2) store reads, which
// never converged in production while the chain kept growing.
func Test_checkCounterConflictingOnCurrentChain_scaling(t *testing.T) {
	InitPrometheusMetrics()

	const chainLength = 300

	ctx := context.Background()
	logger := ulogger.NewErrorTestLogger(t)

	tSettings := test.CreateBaseTestSettings(t)

	utxoStoreURL, err := url.Parse("sqlitememory:///test")
	require.NoError(t, err)

	utxoStore, err := sql.New(ctx, logger, tSettings, utxoStoreURL)
	require.NoError(t, err)

	buildLoserChain(ctx, t, utxoStore, chainLength)

	countingStore := &countingUtxoStore{Store: utxoStore}
	s := &Server{
		logger:    logger,
		utxoStore: countingStore,
		settings:  tSettings,
	}

	err = s.checkCounterConflictingOnCurrentChain(ctx, *tx1.TxIDChainHash(), map[uint32]bool{})
	require.NoError(t, err)

	gets := countingStore.gets.Load()

	// Lower bound first: without it a regression that skips the descendant walk
	// entirely (GetConflictingChildren returning early) would satisfy the upper
	// bound and pass. The walk has to actually visit the chain.
	require.GreaterOrEqualf(t, gets, int64(chainLength),
		"the walk must actually cover the counter-conflicting chain, got only %d Get calls for K=%d",
		gets, chainLength)

	require.LessOrEqualf(t, gets, int64(4*chainLength),
		"blessing a conflicting tx must read the counter-conflicting descendant chain O(K) times, got %d Get calls for K=%d (O(K^2) indicates the per-element re-walk of issue 1391)",
		gets, chainLength)
}

// freezeOutput marks the given output of tx as frozen in the store, so its
// spending data carries the frozen sentinel hash.
func freezeOutput(ctx context.Context, t *testing.T, store utxo.Store, tx *bt.Tx, vout uint32, tSettings *settings.Settings) {
	t.Helper()

	utxoHash, err := util.UTXOHashFromOutput(tx.TxIDChainHash(), tx.Outputs[vout], vout)
	require.NoError(t, err)

	err = store.FreezeUTXOs(ctx, []*utxo.Spend{{
		TxID:     tx.TxIDChainHash(),
		Vout:     vout,
		UTXOHash: utxoHash,
	}}, tSettings)
	require.NoError(t, err)
}

// Test_checkCounterConflictingOnCurrentChain_frozen pins the frozen-sentinel
// rejection semantics across all three detection paths. Dropping the per-member
// re-walk (issue 1391) must not weaken any of them.
func Test_checkCounterConflictingOnCurrentChain_frozen(t *testing.T) {
	InitPrometheusMetrics()

	newStore := func(t *testing.T) (context.Context, utxo.Store, *Server, *settings.Settings) {
		ctx := context.Background()
		logger := ulogger.NewErrorTestLogger(t)
		tSettings := test.CreateBaseTestSettings(t)

		utxoStoreURL, err := url.Parse("sqlitememory:///test")
		require.NoError(t, err)

		utxoStore, err := sql.New(ctx, logger, tSettings, utxoStoreURL)
		require.NoError(t, err)

		s := &Server{
			logger:    logger,
			utxoStore: utxoStore,
			settings:  tSettings,
		}

		return ctx, utxoStore, s, tSettings
	}

	t.Run("frozen direct counter", func(t *testing.T) {
		ctx, utxoStore, s, tSettings := newStore(t)

		_, err := utxoStore.Create(ctx, parentTx1, 122)
		require.NoError(t, err)

		// freeze the parent output the conflicting tx spends: its spending data
		// becomes the frozen sentinel, i.e. the counter-conflicting "tx" is frozen
		freezeOutput(ctx, t, utxoStore, parentTx1, tx1.Inputs[0].PreviousTxOutIndex, tSettings)

		_, err = utxoStore.Create(ctx, tx1, 123, utxo.WithConflicting(true))
		require.NoError(t, err)

		err = s.checkCounterConflictingOnCurrentChain(ctx, *tx1.TxIDChainHash(), map[uint32]bool{})
		require.Error(t, err)
		require.Contains(t, err.Error(), "counter conflicting tx is frozen")
	})

	t.Run("frozen deep in loser cone", func(t *testing.T) {
		ctx, utxoStore, s, tSettings := newStore(t)

		_, err := utxoStore.Create(ctx, parentTx1, 122)
		require.NoError(t, err)

		loser := tx1.Clone()
		loser.Version = 2

		_, err = utxoStore.Spend(ctx, loser, 122)
		require.NoError(t, err)

		_, err = utxoStore.Create(ctx, loser, 122)
		require.NoError(t, err)

		child := spendingChild(t, loser, 0)

		_, err = utxoStore.Spend(ctx, child, 122)
		require.NoError(t, err)

		_, err = utxoStore.Create(ctx, child, 122)
		require.NoError(t, err)

		// freeze the chain tip's output: the frozen sentinel appears deep inside
		// the loser's descendant cone
		freezeOutput(ctx, t, utxoStore, child, 0, tSettings)

		_, err = utxoStore.Create(ctx, tx1, 123, utxo.WithConflicting(true))
		require.NoError(t, err)

		err = s.checkCounterConflictingOnCurrentChain(ctx, *tx1.TxIDChainHash(), map[uint32]bool{})
		require.Error(t, err)
		require.Contains(t, err.Error(), "tx has frozen child")
	})

	t.Run("frozen output on the winner itself", func(t *testing.T) {
		ctx, utxoStore, s, tSettings := newStore(t)

		_, err := utxoStore.Create(ctx, parentTx1, 122)
		require.NoError(t, err)

		loser := tx1.Clone()
		loser.Version = 2

		_, err = utxoStore.Spend(ctx, loser, 122)
		require.NoError(t, err)

		_, err = utxoStore.Create(ctx, loser, 122)
		require.NoError(t, err)

		_, err = utxoStore.Create(ctx, tx1, 123, utxo.WithConflicting(true))
		require.NoError(t, err)

		// freeze one of the winner's own outputs: the sentinel appears in the
		// winner's own descendant cone, which the single remaining walk must still
		// reject
		freezeOutput(ctx, t, utxoStore, tx1, 0, tSettings)

		err = s.checkCounterConflictingOnCurrentChain(ctx, *tx1.TxIDChainHash(), map[uint32]bool{})
		require.Error(t, err)
		require.Contains(t, err.Error(), "child transaction is frozen")
	})
}
