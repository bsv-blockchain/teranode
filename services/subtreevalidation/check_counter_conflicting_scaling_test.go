package subtreevalidation

import (
	"context"
	"net/url"
	"sync/atomic"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
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

// countingUtxoStore wraps a real utxo.Store and counts Get calls. It overrides
// GetConflictingChildren to run the package-level walk against itself, so the
// walk's per-node Get reads are observed by the counter instead of going to the
// underlying store directly.
type countingUtxoStore struct {
	utxo.Store
	gets atomic.Int64
}

func (c *countingUtxoStore) Get(ctx context.Context, hash *chainhash.Hash, f ...fields.FieldName) (*meta.Data, error) {
	c.gets.Add(1)
	return c.Store.Get(ctx, hash, f...)
}

func (c *countingUtxoStore) GetConflictingChildren(ctx context.Context, txHash chainhash.Hash) ([]chainhash.Hash, error) {
	return utxo.GetConflictingChildren(ctx, c, txHash, 0)
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

// Test_checkCounterConflictingOnCurrentChain_scaling reproduces the shape of the
// mainnet 960,828 wedge (issue 1391): a conflicting (mined-winner) tx whose
// counter-conflicting (loser) tx heads a long linear self-spend chain. Blessing
// the winner must read each chain member O(1) times; the pre-fix code re-walked
// the full descendant cone once per descendant, i.e. O(K^2) store reads, which
// never converged in production while the chain kept growing.
func Test_checkCounterConflictingOnCurrentChain_scaling(t *testing.T) {
	const chainLength = 300

	ctx := context.Background()
	logger := ulogger.NewErrorTestLogger(t)

	tSettings := test.CreateBaseTestSettings(t)

	utxoStoreURL, err := url.Parse("sqlitememory:///test")
	require.NoError(t, err)

	utxoStore, err := sql.New(ctx, logger, tSettings, utxoStoreURL)
	require.NoError(t, err)

	_, _, err = utxoStore.SpendAndCreate(ctx, parentTx1, 122, utxo.WithCreateOnly())
	require.NoError(t, err)

	// The loser: spends the same parent output as tx1 (the winner) but is a
	// distinct tx, accepted into the store first.
	loser := tx1.Clone()
	loser.Version = 2

	_, _, err = utxoStore.SpendAndCreate(ctx, loser, 122, utxo.WithSpendOnly())
	require.NoError(t, err)

	_, _, err = utxoStore.SpendAndCreate(ctx, loser, 122, utxo.WithCreateOnly())
	require.NoError(t, err)

	// Extend the loser with a linear chain of descendants, each spending the
	// previous tx's output 0 — the store records every spend in SpendingDatas.
	prev := loser
	for i := 0; i < chainLength; i++ {
		child := spendingChild(t, prev, 0)

		_, _, err = utxoStore.SpendAndCreate(ctx, child, 122, utxo.WithSpendOnly())
		require.NoError(t, err)

		_, _, err = utxoStore.SpendAndCreate(ctx, child, 122, utxo.WithCreateOnly())
		require.NoError(t, err)

		prev = child
	}

	// The winner: mined double-spend of the loser, flagged conflicting locally.
	_, _, err = utxoStore.SpendAndCreate(ctx, tx1, 123, utxo.WithConflicting(true), utxo.WithCreateOnly())
	require.NoError(t, err)

	countingStore := &countingUtxoStore{Store: utxoStore}
	s := &Server{
		logger:    logger,
		utxoStore: countingStore,
		settings:  tSettings,
	}

	err = s.checkCounterConflictingOnCurrentChain(ctx, *tx1.TxIDChainHash(), map[uint32]bool{})
	require.NoError(t, err)

	gets := countingStore.gets.Load()
	require.LessOrEqualf(t, gets, int64(4*chainLength),
		"blessing a conflicting tx must read the counter-conflicting descendant chain O(K) times, got %d Get calls for K=%d (O(K^2) indicates the per-element re-walk of issue 1391)",
		gets, chainLength)
}

// freezeOutput marks the given output of tx as frozen in the store, so its
// spending data carries the frozen sentinel hash.
func freezeOutput(t *testing.T, ctx context.Context, store utxo.Store, tx *bt.Tx, vout uint32, tSettings *settings.Settings) {
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

// Test_checkCounterConflictingOnCurrentChain_budget verifies the walk budget
// fails closed with the distinct walk-limit error when the counter-conflicting
// descendant cone exceeds utxostore_conflictingChildrenMaxNodes.
func Test_checkCounterConflictingOnCurrentChain_budget(t *testing.T) {
	const chainLength = 10

	ctx := context.Background()
	logger := ulogger.NewErrorTestLogger(t)

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.UtxoStore.ConflictingChildrenMaxNodes = 5

	utxoStoreURL, err := url.Parse("sqlitememory:///test")
	require.NoError(t, err)

	utxoStore, err := sql.New(ctx, logger, tSettings, utxoStoreURL)
	require.NoError(t, err)

	_, _, err = utxoStore.SpendAndCreate(ctx, parentTx1, 122, utxo.WithCreateOnly())
	require.NoError(t, err)

	loser := tx1.Clone()
	loser.Version = 2

	_, _, err = utxoStore.SpendAndCreate(ctx, loser, 122, utxo.WithSpendOnly())
	require.NoError(t, err)

	_, _, err = utxoStore.SpendAndCreate(ctx, loser, 122, utxo.WithCreateOnly())
	require.NoError(t, err)

	prev := loser
	for i := 0; i < chainLength; i++ {
		child := spendingChild(t, prev, 0)

		_, _, err = utxoStore.SpendAndCreate(ctx, child, 122, utxo.WithSpendOnly())
		require.NoError(t, err)

		_, _, err = utxoStore.SpendAndCreate(ctx, child, 122, utxo.WithCreateOnly())
		require.NoError(t, err)

		prev = child
	}

	_, _, err = utxoStore.SpendAndCreate(ctx, tx1, 123, utxo.WithConflicting(true), utxo.WithCreateOnly())
	require.NoError(t, err)

	s := &Server{
		logger:    logger,
		utxoStore: utxoStore,
		settings:  tSettings,
	}

	err = s.checkCounterConflictingOnCurrentChain(ctx, *tx1.TxIDChainHash(), map[uint32]bool{})
	require.Error(t, err)
	require.ErrorIs(t, err, errors.ErrUtxoWalkLimitExceeded)
}

// Test_checkCounterConflictingOnCurrentChain_frozen pins the frozen-sentinel
// rejection semantics across all three detection paths, on the SQL store.
func Test_checkCounterConflictingOnCurrentChain_frozen(t *testing.T) {
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

		_, _, err := utxoStore.SpendAndCreate(ctx, parentTx1, 122, utxo.WithCreateOnly())
		require.NoError(t, err)

		// freeze the parent output the conflicting tx spends: its spending data
		// becomes the frozen sentinel, i.e. the counter-conflicting "tx" is frozen
		freezeOutput(t, ctx, utxoStore, parentTx1, 2, tSettings)

		_, _, err = utxoStore.SpendAndCreate(ctx, tx1, 123, utxo.WithConflicting(true), utxo.WithCreateOnly())
		require.NoError(t, err)

		err = s.checkCounterConflictingOnCurrentChain(ctx, *tx1.TxIDChainHash(), map[uint32]bool{})
		require.Error(t, err)
		require.Contains(t, err.Error(), "counter conflicting tx is frozen")
	})

	t.Run("frozen deep in loser cone", func(t *testing.T) {
		ctx, utxoStore, s, tSettings := newStore(t)

		_, _, err := utxoStore.SpendAndCreate(ctx, parentTx1, 122, utxo.WithCreateOnly())
		require.NoError(t, err)

		loser := tx1.Clone()
		loser.Version = 2

		_, _, err = utxoStore.SpendAndCreate(ctx, loser, 122, utxo.WithSpendOnly())
		require.NoError(t, err)

		_, _, err = utxoStore.SpendAndCreate(ctx, loser, 122, utxo.WithCreateOnly())
		require.NoError(t, err)

		child := spendingChild(t, loser, 0)

		_, _, err = utxoStore.SpendAndCreate(ctx, child, 122, utxo.WithSpendOnly())
		require.NoError(t, err)

		_, _, err = utxoStore.SpendAndCreate(ctx, child, 122, utxo.WithCreateOnly())
		require.NoError(t, err)

		// freeze the chain tip's output: the frozen sentinel appears deep inside
		// the loser's descendant cone
		freezeOutput(t, ctx, utxoStore, child, 0, tSettings)

		_, _, err = utxoStore.SpendAndCreate(ctx, tx1, 123, utxo.WithConflicting(true), utxo.WithCreateOnly())
		require.NoError(t, err)

		err = s.checkCounterConflictingOnCurrentChain(ctx, *tx1.TxIDChainHash(), map[uint32]bool{})
		require.Error(t, err)
		require.Contains(t, err.Error(), "tx has frozen child")
	})

	t.Run("frozen output on the winner itself", func(t *testing.T) {
		ctx, utxoStore, s, tSettings := newStore(t)

		_, _, err := utxoStore.SpendAndCreate(ctx, parentTx1, 122, utxo.WithCreateOnly())
		require.NoError(t, err)

		loser := tx1.Clone()
		loser.Version = 2

		_, _, err = utxoStore.SpendAndCreate(ctx, loser, 122, utxo.WithSpendOnly())
		require.NoError(t, err)

		_, _, err = utxoStore.SpendAndCreate(ctx, loser, 122, utxo.WithCreateOnly())
		require.NoError(t, err)

		_, _, err = utxoStore.SpendAndCreate(ctx, tx1, 123, utxo.WithConflicting(true), utxo.WithCreateOnly())
		require.NoError(t, err)

		// freeze one of the winner's own outputs: the sentinel appears in the
		// winner's own descendant cone, which must still reject the bless
		freezeOutput(t, ctx, utxoStore, tx1, 0, tSettings)

		err = s.checkCounterConflictingOnCurrentChain(ctx, *tx1.TxIDChainHash(), map[uint32]bool{})
		require.Error(t, err)
		require.Contains(t, err.Error(), "child transaction is frozen")
	})
}
