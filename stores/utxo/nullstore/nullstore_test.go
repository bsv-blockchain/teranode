package nullstore

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/tests"
	"github.com/bsv-blockchain/teranode/util/cohort"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNullStoreImplementsInterface(t *testing.T) {
	var _ utxo.Store = (*NullStore)(nil)
}

func TestNullStoreCreate(t *testing.T) {
	store := &NullStore{}
	tx := &bt.Tx{}
	_ = tx.FromUTXOs(&bt.UTXO{
		TxIDHash:      &chainhash.Hash{},
		Vout:          0,
		LockingScript: &bscript.Script{},
		Satoshis:      100,
	})
	ctx := context.Background()
	data, err := store.Create(ctx, tx, 0)

	assert.NoError(t, err)
	assert.NotNil(t, data)
}

func TestNullStoreGet(t *testing.T) {
	store := &NullStore{}
	ctx := context.Background()
	hash := &chainhash.Hash{}
	data, err := store.Get(ctx, hash)

	assert.NoError(t, err)
	assert.NotNil(t, data)
}

func TestNullStoreSetLocked(t *testing.T) {
	store := &NullStore{}
	ctx := context.Background()
	err := store.SetLocked(ctx, nil, true)

	assert.NoError(t, err)
}

// TestNullStoreBlockState runs the shared block-state contract cases against the
// null store. It is the only store the shared suite did not cover, which is how
// its block state came to behave differently from every other implementation.
func TestNullStoreBlockState(t *testing.T) {
	t.Run("set block height zero", func(t *testing.T) {
		tests.SetBlockHeightZero(t, &NullStore{})
	})

	t.Run("set block state contract", func(t *testing.T) {
		tests.SetBlockStateContract(t, &NullStore{})
	})

	t.Run("set block state snapshot under concurrency", func(t *testing.T) {
		tests.SetBlockStateSnapshotUnderConcurrency(t, &NullStore{})
	})
}

// TestNullStoreCreateCohort checks the null store carries the cohort label from
// the create options onto the returned metadata, the same way it carries the
// other simple scalar options, so it stays faithful to the Store contract.
func TestNullStoreCreateCohort(t *testing.T) {
	store := &NullStore{}
	tx := &bt.Tx{}
	_ = tx.FromUTXOs(&bt.UTXO{
		TxIDHash:      &chainhash.Hash{},
		Vout:          0,
		LockingScript: &bscript.Script{},
		Satoshis:      100,
	})

	ctx := context.Background()

	t.Run("stamped", func(t *testing.T) {
		data, err := store.Create(ctx, tx, 0, utxo.WithCohort(cohort.ID(1_700_000_000)))
		require.NoError(t, err)
		require.Equal(t, uint32(1_700_000_000), data.Cohort)
	})

	t.Run("default is unset", func(t *testing.T) {
		data, err := store.Create(ctx, tx, 0)
		require.NoError(t, err)
		require.Equal(t, uint32(cohort.Unset), data.Cohort)
	})
}

func TestNullStoreSpendAndCreateMulti(t *testing.T) {
	store, err := NewNullStore()
	require.NoError(t, err)

	w := tests.BuildMultiWorkload(t, 0x70, 3, 4)

	results, err := store.SpendAndCreateMulti(context.Background(), w.Txs, 100)
	require.NoError(t, err)
	require.Len(t, results, len(w.Txs))

	for i, r := range results {
		require.Equal(t, utxo.MultiTxCreated, r.Status, "tx %d: %v", i, r.Err)
	}
}

func TestNullStoreParentOutputsForValidation(t *testing.T) {
	store, err := NewNullStore()
	require.NoError(t, err)

	answers, err := store.ParentOutputsForValidation(context.Background(), []utxo.Outpoint{{Vout: 0}, {Vout: 255}, {Vout: 256}})
	require.NoError(t, err)
	require.Len(t, answers, 3)
	require.Equal(t, utxo.ParentOutputMined, answers[0].Status)
	require.Equal(t, uint32(1), answers[0].Height)
	require.Equal(t, utxo.ParentOutputMined, answers[1].Status)
	require.Equal(t, utxo.ParentOutputNoSuchIndex, answers[2].Status)

	cancelled, cancel := context.WithCancel(context.Background())
	cancel()

	answers, err = store.ParentOutputsForValidation(cancelled, []utxo.Outpoint{{Vout: 0}})
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, answers)
}

// The null store has no conflict WAL. Its recovery methods accept every call and return nothing.
func TestNullStore_ConflictRecoveryMethods(t *testing.T) {
	s := &NullStore{}
	ctx := context.Background()
	id := chainhash.Hash{1}

	require.NoError(t, s.SaveConflictRecovery(ctx, id, &utxo.ConflictRecovery{Version: 1, Phase: utxo.ConflictRecoveryPlanning}))

	r, err := s.LoadConflictRecovery(ctx, id)
	require.NoError(t, err)
	require.Nil(t, r)

	require.NoError(t, s.RestoreConflictSpends(ctx, nil, 1))
	require.NoError(t, s.SetConflictLocks(ctx, id, []chainhash.Hash{{2}}, true))
	require.NoError(t, s.ReleaseConflictLocks(ctx, id, []chainhash.Hash{{2}}))
}
