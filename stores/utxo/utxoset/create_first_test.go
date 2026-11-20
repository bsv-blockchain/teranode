package utxoset

import (
	"testing"

	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/stretchr/testify/require"
)

// TestBeginCreateFirstRollbackReportsFalseAndDeletesNothing pins the no-tentative-state
// contract: every record is final, so the marker call reports false and leaves the record.
func TestBeginCreateFirstRollbackReportsFalseAndDeletesNothing(t *testing.T) {
	s, ctx := newTestStore(t)

	tx := mkTx(t, 2, 5_000)
	_, err := s.Create(ctx, tx, 100)
	require.NoError(t, err)

	began, err := s.BeginCreateFirstRollback(ctx, tx.TxIDChainHash(), nil)
	require.NoError(t, err)
	require.False(t, began)

	require.NoError(t, s.GetMeta(ctx, tx.TxIDChainHash(), &meta.Data{}), "a final record must survive")

	took, err := s.TakeRollbackHold(ctx, tx.TxIDChainHash(), nil)
	require.NoError(t, err)
	require.False(t, took, "no record is rolling back, so no hold can be taken")
	require.NoError(t, s.ReleaseRollbackHold(ctx, tx.TxIDChainHash(), nil))

	lease, leased, err := s.TakeSpendLease(ctx, tx.TxIDChainHash(), nil)
	require.NoError(t, err)
	require.False(t, leased, "no record is creating, so no spend needs a lease")
	require.Nil(t, lease)
	require.NoError(t, s.ReleaseSpendLease(ctx, tx.TxIDChainHash(), nil, []byte{1}))

	live, err := s.SpendLeasesLive(ctx, tx.TxIDChainHash(), nil)
	require.NoError(t, err)
	require.False(t, live, "the store never holds a lease")

	cleared, err := s.ClearCreateFirstHold(ctx, tx.TxIDChainHash(), true)
	require.NoError(t, err)
	require.False(t, cleared, "the store never leaves a hold")
	require.NoError(t, s.GetMeta(ctx, tx.TxIDChainHash(), &meta.Data{}), "a final record must survive")
}
