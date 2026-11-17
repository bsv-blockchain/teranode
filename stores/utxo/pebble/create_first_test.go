package pebble

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/stretchr/testify/require"
)

// TestCreateFirstContractWithoutTentativeState pins the no-tentative-state contract: the store
// declines create-first, every record is final, and no rollback call marks or removes one.
func TestCreateFirstContractWithoutTentativeState(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)

	require.False(t, s.SupportsCreateFirst())

	tx := newExtendedTx(t, 2, 5_000)
	mustCreate(t, s, tx, testStoreHeight)

	hash := tx.TxIDChainHash()

	require.NoError(t, s.FinalizeTransaction(ctx, tx, nil))

	stale, err := s.QueryStaleCreatingTxs(ctx, testStoreHeight+1_000, 0)
	require.NoError(t, err)
	require.Empty(t, stale)

	began, err := s.BeginCreateFirstRollback(ctx, hash, nil)
	require.NoError(t, err)
	require.False(t, began)
	require.NoError(t, s.GetMeta(ctx, hash, &meta.Data{}), "a final record must survive")

	took, err := s.TakeRollbackHold(ctx, hash, nil)
	require.NoError(t, err)
	require.False(t, took, "no record is rolling back, so no hold can be taken")
	require.NoError(t, s.ReleaseRollbackHold(ctx, hash, nil))

	lease, leased, err := s.TakeSpendLease(ctx, hash, nil)
	require.NoError(t, err)
	require.False(t, leased, "no record is creating, so no spend needs a lease")
	require.Nil(t, lease)
	require.NoError(t, s.ReleaseSpendLease(ctx, hash, nil, []byte{1}))

	live, err := s.SpendLeasesLive(ctx, hash, nil)
	require.NoError(t, err)
	require.False(t, live, "the store never holds a lease")

	cleared, err := s.ClearCreateFirstHold(ctx, hash, true)
	require.NoError(t, err)
	require.False(t, cleared, "the store never leaves a hold")
	require.NoError(t, s.GetMeta(ctx, hash, &meta.Data{}), "a final record must survive")
}
