package sql

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/stretchr/testify/require"
)

// assertRespendBehindLoserInSameBatch pins bitcoin-sv/teranode#4934 at the
// in-batch filter, driving sendSpendBatch with an explicit batch so no timing
// is involved.
//
// S0 already owns both parent outputs. The batch then carries a double-spend
// attempt on output 0 AHEAD of S0's idempotent re-spend of output 0, plus
// S0's idempotent re-spend of output 1.
//
// Before the fix the filter handed output 0 to whoever arrived first, so the
// re-spend was refused with ErrSpent naming the double-spend attempt -- a tx
// that never spent anything -- while the attempt itself was refused by the
// backend as already spent. Both claims lost, each blamed on the other.
func assertRespendBehindLoserInSameBatch(t *testing.T, ctx context.Context, store *Store) {
	t.Helper()

	parent := twoOutParent(ctx, t, store, "4934-batch")

	respend := spendOf(t, parent, 1, 0, 1) // S0: legitimate spender of outputs 0 and 1
	loser := spendOf(t, parent, 2, 0)      // double-spend attempt on output 0

	// S0 owns both outputs before the batch under test.
	_, err := store.Spend(ctx, respend, 2)
	require.NoError(t, err, "initial spend must succeed")

	respendSpends, err := utxo.GetSpends(respend)
	require.NoError(t, err)
	require.Len(t, respendSpends, 2)

	loserSpends, err := utxo.GetSpends(loser)
	require.NoError(t, err)
	require.Len(t, loserSpends, 1)
	require.NotEqual(t, respendSpends[0].SpendingData.Bytes(), loserSpends[0].SpendingData.Bytes(),
		"the two claims must differ, or this is not a conflict")

	height := store.GetBlockHeight() + 1
	chans := make([]chan error, 3)
	for i := range chans {
		chans[i] = make(chan error, 1)
	}

	// One batch, loser first: exactly the ordering the filter used to obey.
	store.sendSpendBatch([]*batchSpend{
		{spend: loserSpends[0], blockHeight: height, errCh: chans[0]},
		{spend: respendSpends[0], blockHeight: height, errCh: chans[1]},
		{spend: respendSpends[1], blockHeight: height, errCh: chans[2]},
	})

	loserErr, respendOut0Err, respendOut1Err := <-chans[0], <-chans[1], <-chans[2]
	t.Logf("loser err            : %v", loserErr)
	t.Logf("re-spend output 0 err : %v", respendOut0Err)
	t.Logf("re-spend output 1 err : %v", respendOut1Err)

	// The idempotent re-spend must be accepted on both outputs.
	require.NoError(t, respendOut0Err,
		"idempotent re-spend of output 0 was refused -- the filter followed batch order, not persisted state")
	require.NoError(t, respendOut1Err, "idempotent re-spend of output 1 must succeed")

	// The genuine double-spend is still refused, and now blamed on S0, the
	// persisted spender, rather than on itself. This is the #825 guarantee.
	require.Error(t, loserErr, "the double-spend attempt must still be refused")
	require.True(t, errors.Is(loserErr, errors.ErrSpent), "expected ErrSpent for the loser, got: %v", loserErr)

	var terr *errors.Error
	require.ErrorAs(t, loserErr, &terr)

	var errData *errors.UtxoSpentErrData
	require.True(t, errors.AsData(terr, &errData), "expected the spent-error to carry the real spender")
	require.NotNil(t, errData.SpendingData, "the loser must be told who owns the outpoint")
	require.Equal(t, respendSpends[0].SpendingData.Bytes(), errData.SpendingData.Bytes(),
		"the loser must be blamed on the persisted spender, not on itself")
}

func TestRespendBehindLoserInSameBatchSQLite(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store := setupSharedBatch(ctx, t, "4934batch-sqlite")
	require.NoError(t, store.SetBlockHeight(2))

	assertRespendBehindLoserInSameBatch(t, ctx, store)
}

func TestRespendBehindLoserInSameBatchPostgresBulk(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping Postgres integration test in short mode")
	}

	store, ctx := setupPostgresStore(t)
	store.settings.UtxoStore.BatchSQLOperations = true
	require.NoError(t, store.SetBlockHeight(2))

	assertRespendBehindLoserInSameBatch(t, ctx, store)
}
