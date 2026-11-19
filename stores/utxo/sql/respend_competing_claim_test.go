package sql

import (
	"context"
	"fmt"
	"net/url"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-batcher/v2"
	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// setupSharedBatch builds a store whose spend batcher actually accumulates more
// than one claim per batch, which the respend tests need.
//
// The package's setup() forces BatcherDrainMode=true so batch callbacks fire
// immediately and every batch holds one item. Here drain mode is off and the
// window is wide, so a Spend issued just before another lands in the SAME batch.
func setupSharedBatch(ctx context.Context, t *testing.T, dbName string) *Store {
	t.Helper()
	initPrometheusMetrics()

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.UtxoStore.DBTimeout = 30 * time.Second
	tSettings.UtxoStore.SpendBatcherSize = 256
	tSettings.UtxoStore.SpendBatcherDurationMillis = 200
	tSettings.BatcherDrainMode = false

	utxoStoreURL, err := url.Parse("sqlitememory:///" + dbName)
	require.NoError(t, err)

	store, err := New(ctx, ulogger.TestLogger{}, tSettings, utxoStoreURL)
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close(context.Background()) })

	return store
}

// twoOutParent creates a parent with two outputs and returns it.
func twoOutParent(ctx context.Context, t *testing.T, store *Store, seed string) *bt.Tx {
	t.Helper()

	parent := bt.NewTx()
	prev := chainhash.DoubleHashH([]byte(seed))
	require.NoError(t, parent.From(prev.String(), 0,
		"76a91471d7dd96d9edda09180fe9d57a477b5acc9cad1188ac", 1e8))
	parent.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{bscript.OpTRUE})
	require.NoError(t, parent.PayToAddress("1BUBHgpCBWCsD2Ajbeu7ZgCgvMHsoYHm3W", 1000))
	require.NoError(t, parent.PayToAddress("1BUBHgpCBWCsD2Ajbeu7ZgCgvMHsoYHm3W", 2000))

	_, err := store.Create(ctx, parent, 1)
	require.NoError(t, err)

	return parent
}

// spendOf builds a spend of the given vouts of parent, distinguished by lockTime
// so each variant carries different spending data.
func spendOf(t *testing.T, parent *bt.Tx, lockTime uint32, vouts ...uint32) *bt.Tx {
	t.Helper()

	tx := bt.NewTx()
	for _, v := range vouts {
		require.NoError(t, tx.From(parent.TxIDChainHash().String(), v,
			parent.Outputs[v].LockingScript.String(), parent.Outputs[v].Satoshis))
	}
	require.NoError(t, tx.PayToAddress("1BUBHgpCBWCsD2Ajbeu7ZgCgvMHsoYHm3W", 1))
	tx.LockTime = lockTime

	return tx
}

// assertRespendBehindLoserInSameBatch pins the respend-behind-loser behaviour,
// driving sendSpendBatch with an explicit batch so no timing is involved.
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

// A stored owner need not be one of the requests in the batch. Both losing
// requests must name that owner, never each other.
func TestSpendBatchReportsPersistedOwnerOutsideBatch(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	store := setupSharedBatch(ctx, t, "4934-third-owner")
	require.NoError(t, store.SetBlockHeight(2))
	parent := twoOutParent(ctx, t, store, "4934-third-owner")
	owner := spendOf(t, parent, 1, 0)
	_, err := store.Spend(ctx, owner, 2)
	require.NoError(t, err)
	batch := make([]*batchSpend, 0, 2)
	for _, lockTime := range []uint32{2, 3} {
		spends, spendErr := utxo.GetSpends(spendOf(t, parent, lockTime, 0))
		require.NoError(t, spendErr)
		batch = append(batch, &batchSpend{spend: spends[0], blockHeight: 2, errCh: make(chan error, 1)})
	}
	store.sendSpendBatch(batch)
	for _, item := range batch {
		err := <-item.errCh
		require.ErrorIs(t, err, errors.ErrSpent)
		var data *errors.UtxoSpentErrData
		require.True(t, errors.AsData(err, &data))
		require.Equal(t, owner.TxID(), data.SpendingData.TxID.String())
	}
}

func TestSpendBatchStorageFailureDoesNotInventConflict(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	store := setupSharedBatch(ctx, t, "4934-read-failure")
	require.NoError(t, store.SetBlockHeight(2))
	parent := twoOutParent(ctx, t, store, "4934-read-failure")
	owner := spendOf(t, parent, 1, 0, 1)
	_, err := store.Spend(ctx, owner, 2)
	require.NoError(t, err)
	loserSpends, err := utxo.GetSpends(spendOf(t, parent, 2, 0))
	require.NoError(t, err)
	ownerSpends, err := utxo.GetSpends(owner)
	require.NoError(t, err)
	require.NoError(t, store.db.Close())
	batch := []*batchSpend{
		{spend: loserSpends[0], blockHeight: 2, errCh: make(chan error, 1)},
		{spend: ownerSpends[0], blockHeight: 2, errCh: make(chan error, 1)},
		{spend: ownerSpends[1], blockHeight: 2, errCh: make(chan error, 1)},
	}
	store.sendSpendBatch(batch)
	for _, item := range batch {
		err := <-item.errCh
		require.Error(t, err)
		require.NotErrorIs(t, err, errors.ErrSpent, "storage failure is not evidence of a conflicting owner")
	}
}

// Alternating claims on one unspent outpoint: the first claim wins in the
// store, and every competing claim is refused naming it.
func TestSpendBatchAlternatingClaimsNameTheWinner(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	store := setupSharedBatch(ctx, t, "4934-alternating")
	require.NoError(t, store.SetBlockHeight(2))
	parent := twoOutParent(ctx, t, store, "4934-alternating")
	winnerSpends, err := utxo.GetSpends(spendOf(t, parent, 1, 0))
	require.NoError(t, err)
	loserSpends, err := utxo.GetSpends(spendOf(t, parent, 2, 0))
	require.NoError(t, err)

	winners := make([]*batchSpend, 0, 4)
	losers := make([]*batchSpend, 0, 4)
	batch := make([]*batchSpend, 0, 8)
	for range 4 {
		winner := &batchSpend{spend: winnerSpends[0], blockHeight: 2, errCh: make(chan error, 1)}
		loser := &batchSpend{spend: loserSpends[0], blockHeight: 2, errCh: make(chan error, 1)}
		winners, losers = append(winners, winner), append(losers, loser)
		batch = append(batch, winner, loser)
	}
	store.sendSpendBatch(batch)

	for _, item := range winners {
		require.NoError(t, <-item.errCh)
	}
	for _, item := range losers {
		err := <-item.errCh
		require.ErrorIs(t, err, errors.ErrSpent)
		var data *errors.UtxoSpentErrData
		require.True(t, errors.AsData(err, &data))
		require.Equal(t, winnerSpends[0].SpendingData.Bytes(), data.SpendingData.Bytes())
	}
}

// TestRespendBehindLoserInSameBatchKeepsLegitimateSpends reproduces the
// respend-behind-loser failure.
//
// S0 legitimately spends both of the parent's outputs. A later batch then
// carries a double-spend attempt S1 on output 0 AHEAD of an idempotent re-spend
// of S0. The former duplicate filter awarded the outpoint to the first
// claim in the batch without consulting the persisted spender, so S0's
// idempotent re-spend is rejected with ErrSpent naming S1 -- a transaction that
// never spent anything. Store.Spend then treats that ErrSpent as an unwinnable
// failure (hasUnwinnableFailure) and un-spends output 1, which S0 had already
// re-spent successfully in the same batch.
//
// The test checks both the result and stored state:
//   - the re-spend must not be rejected (it matches the persisted spender);
//   - output 1 must stay spent by S0 even if some other input is refused.
func TestRespendBehindLoserInSameBatchKeepsLegitimateSpends(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store := setupSharedBatch(ctx, t, "4934respend")
	require.NoError(t, store.SetBlockHeight(2))

	parent := twoOutParent(ctx, t, store, "4934-parent")

	s0 := spendOf(t, parent, 1, 0, 1) // legitimate spender of both outputs
	s1 := spendOf(t, parent, 2, 0)    // double-spend attempt on output 0

	// S0 spends both outputs up front.
	_, err := store.Spend(ctx, s0, 2)
	require.NoError(t, err, "initial S0 spend must succeed")

	// Capture the public Spend calls at the dispatch boundary. This fixes
	// batch membership and order without relying on scheduler timing.
	store.spendBatcher.Close()
	queued := make(chan *batchSpend, 3)
	store.spendBatcher = batcher.NewWithPool(1, time.Millisecond, func(items []*batchSpend) {
		queued <- items[0]
	}, false)
	next := func() *batchSpend {
		t.Helper()
		select {
		case item := <-queued:
			return item
		case <-time.After(5 * time.Second):
			t.Fatal("Spend did not enqueue its input")
			return nil
		}
	}
	errS1 := make(chan error, 1)
	go func() {
		_, e := store.Spend(ctx, s1, 2)
		errS1 <- e
	}()
	loserItem := next()
	errS0Ch := make(chan error, 1)
	go func() {
		_, e := store.Spend(ctx, s0, 2)
		errS0Ch <- e
	}()
	first, second := next(), next()
	if first.spend.Vout != 0 {
		first, second = second, first
	}
	store.sendSpendBatch([]*batchSpend{loserItem, first, second})
	errS0 := <-errS0Ch
	s1Err := <-errS1
	t.Logf("S1 err         : %v", s1Err)
	t.Logf("S0 re-spend err: %v", errS0)

	// Defect 1: an idempotent re-spend is a supported path and must succeed.
	require.NoError(t, errS0,
		"idempotent re-spend of S0 was rejected -- the in-batch filter handed the outpoint to S1")

	// The #825 guarantee must survive: the genuine double-spend loser is still
	// refused in the same batch, and is now blamed on S0, the tx that really
	// holds the outpoint, rather than on itself.
	require.Error(t, s1Err, "the double-spend attempt on output 0 must still be refused")
	require.True(t, errors.Is(s1Err, errors.ErrSpent), "expected ErrSpent for the loser, got: %v", s1Err)
	require.Contains(t, s1Err.Error(), s0.TxID(),
		"the loser must be blamed on S0, the persisted spender")

	// Defect 2: both outputs must remain spent by S0. Output 1 is the one the
	// rollback clears, and it is a real consensus-state loss.
	spends, err := utxo.GetSpends(s0)
	require.NoError(t, err)
	require.Len(t, spends, 2)

	for _, sp := range spends {
		resp, getErr := store.GetSpend(ctx, sp)
		require.NoError(t, getErr)
		t.Logf("output %d: status=%v spendingData=%v", sp.Vout, resp.Status, resp.SpendingData)

		require.Equal(t, int(utxo.Status_SPENT), resp.Status,
			"output %d that S0 legitimately spent went back to unspent", sp.Vout)
		require.NotNil(t, resp.SpendingData, "output %d must name its spender", sp.Vout)
		require.Equal(t, s0.TxID(), resp.SpendingData.TxID.String(),
			"output %d is owned by %s, not by S0", sp.Vout, resp.SpendingData.TxID)
	}
}

// Two fresh, different claims on one outpoint in one batch: exactly one wins,
// storage holds the winner, and the loser gets UTXO_SPENT naming the winner.
func assertFreshCompetingClaimsHaveOneWinner(t *testing.T, ctx context.Context, store *Store) {
	t.Helper()

	for round := 0; round < 20; round++ {
		parent := twoOutParent(ctx, t, store, "fresh-competing-"+fmt.Sprint(round))
		a, err := utxo.GetSpends(spendOf(t, parent, 1, 0))
		require.NoError(t, err)
		b, err := utxo.GetSpends(spendOf(t, parent, 2, 0))
		require.NoError(t, err)

		height := store.GetBlockHeight() + 1
		chA, chB := make(chan error, 1), make(chan error, 1)
		batch := []*batchSpend{{spend: a[0], blockHeight: height, errCh: chA}, {spend: b[0], blockHeight: height, errCh: chB}}
		if round%2 == 1 {
			batch[0], batch[1] = batch[1], batch[0]
		}
		store.sendSpendBatch(batch)
		errA, errB := <-chA, <-chB

		require.True(t, (errA == nil) != (errB == nil), "round %d: exactly one claim must win, got a=%v b=%v", round, errA, errB)

		winner, loserErr := a[0], errB
		if errA != nil {
			winner, loserErr = b[0], errA
		}

		require.ErrorIs(t, loserErr, errors.ErrSpent, "round %d", round)
		var data *errors.UtxoSpentErrData
		require.True(t, errors.AsData(loserErr, &data), "round %d: the loser must carry the owner", round)
		require.Equal(t, winner.SpendingData.Bytes(), data.SpendingData.Bytes(), "round %d: the loser must name the winner", round)

		stored, err := store.GetSpend(ctx, winner)
		require.NoError(t, err)
		require.NotNil(t, stored.SpendingData, "round %d", round)
		require.Equal(t, winner.SpendingData.Bytes(), stored.SpendingData.Bytes(), "round %d: storage must hold the winner", round)
	}
}

func TestSpendBatchFreshCompetingClaimsHaveOneWinnerSQLite(t *testing.T) {
	ctx := context.Background()
	store := setupSharedBatch(ctx, t, "exp-fresh-sqlite")
	require.NoError(t, store.SetBlockHeight(2))
	assertFreshCompetingClaimsHaveOneWinner(t, ctx, store)
}

func TestSpendBatchFreshCompetingClaimsHaveOneWinnerPostgres(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping Postgres integration test in short mode")
	}

	for _, bulk := range []bool{false, true} {
		name := "per-row"
		if bulk {
			name = "bulk"
		}
		t.Run(name, func(t *testing.T) {
			store, ctx := setupPostgresStore(t)
			store.settings.UtxoStore.BatchSQLOperations = bulk
			require.NoError(t, store.SetBlockHeight(2))
			assertFreshCompetingClaimsHaveOneWinner(t, ctx, store)
		})
	}
}

func TestRespendBehindLoserInSameBatchPostgresPerRow(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping Postgres integration test in short mode")
	}

	store, ctx := setupPostgresStore(t)
	store.settings.UtxoStore.BatchSQLOperations = false
	require.NoError(t, store.SetBlockHeight(2))
	assertRespendBehindLoserInSameBatch(t, ctx, store)
}
