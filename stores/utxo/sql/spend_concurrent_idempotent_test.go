package sql

import (
	"bytes"
	"context"
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-batcher/v2"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	spendpkg "github.com/bsv-blockchain/teranode/stores/utxo/spend"
	"github.com/stretchr/testify/require"
)

// TestRollbackKeepsAMatchAnotherWriterRecordedBeforeTheUpdate: C spends A:0, which is free
// when the SELECT reads it, and M:0, which records C and carries C's marker, so the call fails
// with a pruned-replay answer. Another attempt at C commits its own spend of A:0 inside the
// window between the SELECT and the UPDATE, so by the time this call writes A:0 the output
// already records exactly this spend. That is an idempotent match, not a spend this call
// wrote, and with a pruned-replay answer in the batch it may be a confirmed historical spend:
// the rollback must leave it. Both ways the UPDATE learns about it are run: the write is
// already committed when the UPDATE starts, or it commits while the UPDATE waits for the row.
//
// Postgres only, for the same reason as TestSpendRefusesMarkerCommittedAfterSelect.
func TestRollbackKeepsAMatchAnotherWriterRecordedBeforeTheUpdate(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping Postgres integration test in short mode")
	}

	for _, tc := range []struct {
		name string
		// commitsWhileUpdateWaits: the other writer holds A:0's row lock when the UPDATE
		// starts and commits once the UPDATE is waiting on it.
		commitsWhileUpdateWaits bool
	}{
		{name: "committed_before_the_update"},
		{name: "committed_while_the_update_waits", commitsWhileUpdateWaits: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store, ctx := setupPostgresStore(t)
			store.settings.UtxoStore.BatchSQLOperations = true
			require.NoError(t, store.SetBlockHeight(1000))

			a := replayParent(t, ctx, store, "1111111111111111111111111111111111111111111111111111111111111111")
			m := replayParent(t, ctx, store, "2222222222222222222222222222222222222222222222222222222222222222")
			c := replaySpender(t, a, m)

			plantHistoricalSpend(t, ctx, store, m, c, 1)

			// C's two inputs must reach the store in one batch. If the batcher sends M on
			// its own, the hook fires in M's batch and A's batch then reads A:0 as already
			// recorded at the SELECT, so the UPDATE never meets the other writer and the
			// test passes without reaching the path it is about. A batch of exactly two,
			// with a fallback far beyond the test's run time, sends them together.
			var batchSizes []int

			store.spendBatcher.Close()
			store.spendBatcher = batcher.NewWithPool(2, time.Minute, func(batch []*batchSpend) {
				batchSizes = append(batchSizes, len(batch))
				store.sendSpendBatch(batch)
			}, false)

			recorded := spendpkg.NewSpendingData(c.TxIDChainHash(), 0).Bytes()
			const writeA = "UPDATE outputs SET spending_data = $2 WHERE idx = 0 AND transaction_id IN (SELECT id FROM transactions WHERE hash = $1)"

			var once sync.Once

			store.afterSpendSelect = func() {
				once.Do(func() {
					if !tc.commitsWhileUpdateWaits {
						_, err := store.db.ExecContext(context.Background(), writeA, a.TxIDChainHash()[:], recorded)
						require.NoError(t, err)

						return
					}

					holder, err := store.db.BeginTx(context.Background(), nil)
					require.NoError(t, err)

					_, err = holder.ExecContext(context.Background(), writeA, a.TxIDChainHash()[:], recorded)
					require.NoError(t, err)

					go func() {
						deadline := time.Now().Add(10 * time.Second)

						for time.Now().Before(deadline) {
							var waiting int

							if err := store.db.QueryRowContext(context.Background(), "SELECT count(*) FROM pg_locks WHERE NOT granted").Scan(&waiting); err == nil && waiting > 0 {
								break
							}

							time.Sleep(10 * time.Millisecond)
						}

						_ = holder.Commit()
					}()
				})
			}
			t.Cleanup(func() { store.afterSpendSelect = nil })

			spends, err := store.Spend(ctx, c, 1000)
			store.afterSpendSelect = nil

			require.Equal(t, []int{2}, batchSizes, "fixture: both of C's inputs must be in one batch")
			require.Error(t, err)
			require.ErrorIs(t, spends[1].Err, errors.ErrUtxoSpendingTxPruned, "fixture: M refuses the replay")
			require.Equal(t, recorded, outputSpendingData(t, ctx, store, a, 0),
				"an output that already recorded this spend when the call wrote it is an idempotent match and stays")
		})
	}
}

// TestRollbackKeepsADuplicateOfAMatchAnotherWriterRecorded: two concurrent Spend calls of
// the same C land in one batch, so A:0 appears twice with the same spending data and the
// batch updates it once, for its representative. As in
// TestRollbackKeepsAMatchAnotherWriterRecordedBeforeTheUpdate, another attempt at C records
// A:0 between the SELECT and the UPDATE, so the representative is an idempotent match, and
// M:0's marker answers both calls with a pruned replay. The duplicate wrote nothing either:
// it must be an idempotent match too, or its call reverses A:0 as a spend it wrote and
// releases what may be a confirmed historical spend.
//
// Postgres only, for the same reason as TestSpendRefusesMarkerCommittedAfterSelect.
func TestRollbackKeepsADuplicateOfAMatchAnotherWriterRecorded(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping Postgres integration test in short mode")
	}

	store, ctx := setupPostgresStore(t)
	store.settings.UtxoStore.BatchSQLOperations = true
	require.NoError(t, store.SetBlockHeight(1000))

	a := replayParent(t, ctx, store, "3333333333333333333333333333333333333333333333333333333333333333")
	m := replayParent(t, ctx, store, "4444444444444444444444444444444444444444444444444444444444444444")
	c := replaySpender(t, a, m)

	plantHistoricalSpend(t, ctx, store, m, c, 1)

	// Both calls' inputs, A:0 and M:0 twice, must be in one batch, or there is no
	// duplicate to inherit anything.
	var batchSizes []int

	store.spendBatcher.Close()
	store.spendBatcher = batcher.NewWithPool(4, time.Minute, func(batch []*batchSpend) {
		batchSizes = append(batchSizes, len(batch))
		store.sendSpendBatch(batch)
	}, false)

	recorded := spendpkg.NewSpendingData(c.TxIDChainHash(), 0).Bytes()

	var once sync.Once

	store.afterSpendSelect = func() {
		once.Do(func() {
			_, err := store.db.ExecContext(context.Background(),
				"UPDATE outputs SET spending_data = $2 WHERE idx = 0 AND transaction_id IN (SELECT id FROM transactions WHERE hash = $1)",
				a.TxIDChainHash()[:], recorded)
			require.NoError(t, err)
		})
	}
	t.Cleanup(func() { store.afterSpendSelect = nil })

	var (
		wg    sync.WaitGroup
		calls [2]struct {
			spends []*utxo.Spend
			err    error
		}
	)

	for i := range calls {
		wg.Add(1)

		go func() {
			defer wg.Done()

			calls[i].spends, calls[i].err = store.Spend(ctx, c, 1000)
		}()
	}

	wg.Wait()
	store.afterSpendSelect = nil

	require.Equal(t, []int{4}, batchSizes, "fixture: both calls' inputs must be in one batch")

	for i, call := range calls {
		require.Error(t, call.err, "call %d", i)
		require.ErrorIs(t, call.spends[1].Err, errors.ErrUtxoSpendingTxPruned, "fixture: M refuses call %d as a replay", i)
	}

	require.Equal(t, recorded, outputSpendingData(t, ctx, store, a, 0),
		"neither call wrote A:0, so neither may reverse it")
}

// TestSpendClassifiesALateMatchByAge: C spends A:0, free when the SELECT reads it, and
// M:0, which another transaction already spent, so the call fails as spent. Between the
// SELECT and the UPDATE another attempt at C records A:0 at height 1,000, while this call
// runs at 1,200. That match is 200 blocks old against the retention's 144, and C has no
// record, so it is a pruned replay and is refused. Read as a young match it would join the
// rollback, and the call's failure would free A:0 for a replay of C. Both ways the UPDATE
// learns about the write are run, on the bulk path (upd_idem or the missed-row re-check)
// and on the per-row path (its re-read after a write that matched nothing).
//
// Postgres only, for the same reason as TestSpendRefusesMarkerCommittedAfterSelect.
func TestSpendClassifiesALateMatchByAge(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping Postgres integration test in short mode")
	}

	for _, tc := range []struct {
		name                    string
		perRow                  bool
		commitsWhileUpdateWaits bool
	}{
		{name: "bulk_committed_before_the_update"},
		{name: "bulk_committed_while_the_update_waits", commitsWhileUpdateWaits: true},
		{name: "per_row_committed_before_the_update", perRow: true},
		{name: "per_row_committed_while_the_update_waits", perRow: true, commitsWhileUpdateWaits: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store, ctx := setupPostgresStore(t)
			store.settings.UtxoStore.BatchSQLOperations = !tc.perRow
			require.NoError(t, store.SetBlockHeight(1200))

			a := replayParent(t, ctx, store, "5555555555555555555555555555555555555555555555555555555555555555")
			m := replayParent(t, ctx, store, "6666666666666666666666666666666666666666666666666666666666666666")
			c := replaySpender(t, a, m)

			other := bytes.Repeat([]byte{0x42}, 36)
			_, err := store.db.ExecContext(ctx,
				"UPDATE outputs SET spending_data = $2, spending_height = 1100 WHERE idx = 0 AND transaction_id IN (SELECT id FROM transactions WHERE hash = $1)",
				m.TxIDChainHash()[:], other)
			require.NoError(t, err)

			var batchSizes []int

			store.spendBatcher.Close()
			store.spendBatcher = batcher.NewWithPool(2, time.Minute, func(batch []*batchSpend) {
				batchSizes = append(batchSizes, len(batch))
				store.sendSpendBatch(batch)
			}, false)

			recorded := spendpkg.NewSpendingData(c.TxIDChainHash(), 0).Bytes()
			const writeA = "UPDATE outputs SET spending_data = $2, spending_height = 1000 WHERE idx = 0 AND transaction_id IN (SELECT id FROM transactions WHERE hash = $1)"

			var once sync.Once

			store.afterSpendSelect = func() {
				once.Do(func() {
					if !tc.commitsWhileUpdateWaits {
						_, err := store.db.ExecContext(context.Background(), writeA, a.TxIDChainHash()[:], recorded)
						require.NoError(t, err)

						return
					}

					holder, err := store.db.BeginTx(context.Background(), nil)
					require.NoError(t, err)

					_, err = holder.ExecContext(context.Background(), writeA, a.TxIDChainHash()[:], recorded)
					require.NoError(t, err)

					go func() {
						deadline := time.Now().Add(10 * time.Second)

						for time.Now().Before(deadline) {
							var waiting int

							if err := store.db.QueryRowContext(context.Background(), "SELECT count(*) FROM pg_locks WHERE NOT granted").Scan(&waiting); err == nil && waiting > 0 {
								break
							}

							time.Sleep(10 * time.Millisecond)
						}

						_ = holder.Commit()
					}()
				})
			}
			t.Cleanup(func() { store.afterSpendSelect = nil })

			spends, err := store.Spend(ctx, c, 1200)
			store.afterSpendSelect = nil

			require.Equal(t, []int{2}, batchSizes, "fixture: both of C's inputs must be in one batch")
			require.Error(t, err)
			require.ErrorIs(t, spends[1].Err, errors.ErrSpent, "fixture: M:0 is spent by another transaction")
			require.ErrorIs(t, spends[0].Err, errors.ErrUtxoSpendingTxPruned, "a match 200 blocks old with no record for C is a pruned replay")
			require.Equal(t, recorded, outputSpendingData(t, ctx, store, a, 0), "the rollback must not free the historical match")
		})
	}
}
