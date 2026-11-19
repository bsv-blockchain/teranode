package sql

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-batcher/v2"
	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	spendpkg "github.com/bsv-blockchain/teranode/stores/utxo/spend"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
)

// TestSpendRefusesMarkerCommittedAfterSelect: the spend path reads the replay
// marker in its SELECT, at READ COMMITTED, and the pruner commits the marker and
// the child's delete together. A marker committed after that SELECT, on an
// output that records exactly this spend, used to be invisible to the spend, so
// the replay was answered as an idempotent success. Each later read re-checks
// the marker, and the replay must be answered as a pruned replay.
//
// The pruner marks only a child the output records as its spender, so every
// case has the output recording this spend by the time the marker lands.
//
// Postgres only. SQLite serialises writers, so the pruner cannot commit inside
// an open spend transaction there, and the hook's insert would block on it.
// Both Postgres spend paths are covered: the bulk path and the per-row one.
func TestSpendRefusesMarkerCommittedAfterSelect(t *testing.T) {
	testSpendRefusesMarkerCommittedAfterSelect(t, "")
}

func TestSpendRefusesMarkerCommittedAfterSelectWithNonDefaultIsolation(t *testing.T) {
	for _, isolation := range []string{"repeatable read", "serializable"} {
		t.Run(isolation, func(t *testing.T) {
			testSpendRefusesMarkerCommittedAfterSelect(t, isolation)
		})
	}
}

func testSpendRefusesMarkerCommittedAfterSelect(t *testing.T, defaultIsolation string) {
	t.Helper()
	if testing.Short() {
		t.Skip("skipping Postgres integration test in short mode")
	}

	for _, tc := range []struct {
		name string
		bulk bool
		// alsoSpent: the replay's own spend of the output is committed in the
		// same window, so the UPDATE finds the row already recording this exact
		// spend and would otherwise report an idempotent success.
		alsoSpent bool
		// spentBefore: the output already records this exact spend before the
		// call, so the SELECT itself answers idempotent and the bulk path never
		// sends the row to the UPDATE; only the marker lands in the window.
		spentBefore bool
		// noRetention: the store keeps no delete-at-height, which gives the bulk UPDATE its
		// own statement without the DAH CTEs, with its own marker guard on upd_idem.
		noRetention bool
	}{
		{name: "bulk_idempotent", bulk: true, alsoSpent: true},
		{name: "bulk_idempotent_no_retention", bulk: true, alsoSpent: true, noRetention: true},
		{name: "per_row_idempotent", alsoSpent: true},
		{name: "bulk_idempotent_at_select", bulk: true, spentBefore: true},
		{name: "per_row_idempotent_at_select", spentBefore: true},
	} {
		bulk := tc.bulk

		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			dbURL := startPostgresContainer(t, ctx)
			if defaultIsolation != "" {
				admin, err := pgx.Connect(ctx, dbURL.String())
				require.NoError(t, err)
				// Only these two fixed test values reach this statement. New
				// store connections must inherit the database default.
				_, err = admin.Exec(ctx, "ALTER DATABASE testdb SET default_transaction_isolation = '"+defaultIsolation+"'")
				require.NoError(t, err)
				require.NoError(t, admin.Close(ctx))
			}
			store := newPostgresTestStore(t, ctx, dbURL)
			t.Cleanup(func() { require.NoError(t, store.Close(ctx)) })
			if defaultIsolation != "" {
				var isolation string
				require.NoError(t, store.db.QueryRowContext(ctx, "SHOW transaction_isolation").Scan(&isolation))
				require.Equal(t, defaultIsolation, isolation)
			}
			store.settings.UtxoStore.BatchSQLOperations = bulk

			if tc.noRetention {
				store.settings.GlobalBlockHeightRetention = 0
				store.settings.UtxoStore.BlockHeightRetentionAdjustment = 0
				require.Zero(t, store.settings.GetUtxoStoreBlockHeightRetention())
			}

			require.NoError(t, store.SetBlockHeight(1000))

			parent := bt.NewTx()
			require.NoError(t, parent.From("1111111111111111111111111111111111111111111111111111111111111111", 0, "51", 30000))
			parent.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{0x51})
			require.NoError(t, parent.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 4000))
			_, err := store.Create(ctx, parent, 1000)
			require.NoError(t, err)

			child := bt.NewTx()
			require.NoError(t, child.From(parent.TxID(), 0, parent.Outputs[0].LockingScript.String(), parent.Outputs[0].Satoshis))
			child.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{0x51})
			require.NoError(t, child.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 3000))

			if tc.spentBefore {
				_, spendErr := store.db.ExecContext(context.Background(),
					"UPDATE outputs SET spending_data = $2 WHERE idx = 0 AND transaction_id IN (SELECT id FROM transactions WHERE hash = $1)",
					parent.TxIDChainHash()[:], spendpkg.NewSpendingData(child.TxIDChainHash(), 0).Bytes())
				require.NoError(t, spendErr)
			}

			// The pruner's commit, landing inside the spend's SELECT-to-UPDATE
			// window on its own connection.
			var once sync.Once

			store.afterSpendSelect = func() {
				once.Do(func() {
					_, insertErr := store.db.ExecContext(context.Background(),
						`INSERT INTO deleted_children (parent_id, child_hash) SELECT id, $2 FROM transactions WHERE hash = $1`,
						parent.TxIDChainHash()[:], child.TxIDChainHash()[:])
					require.NoError(t, insertErr)

					if tc.alsoSpent {
						_, spendErr := store.db.ExecContext(context.Background(),
							"UPDATE outputs SET spending_data = $2 WHERE idx = 0 AND transaction_id IN (SELECT id FROM transactions WHERE hash = $1)",
							parent.TxIDChainHash()[:], spendpkg.NewSpendingData(child.TxIDChainHash(), 0).Bytes())
						require.NoError(t, spendErr)
					}
				})
			}
			t.Cleanup(func() { store.afterSpendSelect = nil })

			spends, err := store.Spend(ctx, child, 1000)
			store.afterSpendSelect = nil

			require.Error(t, err, "a spend by a transaction the pruner just removed must be refused")
			require.Len(t, spends, 1)
			require.ErrorIs(t, spends[0].Err, errors.ErrUtxoSpendingTxPruned,
				"the refusal must be answered as a pruned replay, so the block paths can compensate")
		})
	}
}

// TestBulkSpendDuplicateTakesRepresentativesRefusal: two Spend calls for the
// same replayed transaction can land in one bulk batch. The dedup sends only
// the first (the representative) to the UPDATE; the duplicate takes its answer
// afterwards. When the representative is refused, here as a pruned replay by
// the missed-row re-check because the spend and its marker land after the
// SELECT, the
// duplicate must be refused too. It used to receive nil, and its caller went on
// to recreate the pruned transaction.
//
// Postgres only, for the same reason as TestSpendRefusesMarkerCommittedAfterSelect.
func TestBulkSpendDuplicateTakesRepresentativesRefusal(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping Postgres integration test in short mode")
	}

	store, ctx := setupPostgresStore(t)
	store.settings.UtxoStore.BatchSQLOperations = true
	require.NoError(t, store.SetBlockHeight(1000))

	parent := bt.NewTx()
	require.NoError(t, parent.From("2222222222222222222222222222222222222222222222222222222222222222", 0, "51", 30000))
	parent.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{0x51})
	require.NoError(t, parent.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 4000))
	_, err := store.Create(ctx, parent, 1000)
	require.NoError(t, err)

	child := bt.NewTx()
	require.NoError(t, child.From(parent.TxID(), 0, parent.Outputs[0].LockingScript.String(), parent.Outputs[0].Satoshis))
	child.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{0x51})
	require.NoError(t, child.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 3000))

	utxoHash, err := util.UTXOHashFromOutput(parent.TxIDChainHash(), parent.Outputs[0], 0)
	require.NoError(t, err)

	item := func() *batchSpend {
		return &batchSpend{
			spend: &utxo.Spend{
				TxID:         parent.TxIDChainHash(),
				Vout:         0,
				UTXOHash:     utxoHash,
				SpendingData: spendpkg.NewSpendingData(child.TxIDChainHash(), 0),
			},
			blockHeight: 1000,
			errCh:       make(chan error, 1),
		}
	}

	representative, duplicate := item(), item()

	var once sync.Once

	store.afterSpendSelect = func() {
		once.Do(func() {
			_, insertErr := store.db.ExecContext(context.Background(),
				`INSERT INTO deleted_children (parent_id, child_hash) SELECT id, $2 FROM transactions WHERE hash = $1`,
				parent.TxIDChainHash()[:], child.TxIDChainHash()[:])
			require.NoError(t, insertErr)

			_, spendErr := store.db.ExecContext(context.Background(),
				"UPDATE outputs SET spending_data = $2 WHERE idx = 0 AND transaction_id IN (SELECT id FROM transactions WHERE hash = $1)",
				parent.TxIDChainHash()[:], spendpkg.NewSpendingData(child.TxIDChainHash(), 0).Bytes())
			require.NoError(t, spendErr)
		})
	}
	t.Cleanup(func() { store.afterSpendSelect = nil })

	store.sendSpendBatch([]*batchSpend{representative, duplicate})
	store.afterSpendSelect = nil

	require.ErrorIs(t, <-representative.errCh, errors.ErrUtxoSpendingTxPruned, "precondition: the representative is refused as a pruned replay")
	require.ErrorIs(t, <-duplicate.errCh, errors.ErrUtxoSpendingTxPruned,
		"the duplicate must take its representative's refusal, not a success")
}

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

			plantReplayMarker(t, ctx, store, m, c, 1)

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

	plantReplayMarker(t, ctx, store, m, c, 1)

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
