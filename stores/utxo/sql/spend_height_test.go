package sql

import (
	"context"
	"database/sql"
	"encoding/hex"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/stretchr/testify/require"
)

// heightBackend is one store configuration a spend-height test runs on.
type heightBackend struct {
	name  string
	build func(t *testing.T) (*Store, context.Context)
}

// heightBackends are the three spend implementations: the per-row path on
// SQLite, the per-row path on Postgres, and the bulk CTE path on Postgres.
func heightBackends() []heightBackend {
	return []heightBackend{
		{"sqlite", func(t *testing.T) (*Store, context.Context) {
			ctx, cancel := context.WithCancel(context.Background())
			t.Cleanup(cancel)
			store, _ := setup(ctx, t)

			return store, ctx
		}},
		{"postgres_per_row", func(t *testing.T) (*Store, context.Context) {
			store, ctx := setupPostgresStore(t)
			store.settings.UtxoStore.BatchSQLOperations = false

			return store, ctx
		}},
		{"postgres_bulk", func(t *testing.T) (*Store, context.Context) {
			store, ctx := setupPostgresStore(t)
			store.settings.UtxoStore.BatchSQLOperations = true

			return store, ctx
		}},
	}
}

// newTwoOutputParent builds a parent with two outputs. seed makes its txid distinct.
func newTwoOutputParent(t *testing.T, seed byte) *bt.Tx {
	t.Helper()

	parent := bt.NewTx()
	prev := make([]byte, 32)
	prev[0] = seed
	require.NoError(t, parent.From(hex.EncodeToString(prev), 0, "51", 30000))
	parent.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{0x51})
	require.NoError(t, parent.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 4000))
	require.NoError(t, parent.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 4000))

	return parent
}

// newSpenderOf builds a transaction spending the given outputs of parent, one
// input per vout. salt makes its payment, and so its txid, distinct.
func newSpenderOf(t *testing.T, parent *bt.Tx, salt uint64, vouts ...uint32) *bt.Tx {
	t.Helper()

	child := bt.NewTx()
	for _, vout := range vouts {
		require.NoError(t, child.From(parent.TxID(), vout, parent.Outputs[vout].LockingScript.String(), parent.Outputs[vout].Satoshis))
	}

	for _, in := range child.Inputs {
		in.UnlockingScript = bscript.NewFromBytes([]byte{0x51})
	}

	require.NoError(t, child.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 1000+salt))

	return child
}

// recordedSpendHeight reads the spending_height of parent:vout.
func recordedSpendHeight(t *testing.T, ctx context.Context, store *Store, parent *bt.Tx, vout uint32) sql.NullInt64 {
	t.Helper()

	var height sql.NullInt64

	require.NoError(t, store.db.QueryRowContext(ctx,
		`SELECT o.spending_height FROM outputs o JOIN transactions t ON t.id = o.transaction_id WHERE t.hash = $1 AND o.idx = $2`,
		parent.TxIDChainHash()[:], vout).Scan(&height))

	return height
}

func TestSpendRecordsItsHeight(t *testing.T) {
	for _, backend := range heightBackends() {
		t.Run(backend.name, func(t *testing.T) {
			store, ctx := backend.build(t)
			parent := newTwoOutputParent(t, 1)
			_, err := store.Create(ctx, parent, 1000)
			require.NoError(t, err)
			require.False(t, recordedSpendHeight(t, ctx, store, parent, 0).Valid, "an unspent output has no spend height")

			child := newSpenderOf(t, parent, 1, 0)
			spends, err := store.Spend(ctx, child, 1200)
			require.NoError(t, err)
			require.Equal(t, sql.NullInt64{Int64: 1200, Valid: true}, recordedSpendHeight(t, ctx, store, parent, 0))
			require.False(t, recordedSpendHeight(t, ctx, store, parent, 1).Valid, "the untouched sibling output stays unrecorded")

			// An idempotent re-spend at a later height keeps the height of the first spend.
			_, err = store.Spend(ctx, child, 1203)
			require.NoError(t, err)
			require.Equal(t, sql.NullInt64{Int64: 1200, Valid: true}, recordedSpendHeight(t, ctx, store, parent, 0))

			require.NoError(t, store.Unspend(ctx, spends))
			require.False(t, recordedSpendHeight(t, ctx, store, parent, 0).Valid, "Unspend clears the height with the spending data")

			// The output can be spent again, and records the new height.
			_, err = store.Spend(ctx, child, 1300)
			require.NoError(t, err)
			require.Equal(t, sql.NullInt64{Int64: 1300, Valid: true}, recordedSpendHeight(t, ctx, store, parent, 0))
		})
	}
}

// Two calls at different heights can share one bulk statement, so each row has
// to carry its own height rather than the batch sharing one.
func TestSpendRecordsEachCallsOwnHeightInOneBatch(t *testing.T) {
	store, ctx := setupPostgresStore(t)
	store.settings.UtxoStore.BatchSQLOperations = true

	parent := newTwoOutputParent(t, 4)
	_, err := store.Create(ctx, parent, 1000)
	require.NoError(t, err)

	item := func(child *bt.Tx, height uint32) *batchSpend {
		spends, spendsErr := utxo.GetSpends(child)
		require.NoError(t, spendsErr)

		return &batchSpend{spend: spends[0], blockHeight: height, errCh: make(chan error, 1)}
	}

	first := item(newSpenderOf(t, parent, 4, 0), 1200)
	second := item(newSpenderOf(t, parent, 5, 1), 1300)

	require.False(t, store.trySendSpendBatchBulk(ctx, []*batchSpend{first, second}))
	require.NoError(t, <-first.errCh)
	require.NoError(t, <-second.errCh)

	require.Equal(t, sql.NullInt64{Int64: 1200, Valid: true}, recordedSpendHeight(t, ctx, store, parent, 0))
	require.Equal(t, sql.NullInt64{Int64: 1300, Valid: true}, recordedSpendHeight(t, ctx, store, parent, 1))
}

func TestSpendHeightColumnIsAddedToAnOldDatabase(t *testing.T) {
	for _, backend := range heightBackends()[:2] {
		t.Run(backend.name, func(t *testing.T) {
			store, ctx := backend.build(t)
			parent := newTwoOutputParent(t, 2)
			_, err := store.Create(ctx, parent, 1000)
			require.NoError(t, err)
			_, err = store.Spend(ctx, newSpenderOf(t, parent, 2, 0), 1200)
			require.NoError(t, err)

			_, err = store.db.ExecContext(ctx, `ALTER TABLE outputs DROP COLUMN spending_height`)
			require.NoError(t, err)

			migrate := func() {
				if store.engine == "postgres" {
					require.NoError(t, createPostgresSchema(ctx, store.logger, store.db))
				} else {
					require.NoError(t, createSqliteSchema(store.db))
				}
			}

			migrate()
			require.False(t, recordedSpendHeight(t, ctx, store, parent, 0).Valid, "a row spent before the column existed reads NULL")

			// Running the migration again changes nothing.
			migrate()

			_, err = store.Spend(ctx, newSpenderOf(t, parent, 3, 1), 1250)
			require.NoError(t, err)
			require.Equal(t, sql.NullInt64{Int64: 1250, Valid: true}, recordedSpendHeight(t, ctx, store, parent, 1))
		})
	}
}
