package sql

import (
	"context"
	"net/url"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/stores/utxo/tests"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

// benchSQLStore opens the store on in-memory SQLite, or on a Postgres container
// when pg is set. Postgres is where the create batcher's statements run.
func benchSQLStore(b *testing.B, pg bool) *Store {
	b.Helper()

	ctx := context.Background()

	dbURL, err := url.Parse("sqlitememory:///createbench")
	require.NoError(b, err)

	if pg {
		container, err := postgres.Run(ctx, "postgres:13",
			postgres.WithDatabase("testdb"), postgres.WithUsername("testuser"), postgres.WithPassword("testpass"),
			testcontainers.WithWaitStrategy(wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).WithStartupTimeout(5*time.Minute)),
		)
		test.SkipIfContainerUnavailable(b, err)
		b.Cleanup(func() { _ = container.Terminate(ctx) })

		connStr, err := container.ConnectionString(ctx, "sslmode=disable")
		require.NoError(b, err)

		dbURL, err = url.Parse(connStr)
		require.NoError(b, err)
	}

	tSettings := test.CreateBaseTestSettings(b)
	tSettings.UtxoStore.DBTimeout = 30 * time.Second
	// Flush a batch as soon as a worker is free, so the timing is the work and not
	// the batcher's flush timer.
	tSettings.BatcherDrainMode = true
	tSettings.Pruner.UTXODefensiveEnabled = false

	store, err := New(ctx, ulogger.TestLogger{}, tSettings, dbURL)
	require.NoError(b, err)

	return store
}

// runCreateBench times create-only writes of distinct records, each with the
// options optsFor returns for it.
func runCreateBench(b *testing.B, store *Store, seed byte, optsFor func(i int) []utxo.CreateOption) {
	b.Helper()
	b.ReportAllocs()

	ctx := context.Background()
	roots := tests.BuildMultiWorkload(b, seed, 1, b.N).Roots

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		opts := append(optsFor(i), utxo.WithCreateOnly())
		if _, _, err := store.SpendAndCreate(ctx, roots[i], 900, opts...); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkCreateOnly times one create on each engine. There is no spend, so the
// record write is what it measures.
func BenchmarkCreateOnly(b *testing.B) {
	none := func(int) []utxo.CreateOption { return nil }

	b.Run("sqlite", func(b *testing.B) { runCreateBench(b, benchSQLStore(b, false), 0xc0, none) })
	b.Run("postgres", func(b *testing.B) { runCreateBench(b, benchSQLStore(b, true), 0xc1, none) })
}

// BenchmarkGetFields times one Get of the fields the validator reads.
func BenchmarkGetFields(b *testing.B) {
	run := func(b *testing.B, store *Store, seed byte) {
		ctx := context.Background()
		roots := tests.BuildMultiWorkload(b, seed, 1, 1000).Roots

		for _, root := range roots {
			_, _, err := store.SpendAndCreate(ctx, root, 900, utxo.WithCreateOnly())
			require.NoError(b, err)
		}

		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			_, err := store.Get(ctx, roots[i%len(roots)].TxIDChainHash(), fields.Fee, fields.SizeInBytes, fields.BlockIDs)
			if err != nil {
				b.Fatal(err)
			}
		}
	}

	b.Run("sqlite", func(b *testing.B) { run(b, benchSQLStore(b, false), 0xc2) })
	b.Run("postgres", func(b *testing.B) { run(b, benchSQLStore(b, true), 0xc3) })
}
