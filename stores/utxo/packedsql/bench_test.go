package packedsql

import (
	"context"
	"net/url"
	"testing"

	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	sqlstore "github.com/bsv-blockchain/teranode/stores/utxo/sql"
	"github.com/bsv-blockchain/teranode/stores/utxo/tests"
	"github.com/bsv-blockchain/teranode/test/utils/postgres"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

func TestSpendRoutingIsStableByHash(t *testing.T) {
	const workers = 8

	hash := make([]byte, 32)
	for i := range hash {
		hash[i] = byte(i * 7)
	}

	first := spendWorkerIndex(hash, workers)

	for i := 0; i < 100; i++ {
		require.Equal(t, first, spendWorkerIndex(hash, workers))
	}

	require.GreaterOrEqual(t, first, 0)
	require.Less(t, first, workers)
}

func BenchmarkSuite(b *testing.B) {
	store := newTestStore(b)
	tests.Benchmark(b, store)
}

func BenchmarkSuiteSQLStore(b *testing.B) {
	dsn, cleanup, err := postgres.SetupTestPostgresContainer()
	require.NoError(b, err)

	b.Cleanup(func() { _ = cleanup() })

	storeURL, err := url.Parse(dsn)
	require.NoError(b, err)

	tSettings := settings.NewSettings()

	store, err := sqlstore.New(context.Background(), ulogger.TestLogger{}, tSettings, storeURL)
	require.NoError(b, err)

	b.Cleanup(func() { _ = store.Close(context.Background()) })

	tests.Benchmark(b, store)
}

func BenchmarkCreateAndSpend(b *testing.B) {
	store := newTestStore(b)
	ctx := context.Background()

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		parent := newExtendedTx(b, 2, 1_000_000+uint64(i))

		if _, err := store.Create(ctx, parent, 100); err != nil {
			b.Fatal(err)
		}

		spender := newSpendingTx(b, parent, 0, 1)

		if _, _, err := store.SpendAndCreate(ctx, spender, 101, utxo.WithSpendOnly()); err != nil {
			b.Fatal(err)
		}
	}
}
