package sql

import (
	"context"
	"net/url"
	"testing"

	"github.com/bsv-blockchain/teranode/test/utils/postgres"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// BenchmarkCheckCallerSuppliedBlockID times the check StoreBlock runs, under
// slowPathMu, before every insert with a caller-supplied id. Quick validation
// supplies one for every block it commits, so during catch-up this cost is paid
// once per block with every other block insert waiting behind it.
//
// "reserved" is the normal path: the id is the hash's reservation. "swept" is the
// retry after the reservation was swept, which walks every rule.
func BenchmarkCheckCallerSuppliedBlockID(b *testing.B) {
	b.Run("sqlitememory", func(b *testing.B) {
		storeURL, err := url.Parse("sqlitememory:///")
		require.NoError(b, err)

		benchmarkCheckCallerSuppliedBlockID(b, storeURL)
	})

	b.Run("postgres", func(b *testing.B) {
		connStr, teardown, err := postgres.SetupTestPostgresContainer()
		if err != nil {
			b.Skipf("PostgreSQL container not available: %v", err)
		}

		b.Cleanup(func() { _ = teardown() })

		storeURL, err := url.Parse(connStr)
		require.NoError(b, err)

		benchmarkCheckCallerSuppliedBlockID(b, storeURL)
	})
}

func benchmarkCheckCallerSuppliedBlockID(b *testing.B, storeURL *url.URL) {
	ctx := context.Background()

	s, err := New(ulogger.TestLogger{}, storeURL, test.CreateBaseTestSettings(b))
	require.NoError(b, err)
	b.Cleanup(func() { _ = s.Close(context.Background()) })

	reserved, err := s.AssignBlockID(ctx, block1.Hash())
	require.NoError(b, err)

	b.Run("reserved", func(b *testing.B) {
		for b.Loop() {
			if err := s.checkCallerSuppliedBlockID(ctx, block1.Hash(), reserved); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("swept", func(b *testing.B) {
		_, err := s.db.ExecContext(ctx, `DELETE FROM block_id_reservations WHERE hash = $1`, block1.Hash()[:])
		require.NoError(b, err)

		for b.Loop() {
			if err := s.checkCallerSuppliedBlockID(ctx, block1.Hash(), reserved); err != nil {
				b.Fatal(err)
			}
		}
	})
}
