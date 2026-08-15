package packedsql

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/teranode/test/utils/postgres"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
)

func TestCreateSchemaIdempotentAndValidated(t *testing.T) {
	dsn, cleanup, err := postgres.SetupTestPostgresContainer()
	require.NoError(t, err)

	defer func() { _ = cleanup() }()

	ctx := context.Background()

	pool, err := pgxpool.New(ctx, dsn)
	require.NoError(t, err)

	defer pool.Close()

	require.NoError(t, createSchema(ctx, pool, 4, 64))
	require.NoError(t, createSchema(ctx, pool, 4, 64))

	err = createSchema(ctx, pool, 4, 128)
	require.Error(t, err)
	require.Contains(t, err.Error(), "page_size")

	err = createSchema(ctx, pool, 8, 64)
	require.Error(t, err)
	require.Contains(t, err.Error(), "partitions")

	var n int
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT count(*) FROM pg_inherits WHERE inhparent = 'packed_txs'::regclass`).Scan(&n))
	require.Equal(t, 4, n)
}
