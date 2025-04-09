package postgres

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
)

func TestPostgresStartup(t *testing.T) {
	ctx := context.Background()

	container, err := RunPostgresTestContainer(ctx, "testStartup")
	require.NoError(t, err)

	defer func() {
		if err := container.Terminate(ctx); err != nil {
			t.Fatalf("failed to terminate container: %v", err)
		}
	}()

	connStr := container.ConnectionString()

	conn, err := pgx.Connect(ctx, connStr)
	require.NoError(t, err)
	defer conn.Close(ctx)

	_, err = conn.Exec(ctx, "SELECT 1")
	require.NoError(t, err)
}

func TestInitScript(t *testing.T) {
	ctx := context.Background()
	container, err := RunPostgresTestContainer(ctx, "testInit")
	require.NoError(t, err)

	defer func() {
		require.NoError(t, container.Terminate(ctx))
	}()

	// Connection to the "postgres" database (the default DB) to be able to query the catalog
	connStr := container.ConnectionString()
	conn, err := pgx.Connect(ctx, connStr)

	require.NoError(t, err)

	defer conn.Close(ctx)

	// Verify that "teranode" ROLE exists
	var roleName string
	err = conn.QueryRow(ctx, `SELECT rolname FROM pg_roles WHERE rolname = 'teranode';`).Scan(&roleName)
	require.NoError(t, err)
	require.Equal(t, "teranode", roleName)

	// Verify that "teranode" DB exists
	var dbName string
	err = conn.QueryRow(ctx, `SELECT datname FROM pg_database WHERE datname = 'teranode';`).Scan(&dbName)
	require.NoError(t, err)
	require.Equal(t, "teranode", dbName)

	// Verify that "coinbase" DB exists
	err = conn.QueryRow(ctx, `SELECT datname FROM pg_database WHERE datname = 'coinbase';`).Scan(&dbName)
	require.NoError(t, err)
	require.Equal(t, "coinbase", dbName)
}
