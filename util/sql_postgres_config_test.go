package util

import (
	"context"
	"net/url"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

func postgresURL(user, password, host, database string) string {
	u := url.URL{Scheme: "postgres", User: url.UserPassword(user, password), Host: host, Path: "/" + database}
	return u.String()
}

func TestPostgresConnConfig_ValuesSurviveParsing(t *testing.T) {
	tests := []struct {
		name     string
		url      string
		user     string
		password string
		database string
		host     string
		port     uint16
	}{
		{
			name:     "user without password",
			url:      "postgres://someuser@localhost:5432/my_database",
			user:     "someuser",
			password: "",
			database: "my_database",
			host:     "localhost",
			port:     5432,
		},
		{
			name:     "user with empty password",
			url:      "postgres://someuser:@localhost:5432/my_database",
			user:     "someuser",
			password: "",
			database: "my_database",
			host:     "localhost",
			port:     5432,
		},
		{
			name:     "user and password",
			url:      "postgres://someuser:secret@db.example:6543/my_database?sslmode=disable",
			user:     "someuser",
			password: "secret",
			database: "my_database",
			host:     "db.example",
			port:     6543,
		},
		{
			name:     "password with space, quote, backslash and equals",
			url:      postgresURL("someuser", `p a'ss\w=rd`, "localhost:5432", "my_database"),
			user:     "someuser",
			password: `p a'ss\w=rd`,
			database: "my_database",
			host:     "localhost",
			port:     5432,
		},
		{
			name:     "password that looks like a keyword",
			url:      postgresURL("someuser", "x dbname=other", "localhost:5432", "my_database"),
			user:     "someuser",
			password: "x dbname=other",
			database: "my_database",
			host:     "localhost",
			port:     5432,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			storeURL, err := url.Parse(tt.url)
			require.NoError(t, err)

			cfg, err := postgresConnConfig(storeURL)
			require.NoError(t, err)

			require.Equal(t, tt.user, cfg.User)
			require.Equal(t, tt.password, cfg.Password)
			require.Equal(t, tt.database, cfg.Database)
			require.Equal(t, tt.host, cfg.Host)
			require.Equal(t, tt.port, cfg.Port)
		})
	}
}

// A store URL without a password must still connect to the database it names,
// not silently to the server's default database.
func TestInitPostgresDB_URLWithoutPasswordUsesNamedDatabase(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping postgres-backed test in -short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	pgContainer, err := postgres.Run(ctx,
		"postgres:16",
		postgres.WithDatabase("my_database"),
		postgres.WithUsername("postgres"),
		testcontainers.WithEnv(map[string]string{"POSTGRES_HOST_AUTH_METHOD": "trust"}),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(5*time.Minute),
		),
	)
	test.SkipIfContainerUnavailable(t, err)
	t.Cleanup(func() { _ = pgContainer.Terminate(context.Background()) })

	host, err := pgContainer.Host(ctx)
	require.NoError(t, err)

	port, err := pgContainer.MappedPort(ctx, "5432/tcp")
	require.NoError(t, err)

	storeURL, err := url.Parse("postgres://postgres@" + host + ":" + port.Port() + "/my_database")
	require.NoError(t, err)

	db, err := InitPostgresDB(ulogger.TestLogger{}, storeURL, &settings.Settings{}, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	var current string
	require.NoError(t, db.QueryRowContext(ctx, "SELECT current_database()").Scan(&current))
	require.Equal(t, "my_database", current)
}
