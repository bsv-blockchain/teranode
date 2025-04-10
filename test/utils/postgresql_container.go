package utils

import (
	"context"
	"database/sql"
	"strings"
	"time"

	"github.com/bitcoin-sv/teranode/errors"
	"github.com/lib/pq"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

func SetupTestPostgresContainer() (string, func() error, error) {
	ctx := context.Background()

	dbName := "testdb"
	dbUser := "postgres"
	dbPassword := "password"

	// Implement retry logic with random delays for more reliable container creation
	var (
		postgresC *postgres.PostgresContainer
		err       error
	)

	for attempt := 0; attempt < 3; attempt++ {
		// Add random delay to reduce chance of simultaneous container creation conflicts
		if attempt > 0 {
			// Random delay between 100-600ms
			delay := time.Duration(100+time.Now().Nanosecond()%500) * time.Millisecond
			time.Sleep(delay)
		}

		postgresC, err = postgres.Run(ctx,
			"docker.io/postgres:16-alpine",
			postgres.WithDatabase(dbName),
			postgres.WithUsername(dbUser),
			postgres.WithPassword(dbPassword),
			testcontainers.WithWaitStrategy(
				wait.ForLog("database system is ready to accept connections").
					WithOccurrence(2).WithStartupTimeout(30*time.Second),
				wait.ForListeningPort("5432/tcp")),
		)

		if err == nil {
			break // Successfully created container
		}
	}

	// If all attempts failed, return the last error
	if err != nil {
		return "", nil, err
	}

	connStr, err := postgresC.ConnectionString(ctx)
	if err != nil {
		return "", nil, err
	}

	// Ensure SSL is disabled in the connection string
	// This is needed because the default ConnectionString doesn't always include sslmode=disable
	if !strings.Contains(connStr, "sslmode=") {
		connStr += "&sslmode=disable"
		// If there's no query parameter yet, use ? instead of &
		if !strings.Contains(connStr, "?") {
			connStr = strings.Replace(connStr, "&sslmode=disable", "?sslmode=disable", 1)
		}
	}

	// Add a database validation step to ensure PostgreSQL is truly ready
	// This mitigates the "EOF" error that occurs when the container's port is open
	// but the database isn't fully initialized yet
	if err := validateDatabaseConnection(connStr, 5); err != nil {
		_ = postgresC.Terminate(ctx) // Clean up container if validation fails

		return "", nil, errors.NewProcessingError("database validation failed", err)
	}

	cleanup := func() error {
		return postgresC.Terminate(ctx)
	}

	return connStr, cleanup, nil
}

// validateDatabaseConnection attempts to connect to the database and run a simple query
// to verify it's truly ready for operations. It will retry with exponential backoff.
func validateDatabaseConnection(connStr string, maxRetries int) error {
	var (
		db  *sql.DB
		err error
	)

	// Import the PostgreSQL driver
	_ = pq.Driver{}

	for i := 0; i < maxRetries; i++ {
		// Try to open a connection
		db, err = sql.Open("postgres", connStr)
		if err != nil {
			time.Sleep(time.Duration(500*(i+1)) * time.Millisecond)
			continue
		}

		// Try a simple query to verify the connection works
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		err = db.PingContext(ctx)

		cancel()

		if err == nil {
			// Connection successful, do a sample query to verify
			var result int

			err = db.QueryRowContext(context.Background(), "SELECT 1").Scan(&result)
			if err == nil && result == 1 {
				db.Close()

				return nil // Success!
			}
		}

		// Close the connection before retrying
		if db != nil {
			db.Close()
		}

		// Wait with increasing delay before retry
		time.Sleep(time.Duration(500*(i+1)) * time.Millisecond)
	}

	return errors.NewProcessingError("failed to validate database connection after %d attempts: %v", maxRetries, err)
}
