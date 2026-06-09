package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/lib/pq"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

// sharedConnStr is the base connStr for the single PostgreSQL container shared across
// the test binary. The container is started exactly once via containerOnce; its handle
// is not retained — its lifecycle is bound to the test process / CI runner teardown
// (Ryuk is disabled in CI). containerErr is permanent if that one-time start fails, so a
// transient first-start failure fails every caller in the binary (see startSharedContainer's
// internal 3-attempt retry, which mitigates this).
var (
	containerOnce sync.Once
	sharedConnStr string // base connStr pointing at the default "testdb" database
	containerErr  error

	// dbCounter is incremented atomically to produce unique per-test database names.
	dbCounter atomic.Uint64

	// createMu serialises CREATE DATABASE statements so that concurrent callers
	// (e.g. future t.Parallel tests) never race on template1 being accessed by
	// another CREATE DATABASE.
	createMu sync.Mutex
)

// SetupTestPostgresContainer returns a connection string for a freshly created,
// isolated database on a shared PostgreSQL 16 container.
//
// The shared container is started once per test binary (lazy, via sync.Once) with a
// 3-attempt retry. Every call creates a unique database (testdb_N) on that server and
// returns a connStr pointing at it. The returned cleanup func drops that database.
//
// The public signature is intentionally unchanged so no callers need updating.
func SetupTestPostgresContainer() (string, func() error, error) {
	// Initialise the shared container exactly once.
	containerOnce.Do(func() {
		sharedConnStr, containerErr = startSharedContainer()
	})
	if containerErr != nil {
		return "", nil, containerErr
	}

	// Derive a unique database name for this test.
	n := dbCounter.Add(1)
	dbName := fmt.Sprintf("testdb_%d", n)

	// Build a connStr targeting the postgres system database to run CREATE DATABASE.
	// CREATE DATABASE still copies from template1 by default regardless of which DB we
	// connect to; the point of connecting the admin session to the "postgres" system DB
	// (rather than template1) is that this session is not itself counted as an active
	// template1 user, so it can't block the template copy. Combined with the createMu
	// serialisation below, this avoids "template1 is being accessed by other users".
	adminConnStr, err := swapDatabase(sharedConnStr, "postgres")
	if err != nil {
		return "", nil, errors.NewProcessingError("build admin connStr", err)
	}

	// Serialize CREATE DATABASE to avoid concurrent template access errors.
	createMu.Lock()
	err = execAdmin(adminConnStr, fmt.Sprintf("CREATE DATABASE %s", pq.QuoteIdentifier(dbName)))
	createMu.Unlock()
	if err != nil {
		return "", nil, errors.NewProcessingError("create database %s", dbName, err)
	}

	// Build the per-test connStr.
	testConnStr, err := swapDatabase(sharedConnStr, dbName)
	if err != nil {
		return "", nil, errors.NewProcessingError("build test connStr", err)
	}

	cleanup := func() error {
		// Drop with FORCE so any lingering backend connections are killed.
		// PostgreSQL 16 supports WITH (FORCE). The caller's store connections
		// are terminated by the server; the Go-side pool is abandoned (it goes
		// out of scope with the test), which is acceptable for test helpers.
		dropSQL := fmt.Sprintf(
			"DROP DATABASE IF EXISTS %s WITH (FORCE)",
			pq.QuoteIdentifier(dbName),
		)
		return execAdmin(adminConnStr, dropSQL)
	}

	return testConnStr, cleanup, nil
}

// startSharedContainer starts (with up to 3 attempts) and validates a single
// shared postgres:16-alpine container. It returns the base connection string.
func startSharedContainer() (string, error) {
	ctx := context.Background()

	const (
		dbName     = "testdb"
		dbUser     = "postgres"
		dbPassword = "password"
	)

	var (
		postgresC *postgres.PostgresContainer
		err       error
	)

	for attempt := 0; attempt < 3; attempt++ {
		if attempt > 0 {
			delay := time.Duration(500*attempt) * time.Millisecond
			time.Sleep(delay)
		}

		postgresC, err = postgres.Run(ctx,
			"docker.io/postgres:16-alpine",
			postgres.WithDatabase(dbName),
			postgres.WithUsername(dbUser),
			postgres.WithPassword(dbPassword),
			testcontainers.WithWaitStrategy(
				wait.ForLog("database system is ready to accept connections").
					WithOccurrence(2).WithStartupTimeout(60*time.Second),
				wait.ForListeningPort("5432/tcp"),
			),
		)
		if err == nil {
			break
		}
	}
	if err != nil {
		return "", errors.NewProcessingError("start postgres container", err)
	}

	connStr, err := postgresC.ConnectionString(ctx)
	if err != nil {
		// Don't leak the container if we created it but can't get its conn string.
		_ = postgresC.Terminate(ctx)
		return "", errors.NewProcessingError("get connection string", err)
	}

	// Ensure sslmode=disable is present before storing as sharedConnStr.
	connStr = ensureSSLDisabled(connStr)

	if err := validateDatabaseConnection(connStr, 5); err != nil {
		_ = postgresC.Terminate(ctx)
		return "", errors.NewProcessingError("database validation failed", err)
	}

	// postgresC is intentionally not retained: there is no per-test teardown for the
	// shared container, so it is reaped by the testcontainers Ryuk reaper when the test
	// process exits. Only the per-test databases are dropped (see cleanup above).
	return connStr, nil
}

// swapDatabase replaces the database name component of a postgres connStr (URL form)
// with newDB, preserving all other parameters (user, password, host, port, query).
func swapDatabase(connStr, newDB string) (string, error) {
	u, err := url.Parse(connStr)
	if err != nil {
		return "", errors.NewProcessingError("parse connStr", err)
	}
	u.Path = "/" + newDB
	return u.String(), nil
}

// execAdmin opens a short-lived connection to adminConnStr, executes stmt, then closes.
func execAdmin(adminConnStr, stmt string) error {
	db, err := sql.Open("postgres", adminConnStr)
	if err != nil {
		return errors.NewProcessingError("open admin connection", err)
	}
	defer db.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_, err = db.ExecContext(ctx, stmt)
	return err
}

// ensureSSLDisabled appends sslmode=disable to connStr if not already present.
func ensureSSLDisabled(connStr string) string {
	u, err := url.Parse(connStr)
	if err != nil {
		// Fallback: return unchanged; url.Parse should never fail on a well-formed connStr.
		return connStr
	}
	q := u.Query()
	if q.Get("sslmode") == "" {
		q.Set("sslmode", "disable")
		u.RawQuery = q.Encode()
	}
	return u.String()
}

// validateDatabaseConnection attempts to connect to the database and run a simple query
// to verify it is truly ready for operations. It retries with increasing delays.
func validateDatabaseConnection(connStr string, maxRetries int) error {
	var lastErr error

	for i := 0; i < maxRetries; i++ {
		if i > 0 {
			time.Sleep(time.Duration(500*i) * time.Millisecond)
		}

		db, err := sql.Open("postgres", connStr)
		if err != nil {
			lastErr = err
			continue
		}

		func() {
			defer db.Close()

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			if err = db.PingContext(ctx); err != nil {
				lastErr = err
				return
			}

			var result int
			if err = db.QueryRowContext(ctx, "SELECT 1").Scan(&result); err != nil {
				lastErr = err
				return
			}
			if result == 1 {
				lastErr = nil
			}
		}()

		if lastErr == nil {
			return nil
		}
	}

	// Pass lastErr as the trailing arg so it is wrapped as the cause (preserving the
	// error chain for debugging flaky startup), with %d for the attempt count.
	return errors.NewProcessingError("failed to validate database connection after %d attempts", maxRetries, lastErr)
}
