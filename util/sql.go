package util

import (
	"fmt"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strconv"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/usql"
	"github.com/labstack/gommon/random"
)

type SQLEngine string

const (
	Postgres     SQLEngine = "postgres"
	Sqlite       SQLEngine = "sqlite"
	SqliteMemory SQLEngine = "sqlitememory"
)

// InitSQLDB initializes a SQL database connection based on the provided URL scheme.
// Supports PostgreSQL, SQLite, and in-memory SQLite databases.
// Returns a configured database connection with appropriate settings applied.
// If servicePoolSettings is provided, it will override the global PostgreSQL pool settings.
func InitSQLDB(logger ulogger.Logger, storeURL *url.URL, tSettings *settings.Settings, servicePoolSettings ...*settings.PostgresSettings) (*usql.DB, error) {
	switch storeURL.Scheme {
	case "postgres":
		var poolSettings *settings.PostgresSettings
		if len(servicePoolSettings) > 0 && servicePoolSettings[0] != nil {
			poolSettings = servicePoolSettings[0]
		}
		return InitPostgresDB(logger, storeURL, tSettings, poolSettings)
	case "sqlite", "sqlitememory":
		return InitSQLiteDB(logger, storeURL, tSettings)
	}

	return nil, errors.NewConfigurationError("db: unknown scheme: %s", storeURL.Scheme)
}

// InitPostgresDB initializes a PostgreSQL database connection with connection pooling.
// Extracts connection parameters from the URL and applies SSL mode configuration.
// Sets up connection limits based on the provided settings.
// If servicePoolSettings is provided, it overrides the global PostgreSQL pool settings.
// Otherwise, uses the global PostgresSettings from tSettings.
func InitPostgresDB(logger ulogger.Logger, storeURL *url.URL, tSettings *settings.Settings, servicePoolSettings *settings.PostgresSettings) (*usql.DB, error) {
	dbHost := storeURL.Hostname()
	port := storeURL.Port()
	dbPort, _ := strconv.Atoi(port)
	dbName := storeURL.Path[1:]
	dbUser := ""
	dbPassword := ""

	if storeURL.User != nil {
		dbUser = storeURL.User.Username()
		dbPassword, _ = storeURL.User.Password()
	}

	// Default sslmode to "disable"
	sslMode := "disable"

	// Check if "sslmode" is present in the query parameters
	queryParams := storeURL.Query()
	if val, ok := queryParams["sslmode"]; ok && len(val) > 0 {
		sslMode = val[0] // Use the first value if multiple are provided
	}

	dbInfo := fmt.Sprintf("user=%s password=%s dbname=%s sslmode=%s host=%s port=%d", dbUser, dbPassword, dbName, sslMode, dbHost, dbPort)

	db, err := usql.Open(storeURL.Scheme, dbInfo)
	if err != nil {
		return nil, errors.NewServiceError("failed to open postgres DB", err)
	}

	logger.Infof("Using postgres DB: %s@%s:%d/%s", dbUser, dbHost, dbPort, dbName)

	// Determine which pool settings to use: service-specific override or global defaults
	poolSettings := &tSettings.Postgres
	if servicePoolSettings != nil {
		// Merge service-specific settings with global defaults (zero values use global)
		poolSettings = &settings.PostgresSettings{
			MaxOpenConns:     servicePoolSettings.MaxOpenConns,
			MaxIdleConns:     servicePoolSettings.MaxIdleConns,
			ConnMaxLifetime:  servicePoolSettings.ConnMaxLifetime,
			ConnMaxIdleTime:  servicePoolSettings.ConnMaxIdleTime,
			RetryMaxAttempts: servicePoolSettings.RetryMaxAttempts,
			RetryBaseDelay:   servicePoolSettings.RetryBaseDelay,
			RetryEnabled:     servicePoolSettings.RetryEnabled,
		}
		// Use global defaults for zero values
		if poolSettings.MaxOpenConns == 0 {
			poolSettings.MaxOpenConns = tSettings.Postgres.MaxOpenConns
		}
		if poolSettings.MaxIdleConns == 0 {
			poolSettings.MaxIdleConns = tSettings.Postgres.MaxIdleConns
		}
		if poolSettings.ConnMaxLifetime == 0 {
			poolSettings.ConnMaxLifetime = tSettings.Postgres.ConnMaxLifetime
		}
		if poolSettings.ConnMaxIdleTime == 0 {
			poolSettings.ConnMaxIdleTime = tSettings.Postgres.ConnMaxIdleTime
		}
		if poolSettings.RetryMaxAttempts == 0 {
			poolSettings.RetryMaxAttempts = tSettings.Postgres.RetryMaxAttempts
		}
		if poolSettings.RetryBaseDelay == 0 {
			poolSettings.RetryBaseDelay = tSettings.Postgres.RetryBaseDelay
		}
	}

	// Configure connection pool settings
	db.SetMaxOpenConns(poolSettings.MaxOpenConns)
	db.SetMaxIdleConns(poolSettings.MaxIdleConns)
	db.SetConnMaxLifetime(poolSettings.ConnMaxLifetime)
	db.SetConnMaxIdleTime(poolSettings.ConnMaxIdleTime)

	// Configure retry settings
	db.SetRetryConfig(usql.RetryConfig{
		MaxAttempts: poolSettings.RetryMaxAttempts,
		BaseDelay:   poolSettings.RetryBaseDelay,
		Enabled:     poolSettings.RetryEnabled,
	})

	logger.Infof("PostgreSQL connection pool configured: MaxOpenConns=%d, MaxIdleConns=%d, ConnMaxLifetime=%v, ConnMaxIdleTime=%v",
		poolSettings.MaxOpenConns,
		poolSettings.MaxIdleConns,
		poolSettings.ConnMaxLifetime,
		poolSettings.ConnMaxIdleTime)

	logger.Infof("PostgreSQL retry configured: MaxAttempts=%d, BaseDelay=%v, Enabled=%v",
		poolSettings.RetryMaxAttempts,
		poolSettings.RetryBaseDelay,
		poolSettings.RetryEnabled)

	// Log initial pool metrics
	logPostgresPoolMetrics(logger, db)

	return db, nil
}

// InitSQLiteDB initializes a SQLite database connection with WAL mode and shared cache.
// Supports both file-based and in-memory databases based on the URL scheme.
// Enables foreign keys and configures pragmas for optimal performance.
func InitSQLiteDB(logger ulogger.Logger, storeURL *url.URL, tSettings *settings.Settings) (*usql.DB, error) {
	var filename string

	var err error

	if storeURL.Scheme == "sqlitememory" {
		filename = fmt.Sprintf("file:%s?mode=memory&cache=shared", random.String(16))
	} else {
		folder := tSettings.DataFolder
		dbName := storeURL.Path[1:]

		filename, err = filepath.Abs(path.Join(folder, fmt.Sprintf("%s.db", dbName)))
		if err != nil {
			return nil, errors.NewServiceError("failed to get absolute path for sqlite DB", err)
		}

		// Create the directory containing the database file (handles nested paths like teranode1/blockchain1.db)
		dbDir := filepath.Dir(filename)
		if err = os.MkdirAll(dbDir, 0755); err != nil {
			return nil, errors.NewServiceError("failed to create data folder %s", dbDir, err)
		}

		/* Don't be tempted by a large busy_timeout. Just masks a bigger problem.
		Fail fast. This is 'dev mode' sqlite after all */
		filename = fmt.Sprintf("%s?cache=shared&_pragma=busy_timeout=5000&_pragma=journal_mode=WAL", filename)
	}

	logger.Infof("Using sqlite DB: %s", filename)

	var db *usql.DB

	db, err = usql.Open("sqlite", filename)
	if err != nil {
		return nil, errors.NewServiceError("failed to open sqlite DB", err)
	}

	if _, err = db.Exec(`PRAGMA foreign_keys = ON;`); err != nil {
		_ = db.Close()
		return nil, errors.NewServiceError("could not enable foreign keys support", err)
	}

	if _, err = db.Exec(`PRAGMA locking_mode = SHARED;`); err != nil {
		_ = db.Close()
		return nil, errors.NewServiceError("could not enable shared locking mode", err)
	}

	/* recommend setting max connection to low number - don't hide a problem by allowing infinite connections.
	This is sqlite, our local db, this isn't about performance. Use a small number. See the problem. Fail fast. */
	// db.SetMaxOpenConns(5)
	return db, nil
}

// logPostgresPoolMetrics logs PostgreSQL connection pool statistics including
// open connections, idle connections, wait count, and wait duration.
func logPostgresPoolMetrics(logger ulogger.Logger, db *usql.DB) {
	stats := db.Stats()
	logger.Infof("PostgreSQL connection pool metrics: OpenConnections=%d, InUse=%d, Idle=%d, WaitCount=%d, WaitDuration=%v, MaxIdleClosed=%d, MaxIdleTimeClosed=%d, MaxLifetimeClosed=%d",
		stats.OpenConnections,
		stats.InUse,
		stats.Idle,
		stats.WaitCount,
		stats.WaitDuration,
		stats.MaxIdleClosed,
		stats.MaxIdleTimeClosed,
		stats.MaxLifetimeClosed)
}
