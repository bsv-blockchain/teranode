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
func InitSQLDB(logger ulogger.Logger, storeURL *url.URL, tSettings *settings.Settings) (*usql.DB, error) {
	switch storeURL.Scheme {
	case "postgres":
		return InitPostgresDB(logger, storeURL, tSettings)
	case "sqlite", "sqlitememory":
		return InitSQLiteDB(logger, storeURL, tSettings)
	}

	return nil, errors.NewConfigurationError("db: unknown scheme: %s", storeURL.Scheme)
}

// InitPostgresDB initializes a PostgreSQL database connection with connection pooling.
// Extracts connection parameters from the URL and applies SSL mode configuration.
// Sets up connection limits based on the provided settings.
func InitPostgresDB(logger ulogger.Logger, storeURL *url.URL, tSettings *settings.Settings) (*usql.DB, error) {
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

	db.SetMaxIdleConns(tSettings.UtxoStore.PostgresMaxIdleConns)
	db.SetMaxOpenConns(tSettings.UtxoStore.PostgresMaxOpenConns)

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
