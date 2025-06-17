package util

import (
	"fmt"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strconv"

	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/settings"
	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/bitcoin-sv/teranode/util/usql"
	"github.com/labstack/gommon/random"
)

type SQLEngine string

const (
	Postgres     SQLEngine = "postgres"
	Sqlite       SQLEngine = "sqlite"
	SqliteMemory SQLEngine = "sqlitememory"
)

func InitSQLDB(logger ulogger.Logger, storeURL *url.URL, tSettings *settings.Settings) (*usql.DB, error) {
	switch storeURL.Scheme {
	case "postgres":
		return InitPostgresDB(logger, storeURL, tSettings)
	case "sqlite", "sqlitememory":
		return InitSQLiteDB(logger, storeURL, tSettings)
	}

	return nil, errors.NewConfigurationError("db: unknown scheme: %s", storeURL.Scheme)
}

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

func InitSQLiteDB(logger ulogger.Logger, storeURL *url.URL, tSettings *settings.Settings) (*usql.DB, error) {
	var filename string

	var err error

	if storeURL.Scheme == "sqlitememory" {
		filename = fmt.Sprintf("file:%s?mode=memory&cache=shared", random.String(16))
	} else {
		folder := tSettings.DataFolder
		if err = os.MkdirAll(folder, 0755); err != nil {
			return nil, errors.NewServiceError("failed to create data folder %s", folder, err)
		}

		dbName := storeURL.Path[1:]

		filename, err = filepath.Abs(path.Join(folder, fmt.Sprintf("%s.db", dbName)))
		if err != nil {
			return nil, errors.NewServiceError("failed to get absolute path for sqlite DB", err)
		}

		// filename = fmt.Sprintf("file:%s?cache=shared&mode=rwc", filename)

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
