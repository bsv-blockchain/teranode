package util

import (
	"fmt"
	"github.com/bitcoin-sv/ubsv/errors"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strconv"

	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/bitcoin-sv/ubsv/util/usql"
	"github.com/labstack/gommon/random"
	"github.com/ordishs/gocore"
)

type SQLEngine string

const (
	Postgres     SQLEngine = "postgres"
	Sqlite       SQLEngine = "sqlite"
	SqliteMemory SQLEngine = "sqlitememory"
)

func InitSQLDB(logger ulogger.Logger, storeUrl *url.URL) (*usql.DB, error) {
	switch storeUrl.Scheme {
	case "postgres":
		return InitPostgresDB(logger, storeUrl)
	case "sqlite", "sqlitememory":
		return InitSQLiteDB(logger, storeUrl)
	}

	return nil, errors.NewConfigurationError("unknown scheme: %s", storeUrl.Scheme)
}

func InitPostgresDB(logger ulogger.Logger, storeUrl *url.URL) (*usql.DB, error) {
	dbHost := storeUrl.Hostname()
	port := storeUrl.Port()
	dbPort, _ := strconv.Atoi(port)
	dbName := storeUrl.Path[1:]
	dbUser := ""
	dbPassword := ""
	if storeUrl.User != nil {
		dbUser = storeUrl.User.Username()
		dbPassword, _ = storeUrl.User.Password()
	}

	dbInfo := fmt.Sprintf("user=%s password=%s dbname=%s sslmode=disable host=%s port=%d", dbUser, dbPassword, dbName, dbHost, dbPort)

	db, err := usql.Open(storeUrl.Scheme, dbInfo)
	if err != nil {
		return nil, errors.NewServiceError("failed to open postgres DB", err)
	}

	logger.Infof("Using postgres DB: %s@%s:%d/%s", dbUser, dbHost, dbPort, dbName)

	idleConns, _ := gocore.Config().GetInt("utxo_postgresMaxIdleConns", 10)
	db.SetMaxIdleConns(idleConns)
	maxOpenConns, _ := gocore.Config().GetInt("utxo_postgresMaxOpenConns", 80)
	db.SetMaxOpenConns(maxOpenConns)

	return db, nil
}

func InitSQLiteDB(logger ulogger.Logger, storeUrl *url.URL) (*usql.DB, error) {
	var filename string
	var err error

	if storeUrl.Scheme == "sqlitememory" {
		filename = fmt.Sprintf("file:%s?mode=memory&cache=shared", random.String(16))
	} else {
		folder, _ := gocore.Config().Get("dataFolder", "data")
		if err = os.MkdirAll(folder, 0755); err != nil {
			return nil, errors.NewServiceError("failed to create data folder %s", folder, err)
		}

		dbName := storeUrl.Path[1:]
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
