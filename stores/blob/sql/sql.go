package sql

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"time"

	"github.com/bitcoin-sv/ubsv/stores/blob/options"
	"github.com/labstack/gommon/random"
	_ "github.com/lib/pq"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/ordishs/gocore"
	_ "modernc.org/sqlite"
)

type SQL struct {
	url *url.URL
	db  *sql.DB
}

func New(storeUrl *url.URL) (*SQL, error) {
	var db *sql.DB
	var err error
	var q string

	switch storeUrl.Scheme {
	case "postgres":
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

		db, err = sql.Open(storeUrl.Scheme, dbInfo)
		if err != nil {
			return nil, fmt.Errorf("failed to open postgres DB: %+v", err)
		}

		q = `CREATE TABLE IF NOT EXISTS blob (
	     key VARCHAR(32) PRIMARY KEY
	    ,value BYTEA NOT NULL
	  );`

	case "sqlite", "sqlitememory":
		var filename string
		var err error

		if storeUrl.Scheme == "sqlitememory" {
			filename = fmt.Sprintf("file:%s?mode=memory&cache=shared", random.String(16))
		} else {
			folder, _ := gocore.Config().Get("dataFolder", "data")
			if err = os.MkdirAll(folder, 0755); err != nil {
				return nil, fmt.Errorf("failed to create data folder %s: %+v", folder, err)
			}

			dbName := storeUrl.Path[1:]
			filename, err = filepath.Abs(path.Join(folder, fmt.Sprintf("%s.db", dbName)))
			if err != nil {
				return nil, fmt.Errorf("failed to get absolute path for sqlite DB: %+v", err)
			}

			// filename = fmt.Sprintf("file:%s?cache=shared&mode=rwc", filename)
			filename = fmt.Sprintf("%s?cache=shared&_pragma=busy_timeout=10000&_pragma=journal_mode=WAL", filename)
		}

		db, err = sql.Open("sqlite", filename)
		if err != nil {
			return nil, fmt.Errorf("failed to open sqlite DB: %+v", err)
		}

		if _, err = db.Exec(`PRAGMA locking_mode = SHARED;`); err != nil {
			_ = db.Close()
			return nil, fmt.Errorf("could not enable shared locking mode: %+v", err)
		}

		q = `CREATE TABLE IF NOT EXISTS blob (
			 key VARCHAR(32) PRIMARY KEY
			,value BLOB NOT NULL
		);`

	default:
		return nil, errors.New("unknown database engine")
	}

	_, err = db.Exec(q)
	if err != nil {
		db.Close()
		return nil, err
	}

	return &SQL{
		url: storeUrl,
		db:  db,
	}, nil
}

func (m *SQL) Health(ctx context.Context) (int, string, error) {
	return 0, fmt.Sprintf("%s Store", m.url.Scheme), nil
}

func (m *SQL) Close(_ context.Context) error {
	// noop
	return nil
}

func (m *SQL) SetFromReader(ctx context.Context, key []byte, reader io.ReadCloser, opts ...options.Options) error {
	defer reader.Close()

	b, err := io.ReadAll(reader)
	if err != nil {
		return fmt.Errorf("failed to read data from reader: %w", err)
	}

	return m.Set(ctx, key, b, opts...)
}

func (m *SQL) Set(_ context.Context, hash []byte, value []byte, opts ...options.Options) error {
	// hash should have been a chainhash.Hash
	key := chainhash.Hash(hash)

	// Upsert the key and value
	_, err := m.db.Exec("INSERT INTO blob (key, value) VALUES ($1, $2) ON CONFLICT (key) DO UPDATE SET value = $2", key.CloneBytes(), value)

	return err
}

func (m *SQL) SetTTL(_ context.Context, hash []byte, ttl time.Duration) error {
	return errors.New("TTL is not supported in a sql store")
}

func (m *SQL) GetIoReader(ctx context.Context, key []byte) (io.ReadCloser, error) {
	b, err := m.Get(ctx, key)
	if err != nil {
		return nil, err
	}

	return options.ReaderWrapper{Reader: bytes.NewBuffer(b), Closer: options.ReaderCloser{}}, nil
}

func (m *SQL) Get(_ context.Context, hash []byte) ([]byte, error) {
	// hash should have been a chainhash.Hash
	key := chainhash.Hash(hash)

	var b []byte
	err := m.db.QueryRow("SELECT value FROM blob WHERE key = $1", key.CloneBytes()).Scan(&b)

	return b, err
}

func (m *SQL) Exists(_ context.Context, hash []byte) (bool, error) {
	// hash should have been a chainhash.Hash
	key := chainhash.Hash(hash)

	var v int
	err := m.db.QueryRow("SELECT 1 FROM blob WHERE key = $1", key.CloneBytes()).Scan(&v)

	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}

	return true, nil
}

func (m *SQL) Del(_ context.Context, hash []byte) error {
	// hash should have been a chainhash.Hash
	key := chainhash.Hash(hash)

	// Delete the key
	_, err := m.db.Exec("DELETE FROM blob WHERE key = $1", key.CloneBytes())

	return err
}
