package prereq

import (
	"bytes"
	"database/sql"
	"encoding/hex"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"

	"github.com/bsv-blockchain/go-chaincfg"
	"github.com/bsv-blockchain/teranode/errors"
	_ "github.com/lib/pq"
	_ "modernc.org/sqlite"
)

// ChainCheckResult holds the result of a chain consistency check.
type ChainCheckResult struct {
	OK            bool
	NoDatabase    bool   // true if no blockchain DB exists yet
	ConfiguredNet string // e.g. "regtest"
	StoredNet     string // e.g. "mainnet" - reverse-looked-up, or "unknown"
	StoredHash    string // hex
	ExpectedHash  string // hex
	StoreURL      string // the resolved blockchain store URL
}

var knownNetworks = []string{"mainnet", "testnet", "regtest", "stn", "teratestnet", "tstn"}

// CheckChain verifies the configured network matches the genesis block stored in the blockchain database.
// storeURL is the resolved blockchain_store setting, dataFolder is the resolved dataFolder setting.
func CheckChain(network string, storeURL *url.URL, dataFolder string) *ChainCheckResult {
	result := &ChainCheckResult{
		ConfiguredNet: network,
	}

	// Get expected genesis hash for configured network
	params, err := chaincfg.GetChainParams(network)
	if err != nil {
		result.ExpectedHash = "unknown network: " + network
		return result
	}

	result.ExpectedHash = hex.EncodeToString(params.GenesisHash[:])

	if storeURL == nil {
		result.OK = true
		result.NoDatabase = true
		return result
	}

	result.StoreURL = storeURL.String()

	var hash []byte
	var found bool

	switch storeURL.Scheme {
	case "postgres":
		hash, found = queryPostgresGenesis(storeURL.String())
	case "sqlite":
		dbPath := sqliteDBPath(dataFolder, storeURL)
		if dbPath == "" {
			result.OK = true
			return result
		}

		hash, found = querySQLiteGenesis(dbPath)
	default:
		result.OK = true
		return result
	}

	if !found {
		result.OK = true
		result.NoDatabase = true
		return result
	}

	result.StoredHash = hex.EncodeToString(hash)

	if bytes.Equal(hash, params.GenesisHash[:]) {
		result.OK = true
		return result
	}

	result.StoredNet = identifyNetwork(hash)

	return result
}

func sqliteDBPath(dataFolder string, storeURL *url.URL) string {
	if storeURL.Path == "" || !strings.HasPrefix(storeURL.Path, "/") {
		return ""
	}

	return filepath.Join(dataFolder, storeURL.Path[1:]+".db")
}

func queryPostgresGenesis(connStr string) (hash []byte, found bool) {
	if !bytes.Contains([]byte(connStr), []byte("sslmode")) {
		if bytes.Contains([]byte(connStr), []byte("?")) {
			connStr += "&sslmode=disable"
		} else {
			connStr += "?sslmode=disable"
		}
	}

	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return nil, false
	}
	defer db.Close()

	if err := db.Ping(); err != nil {
		return nil, false
	}

	var h []byte
	err = db.QueryRow("SELECT hash FROM blocks WHERE height = 0").Scan(&h)
	if err != nil {
		return nil, false
	}

	return h, true
}

func querySQLiteGenesis(dbPath string) (hash []byte, found bool) {
	if _, err := os.Stat(dbPath); os.IsNotExist(err) {
		return nil, false
	}

	db, err := sql.Open("sqlite", dbPath+"?mode=ro")
	if err != nil {
		return nil, false
	}
	defer db.Close()

	var h []byte
	err = db.QueryRow("SELECT hash FROM blocks WHERE height = 0").Scan(&h)
	if err != nil {
		return nil, false
	}

	return h, true
}

// identifyNetwork tries to match a genesis hash to a known network name.
func identifyNetwork(hash []byte) string {
	for _, net := range knownNetworks {
		params, err := chaincfg.GetChainParams(net)
		if err != nil {
			continue
		}

		if bytes.Equal(hash, params.GenesisHash[:]) {
			return net
		}
	}

	return "unknown"
}

// DeleteChainData removes the blockchain data from the configured store.
func DeleteChainData(storeURL *url.URL, dataFolder string) error {
	if storeURL != nil {
		switch storeURL.Scheme {
		case "postgres":
			if err := clearPostgresChain(storeURL.String()); err != nil {
				return err
			}
		case "sqlite":
			if dbPath := sqliteDBPath(dataFolder, storeURL); dbPath != "" {
				deleteSQLiteFiles(dbPath)
			}
		}
	}

	// Delete related data directories
	dataDirs := []string{
		filepath.Join(dataFolder, "blockstore"),
		filepath.Join(dataFolder, "subtreestore"),
		filepath.Join(dataFolder, "external"),
	}

	for _, path := range dataDirs {
		if _, err := os.Stat(path); os.IsNotExist(err) {
			continue
		}

		fmt.Printf("  Removing %s\n", path)

		if err := os.RemoveAll(path); err != nil {
			return errors.NewProcessingError("failed to remove %s", path, err)
		}
	}

	return nil
}

func clearPostgresChain(connStr string) error {
	if !bytes.Contains([]byte(connStr), []byte("sslmode")) {
		if bytes.Contains([]byte(connStr), []byte("?")) {
			connStr += "&sslmode=disable"
		} else {
			connStr += "?sslmode=disable"
		}
	}

	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return errors.NewProcessingError("failed to connect to postgres", err)
	}
	defer db.Close()

	if err := db.Ping(); err != nil {
		return errors.NewProcessingError("postgres not reachable", err)
	}

	fmt.Println("  Clearing PostgreSQL blockchain tables...")

	_, _ = db.Exec("TRUNCATE TABLE scheduled_blob_deletions")
	_, _ = db.Exec("TRUNCATE TABLE state")
	_, _ = db.Exec("DELETE FROM blocks")

	fmt.Println("  PostgreSQL tables cleared.")

	return nil
}

func deleteSQLiteFiles(dbPath string) {
	for _, path := range []string{dbPath, dbPath + "-wal", dbPath + "-shm"} {
		if _, err := os.Stat(path); os.IsNotExist(err) {
			continue
		}

		fmt.Printf("  Removing %s\n", path)
		_ = os.Remove(path)
	}
}
