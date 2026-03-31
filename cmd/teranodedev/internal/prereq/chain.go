package prereq

import (
	"bytes"
	"database/sql"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"

	"github.com/bsv-blockchain/go-chaincfg"
	"github.com/bsv-blockchain/teranode/cmd/teranodedev/internal/config"
	"github.com/bsv-blockchain/teranode/settings"
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
// It reads the blockchain_store setting via gocore to determine the actual configured store.
func CheckChain(projectRoot string, cfg *config.Config) *ChainCheckResult {
	result := &ChainCheckResult{
		ConfiguredNet: cfg.Network,
	}

	// Get expected genesis hash for configured network
	params, err := chaincfg.GetChainParams(cfg.Network)
	if err != nil {
		result.ExpectedHash = "unknown network: " + cfg.Network
		return result
	}

	result.ExpectedHash = hex.EncodeToString(params.GenesisHash[:])

	// Load settings with the developer's context to get the actual blockchain_store URL
	ctx := "dev." + cfg.DevName
	tSettings := settings.NewSettings(ctx)

	storeURL := tSettings.BlockChain.StoreURL
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
		dbPath := filepath.Join(tSettings.DataFolder, storeURL.Path[1:]+".db")
		hash, found = querySQLiteGenesis(dbPath)
	default:
		// Unknown store type (aerospike etc.) - skip check
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

func queryPostgresGenesis(connStr string) (hash []byte, found bool) {
	// Ensure sslmode is set
	if connStr[len(connStr)-1] == '/' {
		connStr += "?sslmode=disable"
	} else if !bytes.Contains([]byte(connStr), []byte("sslmode")) {
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
func DeleteChainData(projectRoot string, cfg *config.Config) error {
	ctx := "dev." + cfg.DevName
	tSettings := settings.NewSettings(ctx)

	storeURL := tSettings.BlockChain.StoreURL
	if storeURL != nil {
		switch storeURL.Scheme {
		case "postgres":
			if err := clearPostgresChain(storeURL.String()); err != nil {
				return err
			}
		case "sqlite":
			dbPath := filepath.Join(tSettings.DataFolder, storeURL.Path[1:]+".db")
			deleteSQLiteFiles(dbPath)
		}
	}

	// Delete related data directories
	dataDir := filepath.Join(projectRoot, "data")
	dataDirs := []string{
		filepath.Join(dataDir, "blockstore"),
		filepath.Join(dataDir, "subtreestore"),
		filepath.Join(dataDir, "external"),
	}

	for _, path := range dataDirs {
		if _, err := os.Stat(path); os.IsNotExist(err) {
			continue
		}

		fmt.Printf("  Removing %s\n", path)

		if err := os.RemoveAll(path); err != nil {
			return fmt.Errorf("failed to remove %s: %w", path, err)
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
		return fmt.Errorf("failed to connect to postgres: %w", err)
	}
	defer db.Close()

	if err := db.Ping(); err != nil {
		return fmt.Errorf("postgres not reachable: %w", err)
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
