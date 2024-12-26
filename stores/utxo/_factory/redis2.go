// Package _factory provides a factory for creating UTXO store implementations.
// It supports multiple database backends through build tags and connection URLs.
//
// # Supported Backends
//
// The following storage backends are available:
//   - Aerospike (build tag: aerospike): "aerospike://host:port/namespace/set"
//   - Redis (default): "redis://host:port/db"
//   - Redis2 (optimized): "redis2://host:port/db"
//   - PostgreSQL: "postgres://user:pass@host:port/dbname"
//   - SQLite: "sqlite://path/to/file.db"
//   - SQLite Memory: "sqlitememory://"
//   - In-Memory (build tag: memory): "memory://" (for testing)
//
// # Usage
//
//	import (
//	    "github.com/bitcoin-sv/ubsv/stores/utxo/_factory"
//	    "github.com/bitcoin-sv/ubsv/settings"
//	)
//
//	// Initialize from settings
//	store, err := _factory.NewStore(ctx, logger, settings, "service-name")
//	if err != nil {
//	    log.Fatal(err)
//	}
//
//	// Use the store
//	metadata, err := store.Create(ctx, tx, blockHeight)
//
// # Features
//
// The factory provides:
//   - Automatic database connection management
//   - Optional logging via URL query parameter "logging=true"
//   - Automatic block height updates via blockchain subscription
//   - Graceful shutdown handling
//
// # Configuration
//
// Store configuration is handled through the settings package and connection URLs.
// The URL format depends on the chosen backend. Connection parameters can be
// specified as URL query parameters.
//
// Example URLs:
//
//	postgres://user:pass@localhost:5432/utxo?sslmode=disable&logging=true
//	redis://localhost:6379/0?logging=true
//	aerospike://localhost:3000/test/utxos?logging=true
//
// # Block Height Management
//
// By default, the factory sets up a blockchain subscription to automatically
// update the store's block height and median time. This can be disabled by
// passing false as the startBlockchainListener parameter to NewStore.
//
// # Logging
//
// Logging can be enabled by adding logging=true to the connection URL:
//
//	redis://localhost:6379/0?logging=true
//
// When enabled, all store operations will be logged with:
//   - Operation name
//   - Parameters
//   - Duration
//   - Error status
package _factory

import (
	"context"
	"net/url"

	"github.com/bitcoin-sv/teranode/settings"
	"github.com/bitcoin-sv/teranode/stores/utxo"
	"github.com/bitcoin-sv/teranode/stores/utxo/redis2"
	"github.com/bitcoin-sv/teranode/ulogger"
)

func init() {
	availableDatabases["redis2"] = func(ctx context.Context, logger ulogger.Logger, tSettings *settings.Settings, url *url.URL) (utxo.Store, error) {
		return redis2.New(ctx, logger, url)
	}
}
