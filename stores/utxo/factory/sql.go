// Package factory provides a factory for creating UTXO store implementations.
// It supports multiple database backends through build tags and connection URLs.
//
// # Supported Backends
//
// The following storage backends are available:
//   - Aerospike (build tag: aerospike): "aerospike://host:port/namespace/set"
//   - PostgreSQL: "postgres://user:pass@host:port/dbname"
//   - SQLite: "sqlite://path/to/file.db"
//   - SQLite Memory: "sqlitememory://"
//   - In-Memory (build tag: memory): "memory://" (for testing)
//
// # Usage
//
//	import (
//	    "github.com/bitcoin-sv/ubsv/stores/utxo/factory"
//	    "github.com/bitcoin-sv/ubsv/settings"
//	)
//
//	// Initialize from settings
//	store, err := factory.NewStore(ctx, logger, settings, "service-name")
//	if err != nil {
//	    log.Fatal(err)
//	}
//
//	// Use the store
//	metadata, _, err := store.SpendAndCreate(ctx, tx, blockHeight, utxo.WithCreateOnly())
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
// When enabled, all store operations will be logged with:
//   - Operation name
//   - Parameters
//   - Duration
//   - Error status
package factory

import (
	"context"
	"net/url"

	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/sql"
	"github.com/bsv-blockchain/teranode/ulogger"
)

func init() {
	availableDatabases["postgres"] = func(ctx context.Context, logger ulogger.Logger, tSettings *settings.Settings, url *url.URL) (utxo.Store, error) {
		return sql.New(ctx, logger, tSettings, url)
	}
	availableDatabases["sqlitememory"] = func(ctx context.Context, logger ulogger.Logger, tSettings *settings.Settings, url *url.URL) (utxo.Store, error) {
		return sql.New(ctx, logger, tSettings, url)
	}
	availableDatabases["sqlite"] = func(ctx context.Context, logger ulogger.Logger, tSettings *settings.Settings, url *url.URL) (utxo.Store, error) {
		return sql.New(ctx, logger, tSettings, url)
	}
}
