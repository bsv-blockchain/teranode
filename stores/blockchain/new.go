// Package blockchain provides interfaces and implementations for blockchain data storage and retrieval.
// It offers a comprehensive API for storing, retrieving, and managing blockchain data including blocks,
// headers, transactions, and chain state information.
//
// The package is organized around a central Store interface that defines the contract for all
// blockchain storage operations. Multiple implementations of this interface are provided:
// - SQL implementation supporting PostgreSQL and SQLite backends for production and testing
// - Mock implementation for unit testing and development
//
// The blockchain package is designed with the following key features:
// - Thread-safety for concurrent access from multiple goroutines
// - Performance optimizations including caching for frequently accessed data
// - Support for chain reorganizations and fork handling
// - Extensibility through the functional options pattern
// - Comprehensive error handling with domain-specific error types
//
// This package is a critical component of the Teranode architecture, providing the
// persistence layer for blockchain data and supporting the consensus rules and validation
// logic implemented in higher layers of the system.
package blockchain

import (
	"net/url"

	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/settings"
	"github.com/bitcoin-sv/teranode/stores/blockchain/sql"
	"github.com/bitcoin-sv/teranode/ulogger"
)

// NewStore creates a new blockchain store instance based on the provided URL scheme.
// This factory function abstracts the storage backend implementation details from the caller,
// allowing for different storage solutions to be used interchangeably.
//
// Supported schemes are:
// - postgres: PostgreSQL database backend for production deployments
// - sqlitememory: In-memory SQLite database for testing and development
// - sqlite: File-based SQLite database for persistent storage in testing/development
//
// The function uses the URL to determine which backend implementation to instantiate
// and passes the configuration to the appropriate constructor.
//
// Parameters:
//   - logger: Logger instance for store operations and diagnostics
//   - storeURL: URL containing the store configuration (connection parameters, credentials)
//   - tSettings: Teranode settings that control store behavior and performance parameters
//
// Returns:
//   - Store: A new store instance implementing the blockchain.Store interface
//   - error: Any error encountered during creation (connection issues, invalid parameters)
func NewStore(logger ulogger.Logger, storeURL *url.URL, tSettings *settings.Settings) (Store, error) {
	switch storeURL.Scheme {
	case "postgres":
		fallthrough
	case "sqlitememory":
		fallthrough
	case "sqlite":
		return sql.New(logger, storeURL, tSettings)
	}

	return nil, errors.NewStorageError("unknown scheme: %s", storeURL.Scheme)
}
