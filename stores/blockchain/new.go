// Package blockchain provides interfaces and implementations for blockchain data storage and retrieval.
package blockchain

import (
	"net/url"

	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/settings"
	"github.com/bitcoin-sv/teranode/stores/blockchain/sql"
	"github.com/bitcoin-sv/teranode/ulogger"
)

// NewStore creates a new blockchain store instance based on the provided URL scheme.
// Supported schemes are: postgres, sqlitememory, and sqlite.
//
// Parameters:
//   - logger: Logger instance for store operations
//   - storeURL: URL containing the store configuration
//   - tSettings: Teranode settings
//
// Returns:
//   - Store: A new store instance
//   - error: Any error encountered during creation
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
