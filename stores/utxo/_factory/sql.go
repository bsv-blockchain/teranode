package _factory

import (
	"context"
	"net/url"

	"github.com/bitcoin-sv/ubsv/stores/utxo"
	"github.com/bitcoin-sv/ubsv/stores/utxo/sql"
	"github.com/bitcoin-sv/ubsv/ulogger"
)

func init() {
	availableDatabases["postgres"] = func(ctx context.Context, logger ulogger.Logger, url *url.URL) (utxo.Store, error) {
		return sql.New(ctx, logger, url)
	}
	availableDatabases["sqlitememory"] = func(ctx context.Context, logger ulogger.Logger, url *url.URL) (utxo.Store, error) {
		return sql.New(ctx, logger, url)
	}
	availableDatabases["sqlite"] = func(ctx context.Context, logger ulogger.Logger, url *url.URL) (utxo.Store, error) {
		return sql.New(ctx, logger, url)
	}
}
