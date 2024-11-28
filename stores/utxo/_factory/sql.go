package _factory

import (
	"context"
	"net/url"

	"github.com/bitcoin-sv/ubsv/settings"
	"github.com/bitcoin-sv/ubsv/stores/utxo"
	"github.com/bitcoin-sv/ubsv/stores/utxo/sql"
	"github.com/bitcoin-sv/ubsv/ulogger"
)

func init() {
	availableDatabases["postgres"] = func(ctx context.Context, logger ulogger.Logger, tSettings *settings.Settings, url *url.URL) (utxo.Store, error) {
		return sql.New(ctx, logger, tSettings, url)
	}
	availableDatabases["sqlitememory"] = func(ctx context.Context, logger ulogger.Logger, tSettings *settings.Settings, url *url.URL) (utxo.Store, error) {
		return sql.New(ctx, logger, tSettings, url)
	}
	availableDatabases["sqlite"] = func(ctx context.Context, logger ulogger.Logger, tSettings *settings.Settings, url *url.URL) (utxo.Store, error) {
		return sql.New(ctx, logger, settings.NewSettings(), url)
	}
}
