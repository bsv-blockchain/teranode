package factory

import (
	"context"
	"net/url"

	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/postgres"
	"github.com/bsv-blockchain/teranode/ulogger"
)

func init() {
	availableDatabases["postgres"] = func(ctx context.Context, logger ulogger.Logger, tSettings *settings.Settings, storeURL *url.URL) (utxo.Store, error) {
		store, err := postgres.New(ctx, logger, tSettings, storeURL)
		if err != nil {
			return nil, err
		}
		store.Start(ctx)
		return store, nil
	}
}
