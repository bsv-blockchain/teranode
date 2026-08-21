package factory

import (
	"context"
	"net/url"

	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	pebblestore "github.com/bsv-blockchain/teranode/stores/utxo/pebble"
	"github.com/bsv-blockchain/teranode/ulogger"
)

func init() {
	availableDatabases["pebble"] = func(ctx context.Context, logger ulogger.Logger, tSettings *settings.Settings, u *url.URL) (utxo.Store, error) {
		return pebblestore.New(ctx, logger, tSettings, u)
	}
}
