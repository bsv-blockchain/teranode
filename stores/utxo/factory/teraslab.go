package factory

import (
	"context"
	"net/url"

	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	teraslabstore "github.com/bsv-blockchain/teranode/stores/utxo/teraslab"
	"github.com/bsv-blockchain/teranode/ulogger"
)

func init() {
	availableDatabases["teraslab"] = func(ctx context.Context, logger ulogger.Logger, tSettings *settings.Settings, url *url.URL) (utxo.Store, error) {
		return teraslabstore.New(ctx, logger, tSettings, url)
	}
}
