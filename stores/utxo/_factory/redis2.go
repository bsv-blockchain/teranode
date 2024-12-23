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
