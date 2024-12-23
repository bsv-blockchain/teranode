package _factory

import (
	"context"
	"net/url"

	"github.com/bitcoin-sv/teranode/settings"
	"github.com/bitcoin-sv/teranode/stores/utxo"
	"github.com/bitcoin-sv/teranode/stores/utxo/redis"
	"github.com/bitcoin-sv/teranode/ulogger"
)

func init() {
	availableDatabases["redis"] = func(ctx context.Context, logger ulogger.Logger, _ *settings.Settings, url *url.URL) (utxo.Store, error) {
		return redis.New(ctx, logger, url)
	}
}
