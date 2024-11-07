package _factory

import (
	"context"
	"net/url"

	"github.com/bitcoin-sv/ubsv/stores/utxo"
	"github.com/bitcoin-sv/ubsv/stores/utxo/redis"
	"github.com/bitcoin-sv/ubsv/ulogger"
)

func init() {
	availableDatabases["redis"] = func(ctx context.Context, logger ulogger.Logger, url *url.URL) (utxo.Store, error) {
		return redis.New(ctx, logger, url)
	}
}
