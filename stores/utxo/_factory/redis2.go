package _factory

import (
	"context"
	"net/url"

	"github.com/bitcoin-sv/ubsv/stores/utxo"
	"github.com/bitcoin-sv/ubsv/stores/utxo/redis2"
	"github.com/bitcoin-sv/ubsv/ulogger"
)

func init() {
	availableDatabases["redis2"] = func(ctx context.Context, logger ulogger.Logger, url *url.URL) (utxo.Store, error) {
		return redis2.New(ctx, logger, url)
	}
}
