// //go:build aerospike

package _factory

import (
	"context"
	"net/url"

	"github.com/bitcoin-sv/ubsv/settings"
	"github.com/bitcoin-sv/ubsv/stores/utxo"
	"github.com/bitcoin-sv/ubsv/stores/utxo/aerospike"
	"github.com/bitcoin-sv/ubsv/ulogger"
)

func init() {
	availableDatabases["aerospike"] = func(ctx context.Context, logger ulogger.Logger, tSettings *settings.Settings, url *url.URL) (utxo.Store, error) {
		return aerospike.New(ctx, logger, tSettings, url)
	}
}
