// //go:build aerospike

package _factory

import (
	"context"
	"net/url"

	"github.com/bitcoin-sv/teranode/settings"
	"github.com/bitcoin-sv/teranode/stores/utxo"
	"github.com/bitcoin-sv/teranode/stores/utxo/aerospike"
	"github.com/bitcoin-sv/teranode/ulogger"
)

func init() {
	availableDatabases["aerospike"] = func(ctx context.Context, logger ulogger.Logger, tSettings *settings.Settings, url *url.URL) (utxo.Store, error) {
		return aerospike.New(ctx, logger, tSettings, url)
	}
}
