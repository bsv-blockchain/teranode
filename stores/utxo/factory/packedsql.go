package factory

import (
	"context"
	"net/url"

	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/packedsql"
	"github.com/bsv-blockchain/teranode/ulogger"
)

func init() {
	availableDatabases["packedsql"] = func(ctx context.Context, logger ulogger.Logger, tSettings *settings.Settings, u *url.URL) (utxo.Store, error) {
		return packedsql.New(ctx, logger, tSettings, u)
	}
}
