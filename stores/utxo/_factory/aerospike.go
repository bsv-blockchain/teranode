// //go:build aerospike

package _factory

import (
	"context"
	"net/url"

	"github.com/bitcoin-sv/ubsv/stores/utxo"
	"github.com/bitcoin-sv/ubsv/stores/utxo/aerospike"
	"github.com/bitcoin-sv/ubsv/stores/utxo/aerospike2"
	"github.com/bitcoin-sv/ubsv/ulogger"
)

func init() {
	availableDatabases["aerospike"] = func(_ context.Context, logger ulogger.Logger, url *url.URL) (utxo.Store, error) {
		return aerospike.New(logger, url)
	}

	availableDatabases["aerospike2"] = func(_ context.Context, logger ulogger.Logger, url *url.URL) (utxo.Store, error) {
		return aerospike2.New(logger, url)
	}
}
