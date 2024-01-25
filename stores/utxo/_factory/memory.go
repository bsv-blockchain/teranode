// //go:build memory

package _factory

import (
	"net/url"

	utxostore "github.com/bitcoin-sv/ubsv/stores/utxo"
	"github.com/bitcoin-sv/ubsv/stores/utxo/memory"
	"github.com/bitcoin-sv/ubsv/ulogger"
)

func init() {
	availableDatabases["memory"] = func(logger ulogger.Logger, url *url.URL) (utxostore.Interface, error) {
		return memory.New(true), nil
	}
}
