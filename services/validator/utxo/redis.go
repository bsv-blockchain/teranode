package utxo

import (
	"net/url"

	utxostore "github.com/bitcoin-sv/ubsv/stores/utxo"
	"github.com/bitcoin-sv/ubsv/stores/utxo/redis"
)

func init() {
	availableDatabases["redis"] = func(url *url.URL) (utxostore.Interface, error) {
		return redis.NewRedis(url)
	}
}
