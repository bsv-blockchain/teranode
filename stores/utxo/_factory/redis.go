package _factory

import (
	"net/url"

	"github.com/bitcoin-sv/ubsv/stores/utxo"
	"github.com/bitcoin-sv/ubsv/stores/utxo/redis"
)

func init() {
	availableDatabases["redis"] = func(url *url.URL) (utxo.Interface, error) {
		return redis.NewRedisClient(url)
	}

	availableDatabases["redis-cluster"] = func(url *url.URL) (utxo.Interface, error) {
		return redis.NewRedisCluster(url)
	}

	availableDatabases["redis-ring"] = func(url *url.URL) (utxo.Interface, error) {
		return redis.NewRedisRing(url)
	}
}
