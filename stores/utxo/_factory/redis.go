package _factory

import (
	"net/url"

	"github.com/bitcoin-sv/ubsv/stores/utxo"
	"github.com/bitcoin-sv/ubsv/stores/utxo/redis"
	"github.com/bitcoin-sv/ubsv/ulogger"
)

func init() {
	availableDatabases["redis"] = func(logger ulogger.Logger, url *url.URL) (utxo.Interface, error) {
		return redis.NewRedisClient(logger, url)
	}

	availableDatabases["redis-cluster"] = func(logger ulogger.Logger, url *url.URL) (utxo.Interface, error) {
		return redis.NewRedisCluster(logger, url)
	}

	availableDatabases["redis-ring"] = func(logger ulogger.Logger, url *url.URL) (utxo.Interface, error) {
		return redis.NewRedisRing(logger, url)
	}
}
