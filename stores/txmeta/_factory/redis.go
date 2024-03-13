package _factory

import (
	"net/url"

	"github.com/bitcoin-sv/ubsv/stores/txmeta"
	"github.com/bitcoin-sv/ubsv/stores/txmeta/redis"
	"github.com/bitcoin-sv/ubsv/ulogger"
)

func init() {
	availableDatabases["redis"] = func(logger ulogger.Logger, url *url.URL) (txmeta.Store, error) {
		return redis.NewRedisClient(logger, url)
	}

	availableDatabases["redis-cluster"] = func(logger ulogger.Logger, url *url.URL) (txmeta.Store, error) {
		return redis.NewRedisCluster(logger, url)
	}

	availableDatabases["redis-ring"] = func(logger ulogger.Logger, url *url.URL) (txmeta.Store, error) {
		return redis.NewRedisRing(logger, url)
	}
}
