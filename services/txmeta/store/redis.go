package store

import (
	"net/url"

	"github.com/bitcoin-sv/ubsv/stores/txmeta"
	"github.com/bitcoin-sv/ubsv/stores/txmeta/redis"
)

func init() {
	availableDatabases["redis"] = func(url *url.URL) (txmeta.Store, error) {
		return redis.NewRedisClient(url)
	}

	availableDatabases["redis-cluster"] = func(url *url.URL) (txmeta.Store, error) {
		return redis.NewRedisCluster(url)
	}

	availableDatabases["redis-ring"] = func(url *url.URL) (txmeta.Store, error) {
		return redis.NewRedisRing(url)
	}
}
