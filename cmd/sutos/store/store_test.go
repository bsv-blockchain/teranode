//go:build manual_tests

package store

import (
	"context"
	"fmt"
	"testing"

	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// /opt/homebrew/opt/redis/bin/redis-server --port 6379 --save "
// /opt/homebrew/opt/redis/bin/redis-server --port 6380 --save "
// /opt/homebrew/opt/redis/bin/redis-server --port 6381 --save "
// /opt/homebrew/opt/redis/bin/redis-server --port 6382 --save "

func TestRing(t *testing.T) {
	rdb := redis.NewRing(&redis.RingOptions{
		Addrs: map[string]string{
			"shard1": "localhost:6379",
			"shard2": "localhost:6380",
			"shard3": "localhost:6381",
			"shard4": "localhost:6382",
		},
		// NewConsistentHash: func(shards []string) redis.ConsistentHash {
		// 	// https://dgryski.medium.com/consistent-hashing-algorithmic-tradeoffs-ef6b8e2fcae8
		// 	return consistenthash.New(100, crc32.ChecksumIEEE)
		// },
	})

	ctx := context.Background()

	for name, addr := range rdb.Options().Addrs {
		i := redis.NewClient(&redis.Options{
			Addr: addr,
		})
		t.Logf("%s: %v", name, i.DBSize(ctx))
	}

	for i := 0; i < 10_000; i++ {
		k := chainhash.HashH([]byte(fmt.Sprintf("%d", i)))
		v := fmt.Sprintf("VALUE %d", i)

		statusCmd := rdb.Set(ctx, k.String(), v, 0)
		require.NoError(t, statusCmd.Err())
	}

	for name, addr := range rdb.Options().Addrs {
		i := redis.NewClient(&redis.Options{
			Addr: addr,
		})
		t.Logf("%s: %v", name, i.DBSize(ctx))
	}

	for i := 0; i < 10_000; i++ {
		k := chainhash.HashH([]byte(fmt.Sprintf("%d", i)))
		v := fmt.Sprintf("VALUE %d", i)

		statusCmd2 := rdb.Get(ctx, k.String())
		require.NoError(t, statusCmd2.Err())
		assert.Equal(t, v, statusCmd2.Val())
	}
}
