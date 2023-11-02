//go:build manual_tests

package redis

import (
	"context"
	"net/url"
	"testing"

	"github.com/bitcoin-sv/ubsv/stores/txmeta/tests"
	"github.com/stretchr/testify/require"
)

var (
	redisUrl, _ = url.Parse("redis://localhost:6379")
)

func TestRedis(t *testing.T) {
	t.Run("Redis set", func(t *testing.T) {
		db := New(redisUrl)
		err := db.Delete(context.Background(), tests.Tx1.TxIDChainHash())
		require.NoError(t, err)

		tests.Store(t, db)
	})
}

func TestRedisSanity(t *testing.T) {
	db := New(redisUrl)
	tests.Sanity(t, db)
}

func BenchmarkRedis(b *testing.B) {
	db := New(redisUrl)
	tests.Benchmark(b, db)
}
