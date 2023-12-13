package txmetacache

import (
	"context"
	"testing"

	"github.com/bitcoin-sv/ubsv/stores/txmeta"
	"github.com/bitcoin-sv/ubsv/stores/txmeta/memory"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

var (
	coinbaseTx, _ = bt.NewTxFromString("01000000010000000000000000000000000000000000000000000000000000000000000000ffffffff1a03a403002f746572616e6f64652f9f9fba46d5a08a6be11ddb2dffffffff0a0065cd1d000000001976a914d1a5c9ee12cade94281609fc8f96bbc95db6335488ac0065cd1d000000001976a914d1a5c9ee12cade94281609fc8f96bbc95db6335488ac0065cd1d000000001976a914d1a5c9ee12cade94281609fc8f96bbc95db6335488ac0065cd1d000000001976a914d1a5c9ee12cade94281609fc8f96bbc95db6335488ac0065cd1d000000001976a914d1a5c9ee12cade94281609fc8f96bbc95db6335488ac0065cd1d000000001976a914d1a5c9ee12cade94281609fc8f96bbc95db6335488ac0065cd1d000000001976a914d1a5c9ee12cade94281609fc8f96bbc95db6335488ac0065cd1d000000001976a914d1a5c9ee12cade94281609fc8f96bbc95db6335488ac0065cd1d000000001976a914d1a5c9ee12cade94281609fc8f96bbc95db6335488ac0065cd1d000000001976a914d1a5c9ee12cade94281609fc8f96bbc95db6335488ac00000000")
)

func Test_txMetaCache_GetMeta(t *testing.T) {
	t.Run("test empty", func(t *testing.T) {
		ctx := context.Background()

		c := NewTxMetaCache(ulogger.TestLogger{}, memory.New(ulogger.TestLogger{}))
		_, err := c.GetMeta(ctx, &chainhash.Hash{})
		require.Error(t, err)
	})

	t.Run("test in cache", func(t *testing.T) {
		ctx := context.Background()

		c := NewTxMetaCache(ulogger.TestLogger{}, memory.New(ulogger.TestLogger{}))

		meta, err := c.Create(ctx, coinbaseTx)
		require.NoError(t, err)

		hash, _ := chainhash.NewHashFromStr("a6fa2d4d23292bef7e13ffbb8c03168c97c457e1681642bf49b3e2ba7d26bb89")
		metaGet, err := c.GetMeta(ctx, hash)
		require.NoError(t, err)

		require.Equal(t, meta, metaGet)
	})
}

func Benchmark_txMetaCache_Set(b *testing.B) {
	c := NewTxMetaCache(ulogger.TestLogger{}, memory.New(ulogger.TestLogger{}))
	cache := c.(*TxMetaCache)

	hashes := make([]chainhash.Hash, b.N)
	for i := 0; i < b.N; i++ {
		hashes[i] = chainhash.HashH([]byte(string(rune(i))))
	}

	b.ResetTimer()

	g := errgroup.Group{}
	for i := 0; i < b.N; i++ {
		i := i
		g.Go(func() error {
			return cache.SetCache(&hashes[i], &txmeta.Data{})
		})
	}

	err := g.Wait()
	require.NoError(b, err)
}
