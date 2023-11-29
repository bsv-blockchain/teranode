package blockvalidation

import (
	"context"
	"testing"

	"github.com/bitcoin-sv/ubsv/stores/txmeta"
	"github.com/bitcoin-sv/ubsv/stores/txmeta/memory"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

func Test_txMetaCache_GetMeta(t *testing.T) {
	t.Run("test empty", func(t *testing.T) {
		ctx := context.Background()

		c := newTxMetaCache(memory.New(ulogger.TestLogger{}))
		_, err := c.GetMeta(ctx, &chainhash.Hash{})
		require.Error(t, err)
	})

	t.Run("test in cache", func(t *testing.T) {
		ctx := context.Background()

		c := newTxMetaCache(memory.New(ulogger.TestLogger{}))

		meta, err := c.Create(ctx, coinbaseTx)
		require.NoError(t, err)

		hash, _ := chainhash.NewHashFromStr("8c14f0db3df150123e6f3dbbf30f8b955a8249b62ac1d1ff16284aefa3d06d87")
		metaGet, err := c.GetMeta(ctx, hash)
		require.NoError(t, err)

		require.Equal(t, meta, metaGet)
	})
}

func Benchmark_txMetaCache_Set(b *testing.B) {
	c := newTxMetaCache(memory.New(ulogger.TestLogger{}))
	cache := c.(*txMetaCache)

	hashes := make([]chainhash.Hash, b.N)
	for i := 0; i < b.N; i++ {
		hashes[i] = chainhash.HashH([]byte(string(rune(i))))
	}

	b.ResetTimer()

	g := errgroup.Group{}
	for i := 0; i < b.N; i++ {
		i := i
		g.Go(func() error {
			return cache.SetCache(&hashes[i], txmeta.Data{})
		})
	}

	err := g.Wait()
	require.NoError(b, err)
}
