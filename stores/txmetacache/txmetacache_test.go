package txmetacache

import (
	"context"
	"testing"

	"github.com/bitcoin-sv/ubsv/stores/txmeta"
	"github.com/bitcoin-sv/ubsv/stores/txmeta/memory"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

var (
	coinbaseTx, _ = bt.NewTxFromString("01000000010000000000000000000000000000000000000000000000000000000000000000ffffffff1a03a403002f746572616e6f64652f9f9fba46d5a08a6be11ddb2dffffffff0a0065cd1d000000001976a914d1a5c9ee12cade94281609fc8f96bbc95db6335488ac0065cd1d000000001976a914d1a5c9ee12cade94281609fc8f96bbc95db6335488ac0065cd1d000000001976a914d1a5c9ee12cade94281609fc8f96bbc95db6335488ac0065cd1d000000001976a914d1a5c9ee12cade94281609fc8f96bbc95db6335488ac0065cd1d000000001976a914d1a5c9ee12cade94281609fc8f96bbc95db6335488ac0065cd1d000000001976a914d1a5c9ee12cade94281609fc8f96bbc95db6335488ac0065cd1d000000001976a914d1a5c9ee12cade94281609fc8f96bbc95db6335488ac0065cd1d000000001976a914d1a5c9ee12cade94281609fc8f96bbc95db6335488ac0065cd1d000000001976a914d1a5c9ee12cade94281609fc8f96bbc95db6335488ac0065cd1d000000001976a914d1a5c9ee12cade94281609fc8f96bbc95db6335488ac00000000")
)

func Test_txMetaCache_GetMeta(t *testing.T) {
	t.Run("test empty", func(t *testing.T) {
		ctx := context.Background()

		c := NewTxMetaCache(ctx, ulogger.TestLogger{}, memory.New(ulogger.TestLogger{}))
		_, err := c.GetMeta(ctx, &chainhash.Hash{})
		require.Error(t, err)
	})

	t.Run("test in cache", func(t *testing.T) {
		ctx := context.Background()

		c := NewTxMetaCache(ctx, ulogger.TestLogger{}, memory.New(ulogger.TestLogger{}))

		meta, err := c.Create(ctx, coinbaseTx)
		require.NoError(t, err)

		hash, _ := chainhash.NewHashFromStr("a6fa2d4d23292bef7e13ffbb8c03168c97c457e1681642bf49b3e2ba7d26bb89")
		metaGet, err := c.GetMeta(ctx, hash)
		require.NoError(t, err)

		require.Equal(t, meta, metaGet)
	})

	t.Run("test set cache", func(t *testing.T) {
		ctx := context.Background()

		c := NewTxMetaCache(ctx, ulogger.TestLogger{}, memory.New(ulogger.TestLogger{}))

		meta := &txmeta.Data{
			Fee:            100,
			SizeInBytes:    111,
			ParentTxHashes: []chainhash.Hash{},
		}

		hash, _ := chainhash.NewHashFromStr("a6fa2d4d23292bef7e13ffbb8c03168c97c457e1681642bf49b3e2ba7d26bb89")

		err := c.(*TxMetaCache).SetCache(hash, meta)
		require.NoError(t, err)

		metaGet, err := c.GetMeta(ctx, hash)
		require.NoError(t, err)

		require.Equal(t, meta, metaGet)
	})
}

func Benchmark_txMetaCache_Set(b *testing.B) {
	ctx := context.Background()
	logger := ulogger.TestLogger{}
	c := NewTxMetaCache(ctx, logger, memory.New(logger))
	cache := c.(*TxMetaCache)

	// Pre-generate all hashes
	hashes := make([]chainhash.Hash, b.N)
	for i := 0; i < b.N; i++ {
		hashes[i] = chainhash.HashH([]byte(string(rune(i))))
	}

	b.ResetTimer()

	g := new(errgroup.Group)
	for i := 0; i < b.N; i++ {
		hash := hashes[i]
		g.Go(func() error {
			return cache.SetCache(&hash, &txmeta.Data{})
		})
	}

	err := g.Wait()
	require.NoError(b, err)
}

func Benchmark_txMetaCache_Get(b *testing.B) {
	ctx := context.Background()
	logger := ulogger.TestLogger{}
	c := NewTxMetaCache(ctx, logger, memory.New(logger))
	cache := c.(*TxMetaCache)

	meta := &txmeta.Data{
		Fee:            100,
		SizeInBytes:    111,
		ParentTxHashes: []chainhash.Hash{},
	}

	iterationCount := 50_000

	// Pre-generate and pre-populate the cache
	hashes := make([]chainhash.Hash, iterationCount)
	for i := 0; i < iterationCount; i++ {
		hash := chainhash.HashH([]byte(string(rune(i))))
		hashes[i] = hash
		if err := cache.SetCache(&hash, meta); err != nil {
			b.Fatalf("pre-population of cache failed: %v", err)
		}
	}

	b.ResetTimer()

	g := new(errgroup.Group)
	for i := 0; i < iterationCount; i++ {
		hash := hashes[i]
		g.Go(func() error {
			_, err := cache.GetMeta(context.Background(), &hash) //GetCache(&hash)
			//require.True(b, found, "cache miss")
			if err != nil {
				b.Fatalf("cache miss")
			}
			return nil
		})
	}

	err := g.Wait()
	require.NoError(b, err)
}

func Test_txMetaCache_GetMeta_Expiry(t *testing.T) {
	ctx := context.Background()
	c := NewTxMetaCache(ctx, ulogger.TestLogger{}, memory.New(ulogger.TestLogger{}), 32)
	cache := c.(*TxMetaCache)

	for i := 0; i < 6000000; i++ {
		hash := chainhash.HashH([]byte(string(rune(i))))
		_ = cache.SetCache(&hash, &txmeta.Data{})
	}

	//assert.Equal(t, 32*1024*1024, cache.BytesSize(), "map should not have exceeded max size")

	//make sure newly added items are not expired
	hash := chainhash.HashH([]byte(string(rune(999_999_999))))
	_ = cache.SetCache(&hash, &txmeta.Data{})

	txmetaLatest, err := cache.Get(ctx, &hash)
	assert.NoError(t, err)
	assert.NotNil(t, txmetaLatest)

}
