package txmetacache

import (
	"context"
	"fmt"
	"github.com/bitcoin-sv/ubsv/stores/utxo/meta"
	"testing"
	"unsafe"

	"github.com/bitcoin-sv/ubsv/stores/utxo/memory"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/bitcoin-sv/ubsv/util"
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
	// skip due to size requirements of the cache, use cache size / 1024 and number of buckets / 1024 for testing
	util.SkipVeryLongTests(t)

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

		metaData := &meta.Data{
			Fee:            100,
			SizeInBytes:    111,
			ParentTxHashes: []chainhash.Hash{},
		}

		hash, _ := chainhash.NewHashFromStr("a6fa2d4d23292bef7e13ffbb8c03168c97c457e1681642bf49b3e2ba7d26bb89")

		err := c.(*TxMetaCache).SetCache(hash, metaData)
		require.NoError(t, err)

		metaGet, err := c.GetMeta(ctx, hash)
		require.NoError(t, err)

		require.Equal(t, metaData, metaGet)
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
			return cache.SetCache(&hash, &meta.Data{})
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

	meta := &meta.Data{
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
		i := i
		g.Go(func() error {
			data, err := cache.GetMeta(context.Background(), &hash)
			_ = data
			if err != nil {
				b.Fatalf("cache miss, iteration %d: %v", i, err)
			}
			fmt.Println("data size: ", unsafe.Sizeof(data))
			return nil
		})
	}

	err := g.Wait()
	require.NoError(b, err)
}

func Test_txMetaCache_GetMeta_Expiry(t *testing.T) {
	// skip due to size requirements of the cache, use cache size / 1024 and number of buckets / 1024 for testing
	util.SkipVeryLongTests(t)
	ctx := context.Background()
	c := NewTxMetaCache(ctx, ulogger.TestLogger{}, memory.New(ulogger.TestLogger{}), 2048)
	cache := c.(*TxMetaCache)
	var err error

	for i := 0; i < 2_000_000; i++ {
		hash := chainhash.HashH([]byte(string(rune(i))))
		err = cache.SetCache(&hash, &meta.Data{})
		require.NoError(t, err)
	}

	//make sure newly added items are not expired
	hash := chainhash.HashH([]byte(string(rune(999_999_999))))
	_ = cache.SetCache(&hash, &meta.Data{})

	txmetaLatest, err := cache.Get(ctx, &hash)
	assert.NoError(t, err)
	assert.NotNil(t, txmetaLatest)

}

func TestMap(t *testing.T) {
	m := make(map[chainhash.Hash]*meta.Data)

	hash1, _ := chainhash.NewHashFromStr("000000000000000004c636f1bf72da9bdea11677ea3eefbde93ce0358ef28c30")
	hash2, _ := chainhash.NewHashFromStr("000000000000000004c636f1bf72da9bdea11677ea3eefbde93ce0358ef28c30")

	assert.Equal(t, hash1, hash2)

	m[*hash1] = &meta.Data{}
	m[*hash2] = &meta.Data{}

	assert.Equal(t, 1, len(m))
}
