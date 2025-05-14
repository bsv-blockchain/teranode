package txmetacache

import (
	"context"
	"fmt"
	"testing"
	"unsafe"

	"github.com/bitcoin-sv/teranode/settings"
	"github.com/bitcoin-sv/teranode/stores/utxo/fields"
	"github.com/bitcoin-sv/teranode/stores/utxo/memory"
	"github.com/bitcoin-sv/teranode/stores/utxo/meta"
	"github.com/bitcoin-sv/teranode/ulogger"
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

		c, _ := NewTxMetaCache(ctx, settings.NewSettings(), ulogger.TestLogger{}, memory.New(ulogger.TestLogger{}), Unallocated)
		_, err := c.GetMeta(ctx, &chainhash.Hash{})
		require.Error(t, err)
	})

	t.Run("test in cache", func(t *testing.T) {
		ctx := context.Background()

		c, err := NewTxMetaCache(ctx, settings.NewSettings(), ulogger.TestLogger{}, memory.New(ulogger.TestLogger{}), Unallocated)
		require.NoError(t, err)

		meta, err := c.Create(ctx, coinbaseTx, 100)
		require.NoError(t, err)

		hash, _ := chainhash.NewHashFromStr("a6fa2d4d23292bef7e13ffbb8c03168c97c457e1681642bf49b3e2ba7d26bb89")
		metaGet, err := c.GetMeta(ctx, hash)
		require.NoError(t, err)

		require.Equal(t, meta, metaGet)
	})

	t.Run("test set cache", func(t *testing.T) {
		ctx := context.Background()

		c, _ := NewTxMetaCache(ctx, settings.NewSettings(), ulogger.TestLogger{}, memory.New(ulogger.TestLogger{}), Unallocated)

		metaData := &meta.Data{
			Fee:            100,
			SizeInBytes:    111,
			ParentTxHashes: []chainhash.Hash{},
			BlockIDs:       make([]uint32, 0),
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
	c, _ := NewTxMetaCache(ctx, settings.NewSettings(), logger, memory.New(logger), Unallocated)
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
	c, _ := NewTxMetaCache(ctx, settings.NewSettings(), logger, memory.New(logger), Unallocated)
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

func TestMap(t *testing.T) {
	m := make(map[chainhash.Hash]*meta.Data)

	hash1, _ := chainhash.NewHashFromStr("000000000000000004c636f1bf72da9bdea11677ea3eefbde93ce0358ef28c30")
	hash2, _ := chainhash.NewHashFromStr("000000000000000004c636f1bf72da9bdea11677ea3eefbde93ce0358ef28c30")

	assert.Equal(t, hash1, hash2)

	m[*hash1] = &meta.Data{}
	m[*hash2] = &meta.Data{}

	assert.Equal(t, 1, len(m))
}

func Test_txMetaCache_HeightEncoding(t *testing.T) {
	t.Run("test height encoding and retrieval", func(t *testing.T) {
		ctx := context.Background()
		utxoStore := memory.New(ulogger.TestLogger{})
		c, _ := NewTxMetaCache(ctx, settings.NewSettings(), ulogger.TestLogger{}, utxoStore, Unallocated)
		cache := c.(*TxMetaCache)

		// Set initial block height
		err := utxoStore.SetBlockHeight(100)
		require.NoError(t, err)

		// Create and set a transaction
		metaData := &meta.Data{
			Fee:            100,
			SizeInBytes:    111,
			ParentTxHashes: []chainhash.Hash{},
			BlockIDs:       make([]uint32, 0),
		}

		hash, _ := chainhash.NewHashFromStr("a6fa2d4d23292bef7e13ffbb8c03168c97c457e1681642bf49b3e2ba7d26bb89")
		err = cache.SetCache(hash, metaData)
		require.NoError(t, err)

		// Verify the height was encoded correctly
		cachedBytes := make([]byte, 0)
		err = cache.cache.Get(&cachedBytes, hash[:])
		require.NoError(t, err)
		require.Greater(t, len(cachedBytes), 4) // Should have at least 4 bytes for height

		// Read back the height
		height := readHeightFromValue(cachedBytes)
		require.Equal(t, uint32(100), height)
	})

	t.Run("test height-based cache invalidation", func(t *testing.T) {
		ctx := context.Background()
		utxoStore := memory.New(ulogger.TestLogger{})
		c, _ := NewTxMetaCache(ctx, settings.NewSettings(), ulogger.TestLogger{}, utxoStore, Unallocated)
		cache := c.(*TxMetaCache)

		// Set initial block height
		err := utxoStore.SetBlockHeight(100)
		require.NoError(t, err)

		// Create and set a transaction
		metaData := &meta.Data{
			Fee:            100,
			SizeInBytes:    111,
			ParentTxHashes: []chainhash.Hash{},
			BlockIDs:       make([]uint32, 0),
		}

		hash, _ := chainhash.NewHashFromStr("a6fa2d4d23292bef7e13ffbb8c03168c97c457e1681642bf49b3e2ba7d26bb89")
		err = cache.SetCache(hash, metaData)
		require.NoError(t, err)

		// Advance block height beyond cache retention period
		err = utxoStore.SetBlockHeight(200)
		require.NoError(t, err)

		// Try to get the transaction - should be nil due to height-based invalidation
		metaGet, err := cache.GetMeta(ctx, hash)
		require.NoError(t, err)
		require.Nil(t, metaGet)
	})

	t.Run("test multiple transactions with different heights", func(t *testing.T) {
		ctx := context.Background()
		utxoStore := memory.New(ulogger.TestLogger{})

		c, err := NewTxMetaCache(ctx, settings.NewSettings(), ulogger.TestLogger{}, utxoStore, Unallocated)
		require.NoError(t, err)

		cache := c.(*TxMetaCache)

		// Set initial block height
		err = utxoStore.SetBlockHeight(100)
		require.NoError(t, err)

		// Create first transaction
		metaData1 := &meta.Data{
			Fee:            100,
			SizeInBytes:    111,
			ParentTxHashes: []chainhash.Hash{},
			BlockIDs:       make([]uint32, 0),
		}

		hash1, err := chainhash.NewHashFromStr("a6fa2d4d23292bef7e13ffbb8c03168c97c457e1681642bf49b3e2ba7d26bb89")
		require.NoError(t, err)
		err = cache.SetCache(hash1, metaData1)
		require.NoError(t, err)

		// verify it is retrievable now
		metaGet1Initial, err := cache.GetMeta(ctx, hash1)
		require.NoError(t, err)
		require.NotNil(t, metaGet1Initial)

		// Advance block height
		err = utxoStore.SetBlockHeight(150)
		require.NoError(t, err)

		// Create second transaction
		metaData2 := &meta.Data{
			Fee:            100,
			SizeInBytes:    111,
			ParentTxHashes: []chainhash.Hash{},
			BlockIDs:       make([]uint32, 0),
		}

		hash2, err := chainhash.NewHashFromStr("b7fa2d4d23292bef7e13ffbb8c03168c97c457e1681642bf49b3e2ba7d26bb90")
		require.NoError(t, err)
		err = cache.SetCache(hash2, metaData2)
		require.NoError(t, err)

		// Verify first transaction is not retrievable
		metaGet1, err := cache.GetMeta(ctx, hash1)
		require.NoError(t, err)
		require.Nil(t, metaGet1)

		// Verify second transaction is retrievable
		metaGet2, err := cache.GetMeta(ctx, hash2)
		require.NoError(t, err)
		require.NotNil(t, metaGet2)
	})

	t.Run("test edge cases", func(t *testing.T) {
		ctx := context.Background()
		utxoStore := memory.New(ulogger.TestLogger{})
		c, _ := NewTxMetaCache(ctx, settings.NewSettings(), ulogger.TestLogger{}, utxoStore, Unallocated)
		cache := c.(*TxMetaCache)

		// Test with zero height
		err := utxoStore.SetBlockHeight(0)
		require.NoError(t, err)

		metaData := &meta.Data{
			Fee:            100,
			SizeInBytes:    111,
			ParentTxHashes: []chainhash.Hash{},
			BlockIDs:       make([]uint32, 0),
		}

		hash, _ := chainhash.NewHashFromStr("a6fa2d4d23292bef7e13ffbb8c03168c97c457e1681642bf49b3e2ba7d26bb89")
		err = cache.SetCache(hash, metaData)
		require.NoError(t, err)

		// Verify height encoding
		cachedBytes := make([]byte, 0)
		err = cache.cache.Get(&cachedBytes, hash[:])
		require.NoError(t, err)

		height := readHeightFromValue(cachedBytes)
		require.Equal(t, uint32(0), height)

		// Test with maximum uint32 height
		err = utxoStore.SetBlockHeight(^uint32(0))
		require.NoError(t, err)

		err = cache.SetCache(hash, metaData)
		require.NoError(t, err)

		err = cache.cache.Get(&cachedBytes, hash[:])
		require.NoError(t, err)

		height = readHeightFromValue(cachedBytes)
		require.Equal(t, ^uint32(0), height)
	})
}

func Test_txMetaCache_GetFunctions(t *testing.T) {
	t.Run("test GetMetaCached with height encoding", func(t *testing.T) {
		ctx := context.Background()
		utxoStore := memory.New(ulogger.TestLogger{})
		c, err := NewTxMetaCache(ctx, settings.NewSettings(), ulogger.TestLogger{}, utxoStore, Unallocated)
		require.NoError(t, err)

		cache := c.(*TxMetaCache)

		// Set initial block height
		err = utxoStore.SetBlockHeight(100)
		require.NoError(t, err)

		// Create and set a transaction
		metaData := &meta.Data{
			Fee:            100,
			SizeInBytes:    111,
			ParentTxHashes: []chainhash.Hash{},
			BlockIDs:       make([]uint32, 0),
		}

		hash, err := chainhash.NewHashFromStr("a6fa2d4d23292bef7e13ffbb8c03168c97c457e1681642bf49b3e2ba7d26bb89")
		require.NoError(t, err)
		err = cache.SetCache(hash, metaData)
		require.NoError(t, err)

		// Test GetMetaCached
		metaGet := cache.GetMetaCached(ctx, hash)
		require.NotNil(t, metaGet)
		require.Equal(t, metaData.Fee, metaGet.Fee)
		require.Equal(t, metaData.SizeInBytes, metaGet.SizeInBytes)

		// Advance block height beyond cache retention period
		err = utxoStore.SetBlockHeight(200)
		require.NoError(t, err)

		// Test GetMetaCached after height advancement
		metaGet = cache.GetMetaCached(ctx, hash)
		require.Nil(t, metaGet)
	})

	t.Run("test Get with height encoding", func(t *testing.T) {
		ctx := context.Background()
		utxoStore := memory.New(ulogger.TestLogger{})

		c, err := NewTxMetaCache(ctx, settings.NewSettings(), ulogger.TestLogger{}, utxoStore, Unallocated)
		require.NoError(t, err)

		cache := c.(*TxMetaCache)

		// Set initial block height
		err = utxoStore.SetBlockHeight(100)
		require.NoError(t, err)

		// Create and set a transaction
		metaData := &meta.Data{
			Fee:            100,
			SizeInBytes:    111,
			ParentTxHashes: []chainhash.Hash{},
			BlockIDs:       make([]uint32, 0),
		}

		hash, err := chainhash.NewHashFromStr("a6fa2d4d23292bef7e13ffbb8c03168c97c457e1681642bf49b3e2ba7d26bb89")
		require.NoError(t, err)
		err = cache.SetCache(hash, metaData)
		require.NoError(t, err)

		// Test Get
		metaGet, err := cache.Get(ctx, hash)
		require.NoError(t, err)
		require.NotNil(t, metaGet)
		require.Equal(t, metaData.Fee, metaGet.Fee)
		require.Equal(t, metaData.SizeInBytes, metaGet.SizeInBytes)

		// Advance block height beyond cache retention period
		err = utxoStore.SetBlockHeight(200)
		require.NoError(t, err)

		// Test Get after height advancement
		metaGet, err = cache.Get(ctx, hash)
		require.NoError(t, err)
		require.Nil(t, metaGet)
	})

	t.Run("test Get with specific fields", func(t *testing.T) {
		ctx := context.Background()
		utxoStore := memory.New(ulogger.TestLogger{})

		c, err := NewTxMetaCache(ctx, settings.NewSettings(), ulogger.TestLogger{}, utxoStore, Unallocated)
		require.NoError(t, err)

		cache := c.(*TxMetaCache)

		// Set initial block height
		err = utxoStore.SetBlockHeight(100)
		require.NoError(t, err)

		// Create and set a transaction with specific fields
		metaData := &meta.Data{
			Fee:            100,
			SizeInBytes:    111,
			ParentTxHashes: []chainhash.Hash{},
			BlockIDs:       []uint32{1, 2, 3},
		}

		hash, err := chainhash.NewHashFromStr("a6fa2d4d23292bef7e13ffbb8c03168c97c457e1681642bf49b3e2ba7d26bb89")
		require.NoError(t, err)
		err = cache.SetCache(hash, metaData)
		require.NoError(t, err)

		// Test Get with specific fields
		metaGet, err := cache.Get(ctx, hash, fields.Fee, fields.SizeInBytes)
		require.NoError(t, err)
		require.NotNil(t, metaGet)
		require.Equal(t, metaData.Fee, metaGet.Fee)
		require.Equal(t, metaData.SizeInBytes, metaGet.SizeInBytes)
		require.Empty(t, metaGet.BlockIDs) // BlockIDs should be empty as not requested
	})

	t.Run("test Get with non-existent hash", func(t *testing.T) {
		ctx := context.Background()
		utxoStore := memory.New(ulogger.TestLogger{})

		c, err := NewTxMetaCache(ctx, settings.NewSettings(), ulogger.TestLogger{}, utxoStore, Unallocated)
		require.NoError(t, err)

		cache := c.(*TxMetaCache)

		// Test Get with non-existent hash
		hash, err := chainhash.NewHashFromStr("a6fa2d4d23292bef7e13ffbb8c03168c97c457e1681642bf49b3e2ba7d26bb89")
		require.NoError(t, err)
		metaGet, err := cache.Get(ctx, hash)
		require.Error(t, err)
		require.Nil(t, metaGet)

		// Test GetMetaCached with non-existent hash
		metaGet = cache.GetMetaCached(ctx, hash)
		require.Nil(t, metaGet)
	})
}

func Test_txMetaCache_MultiOperations(t *testing.T) {
	t.Run("test SetCacheMulti with height encoding", func(t *testing.T) {
		ctx := context.Background()
		utxoStore := memory.New(ulogger.TestLogger{})
		c, err := NewTxMetaCache(ctx, settings.NewSettings(), ulogger.TestLogger{}, utxoStore, Unallocated)
		require.NoError(t, err)

		cache := c.(*TxMetaCache)

		// Set initial block height
		err = utxoStore.SetBlockHeight(100)
		require.NoError(t, err)

		// Create multiple transactions
		metaData1 := &meta.Data{
			Fee:            100,
			SizeInBytes:    111,
			ParentTxHashes: []chainhash.Hash{},
			BlockIDs:       make([]uint32, 0),
		}

		metaData2 := &meta.Data{
			Fee:            200,
			SizeInBytes:    222,
			ParentTxHashes: []chainhash.Hash{},
			BlockIDs:       make([]uint32, 0),
		}

		hash1, err := chainhash.NewHashFromStr("a6fa2d4d23292bef7e13ffbb8c03168c97c457e1681642bf49b3e2ba7d26bb89")
		require.NoError(t, err)
		hash2, err := chainhash.NewHashFromStr("b7fa2d4d23292bef7e13ffbb8c03168c97c457e1681642bf49b3e2ba7d26bb90")
		require.NoError(t, err)

		// Convert to bytes for SetCacheMulti
		metaBytes1 := metaData1.Bytes()
		metaBytes2 := metaData2.Bytes()

		// Set multiple transactions
		err = cache.SetCacheMulti([][]byte{hash1[:], hash2[:]}, [][]byte{metaBytes1, metaBytes2})
		require.NoError(t, err)

		// Verify heights are encoded correctly
		cachedBytes1 := make([]byte, 0)
		cachedBytes2 := make([]byte, 0)

		err = cache.cache.Get(&cachedBytes1, hash1[:])
		require.NoError(t, err)

		err = cache.cache.Get(&cachedBytes2, hash2[:])
		require.NoError(t, err)

		height1 := readHeightFromValue(cachedBytes1)
		require.Equal(t, uint32(100), height1)

		height2 := readHeightFromValue(cachedBytes2)
		require.Equal(t, uint32(100), height2)

		// Verify data can be retrieved
		metaGet1, err := cache.GetMeta(ctx, hash1)
		require.NoError(t, err)
		require.NotNil(t, metaGet1)
		require.Equal(t, metaData1.Fee, metaGet1.Fee)

		metaGet2, err := cache.GetMeta(ctx, hash2)
		require.NoError(t, err)
		require.NotNil(t, metaGet2)
		require.Equal(t, metaData2.Fee, metaGet2.Fee)
	})

	t.Run("test multi operations with height advancement", func(t *testing.T) {
		ctx := context.Background()
		utxoStore := memory.New(ulogger.TestLogger{})

		c, err := NewTxMetaCache(ctx, settings.NewSettings(), ulogger.TestLogger{}, utxoStore, Unallocated)
		require.NoError(t, err)

		cache := c.(*TxMetaCache)

		// Set initial block height
		err = utxoStore.SetBlockHeight(100)
		require.NoError(t, err)

		// Create multiple transactions
		metaData1 := &meta.Data{
			Fee:            100,
			SizeInBytes:    111,
			ParentTxHashes: []chainhash.Hash{},
			BlockIDs:       make([]uint32, 0),
		}

		metaData2 := &meta.Data{
			Fee:            200,
			SizeInBytes:    222,
			ParentTxHashes: []chainhash.Hash{},
			BlockIDs:       make([]uint32, 0),
		}

		hash1, err := chainhash.NewHashFromStr("a6fa2d4d23292bef7e13ffbb8c03168c97c457e1681642bf49b3e2ba7d26bb89")
		require.NoError(t, err)
		hash2, err := chainhash.NewHashFromStr("b7fa2d4d23292bef7e13ffbb8c03168c97c457e1681642bf49b3e2ba7d26bb90")
		require.NoError(t, err)

		// Set first transaction
		err = cache.SetCacheMulti([][]byte{hash1[:]}, [][]byte{metaData1.Bytes()})
		require.NoError(t, err)

		// Advance block height
		err = utxoStore.SetBlockHeight(150)
		require.NoError(t, err)

		// Set second transaction
		err = cache.SetCacheMulti([][]byte{hash2[:]}, [][]byte{metaData2.Bytes()})
		require.NoError(t, err)

		// Verify first transaction is not retrievable (too old)
		metaGet1, err := cache.GetMeta(ctx, hash1)
		require.NoError(t, err)
		require.Nil(t, metaGet1)

		// Verify second transaction is retrievable
		metaGet2, err := cache.GetMeta(ctx, hash2)
		require.NoError(t, err)
		require.NotNil(t, metaGet2)
		require.Equal(t, metaData2.Fee, metaGet2.Fee)
	})

	t.Run("test multi operations with empty data", func(t *testing.T) {
		ctx := context.Background()
		utxoStore := memory.New(ulogger.TestLogger{})

		c, err := NewTxMetaCache(ctx, settings.NewSettings(), ulogger.TestLogger{}, utxoStore, Unallocated)
		require.NoError(t, err)

		cache := c.(*TxMetaCache)

		// Set initial block height
		err = utxoStore.SetBlockHeight(100)
		require.NoError(t, err)

		// Test with empty data
		hash, err := chainhash.NewHashFromStr("a6fa2d4d23292bef7e13ffbb8c03168c97c457e1681642bf49b3e2ba7d26bb89")
		require.NoError(t, err)

		// Set empty data
		err = cache.SetCacheMulti([][]byte{hash[:]}, [][]byte{[]byte{}})
		require.NoError(t, err)

		// Verify height is still encoded
		cachedBytes := make([]byte, 0)
		err = cache.cache.Get(&cachedBytes, hash[:])
		require.NoError(t, err)
		require.Equal(t, 4, len(cachedBytes)) // Should only contain height
		height := readHeightFromValue(cachedBytes)
		require.Equal(t, uint32(100), height)
	})
}
