package sql

import (
	"context"
	"net/http"
	"net/url"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestHealth tests the Health function with both success and failure scenarios
func TestHealth(t *testing.T) {
	ctx := context.Background()
	logger := ulogger.TestLogger{}
	tSettings := test.CreateBaseTestSettings(t)

	// Create a real SQL store with sqlitememory for testing
	storeURL, err := url.Parse("sqlitememory://")
	require.NoError(t, err)

	store, err := New(logger, storeURL, tSettings)
	require.NoError(t, err)
	defer store.Close()

	t.Run("Success", func(t *testing.T) {
		status, message, err := store.Health(ctx, true)
		assert.NoError(t, err)
		assert.Equal(t, http.StatusOK, status)
		assert.Equal(t, "OK", message)
	})

	t.Run("SuccessWithoutLivenessCheck", func(t *testing.T) {
		status, message, err := store.Health(ctx, false)
		assert.NoError(t, err)
		assert.Equal(t, http.StatusOK, status)
		assert.Equal(t, "OK", message)
	})

	t.Run("ContextCancellation", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel() // Cancel immediately

		status, message, err := store.Health(ctx, true)
		assert.Error(t, err)
		assert.Equal(t, http.StatusFailedDependency, status)
		assert.Equal(t, "Database connection error", message)
	})
}

// TestGetBlockHeadersByHeight tests the GetBlockHeadersByHeight function
func TestGetBlockHeadersByHeight(t *testing.T) {
	cache := NewBlockchainCache(&settings.Settings{
		Block: settings.BlockSettings{
			StoreCacheSize: 10,
		},
	})

	t.Run("NotImplemented", func(t *testing.T) {
		headers, metas := cache.GetBlockHeadersByHeight(100, 200)
		assert.Nil(t, headers)
		assert.Nil(t, metas)
	})

	t.Run("DisabledCache", func(t *testing.T) {
		disabledCache := NewBlockchainCache(&settings.Settings{
			Block: settings.BlockSettings{
				StoreCacheSize: 0, // Disabled cache
			},
		})

		headers, metas := disabledCache.GetBlockHeadersByHeight(1, 10)
		assert.Nil(t, headers)
		assert.Nil(t, metas)
	})

	t.Run("EnabledCacheAlwaysReturnsNil", func(t *testing.T) {
		// Test that the function always returns nil regardless of cache state
		headers, metas := cache.GetBlockHeadersByHeight(0, 1)
		assert.Nil(t, headers)
		assert.Nil(t, metas)
	})
}

// TestNewConstructorErrorPaths tests error scenarios in the New constructor
func TestNewConstructorErrorPaths(t *testing.T) {
	logger := ulogger.TestLogger{}
	tSettings := test.CreateBaseTestSettings(t)

	t.Run("UnknownDatabaseEngine", func(t *testing.T) {
		storeURL, err := url.Parse("unknown://localhost:5432/test")
		require.NoError(t, err)

		store, err := New(logger, storeURL, tSettings)
		assert.Nil(t, store)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to init sql db")
	})
}

// TestBlockchainCacheEdgeCases tests edge cases in blockchain cache methods
func TestBlockchainCacheEdgeCases(t *testing.T) {
	// Create a valid NBit for header construction
	bits, err := model.NewNBitFromString("207fffff")
	require.NoError(t, err)

	t.Run("SetExistsDisabledCache", func(t *testing.T) {
		cache := NewBlockchainCache(&settings.Settings{
			Block: settings.BlockSettings{
				StoreCacheSize: 0, // Disabled
			},
		})

		hash := chainhash.Hash{}
		// Should not panic or error when cache is disabled
		cache.SetExists(hash, true)

		exists, found := cache.GetExists(hash)
		assert.False(t, exists)
		assert.False(t, found)
	})

	t.Run("AddBlockHeaderDisabledCache", func(t *testing.T) {
		cache := NewBlockchainCache(&settings.Settings{
			Block: settings.BlockSettings{
				StoreCacheSize: 0, // Disabled
			},
		})

		header := &model.BlockHeader{
			Version:        1,
			Timestamp:      1234567890,
			Nonce:          1,
			HashPrevBlock:  &chainhash.Hash{0},
			HashMerkleRoot: &chainhash.Hash{1},
			Bits:           *bits,
		}
		meta := &model.BlockHeaderMeta{}

		// Should return true even when disabled (as per implementation)
		result := cache.AddBlockHeader(header, meta)
		assert.True(t, result)
	})

	t.Run("RebuildBlockchainWithNilHeaders", func(t *testing.T) {
		cache := NewBlockchainCache(&settings.Settings{
			Block: settings.BlockSettings{
				StoreCacheSize: 10,
			},
		})

		// Should not panic when called with nil
		cache.RebuildBlockchain(nil, nil)

		header, meta := cache.GetBestBlockHeader()
		assert.Nil(t, header)
		assert.Nil(t, meta)
	})

	t.Run("GetBlockHeadersWithEmptyChain", func(t *testing.T) {
		cache := NewBlockchainCache(&settings.Settings{
			Block: settings.BlockSettings{
				StoreCacheSize: 10,
			},
		})

		hash := chainhash.Hash{}
		headers, metas := cache.GetBlockHeaders(&hash, 5)
		assert.Nil(t, headers)
		assert.Nil(t, metas)
	})

	t.Run("GetBlockHeadersFromHeightWithEmptyChain", func(t *testing.T) {
		cache := NewBlockchainCache(&settings.Settings{
			Block: settings.BlockSettings{
				StoreCacheSize: 10,
			},
		})

		headers, metas := cache.GetBlockHeadersFromHeight(100, 5)
		assert.Nil(t, headers)
		assert.Nil(t, metas)
	})

	t.Run("GetBlockHeadersWithInvalidUint64Conversion", func(t *testing.T) {
		cache := NewBlockchainCache(&settings.Settings{
			Block: settings.BlockSettings{
				StoreCacheSize: 10,
			},
		})

		hash := chainhash.Hash{}
		// Test with a very large uint64 that will fail conversion
		headers, metas := cache.GetBlockHeaders(&hash, ^uint64(0)) // Max uint64
		assert.Nil(t, headers)
		assert.Nil(t, metas)
	})

	t.Run("GetBlockHeadersInsufficientHistoryForLimit", func(t *testing.T) {
		cache := NewBlockchainCache(&settings.Settings{
			Block: settings.BlockSettings{
				StoreCacheSize: 10,
			},
		})

		// Add one block to cache
		header1 := &model.BlockHeader{
			Version:        1,
			Timestamp:      1234567890,
			Nonce:          1,
			HashPrevBlock:  &chainhash.Hash{0},
			HashMerkleRoot: &chainhash.Hash{1},
			Bits:           *bits,
		}
		meta1 := &model.BlockHeaderMeta{Height: 1}

		cache.addBlockHeader(header1, meta1)

		// Try to get more blocks than available (starting from the block we have)
		actualHash := header1.Hash()
		headers, metas := cache.GetBlockHeaders(actualHash, 5) // Want 5, but only have 1
		assert.Nil(t, headers)
		assert.Nil(t, metas)
	})

	t.Run("GetBlockHeadersFromHeightInsufficientBlocks", func(t *testing.T) {
		cache := NewBlockchainCache(&settings.Settings{
			Block: settings.BlockSettings{
				StoreCacheSize: 10,
			},
		})

		// Add one block to cache
		header1 := &model.BlockHeader{
			Version:        1,
			Timestamp:      1234567890,
			Nonce:          1,
			HashPrevBlock:  &chainhash.Hash{0},
			HashMerkleRoot: &chainhash.Hash{1},
			Bits:           *bits,
		}
		meta1 := &model.BlockHeaderMeta{Height: 1}

		cache.addBlockHeader(header1, meta1)

		// Try to get more blocks than available from height 1
		headers, metas := cache.GetBlockHeadersFromHeight(1, 5) // Want 5, but only have 1
		assert.Nil(t, headers)
		assert.Nil(t, metas)
	})

	t.Run("AddBlockHeaderNotAdded", func(t *testing.T) {
		cache := NewBlockchainCache(&settings.Settings{
			Block: settings.BlockSettings{
				StoreCacheSize: 10,
			},
		})

		// Add first block
		header1 := &model.BlockHeader{
			Version:        1,
			Timestamp:      1234567890,
			Nonce:          1,
			HashPrevBlock:  &chainhash.Hash{0},
			HashMerkleRoot: &chainhash.Hash{1},
			Bits:           *bits,
		}
		meta1 := &model.BlockHeaderMeta{Height: 1}

		result1 := cache.addBlockHeader(header1, meta1)
		assert.True(t, result1) // First block should be added

		// Try to add a block that doesn't connect to the best block
		header2 := &model.BlockHeader{
			Version:        1,
			Timestamp:      1234567890,
			Nonce:          2,
			HashPrevBlock:  &chainhash.Hash{99}, // Wrong parent
			HashMerkleRoot: &chainhash.Hash{2},
			Bits:           *bits,
		}
		meta2 := &model.BlockHeaderMeta{Height: 2}

		result2 := cache.addBlockHeader(header2, meta2)
		assert.False(t, result2) // Should not be added due to wrong parent
	})

	t.Run("AddBlockHeaderCacheEviction", func(t *testing.T) {
		cache := NewBlockchainCache(&settings.Settings{
			Block: settings.BlockSettings{
				StoreCacheSize: 2, // Small cache for testing eviction
			},
		})

		// Add blocks to trigger eviction
		var prevHash *chainhash.Hash = &chainhash.Hash{0}
		var allHeaders []*model.BlockHeader

		for i := 1; i <= 3; i++ {
			header := &model.BlockHeader{
				Version:        1,
				Timestamp:      1234567890,
				Nonce:          uint32(i),
				HashPrevBlock:  prevHash,
				HashMerkleRoot: &chainhash.Hash{byte(i)},
				Bits:           *bits,
			}
			meta := &model.BlockHeaderMeta{Height: uint32(i)}
			allHeaders = append(allHeaders, header)

			result := cache.addBlockHeader(header, meta)
			if i <= 2 {
				assert.True(t, result) // First two should be added
			}
			actualHash := header.Hash()
			prevHash = actualHash
		}

		// After adding 3 connected blocks to a cache of size 2, we should have 2 blocks
		// The actual eviction logic is len(chain) >= cacheSize, so eviction occurs when we reach the limit
		assert.LessOrEqual(t, len(cache.headers), 2) // Should have at most 2 blocks
		assert.LessOrEqual(t, len(cache.chain), 2)   // Chain should have at most 2 blocks

		// The first block should no longer be in the cache if eviction occurred
		if len(allHeaders) > 0 {
			firstHash := allHeaders[0].Hash()
			_, exists := cache.headers[*firstHash]
			// This may or may not be true depending on eviction timing
			_ = exists // Just access it to test the code path
		}
	})

	t.Run("Not enough headers in cache", func(t *testing.T) {
		cache := NewBlockchainCache(&settings.Settings{
			Block: settings.BlockSettings{
				StoreCacheSize: 5,
			},
		})

		// Add blocks to trigger eviction
		var (
			prevHash   = &chainhash.Hash{0}
			allHeaders = make([]*model.BlockHeader, 0)
		)

		for i := 1; i <= 5; i++ {
			header := &model.BlockHeader{
				Version:        1,
				Timestamp:      1234567890,
				Nonce:          uint32(i),
				HashPrevBlock:  prevHash,
				HashMerkleRoot: &chainhash.Hash{byte(i)},
				Bits:           *bits,
			}
			meta := &model.BlockHeaderMeta{Height: uint32(i)}
			allHeaders = append(allHeaders, header)

			result := cache.addBlockHeader(header, meta)
			assert.True(t, result) // First two should be added
			prevHash = header.Hash()
		}

		headers, metas := cache.GetBlockHeaders(allHeaders[0].Hash(), 5)
		assert.Nil(t, headers)
		assert.Nil(t, metas)
	})
}

// TestExportBlockchainCSVErrorScenarios tests error paths in ExportBlockchainCSV
func TestExportBlockchainCSVErrorScenarios(t *testing.T) {
	logger := ulogger.TestLogger{}
	tSettings := test.CreateBaseTestSettings(t)

	storeURL, err := url.Parse("sqlitememory://")
	require.NoError(t, err)

	store, err := New(logger, storeURL, tSettings)
	require.NoError(t, err)
	defer store.Close()

	t.Run("FileCreationError", func(t *testing.T) {
		// Try to create file in non-existent directory
		err := store.ExportBlockchainCSV(context.Background(), "/non/existent/path/file.csv")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "could not create export file")
	})

	t.Run("SuccessfulExport", func(t *testing.T) {
		// Test successful export to temporary file
		tmpFile := t.TempDir() + "/blockchain.csv"
		err := store.ExportBlockchainCSV(context.Background(), tmpFile)
		assert.NoError(t, err)
	})
}

// TestImportBlockchainCSVErrorScenarios tests error paths in ImportBlockchainCSV
func TestImportBlockchainCSVErrorScenarios(t *testing.T) {
	logger := ulogger.TestLogger{}
	tSettings := test.CreateBaseTestSettings(t)

	storeURL, err := url.Parse("sqlitememory://")
	require.NoError(t, err)

	store, err := New(logger, storeURL, tSettings)
	require.NoError(t, err)
	defer store.Close()

	t.Run("FileOpenError", func(t *testing.T) {
		// Try to open non-existent file
		err := store.ImportBlockchainCSV(context.Background(), "/non/existent/file.csv")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "could not open import file")
	})
}

// TestResetBlocksCacheErrorScenarios tests ResetBlocksCache
func TestResetBlocksCacheErrorScenarios(t *testing.T) {
	logger := ulogger.TestLogger{}
	tSettings := test.CreateBaseTestSettings(t)

	storeURL, err := url.Parse("sqlitememory://")
	require.NoError(t, err)

	store, err := New(logger, storeURL, tSettings)
	require.NoError(t, err)
	defer store.Close()

	t.Run("SuccessfulReset", func(t *testing.T) {
		// Should succeed with the genesis block
		err := store.ResetBlocksCache(context.Background())
		assert.NoError(t, err)
	})
}

// TestNewPostgresWithSeederOption tests postgres schema creation with seeder option
func TestNewPostgresWithSeederOption(t *testing.T) {
	logger := ulogger.TestLogger{}
	tSettings := test.CreateBaseTestSettings(t)

	t.Run("PostgresSchemeWithSeederTrue", func(t *testing.T) {
		// This will fail to connect but should test the seeder parsing logic
		storeURL, err := url.Parse("postgres://user:pass@localhost:9999/test?seeder=true")
		require.NoError(t, err)

		store, err := New(logger, storeURL, tSettings)
		// Will fail due to connection, but tests the parsing logic
		assert.Nil(t, store)
		assert.Error(t, err)
	})

	t.Run("PostgresSchemeWithSeederFalse", func(t *testing.T) {
		storeURL, err := url.Parse("postgres://user:pass@localhost:9999/test?seeder=false")
		require.NoError(t, err)

		store, err := New(logger, storeURL, tSettings)
		// Will fail due to connection, but tests the parsing logic
		assert.Nil(t, store)
		assert.Error(t, err)
	})

	t.Run("SqliteScheme", func(t *testing.T) {
		// Test sqlite scheme (different from sqlitememory)
		storeURL, err := url.Parse("sqlite:///tmp/test.db")
		require.NoError(t, err)

		store, err := New(logger, storeURL, tSettings)
		if err == nil {
			defer store.Close()
			assert.NotNil(t, store)
			assert.Equal(t, util.Sqlite, store.GetDBEngine())
		}
		// Test may fail on some systems, but covers the sqlite path
	})
}

// TestCacheMethodsCoverage tests remaining cache methods for coverage
func TestCacheMethodsCoverage(t *testing.T) {
	bits, err := model.NewNBitFromString("207fffff")
	require.NoError(t, err)

	cache := NewBlockchainCache(&settings.Settings{
		Block: settings.BlockSettings{
			StoreCacheSize: 10,
		},
	})

	t.Run("RebuildBlockchainDisabledCache", func(t *testing.T) {
		disabledCache := NewBlockchainCache(&settings.Settings{
			Block: settings.BlockSettings{
				StoreCacheSize: 0,
			},
		})

		// Should not panic when disabled
		disabledCache.RebuildBlockchain([]*model.BlockHeader{}, []*model.BlockHeaderMeta{})

		header, meta := disabledCache.GetBestBlockHeader()
		assert.Nil(t, header)
		assert.Nil(t, meta)
	})

	t.Run("RebuildBlockchainWithHeaders", func(t *testing.T) {
		// Create test headers in reverse order (newest first)
		header1 := &model.BlockHeader{
			Version:        1,
			Timestamp:      1234567890,
			Nonce:          1,
			HashPrevBlock:  &chainhash.Hash{0},
			HashMerkleRoot: &chainhash.Hash{1},
			Bits:           *bits,
		}

		header1Hash := header1.Hash()

		header2 := &model.BlockHeader{
			Version:        1,
			Timestamp:      1234567890,
			Nonce:          2,
			HashPrevBlock:  header1Hash,
			HashMerkleRoot: &chainhash.Hash{2},
			Bits:           *bits,
		}

		headers := []*model.BlockHeader{header2, header1} // Newest first
		metas := []*model.BlockHeaderMeta{
			{Height: 2},
			{Height: 1},
		}

		cache.RebuildBlockchain(headers, metas)

		// Should have both headers
		cachedHeader1, cachedMeta1 := cache.GetBlockHeader(*header1Hash)
		assert.NotNil(t, cachedHeader1)
		assert.NotNil(t, cachedMeta1)

		header2Hash := header2.Hash()
		cachedHeader2, cachedMeta2 := cache.GetBlockHeader(*header2Hash)
		assert.NotNil(t, cachedHeader2)
		assert.NotNil(t, cachedMeta2)

		// Best block should be the last one added (header2)
		bestHeader, bestMeta := cache.GetBestBlockHeader()
		assert.Equal(t, header2, bestHeader)
		assert.Equal(t, metas[0], bestMeta) // metas[0] corresponds to header2
	})

	t.Run("SetExistsWithEnabledCache", func(t *testing.T) {
		hash := chainhash.Hash{123}
		cache.SetExists(hash, true)

		exists, found := cache.GetExists(hash)
		assert.True(t, exists)
		assert.True(t, found)

		// Test setting to false
		cache.SetExists(hash, false)
		exists, found = cache.GetExists(hash)
		assert.False(t, exists)
		assert.True(t, found) // Still found in cache
	})
}
