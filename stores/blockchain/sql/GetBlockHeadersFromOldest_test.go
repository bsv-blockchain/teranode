package sql

import (
	"context"
	"net/url"
	"testing"

	"github.com/bitcoin-sv/teranode/model"
	"github.com/bitcoin-sv/teranode/test/utils/postgres"
	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/bitcoin-sv/teranode/util/test"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSQLGetBlockHeadersFromOldest(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)

	t.Run("empty - no error", func(t *testing.T) {
		storeURL, err := url.Parse("sqlitememory:///")
		require.NoError(t, err)

		s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
		require.NoError(t, err)

		headers, metas, err := s.GetBlockHeadersFromOldest(context.Background(), block2.Hash(), block2.Hash(), 2)
		require.NoError(t, err)
		assert.Equal(t, 0, len(headers))
		assert.Equal(t, 0, len(metas))
	})

	t.Run("single block chain - from genesis", func(t *testing.T) {
		storeURL, err := url.Parse("sqlitememory:///")
		require.NoError(t, err)

		s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
		require.NoError(t, err)

		genesisHash := tSettings.ChainCfgParams.GenesisHash

		headers, metas, err := s.GetBlockHeadersFromOldest(context.Background(), genesisHash, genesisHash, 1)
		require.NoError(t, err)
		assert.Equal(t, 1, len(headers))
		assert.Equal(t, 1, len(metas))
		assert.Equal(t, genesisHash.String(), headers[0].Hash().String())
		assert.Equal(t, uint32(0), metas[0].Height)
	})

	t.Run("single block chain - from block1", func(t *testing.T) {
		storeURL, err := url.Parse("sqlitememory:///")
		require.NoError(t, err)

		s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
		require.NoError(t, err)

		_, _, err = s.StoreBlock(context.Background(), block1, "test_peer")
		require.NoError(t, err)

		headers, metas, err := s.GetBlockHeadersFromOldest(context.Background(), block1.Hash(), tSettings.ChainCfgParams.GenesisHash, 2)
		require.NoError(t, err)
		assert.Equal(t, 2, len(headers))
		assert.Equal(t, 2, len(metas))

		// Should return in ascending order: genesis first, then block1
		assert.Equal(t, "0f9188f13cb7b2c71f2a335e3a4fc328bf5beb436012afca590b1a11466e2206", headers[0].Hash().String())
		assert.Equal(t, uint32(0), metas[0].Height)
		assert.Equal(t, block1.Header.Hash(), headers[1].Hash())
		assert.Equal(t, uint32(1), metas[1].Height)
		assert.Equal(t, "test_peer", metas[1].PeerID)
	})

	t.Run("multiple block chain - ascending order", func(t *testing.T) {
		storeURL, err := url.Parse("sqlitememory:///")
		require.NoError(t, err)

		s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
		require.NoError(t, err)

		_, _, err = s.StoreBlock(context.Background(), block1, "test_peer")
		require.NoError(t, err)

		_, _, err = s.StoreBlock(context.Background(), block2, "test_peer")
		require.NoError(t, err)

		headers, metas, err := s.GetBlockHeadersFromOldest(context.Background(), block2.Hash(), tSettings.ChainCfgParams.GenesisHash, 3)
		require.NoError(t, err)
		assert.Equal(t, 3, len(headers))
		assert.Equal(t, 3, len(metas))

		// Should return in ascending order: genesis, block1, block2
		assert.Equal(t, "0f9188f13cb7b2c71f2a335e3a4fc328bf5beb436012afca590b1a11466e2206", headers[0].Hash().String())
		assert.Equal(t, uint32(0), metas[0].Height)
		assert.Equal(t, block1.Header.Hash(), headers[1].Hash())
		assert.Equal(t, uint32(1), metas[1].Height)
		assert.Equal(t, block2.Header.Hash(), headers[2].Hash())
		assert.Equal(t, uint32(2), metas[2].Height)
		assert.Equal(t, "test_peer", metas[2].PeerID)
	})

	t.Run("limited headers - request fewer than available", func(t *testing.T) {
		storeURL, err := url.Parse("sqlitememory:///")
		require.NoError(t, err)

		s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
		require.NoError(t, err)

		_, _, err = s.StoreBlock(context.Background(), block1, "test_peer")
		require.NoError(t, err)

		_, _, err = s.StoreBlock(context.Background(), block2, "test_peer")
		require.NoError(t, err)

		headers, metas, err := s.GetBlockHeadersFromOldest(context.Background(), block2.Hash(), block1.Hash(), 2)
		require.NoError(t, err)
		assert.Equal(t, 2, len(headers))
		assert.Equal(t, 2, len(metas))

		// Should return in ascending order: block1, block2 (starting from block1)
		assert.Equal(t, block1.Header.Hash(), headers[0].Hash())
		assert.Equal(t, uint32(1), metas[0].Height)
		assert.Equal(t, block2.Header.Hash(), headers[1].Hash())
		assert.Equal(t, uint32(2), metas[1].Height)
	})

	t.Run("fork scenario - alternative chain", func(t *testing.T) {
		storeURL, err := url.Parse("sqlitememory:///")
		require.NoError(t, err)

		s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
		require.NoError(t, err)

		_, _, err = s.StoreBlock(context.Background(), block1, "test_peer")
		require.NoError(t, err)

		_, _, err = s.StoreBlock(context.Background(), block2, "test_peer")
		require.NoError(t, err)

		// Create alternative block2
		block2Alt := &model.Block{
			Header: &model.BlockHeader{
				Version:        1,
				Timestamp:      1231469744,
				Nonce:          1639830026,
				HashPrevBlock:  block2PrevBlockHash,
				HashMerkleRoot: block2MerkleRootHash,
				Bits:           *bits,
			},
			CoinbaseTx:       coinbaseTx2,
			TransactionCount: 1,
			Subtrees: []*chainhash.Hash{
				subtree,
			},
		}

		_, _, err = s.StoreBlock(context.Background(), block2Alt, "test_peer")
		require.NoError(t, err)

		headers, metas, err := s.GetBlockHeadersFromOldest(context.Background(), block2Alt.Hash(), tSettings.ChainCfgParams.GenesisHash, 3)
		require.NoError(t, err)
		assert.Equal(t, 3, len(headers))

		// Should return in ascending order: genesis, block1, block2Alt
		assert.Equal(t, "0f9188f13cb7b2c71f2a335e3a4fc328bf5beb436012afca590b1a11466e2206", headers[0].Hash().String())
		assert.Equal(t, uint32(0), metas[0].Height)
		assert.Equal(t, block1.Header.Hash(), headers[1].Hash())
		assert.Equal(t, uint32(1), metas[1].Height)
		assert.Equal(t, block2Alt.Header.Hash(), headers[2].Hash())
		assert.Equal(t, uint32(2), metas[2].Height)
	})

	t.Run("request more headers than available", func(t *testing.T) {
		storeURL, err := url.Parse("sqlitememory:///")
		require.NoError(t, err)

		s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
		require.NoError(t, err)

		_, _, err = s.StoreBlock(context.Background(), block1, "test_peer")
		require.NoError(t, err)

		headers, metas, err := s.GetBlockHeadersFromOldest(context.Background(), block1.Hash(), tSettings.ChainCfgParams.GenesisHash, 10)
		require.NoError(t, err)
		assert.Equal(t, 2, len(headers)) // Only genesis and block1 available

		// Should return in ascending order: genesis, block1
		assert.Equal(t, "0f9188f13cb7b2c71f2a335e3a4fc328bf5beb436012afca590b1a11466e2206", headers[0].Hash().String())
		assert.Equal(t, uint32(0), metas[0].Height)
		assert.Equal(t, block1.Header.Hash(), headers[1].Hash())
		assert.Equal(t, uint32(1), metas[1].Height)
	})

	t.Run("invalid block hash", func(t *testing.T) {
		storeURL, err := url.Parse("sqlitememory:///")
		require.NoError(t, err)

		s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
		require.NoError(t, err)

		invalidHash := &chainhash.Hash{}
		headers, metas, err := s.GetBlockHeadersFromOldest(context.Background(), invalidHash, invalidHash, 1)
		require.NoError(t, err)
		assert.Equal(t, 0, len(headers))
		assert.Equal(t, 0, len(metas))
	})
}

// setupPostgresTestStoreForOldest creates a PostgreSQL test store with container setup and cleanup
func setupPostgresTestStoreForOldest(t *testing.T) (*SQL, func()) {
	t.Helper()

	connStr, teardown, err := postgres.SetupTestPostgresContainer()
	if err != nil {
		t.Skipf("PostgreSQL container not available: %v", err)
	}

	storeURL, err := url.Parse(connStr)
	require.NoError(t, err)

	tSettings := test.CreateBaseTestSettings(t)
	s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
	require.NoError(t, err)

	cleanup := func() {
		if teardownErr := teardown(); teardownErr != nil {
			t.Logf("Failed to teardown PostgreSQL container: %v", teardownErr)
		}
	}

	return s, cleanup
}

// storeTestBlocksForOldest stores and processes the standard test blocks (block1, block2)
func storeTestBlocksForOldest(t *testing.T, s *SQL) {
	t.Helper()

	// Store block1
	_, _, err := s.StoreBlock(context.Background(), block1, "test_peer")
	require.NoError(t, err)

	// Store block2
	_, _, err = s.StoreBlock(context.Background(), block2, "test_peer")
	require.NoError(t, err)
}

func TestSQLGetBlockHeadersFromOldestPostgreSQL(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping PostgreSQL tests in short mode")
	}

	t.Run("postgres container - basic functionality", func(t *testing.T) {
		s, cleanup := setupPostgresTestStoreForOldest(t)
		defer cleanup()

		storeTestBlocksForOldest(t, s)

		tSettings := test.CreateBaseTestSettings(t)
		headers, metas, err := s.GetBlockHeadersFromOldest(context.Background(), block2.Hash(), tSettings.ChainCfgParams.GenesisHash, 3)
		require.NoError(t, err)
		require.Len(t, headers, 3)
		require.Len(t, metas, 3)

		// Should return in ascending order: genesis, block1, block2
		assert.Equal(t, "0f9188f13cb7b2c71f2a335e3a4fc328bf5beb436012afca590b1a11466e2206", headers[0].Hash().String())
		assert.Equal(t, uint32(0), metas[0].Height)
		assert.Equal(t, block1.Header.Hash(), headers[1].Hash())
		assert.Equal(t, uint32(1), metas[1].Height)
		assert.Equal(t, block2.Header.Hash(), headers[2].Hash())
		assert.Equal(t, uint32(2), metas[2].Height)
	})

	t.Run("postgres container - fork scenario", func(t *testing.T) {
		s, cleanup := setupPostgresTestStoreForOldest(t)
		defer cleanup()

		storeTestBlocksForOldest(t, s)

		// Create alternative block2
		block2Alt := &model.Block{
			Header: &model.BlockHeader{
				Version:        1,
				Timestamp:      1231469744,
				Nonce:          1639830026,
				HashPrevBlock:  block2PrevBlockHash,
				HashMerkleRoot: block2MerkleRootHash,
				Bits:           *bits,
			},
			CoinbaseTx:       coinbaseTx2,
			TransactionCount: 1,
			Subtrees: []*chainhash.Hash{
				subtree,
			},
		}

		_, _, err := s.StoreBlock(context.Background(), block2Alt, "test_peer")
		require.NoError(t, err)

		tSettings := test.CreateBaseTestSettings(t)
		headers, metas, err := s.GetBlockHeadersFromOldest(context.Background(), block2Alt.Hash(), tSettings.ChainCfgParams.GenesisHash, 3)
		require.NoError(t, err)
		require.Len(t, headers, 3)
		require.Len(t, metas, 3)

		// Should return in ascending order: genesis, block1, block2Alt
		assert.Equal(t, "0f9188f13cb7b2c71f2a335e3a4fc328bf5beb436012afca590b1a11466e2206", headers[0].Hash().String())
		assert.Equal(t, uint32(0), metas[0].Height)
		assert.Equal(t, block1.Header.Hash(), headers[1].Hash())
		assert.Equal(t, uint32(1), metas[1].Height)
		assert.Equal(t, block2Alt.Header.Hash(), headers[2].Hash())
		assert.Equal(t, uint32(2), metas[2].Height)
	})

	t.Run("postgres container - large locator array", func(t *testing.T) {
		s, cleanup := setupPostgresTestStoreForOldest(t)
		defer cleanup()

		// Generate a longer chain for stress testing
		generatedBlocks := generateBlockHeaders(t, s, 10)

		// Test retrieving from a block that allows getting 5 headers forward
		// Use the last block as chain tip, and start from 5 blocks before the end
		lastBlock := generatedBlocks[len(generatedBlocks)-1]
		startBlock := generatedBlocks[len(generatedBlocks)-5] // 5 blocks from the end
		headers, metas, err := s.GetBlockHeadersFromOldest(context.Background(), lastBlock.Hash(), startBlock.Hash(), 5)
		require.NoError(t, err)
		require.Len(t, headers, 5)
		require.Len(t, metas, 5)

		// Verify ascending order
		for i := 1; i < len(headers); i++ {
			assert.True(t, metas[i-1].Height < metas[i].Height,
				"Headers should be in ascending order by height")
		}
	})

	t.Run("postgres container - empty result", func(t *testing.T) {
		s, cleanup := setupPostgresTestStoreForOldest(t)
		defer cleanup()

		invalidHash := &chainhash.Hash{}
		headers, metas, err := s.GetBlockHeadersFromOldest(context.Background(), invalidHash, invalidHash, 1)
		require.NoError(t, err)
		assert.Equal(t, 0, len(headers))
		assert.Equal(t, 0, len(metas))
	})
}

func BenchmarkGetBlockHeadersFromOldest(b *testing.B) {
	tSettings := test.CreateBaseTestSettings(b)

	storeURL, err := url.Parse("sqlitememory:///")
	require.NoError(b, err)

	s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
	require.NoError(b, err)

	// Setup test data
	_, _, err = s.StoreBlock(context.Background(), block1, "test_peer")
	require.NoError(b, err)

	_, _, err = s.StoreBlock(context.Background(), block2, "test_peer")
	require.NoError(b, err)

	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, err = s.GetBlockHeadersFromOldest(ctx, block2.Hash(), tSettings.ChainCfgParams.GenesisHash, 3)
		require.NoError(b, err)
	}
}
