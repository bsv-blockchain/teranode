package sql

import (
	"net/url"
	"testing"

	"github.com/bitcoin-sv/teranode/test/utils/postgres"
	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/bitcoin-sv/teranode/util/test"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSQLGetLatestBlockHeaderFromBlockLocator(t *testing.T) {
	tSettings := test.CreateBaseTestSettings()

	t.Run("genesis block only - empty locator", func(t *testing.T) {
		storeURL, err := url.Parse("sqlitememory:///")
		require.NoError(t, err)

		s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
		require.NoError(t, err)

		genesisHash := tSettings.ChainCfgParams.GenesisHash
		blockLocator := []chainhash.Hash{}

		header, meta, err := s.GetLatestBlockHeaderFromBlockLocator(t.Context(), genesisHash, blockLocator)
		require.Error(t, err, "blockLocator cannot be empty")
		require.Nil(t, header)
		require.Nil(t, meta)
	})

	t.Run("genesis block only - locator with genesis", func(t *testing.T) {
		storeURL, err := url.Parse("sqlitememory:///")
		require.NoError(t, err)

		s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
		require.NoError(t, err)

		genesisHash := tSettings.ChainCfgParams.GenesisHash
		blockLocator := []chainhash.Hash{*genesisHash}

		header, meta, err := s.GetLatestBlockHeaderFromBlockLocator(t.Context(), genesisHash, blockLocator)
		require.NoError(t, err)
		require.NotNil(t, header)
		require.NotNil(t, meta)

		assert.Equal(t, uint32(0), meta.Height)
		assert.Equal(t, uint32(1), header.Version)
		assertRegtestGenesis(t, header)
	})

	t.Run("single chain - find latest block in locator", func(t *testing.T) {
		storeURL, err := url.Parse("sqlitememory:///")
		require.NoError(t, err)

		s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
		require.NoError(t, err)

		// Store blocks in sequence: genesis -> block1 -> block2
		_, _, err = s.StoreBlock(t.Context(), block1, "test_peer")
		require.NoError(t, err)
		err = s.SetBlockProcessedAt(t.Context(), block1.Hash())
		require.NoError(t, err)

		_, _, err = s.StoreBlock(t.Context(), block2, "test_peer")
		require.NoError(t, err)
		err = s.SetBlockProcessedAt(t.Context(), block2.Hash())
		require.NoError(t, err)

		// Locator contains both blocks - should return the latest (block2)
		blockLocator := []chainhash.Hash{*block1.Hash(), *block2.Hash()}

		header, meta, err := s.GetLatestBlockHeaderFromBlockLocator(t.Context(), block2.Hash(), blockLocator)
		require.NoError(t, err)
		require.NotNil(t, header)
		require.NotNil(t, meta)

		assert.Equal(t, uint32(2), meta.Height)
		assert.Equal(t, block2.Header.Version, header.Version)
		assert.Equal(t, block2.Header.Timestamp, header.Timestamp)
		assert.Equal(t, block2.Header.Nonce, header.Nonce)
	})

	t.Run("single chain - locator with only older block", func(t *testing.T) {
		storeURL, err := url.Parse("sqlitememory:///")
		require.NoError(t, err)

		s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
		require.NoError(t, err)

		// Store blocks in sequence: genesis -> block1 -> block2
		_, _, err = s.StoreBlock(t.Context(), block1, "test_peer")
		require.NoError(t, err)
		err = s.SetBlockProcessedAt(t.Context(), block1.Hash())
		require.NoError(t, err)

		_, _, err = s.StoreBlock(t.Context(), block2, "test_peer")
		require.NoError(t, err)
		err = s.SetBlockProcessedAt(t.Context(), block2.Hash())
		require.NoError(t, err)

		// Locator contains only block1 - should return block1
		blockLocator := []chainhash.Hash{*block1.Hash()}

		header, meta, err := s.GetLatestBlockHeaderFromBlockLocator(t.Context(), block2.Hash(), blockLocator)
		require.NoError(t, err)
		require.NotNil(t, header)
		require.NotNil(t, meta)

		assert.Equal(t, uint32(1), meta.Height)
		assert.Equal(t, block1.Header.Version, header.Version)
		assert.Equal(t, block1.Header.Timestamp, header.Timestamp)
		assert.Equal(t, block1.Header.Nonce, header.Nonce)
	})

	t.Run("fork scenario - find common ancestor", func(t *testing.T) {
		storeURL, err := url.Parse("sqlitememory:///")
		require.NoError(t, err)

		s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
		require.NoError(t, err)

		// Store main chain: genesis -> block1 -> block2
		_, _, err = s.StoreBlock(t.Context(), block1, "test_peer")
		require.NoError(t, err)
		err = s.SetBlockProcessedAt(t.Context(), block1.Hash())
		require.NoError(t, err)

		_, _, err = s.StoreBlock(t.Context(), block2, "test_peer")
		require.NoError(t, err)
		err = s.SetBlockProcessedAt(t.Context(), block2.Hash())
		require.NoError(t, err)

		// Create alternative block at height 2
		altBlock2 := createAlternativeBlock2()
		_, _, err = s.StoreBlock(t.Context(), altBlock2, "test_peer")
		require.NoError(t, err)

		// Locator from alternative chain should find common ancestor (block1)
		blockLocator := []chainhash.Hash{*altBlock2.Hash(), *block1.Hash()}

		header, meta, err := s.GetLatestBlockHeaderFromBlockLocator(t.Context(), block2.Hash(), blockLocator)
		require.NoError(t, err)
		require.NotNil(t, header)
		require.NotNil(t, meta)

		// Should return block1 as it's the latest common ancestor in the main chain
		assert.Equal(t, uint32(1), meta.Height)
		assert.Equal(t, block1.Header.Version, header.Version)
	})

	t.Run("no matching blocks in locator", func(t *testing.T) {
		storeURL, err := url.Parse("sqlitememory:///")
		require.NoError(t, err)

		s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
		require.NoError(t, err)

		// Store some blocks
		_, _, err = s.StoreBlock(t.Context(), block1, "test_peer")
		require.NoError(t, err)
		err = s.SetBlockProcessedAt(t.Context(), block1.Hash())
		require.NoError(t, err)

		// Create a random hash not in the chain
		randomHash, err := chainhash.NewHashFromStr("1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef")
		require.NoError(t, err)

		blockLocator := []chainhash.Hash{*randomHash}

		header, meta, err := s.GetLatestBlockHeaderFromBlockLocator(t.Context(), block1.Hash(), blockLocator)
		require.Error(t, err, "no matching blocks found in locator")
		require.Nil(t, header)
		require.Nil(t, meta)
	})

	t.Run("multiple blocks in locator - returns highest", func(t *testing.T) {
		storeURL, err := url.Parse("sqlitememory:///")
		require.NoError(t, err)

		s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
		require.NoError(t, err)

		// Store blocks in sequence: genesis -> block1 -> block2 -> block3
		_, _, err = s.StoreBlock(t.Context(), block1, "test_peer")
		require.NoError(t, err)
		err = s.SetBlockProcessedAt(t.Context(), block1.Hash())
		require.NoError(t, err)

		_, _, err = s.StoreBlock(t.Context(), block2, "test_peer")
		require.NoError(t, err)
		err = s.SetBlockProcessedAt(t.Context(), block2.Hash())
		require.NoError(t, err)

		// Create block3 on top of block2
		block3OnMain := createBlock3OnFork(block2)
		_, _, err = s.StoreBlock(t.Context(), block3OnMain, "test_peer")
		require.NoError(t, err)
		err = s.SetBlockProcessedAt(t.Context(), block3OnMain.Hash())
		require.NoError(t, err)

		// Locator contains blocks at different heights - should return the highest
		genesisHash := tSettings.ChainCfgParams.GenesisHash
		blockLocator := []chainhash.Hash{*genesisHash, *block1.Hash(), *block2.Hash(), *block3OnMain.Hash()}

		header, meta, err := s.GetLatestBlockHeaderFromBlockLocator(t.Context(), block3OnMain.Hash(), blockLocator)
		require.NoError(t, err)
		require.NotNil(t, header)
		require.NotNil(t, meta)

		// Should return block3 as it has the highest height
		assert.Equal(t, uint32(3), meta.Height)
		assert.Equal(t, block3OnMain.Header.Version, header.Version)
		assert.Equal(t, block3OnMain.Header.Timestamp, header.Timestamp)
	})

	t.Run("invalid best block hash", func(t *testing.T) {
		storeURL, err := url.Parse("sqlitememory:///")
		require.NoError(t, err)

		s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
		require.NoError(t, err)

		// Use a non-existent hash as best block
		randomHash, err := chainhash.NewHashFromStr("1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef")
		require.NoError(t, err)

		genesisHash := tSettings.ChainCfgParams.GenesisHash
		blockLocator := []chainhash.Hash{*genesisHash}

		header, meta, err := s.GetLatestBlockHeaderFromBlockLocator(t.Context(), randomHash, blockLocator)
		require.Error(t, err, "no matching blocks found in locator")
		require.Nil(t, header)
		require.Nil(t, meta)
	})

	t.Run("large locator array", func(t *testing.T) {
		storeURL, err := url.Parse("sqlitememory:///")
		require.NoError(t, err)

		s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
		require.NoError(t, err)

		// Store a few blocks
		_, _, err = s.StoreBlock(t.Context(), block1, "test_peer")
		require.NoError(t, err)
		err = s.SetBlockProcessedAt(t.Context(), block1.Hash())
		require.NoError(t, err)

		_, _, err = s.StoreBlock(t.Context(), block2, "test_peer")
		require.NoError(t, err)
		err = s.SetBlockProcessedAt(t.Context(), block2.Hash())
		require.NoError(t, err)

		// Create a large locator with many random hashes and a few valid ones
		blockLocator := make([]chainhash.Hash, 100)
		for i := 0; i < 98; i++ {
			// Generate random hashes
			randomBytes := make([]byte, 32)
			for j := range randomBytes {
				randomBytes[j] = byte(i + j)
			}
			hash, err := chainhash.NewHash(randomBytes)
			require.NoError(t, err)
			blockLocator[i] = *hash
		}
		// Add valid blocks at the end
		blockLocator[98] = *block1.Hash()
		blockLocator[99] = *block2.Hash()

		header, meta, err := s.GetLatestBlockHeaderFromBlockLocator(t.Context(), block2.Hash(), blockLocator)
		require.NoError(t, err)
		require.NotNil(t, header)
		require.NotNil(t, meta)

		// Should still find and return block2 (highest valid block)
		assert.Equal(t, uint32(2), meta.Height)
		assert.Equal(t, block2.Header.Version, header.Version)
	})
}

// setupPostgresTestStore creates a PostgreSQL test store with container setup and cleanup
func setupPostgresTestStore(t *testing.T) (*SQL, func()) {
	t.Helper()

	connStr, teardown, err := postgres.SetupTestPostgresContainer()
	if err != nil {
		t.Skipf("PostgreSQL container not available: %v", err)
	}

	storeURL, err := url.Parse(connStr)
	require.NoError(t, err)

	tSettings := test.CreateBaseTestSettings()
	s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
	require.NoError(t, err)

	cleanup := func() {
		if teardownErr := teardown(); teardownErr != nil {
			t.Logf("Failed to teardown PostgreSQL container: %v", teardownErr)
		}
	}

	return s, cleanup
}

// storeTestBlocks stores and processes the standard test blocks (block1, block2)
func storeTestBlocks(t *testing.T, s *SQL) {
	t.Helper()

	// Store block1
	_, _, err := s.StoreBlock(t.Context(), block1, "test_peer")
	require.NoError(t, err)
	err = s.SetBlockProcessedAt(t.Context(), block1.Hash())
	require.NoError(t, err)

	// Store block2
	_, _, err = s.StoreBlock(t.Context(), block2, "test_peer")
	require.NoError(t, err)
	err = s.SetBlockProcessedAt(t.Context(), block2.Hash())
	require.NoError(t, err)
}

func TestSQLGetLatestBlockHeaderFromBlockLocatorPostgreSQL(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping PostgreSQL tests in short mode")
	}

	t.Run("postgres container - basic functionality", func(t *testing.T) {
		s, cleanup := setupPostgresTestStore(t)
		defer cleanup()

		storeTestBlocks(t, s)

		blockLocator := []chainhash.Hash{*block1.Hash(), *block2.Hash()}

		header, meta, err := s.GetLatestBlockHeaderFromBlockLocator(t.Context(), block2.Hash(), blockLocator)
		require.NoError(t, err)
		require.NotNil(t, header)
		require.NotNil(t, meta)

		assert.Equal(t, uint32(2), meta.Height)
		assert.Equal(t, block2.Header.Version, header.Version)
		assert.Equal(t, block2.Header.Timestamp, header.Timestamp)
		assert.Equal(t, block2.Header.Nonce, header.Nonce)
	})

	t.Run("postgres container - fork scenario", func(t *testing.T) {
		s, cleanup := setupPostgresTestStore(t)
		defer cleanup()

		storeTestBlocks(t, s)

		// Create alternative block at height 2
		altBlock2 := createAlternativeBlock2()
		_, _, err := s.StoreBlock(t.Context(), altBlock2, "test_peer")
		require.NoError(t, err)

		// Locator from alternative chain should find common ancestor (block1)
		blockLocator := []chainhash.Hash{*altBlock2.Hash(), *block1.Hash()}

		header, meta, err := s.GetLatestBlockHeaderFromBlockLocator(t.Context(), block2.Hash(), blockLocator)
		require.NoError(t, err)
		require.NotNil(t, header)
		require.NotNil(t, meta)

		// Should return block1 as it's the latest common ancestor in the main chain
		assert.Equal(t, uint32(1), meta.Height)
		assert.Equal(t, block1.Header.Version, header.Version)
	})

	t.Run("postgres container - large locator array", func(t *testing.T) {
		s, cleanup := setupPostgresTestStore(t)
		defer cleanup()

		storeTestBlocks(t, s)

		// Create a large locator with many random hashes and a few valid ones
		// This specifically tests PostgreSQL's ANY() function with large arrays
		blockLocator := make([]chainhash.Hash, 500)
		for i := 0; i < 498; i++ {
			// Generate random hashes
			randomBytes := make([]byte, 32)
			for j := range randomBytes {
				randomBytes[j] = byte(i + j)
			}
			hash, err := chainhash.NewHash(randomBytes)
			require.NoError(t, err)
			blockLocator[i] = *hash
		}
		// Add valid blocks at the end
		blockLocator[498] = *block1.Hash()
		blockLocator[499] = *block2.Hash()

		header, meta, err := s.GetLatestBlockHeaderFromBlockLocator(t.Context(), block2.Hash(), blockLocator)
		require.NoError(t, err)
		require.NotNil(t, header)
		require.NotNil(t, meta)

		// Should still find and return block2 (highest valid block)
		assert.Equal(t, uint32(2), meta.Height)
		assert.Equal(t, block2.Header.Version, header.Version)
	})

	t.Run("postgres container - empty locator", func(t *testing.T) {
		s, cleanup := setupPostgresTestStore(t)
		defer cleanup()

		tSettings := test.CreateBaseTestSettings()
		genesisHash := tSettings.ChainCfgParams.GenesisHash
		blockLocator := []chainhash.Hash{}

		header, meta, err := s.GetLatestBlockHeaderFromBlockLocator(t.Context(), genesisHash, blockLocator)
		require.Error(t, err, "blockLocator cannot be empty")
		require.Nil(t, header)
		require.Nil(t, meta)
	})
}

func BenchmarkGetLatestBlockHeaderFromBlockLocator(b *testing.B) {
	tSettings := test.CreateBaseTestSettings()
	storeURL, err := url.Parse("sqlitememory:///")
	require.NoError(b, err)

	s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
	require.NoError(b, err)

	// Setup test data
	_, _, err = s.StoreBlock(b.Context(), block1, "test_peer")
	require.NoError(b, err)
	err = s.SetBlockProcessedAt(b.Context(), block1.Hash())
	require.NoError(b, err)

	_, _, err = s.StoreBlock(b.Context(), block2, "test_peer")
	require.NoError(b, err)
	err = s.SetBlockProcessedAt(b.Context(), block2.Hash())
	require.NoError(b, err)

	blockLocator := []chainhash.Hash{*block1.Hash(), *block2.Hash()}
	ctx := b.Context()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, err = s.GetLatestBlockHeaderFromBlockLocator(ctx, block2.Hash(), blockLocator)
		require.NoError(b, err)
	}
}
