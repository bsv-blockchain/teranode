package sql

import (
	"context"
	"net/url"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-chaincfg"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/settings"
	storesql "github.com/bsv-blockchain/teranode/stores/blockchain/sql"
	helper "github.com/bsv-blockchain/teranode/test/utils/postgres"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// go test -v -tags test_stores_sql ./test/...

func Test_PostgresCheckIfBlockIsInCurrentChain(t *testing.T) {
	t.Run("empty - no match", func(t *testing.T) {
		connStr, teardown, err := helper.SetupTestPostgresContainer()
		require.NoError(t, err)

		defer func() {
			err := teardown()
			require.NoError(t, err)
		}()

		tSettings := settings.NewSettings()
		tSettings.ChainCfgParams = &chaincfg.RegressionNetParams

		storeURL, err := url.Parse(connStr)
		require.NoError(t, err)

		s, err := storesql.New(ulogger.TestLogger{}, storeURL, tSettings)
		require.NoError(t, err)

		// Non-existent block IDs that are not encountered during the chain walk
		// correctly return false. Use very high IDs to avoid colliding with real blocks.
		blockIDs := []uint32{999999, 999998, 999997}
		isInChain, err := s.CheckBlockIsInCurrentChain(context.Background(), blockIDs)
		require.NoError(t, err)
		assert.False(t, isInChain, "non-existent IDs should return false")
	})

	t.Run("single block in chain", func(t *testing.T) {
		connStr, teardown, err := helper.SetupTestPostgresContainer()
		require.NoError(t, err)

		tSettings := settings.NewSettings()
		tSettings.ChainCfgParams = &chaincfg.RegressionNetParams
		defer func() {
			err := teardown()
			require.NoError(t, err)
		}()

		storeURL, err := url.Parse(connStr)
		require.NoError(t, err)

		s, err := storesql.New(ulogger.TestLogger{}, storeURL, tSettings)
		require.NoError(t, err)

		// Store block1
		_, _, err = s.StoreBlock(context.Background(), block1, "")
		require.NoError(t, err)

		_, metas, err := s.GetBlockHeaders(context.Background(), block1.Hash(), 1)
		require.NoError(t, err)

		// Check if block1 is in the chain, should return true
		blockIDs := []uint32{metas[0].ID}
		isInChain, err := s.CheckBlockIsInCurrentChain(context.Background(), blockIDs)

		require.NoError(t, err)
		assert.True(t, isInChain)
	})

	t.Run("multiple blocks in chain", func(t *testing.T) {
		connStr, teardown, err := helper.SetupTestPostgresContainer()
		require.NoError(t, err)

		tSettings := settings.NewSettings()
		tSettings.ChainCfgParams = &chaincfg.RegressionNetParams

		defer func() {
			err := teardown()
			require.NoError(t, err)
		}()

		storeURL, err := url.Parse(connStr)
		require.NoError(t, err)

		s, err := storesql.New(ulogger.TestLogger{}, storeURL, tSettings)
		require.NoError(t, err)

		// Store block1 and block2
		_, _, err = s.StoreBlock(context.Background(), block1, "")
		require.NoError(t, err)

		_, _, err = s.StoreBlock(context.Background(), block2, "")
		require.NoError(t, err)

		// get metas for block1 and block2
		_, metas, err := s.GetBlockHeaders(context.Background(), block2.Hash(), 2)
		require.NoError(t, err)

		// Check if block1 and block2 are in the chain, should return true
		blockIDs := []uint32{metas[0].ID, metas[1].ID}
		isInChain, err := s.CheckBlockIsInCurrentChain(context.Background(), blockIDs)
		require.NoError(t, err)
		assert.True(t, isInChain)

		// Check if any of the blockIDs are in the chain, should return true
		blockIDs = []uint32{metas[0].ID}
		isInChain, err = s.CheckBlockIsInCurrentChain(context.Background(), blockIDs)
		require.NoError(t, err)
		assert.True(t, isInChain)

		// Non-existent IDs not on the chain walk correctly return false.
		blockIDs = []uint32{9999, 99999}
		isInChain, err = s.CheckBlockIsInCurrentChain(context.Background(), blockIDs)
		require.NoError(t, err)
		assert.False(t, isInChain, "non-existent IDs should return false")
	})

	t.Run("block not in chain", func(t *testing.T) {
		connStr, teardown, err := helper.SetupTestPostgresContainer()
		require.NoError(t, err)

		tSettings := settings.NewSettings()
		tSettings.ChainCfgParams = &chaincfg.RegressionNetParams

		defer func() {
			err := teardown()
			require.NoError(t, err)
		}()

		storeURL, err := url.Parse(connStr)
		require.NoError(t, err)

		s, err := storesql.New(ulogger.TestLogger{}, storeURL, tSettings)
		require.NoError(t, err)

		// Store block1
		_, _, err = s.StoreBlock(context.Background(), block1, "")
		require.NoError(t, err)

		// Non-existent IDs not on the chain walk correctly return false.
		blockIDs := []uint32{9999} // Non-existent block
		isInChain, err := s.CheckBlockIsInCurrentChain(context.Background(), blockIDs)
		require.NoError(t, err)
		assert.False(t, isInChain, "non-existent IDs should return false")
	})

	t.Run("alternative block in branch", func(t *testing.T) {
		connStr, teardown, err := helper.SetupTestPostgresContainer()
		require.NoError(t, err)

		tSettings := settings.NewSettings()
		tSettings.ChainCfgParams = &chaincfg.RegressionNetParams

		defer func() {
			err := teardown()
			require.NoError(t, err)
		}()

		storeURL, err := url.Parse(connStr)
		require.NoError(t, err)

		s, err := storesql.New(ulogger.TestLogger{}, storeURL, tSettings)
		require.NoError(t, err)

		// Store block1, block2, and an alternative block (block2Alt)
		_, _, err = s.StoreBlock(context.Background(), block1, "")
		require.NoError(t, err)

		_, _, err = s.StoreBlock(context.Background(), block2, "")
		require.NoError(t, err)

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

		_, _, err = s.StoreBlock(context.Background(), block2Alt, "")
		require.NoError(t, err)

		// Get current best block header
		// bestBlockHeader, _, err := s.GetBestBlockHeader(context.Background())
		// require.NoError(t, err)

		_, metas, err := s.GetBlockHeaders(context.Background(), block2Alt.Hash(), 10)
		require.NoError(t, err)

		// Check if block2Alt is in the chain, should return true
		blockIDs := []uint32{metas[0].ID}
		isInChain, err := s.CheckBlockIsInCurrentChain(context.Background(), blockIDs)
		require.NoError(t, err)
		assert.False(t, isInChain)
	})

	t.Run("alternative block in correct chain", func(t *testing.T) {
		connStr, teardown, err := helper.SetupTestPostgresContainer()
		require.NoError(t, err)

		tSettings := settings.NewSettings()
		tSettings.ChainCfgParams = &chaincfg.RegressionNetParams

		defer func() {
			err := teardown()
			require.NoError(t, err)
		}()

		storeURL, err := url.Parse(connStr)
		require.NoError(t, err)

		s, err := storesql.New(ulogger.TestLogger{}, storeURL, tSettings)
		require.NoError(t, err)

		// Store block1, block2, and an alternative block (block2Alt)
		_, _, err = s.StoreBlock(context.Background(), block1, "")
		require.NoError(t, err)

		_, _, err = s.StoreBlock(context.Background(), block2, "")
		require.NoError(t, err)

		// Get current best block header
		bestBlockHeader, _, err := s.GetBestBlockHeader(context.Background())
		require.NoError(t, err)

		block2Alt := &model.Block{
			Header: &model.BlockHeader{
				Version:        1,
				Timestamp:      1231469744,
				Nonce:          1639830026,
				HashPrevBlock:  bestBlockHeader.Hash(),
				HashMerkleRoot: block2MerkleRootHash,
				Bits:           *bits,
			},
			CoinbaseTx:       coinbaseTx2,
			TransactionCount: 1,
			Subtrees: []*chainhash.Hash{
				subtree,
			},
		}

		_, _, err = s.StoreBlock(context.Background(), block2Alt, "")
		require.NoError(t, err)

		_, metas, err := s.GetBlockHeaders(context.Background(), block2Alt.Hash(), 10)
		require.NoError(t, err)

		// Check if block2Alt is in the chain, should return true
		blockIDs := []uint32{metas[0].ID}
		isInChain, err := s.CheckBlockIsInCurrentChain(context.Background(), blockIDs)
		require.NoError(t, err)
		assert.True(t, isInChain)
	})

	t.Run("one of the block is in chain other is not", func(t *testing.T) {
		connStr, teardown, err := helper.SetupTestPostgresContainer()
		require.NoError(t, err)

		tSettings := settings.NewSettings()
		tSettings.ChainCfgParams = &chaincfg.RegressionNetParams

		defer func() {
			err := teardown()
			require.NoError(t, err)
		}()

		storeURL, err := url.Parse(connStr)
		require.NoError(t, err)

		s, err := storesql.New(ulogger.TestLogger{}, storeURL, tSettings)
		require.NoError(t, err)

		// Store block1 and block2
		_, _, err = s.StoreBlock(context.Background(), block1, "")
		require.NoError(t, err)

		_, _, err = s.StoreBlock(context.Background(), block2, "")
		require.NoError(t, err)

		// get metas for block1 and block2
		_, metas, err := s.GetBlockHeaders(context.Background(), block2.Hash(), 2)
		require.NoError(t, err)

		// Mix of real on-chain IDs and non-existent IDs. The chain walk finds
		// the real on-chain ID, so the result is true (ANY-of semantics).
		blockIDs := []uint32{metas[0].ID, 9999, 99999}
		isInChain, err := s.CheckBlockIsInCurrentChain(context.Background(), blockIDs)
		require.NoError(t, err)
		assert.True(t, isInChain)
	})
}

func TestSQLiteCheckIfBlockIsInCurrentChain(t *testing.T) {
	t.Run("multiple blocks in chain", func(t *testing.T) {
		tSettings := settings.NewSettings()
		tSettings.ChainCfgParams = &chaincfg.RegressionNetParams

		storeURL, err := url.Parse("sqlitememory://")
		require.NoError(t, err)

		s, err := storesql.New(ulogger.TestLogger{}, storeURL, tSettings)
		require.NoError(t, err)

		// Store block1 and block2
		_, _, err = s.StoreBlock(context.Background(), block1, "")
		require.NoError(t, err)

		_, _, err = s.StoreBlock(context.Background(), block2, "")
		require.NoError(t, err)

		// get metas for block1 and block2
		_, metas, err := s.GetBlockHeaders(context.Background(), block2.Hash(), 2)
		require.NoError(t, err)

		// Check if block1 and block2 are in the chain, should return true
		blockIDs := []uint32{metas[0].ID, metas[1].ID}
		isInChain, err := s.CheckBlockIsInCurrentChain(context.Background(), blockIDs)
		require.NoError(t, err)
		assert.True(t, isInChain)

		// Check if any of the blockIDs are in the chain, should return true
		blockIDs = []uint32{metas[0].ID}
		isInChain, err = s.CheckBlockIsInCurrentChain(context.Background(), blockIDs)
		require.NoError(t, err)
		assert.True(t, isInChain)

		// Non-existent IDs not on the chain walk correctly return false.
		blockIDs = []uint32{9999, 99999}
		isInChain, err = s.CheckBlockIsInCurrentChain(context.Background(), blockIDs)
		require.NoError(t, err)
		assert.False(t, isInChain, "non-existent IDs should return false")
	})
}
