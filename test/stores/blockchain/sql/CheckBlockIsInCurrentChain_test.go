//go:build test_all || test_stores || test_stores_sql

package sql

import (
	"context"
	"net/url"
	"testing"

	"github.com/bitcoin-sv/ubsv/chaincfg"
	"github.com/bitcoin-sv/ubsv/model"
	storesql "github.com/bitcoin-sv/ubsv/stores/blockchain/sql"
	helper "github.com/bitcoin-sv/ubsv/test/utils"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/libsv/go-bt/v2/chainhash"
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

		storeURL, err := url.Parse(connStr)
		require.NoError(t, err)

		s, err := storesql.New(ulogger.TestLogger{}, storeURL, &chaincfg.MainNetParams)
		require.NoError(t, err)

		// No blocks stored, should return false
		blockIDs := []uint32{1, 2, 3}
		isInChain, err := s.CheckBlockIsInCurrentChain(context.Background(), blockIDs)
		require.NoError(t, err)
		assert.False(t, isInChain)
	})

	t.Run("single block in chain", func(t *testing.T) {
		connStr, teardown, err := helper.SetupTestPostgresContainer()
		require.NoError(t, err)

		defer func() {
			err := teardown()
			require.NoError(t, err)
		}()

		storeURL, err := url.Parse(connStr)
		require.NoError(t, err)

		s, err := storesql.New(ulogger.TestLogger{}, storeURL, &chaincfg.MainNetParams)
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

		defer func() {
			err := teardown()
			require.NoError(t, err)
		}()

		storeURL, err := url.Parse(connStr)
		require.NoError(t, err)

		s, err := storesql.New(ulogger.TestLogger{}, storeURL, &chaincfg.MainNetParams)
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

		// Check if any of the blockIDs are in the chain, should return false
		blockIDs = []uint32{9999, 99999}
		isInChain, err = s.CheckBlockIsInCurrentChain(context.Background(), blockIDs)
		require.NoError(t, err)
		assert.False(t, isInChain)
	})

	t.Run("block not in chain", func(t *testing.T) {
		connStr, teardown, err := helper.SetupTestPostgresContainer()
		require.NoError(t, err)

		defer func() {
			err := teardown()
			require.NoError(t, err)
		}()

		storeURL, err := url.Parse(connStr)
		require.NoError(t, err)

		s, err := storesql.New(ulogger.TestLogger{}, storeURL, &chaincfg.MainNetParams)
		require.NoError(t, err)

		// Store block1
		_, _, err = s.StoreBlock(context.Background(), block1, "")
		require.NoError(t, err)

		// Check if a non-existent block is in the chain, should return false
		blockIDs := []uint32{9999} // Non-existent block
		isInChain, err := s.CheckBlockIsInCurrentChain(context.Background(), blockIDs)
		require.NoError(t, err)
		assert.False(t, isInChain)
	})

	t.Run("alternative block in branch", func(t *testing.T) {
		connStr, teardown, err := helper.SetupTestPostgresContainer()
		require.NoError(t, err)

		defer func() {
			err := teardown()
			require.NoError(t, err)
		}()

		storeURL, err := url.Parse(connStr)
		require.NoError(t, err)

		s, err := storesql.New(ulogger.TestLogger{}, storeURL, &chaincfg.MainNetParams)
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

		defer func() {
			err := teardown()
			require.NoError(t, err)
		}()

		storeURL, err := url.Parse(connStr)
		require.NoError(t, err)

		s, err := storesql.New(ulogger.TestLogger{}, storeURL, &chaincfg.MainNetParams)
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

		defer func() {
			err := teardown()
			require.NoError(t, err)
		}()

		storeURL, err := url.Parse(connStr)
		require.NoError(t, err)

		s, err := storesql.New(ulogger.TestLogger{}, storeURL, &chaincfg.MainNetParams)
		require.NoError(t, err)

		// Store block1 and block2
		_, _, err = s.StoreBlock(context.Background(), block1, "")
		require.NoError(t, err)

		_, _, err = s.StoreBlock(context.Background(), block2, "")
		require.NoError(t, err)

		// get metas for block1 and block2
		_, metas, err := s.GetBlockHeaders(context.Background(), block2.Hash(), 2)
		require.NoError(t, err)

		// Check if any of the blockIDs are in the chain, should return true
		blockIDs := []uint32{metas[0].ID, 9999, 99999}
		isInChain, err := s.CheckBlockIsInCurrentChain(context.Background(), blockIDs)
		require.NoError(t, err)
		assert.True(t, isInChain)
	})
}
