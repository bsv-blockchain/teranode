package sql

import (
	"context"
	"fmt"
	"github.com/bitcoin-sv/ubsv/model"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"net/url"
	"testing"
)

func Test_CheckIfBlockIsInCurrentChain(t *testing.T) {
	t.Run("empty - no match", func(t *testing.T) {
		storeURL, err := url.Parse("sqlitememory:///")
		require.NoError(t, err)

		s, err := New(ulogger.TestLogger{}, storeURL)
		require.NoError(t, err)

		// No blocks stored, should return false
		blockIDs := []uint32{1, 2, 3}
		isInChain, err := s.CheckIfBlockIsInCurrentChain(context.Background(), blockIDs)
		require.NoError(t, err)
		assert.False(t, isInChain)
	})

	t.Run("single block in chain", func(t *testing.T) {
		storeURL, err := url.Parse("sqlitememory:///")
		require.NoError(t, err)

		s, err := New(ulogger.TestLogger{}, storeURL)
		require.NoError(t, err)

		// Store block1
		_, _, err = s.StoreBlock(context.Background(), block1, "")
		require.NoError(t, err)

		// get meta for block1
		_, metas, err := s.GetBlockHeaders(context.Background(), block1.Hash(), 1)

		// need to get block ID.
		// one way is through the GetBlockHeaders function

		// Check if block1 is in the chain, should return true
		blockIDs := []uint32{metas[0].ID}
		isInChain, err := s.CheckIfBlockIsInCurrentChain(context.Background(), blockIDs)
		fmt.Println("ERROR IS: ", err)
		require.NoError(t, err)
		assert.True(t, isInChain)
	})

	t.Run("multiple blocks in chain", func(t *testing.T) {
		storeURL, err := url.Parse("sqlitememory:///")
		require.NoError(t, err)

		s, err := New(ulogger.TestLogger{}, storeURL)
		require.NoError(t, err)

		// Store block1 and block2
		_, _, err = s.StoreBlock(context.Background(), block1, "")
		require.NoError(t, err)

		_, _, err = s.StoreBlock(context.Background(), block2, "")
		require.NoError(t, err)

		// get metas for block1 and block2
		_, metas, err := s.GetBlockHeaders(context.Background(), block2.Hash(), 2)

		// Check if block1 and block2 are in the chain, should return true
		blockIDs := []uint32{metas[0].ID, metas[1].ID}
		isInChain, err := s.CheckIfBlockIsInCurrentChain(context.Background(), blockIDs)
		require.NoError(t, err)
		assert.True(t, isInChain)
	})

	t.Run("block not in chain", func(t *testing.T) {
		storeURL, err := url.Parse("sqlitememory:///")
		require.NoError(t, err)

		s, err := New(ulogger.TestLogger{}, storeURL)
		require.NoError(t, err)

		// Store block1
		_, _, err = s.StoreBlock(context.Background(), block1, "")
		require.NoError(t, err)

		// Check if a non-existent block is in the chain, should return false
		blockIDs := []uint32{9999} // Non-existent block
		isInChain, err := s.CheckIfBlockIsInCurrentChain(context.Background(), blockIDs)
		require.NoError(t, err)
		assert.False(t, isInChain)
	})

	t.Run("alternative chain block in chain", func(t *testing.T) {
		storeURL, err := url.Parse("sqlitememory:///")
		require.NoError(t, err)

		s, err := New(ulogger.TestLogger{}, storeURL)
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

		// get meta for block1
		_, metas, err := s.GetBlockHeaders(context.Background(), block2Alt.Hash(), 1)

		// Check if block2Alt is in the chain, should return true
		blockIDs := []uint32{metas[0].ID}
		isInChain, err := s.CheckIfBlockIsInCurrentChain(context.Background(), blockIDs)
		require.NoError(t, err)
		assert.True(t, isInChain)
	})
}
