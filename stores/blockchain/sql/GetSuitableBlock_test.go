package sql

import (
	"context"
	"net/url"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

func Test_getMedianBlock(t *testing.T) {
	blocks := make([]*model.SuitableBlock, 0, 3)

	blocks = append(blocks, &model.SuitableBlock{
		Time: 1,
	})
	blocks = append(blocks, &model.SuitableBlock{
		Time: 3,
	})
	blocks = append(blocks, &model.SuitableBlock{
		Time: 2,
	})

	b, err := getMedianBlock(blocks)
	require.NoError(t, err)
	require.Equal(t, uint32(2), b.Time)
}

func TestSQLGetSuitableBlock(t *testing.T) {
	storeURL, err := url.Parse("sqlitememory:///")
	require.NoError(t, err)

	tSettings := test.CreateBaseTestSettings(t)

	s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
	require.NoError(t, err)

	_, _, err = s.StoreBlock(context.Background(), block1, "")
	require.NoError(t, err)

	_, _, err = s.StoreBlock(context.Background(), block2, "")
	require.NoError(t, err)

	_, _, err = s.StoreBlock(context.Background(), block3, "")
	require.NoError(t, err)

	suitableBlock, err := s.GetSuitableBlock(context.Background(), block3Hash)
	require.NoError(t, err)
	// suitable block should be block3
	require.Equal(t, block2.Hash()[:], suitableBlock.Hash)
}

// TestSQLGetSuitableBlock_IdenticalTimestamps tests that when blocks have identical timestamps,
// the sorting is deterministic and matches bitcoin-sv behavior. This test recreates the scenario
// where blocks at heights 1697038, 1697039, and 1697040 have timestamps 1759594908, 1759594909, 1759594908.
// The expected behavior is that the SQL query returns blocks in [grandparent, parent, current] order
// (ORDER BY depth DESC), which after sorting by timestamp should select the same median block as bitcoin-sv.
func TestSQLGetSuitableBlock_IdenticalTimestamps(t *testing.T) {
	storeURL, err := url.Parse("sqlitememory:///")
	require.NoError(t, err)

	tSettings := test.CreateBaseTestSettings(t)

	s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
	require.NoError(t, err)

	ctx := context.Background()

	// Create a chain of 3 blocks with specific timestamps that mirror the real scenario
	// Block at height 1697038 (grandparent) - timestamp 1759594908
	grandparentBlock := &model.Block{
		Header: &model.BlockHeader{
			Version:        1,
			Timestamp:      1759594908,
			Nonce:          1,
			HashPrevBlock:  hashPrevBlock,
			HashMerkleRoot: hashMerkleRoot,
			Bits:           *bits,
		},
		Height:           1697038,
		CoinbaseTx:       coinbaseTx,
		TransactionCount: 1,
		Subtrees:         []*chainhash.Hash{subtree},
	}

	_, _, err = s.StoreBlock(ctx, grandparentBlock, "")
	require.NoError(t, err)

	// Block at height 1697039 (parent) - timestamp 1759594909
	parentBlock := &model.Block{
		Header: &model.BlockHeader{
			Version:        1,
			Timestamp:      1759594909,
			Nonce:          2,
			HashPrevBlock:  grandparentBlock.Hash(),
			HashMerkleRoot: hashMerkleRoot,
			Bits:           *bits,
		},
		Height:           1697039,
		CoinbaseTx:       coinbaseTx2,
		TransactionCount: 1,
		Subtrees:         []*chainhash.Hash{subtree},
	}

	_, _, err = s.StoreBlock(ctx, parentBlock, "")
	require.NoError(t, err)

	// Block at height 1697040 (current) - timestamp 1759594908 (same as grandparent!)
	currentBlock := &model.Block{
		Header: &model.BlockHeader{
			Version:        1,
			Timestamp:      1759594908,
			Nonce:          3,
			HashPrevBlock:  parentBlock.Hash(),
			HashMerkleRoot: hashMerkleRoot,
			Bits:           *bits,
		},
		Height:           1697040,
		CoinbaseTx:       coinbaseTx3,
		TransactionCount: 1,
		Subtrees:         []*chainhash.Hash{subtree},
	}

	_, _, err = s.StoreBlock(ctx, currentBlock, "")
	require.NoError(t, err)

	// Get suitable block for the current block
	suitableBlock, err := s.GetSuitableBlock(ctx, currentBlock.Hash())
	require.NoError(t, err)

	// The sorting algorithm should work as follows:
	// Initial order (with ORDER BY depth DESC): [grandparent(1759594908), parent(1759594909), current(1759594908)]
	// After sorting by timestamp: [grandparent(1759594908), current(1759594908), parent(1759594909)]
	// Median (middle element) = current block
	//
	// This matches bitcoin-sv behavior because:
	// - bitcoin-sv creates array as: blocks[0]=grandparent, blocks[1]=parent, blocks[2]=current
	// - After sorting: [grandparent, current, parent]
	// - Returns blocks[1] which is current
	require.Equal(t, currentBlock.Hash()[:], suitableBlock.Hash, "suitable block should be the current block when timestamps collide")
}
