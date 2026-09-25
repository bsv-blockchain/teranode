package sql

import (
	"context"
	"net/url"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// The walk follows parent links from whichever hash it is given, so two fork tips at the same
// height each return their own lineage, and neither answer depends on which of them the store
// currently flags as the main chain.
func TestGetBlockHeadersByParentLinksFollowsEachForkTipsOwnLineage(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)

	storeURL, err := url.Parse("sqlitememory:///")
	require.NoError(t, err)

	s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
	require.NoError(t, err)

	ctx := context.Background()

	_, _, err = s.StoreBlock(ctx, block1, "test_peer")
	require.NoError(t, err)

	_, _, err = s.StoreBlock(ctx, block2, "test_peer")
	require.NoError(t, err)

	// A competing block at height 2 on the same parent. It is stored second, so block2 stays the
	// best block and blockAlternative2 is the fork tip.
	_, _, err = s.StoreBlock(ctx, blockAlternative2, "test_peer")
	require.NoError(t, err)

	for _, tip := range []struct {
		name string
		hash *chainhash.Hash
	}{
		{"best tip", block2.Hash()},
		{"fork tip", blockAlternative2.Hash()},
	} {
		t.Run(tip.name, func(t *testing.T) {
			headers, metas, err := s.GetBlockHeadersByParentLinks(ctx, tip.hash, 3)
			require.NoError(t, err)
			require.Len(t, headers, 3)
			require.Len(t, metas, 3)

			require.Equal(t, tip.hash, headers[0].Hash(), "the walk starts at the hash it was given")
			require.Equal(t, uint32(2), metas[0].Height)

			require.Equal(t, block1.Hash(), headers[1].Hash())
			require.Equal(t, uint32(1), metas[1].Height)

			require.Equal(t, hashPrevBlock, headers[2].Hash(), "the network's genesis block, which block 1 names as its parent")
			require.Equal(t, uint32(0), metas[2].Height)

			// Every step is proven by the child's previous hash naming the parent.
			for i := 0; i+1 < len(headers); i++ {
				require.Equal(t, headers[i+1].Hash(), headers[i].HashPrevBlock)
			}
		})
	}
}

// Asking for more headers than the chain holds stops at genesis, and asking from a hash the
// store does not hold returns nothing. Neither is an error: the caller proves the length.
func TestGetBlockHeadersByParentLinksShortAnswers(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)

	storeURL, err := url.Parse("sqlitememory:///")
	require.NoError(t, err)

	s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
	require.NoError(t, err)

	ctx := context.Background()

	_, _, err = s.StoreBlock(ctx, block1, "test_peer")
	require.NoError(t, err)

	headers, _, err := s.GetBlockHeadersByParentLinks(ctx, block1.Hash(), 10)
	require.NoError(t, err)
	require.Len(t, headers, 2, "block 1 and genesis, then the walk ends")

	headers, metas, err := s.GetBlockHeadersByParentLinks(ctx, block3Hash, 10)
	require.NoError(t, err)
	require.Empty(t, headers)
	require.Empty(t, metas)
}
