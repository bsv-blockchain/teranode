package sql

import (
	"context"
	"net/url"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

func TestSQL_GetBlockInChainByHeightHash(t *testing.T) {
	// Setup test database
	store := setupTestStore(t)
	ctx := context.Background()

	// Create a fork structure like this:
	//
	// Block0 -> Block1 -> Block2A -> Block3A
	//                  -> Block2B -> Block3B
	//
	// Where Block2A and Block2B are at the same height but different chains

	block0, err := store.GetBlockByHeight(ctx, 0)
	if err != nil {
		t.Fatalf("Failed to find genesis block: %v", err)
	}

	block1 := createTestBlock(t, 1, block0.Hash())

	// Create fork A
	block2A := createTestBlock(t, 2, block1.Hash())
	block3A := createTestBlock(t, 3, block2A.Hash())

	// Create fork B
	block2B := createTestBlock(t, 4, block1.Hash())
	block3B := createTestBlock(t, 5, block2B.Hash())

	// Store all blocks
	blocks := []*model.Block{block1, block2A, block3A, block2B, block3B}
	for _, block := range blocks {
		_, _, err := store.StoreBlock(ctx, block, "")
		require.NoError(t, err)
	}

	tests := []struct {
		name      string
		height    uint32
		startHash *chainhash.Hash
		wantBlock *model.Block
		wantErr   bool
	}{
		{
			name:      "get block2A using block3A as start",
			height:    2,
			startHash: block3A.Hash(),
			wantBlock: block2A,
			wantErr:   false,
		},
		{
			name:      "get block2B using block3B as start",
			height:    2,
			startHash: block3B.Hash(),
			wantBlock: block2B,
			wantErr:   false,
		},
		{
			name:      "get common ancestor block1 from fork A",
			height:    1,
			startHash: block3A.Hash(),
			wantBlock: block1,
			wantErr:   false,
		},
		{
			name:      "get common ancestor block1 from fork B",
			height:    1,
			startHash: block3B.Hash(),
			wantBlock: block1,
			wantErr:   false,
		},
		{
			name:      "invalid height returns error",
			height:    99,
			startHash: block3A.Hash(),
			wantBlock: nil,
			wantErr:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, _, err := store.GetBlockInChainByHeightHash(ctx, tt.height, tt.startHash)
			if tt.wantErr {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)
			require.NotNil(t, got)
			require.Equal(t, tt.wantBlock.Hash().String(), got.Hash().String())
		})
	}
}

// TestSQL_GetBlockInChainByHeightHash_FastPathEquivalence verifies the #1018
// on_main_chain fast path returns results identical to the recursive CTE for a
// start hash that is on the main chain, and that the mainChainRebuilding guard
// forces the CTE path. It is independent of which fork wins the chain-work tie:
// the main-chain tip is discovered dynamically.
func TestSQL_GetBlockInChainByHeightHash_FastPathEquivalence(t *testing.T) {
	store := setupTestStore(t)
	ctx := context.Background()

	block0, err := store.GetBlockByHeight(ctx, 0)
	require.NoError(t, err)

	// Linear main chain plus a competing fork off block1, so on_main_chain is
	// meaningfully set for some rows and false for others.
	block1 := createTestBlock(t, 1, block0.Hash())
	block2 := createTestBlock(t, 2, block1.Hash())
	block3 := createTestBlock(t, 3, block2.Hash())
	forkB := createTestBlock(t, 99, block1.Hash()) // sibling of block2, off main

	for _, b := range []*model.Block{block1, block2, block3, forkB} {
		_, _, err := store.StoreBlock(ctx, b, "")
		require.NoError(t, err)
	}

	// Discover the actual main-chain tip (chain-work tie-break is the store's
	// business, not this test's assumption).
	_, tipMeta, err := store.GetBestBlockHeader(ctx)
	require.NoError(t, err)
	tipHeight := tipMeta.Height

	tip, err := store.GetBlockByHeight(ctx, tipHeight)
	require.NoError(t, err)
	tipHash := tip.Hash()

	// Sanity: the preflight must see the tip as on the main chain.
	var tipOnMain bool
	require.NoError(t, store.db.QueryRowContext(ctx,
		`SELECT COALESCE((SELECT on_main_chain FROM blocks WHERE hash = $1 LIMIT 1), false)`,
		tipHash.CloneBytes(),
	).Scan(&tipOnMain))
	require.True(t, tipOnMain, "main-chain tip must have on_main_chain=true")

	for h := uint32(0); h <= tipHeight; h++ {
		// Fast path (guard == 0).
		require.Equal(t, int32(0), store.mainChainRebuilding.Load())
		fast, _, err := store.GetBlockInChainByHeightHash(ctx, h, tipHash)
		require.NoError(t, err, "fast path height %d", h)
		store.responseCache.DeleteAll() // avoid the cache masking the CTE branch

		// CTE path (guard > 0 forces fallback even with an on-main start).
		store.mainChainRebuilding.Add(1)
		cte, _, err := store.GetBlockInChainByHeightHash(ctx, h, tipHash)
		store.mainChainRebuilding.Add(-1)
		require.NoError(t, err, "cte path height %d", h)
		store.responseCache.DeleteAll()

		require.Equal(t, fast.Hash().String(), cte.Hash().String(),
			"fast path and CTE disagree at height %d", h)

		// Both must equal the canonical main-chain block at that height.
		mainByHeight, err := store.GetBlockByHeight(ctx, h)
		require.NoError(t, err)
		require.Equal(t, mainByHeight.Hash().String(), fast.Hash().String(),
			"fast path is not the main-chain block at height %d", h)
	}

	// Edge: height == startHash.height returns the start block itself.
	startAtTip, _, err := store.GetBlockInChainByHeightHash(ctx, tipHeight, tipHash)
	require.NoError(t, err)
	require.Equal(t, tipHash.String(), startAtTip.Hash().String())

	// Edge: height > startHash.height must yield BlockNotFound on the fast path,
	// identical to the CTE (which never walks above the seed height).
	_, _, err = store.GetBlockInChainByHeightHash(ctx, tipHeight+1, tipHash)
	require.Error(t, err)
}

// Helper function to create a test block
func createTestBlock(t *testing.T, nonce uint32, previousHash *chainhash.Hash) *model.Block {
	t.Helper()

	coinbase, err := bt.NewTxFromString(model.CoinbaseHex)
	require.NoError(t, err)

	bits, err := model.NewNBitFromString("1d00ffff")
	require.NoError(t, err)

	block := &model.Block{
		Header: &model.BlockHeader{
			Version:        1,
			Timestamp:      uint32(time.Now().Unix()), // nolint:gosec
			Nonce:          nonce,
			Bits:           *bits,
			HashPrevBlock:  previousHash,
			HashMerkleRoot: &chainhash.Hash{},
		},
		CoinbaseTx:       coinbase,
		TransactionCount: 1,
		SizeInBytes:      80,
	}

	return block
}

// Helper function to set up the test store
func setupTestStore(t *testing.T) *SQL {
	t.Helper()

	tSettings := test.CreateBaseTestSettings(t)

	storeURL, err := url.Parse("sqlitememory:///")
	require.NoError(t, err)

	store, err := New(ulogger.TestLogger{}, storeURL, tSettings)
	require.NoError(t, err)

	return store
}
