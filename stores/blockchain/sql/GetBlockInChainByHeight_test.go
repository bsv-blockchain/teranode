package sql

import (
	"context"
	"net/url"
	"testing"
	"time"

	"github.com/bitcoin-sv/teranode/model"
	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/bitcoin-sv/teranode/util/test"
	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
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

	tSettings := test.CreateBaseTestSettings()

	storeURL, err := url.Parse("sqlitememory:///")
	require.NoError(t, err)

	store, err := New(ulogger.TestLogger{}, storeURL, tSettings)
	require.NoError(t, err)

	return store
}
