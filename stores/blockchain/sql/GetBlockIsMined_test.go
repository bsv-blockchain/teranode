package sql

import (
	"context"
	"net/url"
	"testing"

	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/model"
	"github.com/bitcoin-sv/teranode/settings"
	"github.com/bitcoin-sv/teranode/stores/blockchain/options"
	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetBlockIsMined(t *testing.T) {
	// Setup test logger and database
	logger := ulogger.TestLogger{}
	dbURL, err := url.Parse("sqlitememory:///")
	require.NoError(t, err)

	store, err := New(logger, dbURL, settings.NewSettings())
	require.NoError(t, err)
	defer store.Close()

	// Get genesis block to use as previous block
	genesisBlock, err := store.GetBlockByID(context.Background(), 0)
	require.NoError(t, err)

	// Create test data that properly references the genesis block
	hashMerkleRoot, err := chainhash.NewHashFromStr("d1de05a65845a49ad63eed887c4cf7cc824e02b5d10de82829f740b748b9737f")
	require.NoError(t, err)

	bits, err := model.NewNBitFromString("207fffff")
	require.NoError(t, err)

	coinbaseTx, err := bt.NewTxFromString("01000000010000000000000000000000000000000000000000000000000000000000000000ffffffff17030100002f6d312d65752fb670097da68d1b768d8b21f6ffffffff03ac505763000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88acaa505763000000001976a9143c22b6d9ba7b50b6d6e615c69d11ecb2ba3db14588acaa505763000000001976a914b7177c7deb43f3869eabc25cfd9f618215f34d5588ac00000000")
	require.NoError(t, err)

	subtree, err := chainhash.NewHashFromStr("0e3e2357e806b6cdb1f70b54c3a3a17b6714ee1f0e68bebb44a74b1efd512098")
	require.NoError(t, err)

	testBlock := &model.Block{
		Header: &model.BlockHeader{
			Version:        1,
			Timestamp:      1729259727,
			Nonce:          0,
			HashPrevBlock:  genesisBlock.Hash(),
			HashMerkleRoot: hashMerkleRoot,
			Bits:           *bits,
		},
		Height:           1,
		CoinbaseTx:       coinbaseTx,
		TransactionCount: 1,
		Subtrees: []*chainhash.Hash{
			subtree,
		},
	}

	testCases := []struct {
		name      string
		blockHash *chainhash.Hash
		setup     func(*SQL) error
		expected  bool
		expectErr *errors.Error
	}{
		{
			name:      "Block is mined",
			blockHash: testBlock.Hash(),
			setup: func(store *SQL) error {
				_, _, err := store.StoreBlock(context.Background(), testBlock, "test", options.WithMinedSet(true))
				return err
			},
			expected:  true,
			expectErr: nil,
		},
		{
			name:      "Block is not mined",
			blockHash: testBlock.Hash(),
			setup: func(store *SQL) error {
				_, _, err := store.StoreBlock(context.Background(), testBlock, "test", options.WithMinedSet(false))
				return err
			},
			expected:  false,
			expectErr: nil,
		},
		{
			name:      "Block not found",
			blockHash: testBlock.Hash(),
			setup: func(store *SQL) error {
				return nil
			},
			expected:  false,
			expectErr: errors.ErrBlockNotFound,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Setup test logger and database for each subtest
			logger := ulogger.TestLogger{}
			dbURL, err := url.Parse("sqlitememory:///")
			require.NoError(t, err)

			subStore, err := New(logger, dbURL, settings.NewSettings())
			require.NoError(t, err)
			defer subStore.Close()

			err = tc.setup(subStore)
			require.NoError(t, err)

			isMined, err := subStore.GetBlockIsMined(context.Background(), tc.blockHash)
			if tc.expectErr != nil {
				assert.Error(t, err)
				assert.True(t, errors.Is(err, tc.expectErr))
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tc.expected, isMined)
			}
		})
	}
}
