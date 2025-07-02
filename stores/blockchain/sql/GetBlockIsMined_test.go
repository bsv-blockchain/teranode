package sql

import (
	"context"
	"net/url"
	"testing"

	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/settings"
	"github.com/bitcoin-sv/teranode/stores/blockchain/options"
	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetBlockIsMined(t *testing.T) {
	testCases := []struct {
		name      string
		blockHash *chainhash.Hash
		setup     func(*SQL) error
		expected  bool
		expectErr *errors.Error
	}{
		{
			name:      "Block is mined",
			blockHash: block1.Hash(),
			setup: func(store *SQL) error {
				_, _, err := store.StoreBlock(context.Background(), block1, "test", options.WithMinedSet(true))
				return err
			},
			expected:  true,
			expectErr: nil,
		},
		{
			name:      "Block is not mined",
			blockHash: block1.Hash(),
			setup: func(store *SQL) error {
				_, _, err := store.StoreBlock(context.Background(), block1, "test", options.WithMinedSet(false))
				return err
			},
			expected:  false,
			expectErr: nil,
		},
		{
			name:      "Block not found",
			blockHash: block1.Hash(),
			setup: func(store *SQL) error {
				return nil
			},
			expected:  false,
			expectErr: errors.ErrBlockNotFound,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Setup test logger and database
			logger := ulogger.TestLogger{}
			dbURL, err := url.Parse("sqlitememory:///")
			require.NoError(t, err)

			store, err := New(logger, dbURL, settings.NewSettings())
			require.NoError(t, err)
			defer store.Close()

			err = tc.setup(store)
			require.NoError(t, err)

			isMined, err := store.GetBlockIsMined(context.Background(), tc.blockHash)
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
