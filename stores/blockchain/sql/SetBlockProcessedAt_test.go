package sql

import (
	"context"
	"net/url"
	"testing"
	"time"

	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/settings"
	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSetBlockProcessedAt(t *testing.T) {
	// Setup test logger and database
	logger := ulogger.TestLogger{}
	dbURL, err := url.Parse("sqlitememory:///")
	require.NoError(t, err)

	store, err := New(logger, dbURL, settings.NewSettings())
	require.NoError(t, err)
	defer store.Close()

	// Create a block directly in the database to bypass validation
	blockHash := chainhash.Hash{}
	err = blockHash.SetBytes([]byte("0123456789abcdef0123456789abcdef"))
	require.NoError(t, err)

	// Insert a block directly into the database
	q := `
		INSERT INTO blocks (
			version, hash, previous_hash, merkle_root, block_time, n_bits, nonce, 
			height, chain_work, tx_count, size_in_bytes, subtree_count, subtrees, 
			coinbase_tx, invalid, mined_set, subtrees_set, peer_id
		) VALUES (
			1, $1, $2, $3, $4, $5, $6, 
			$7, $8, $9, $10, $11, $12, 
			$13, $14, $15, $16, $17
		)
	`

	//nolint:gosec
	_, err = store.db.ExecContext(
		context.Background(),
		q,
		blockHash.CloneBytes(),         // hash
		make([]byte, 32),               // previous_hash
		make([]byte, 32),               // merkle_root
		uint32(time.Now().Unix()),      // block_time
		[]byte{0xff, 0xff, 0x00, 0x1d}, // n_bits
		uint32(0),                      // nonce
		uint32(1),                      // height
		make([]byte, 32),               // chain_work
		uint32(1),                      // tx_count
		uint32(100),                    // size_in_bytes
		uint32(1),                      // subtree_count
		[]byte{0x01},                   // subtrees
		make([]byte, 32),               // coinbase_tx
		false,                          // invalid
		false,                          // mined_set
		false,                          // subtrees_set
		"test",                         // peer_id
	)
	require.NoError(t, err)

	// Create a non-existent block hash for testing
	nonExistentHash, _ := chainhash.NewHashFromStr("0000000000000000000000000000000000000000000000000000000000000001")

	testCases := []struct {
		name      string
		blockHash *chainhash.Hash
		clear     bool
		expectErr bool
		errType   error
	}{
		{
			name:      "Set processed_at timestamp",
			blockHash: &blockHash,
			clear:     false,
			expectErr: false,
		},
		{
			name:      "Clear processed_at timestamp",
			blockHash: &blockHash,
			clear:     true,
			expectErr: false,
		},
		{
			name:      "Block not found",
			blockHash: nonExistentHash,
			clear:     false,
			expectErr: true,
			errType:   errors.ErrStorageError,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// If this is the second test case (clear timestamp), first set the timestamp
			if tc.name == "Clear processed_at timestamp" {
				err := store.SetBlockProcessedAt(context.Background(), tc.blockHash)
				require.NoError(t, err, "Failed to set timestamp before clearing test")
			}

			// Test SetBlockProcessedAt
			err := store.SetBlockProcessedAt(context.Background(), tc.blockHash, tc.clear)

			if tc.expectErr {
				assert.Error(t, err)

				if tc.errType != nil {
					assert.True(t, errors.Is(err, tc.errType))
				}
			} else {
				assert.NoError(t, err)

				// Verify the processed_at field was updated correctly
				// We need to query the database directly since there's no GetBlockProcessedAt method
				var processedAt interface{}

				q := `SELECT processed_at FROM blocks WHERE hash = $1`

				err = store.db.QueryRowContext(context.Background(), q, tc.blockHash.CloneBytes()).Scan(&processedAt)
				require.NoError(t, err)

				if tc.clear {
					// If clear is true, processed_at should be NULL
					assert.Nil(t, processedAt)
				} else {
					// If clear is false, processed_at should not be NULL
					assert.NotNil(t, processedAt)
				}
			}
		})
	}
}
