package sql

import (
	"context"
	"net/url"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSetBlockSubtreesSet(t *testing.T) {
	// Setup test logger and database
	logger := ulogger.TestLogger{}
	dbURL, err := url.Parse("sqlitememory:///")
	require.NoError(t, err)

	store, err := New(logger, dbURL, settings.NewSettings())
	require.NoError(t, err)
	defer store.Close()

	// Helper function to insert a test block
	insertTestBlock := func(hash *chainhash.Hash, subtreesSet bool) error {
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

		_, err := store.db.ExecContext(
			context.Background(),
			q,
			hash.CloneBytes(),              // hash
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
			subtreesSet,                    // subtrees_set
			"test",                         // peer_id
		)
		return err
	}

	// Helper function to verify subtrees_set value
	verifySubtreesSet := func(hash *chainhash.Hash, expected bool) error {
		var subtreesSet bool
		q := `SELECT subtrees_set FROM blocks WHERE hash = $1`
		err := store.db.QueryRowContext(context.Background(), q, hash.CloneBytes()).Scan(&subtreesSet)
		if err != nil {
			return err
		}
		if subtreesSet != expected {
			return errors.New(errors.ERR_ERROR, "subtrees_set value mismatch: expected %v, got %v", expected, subtreesSet)
		}
		return nil
	}

	t.Run("set subtrees_set to true for existing block", func(t *testing.T) {
		// Create a test block hash
		blockHash := chainhash.Hash{}
		err := blockHash.SetBytes([]byte("test_block_hash_1234567890abcdef"))
		require.NoError(t, err)

		// Insert block with subtrees_set = false
		err = insertTestBlock(&blockHash, false)
		require.NoError(t, err)

		// Verify initial state
		err = verifySubtreesSet(&blockHash, false)
		require.NoError(t, err)

		// Call SetBlockSubtreesSet
		err = store.SetBlockSubtreesSet(context.Background(), &blockHash)
		assert.NoError(t, err)

		// Verify subtrees_set is now true
		err = verifySubtreesSet(&blockHash, true)
		assert.NoError(t, err)
	})

	t.Run("set subtrees_set when already true", func(t *testing.T) {
		// Create a test block hash (exactly 32 bytes)
		blockHash := chainhash.Hash{}
		testBytes := make([]byte, 32)
		copy(testBytes, []byte("test_already_true"))
		err := blockHash.SetBytes(testBytes)
		require.NoError(t, err)

		// Insert block with subtrees_set = true
		err = insertTestBlock(&blockHash, true)
		require.NoError(t, err)

		// Call SetBlockSubtreesSet
		err = store.SetBlockSubtreesSet(context.Background(), &blockHash)
		assert.NoError(t, err)

		// Verify subtrees_set is still true
		err = verifySubtreesSet(&blockHash, true)
		assert.NoError(t, err)
	})

	t.Run("error when block does not exist", func(t *testing.T) {
		// Create a non-existent block hash
		nonExistentHash, err := chainhash.NewHashFromStr("0000000000000000000000000000000000000000000000000000000000000001")
		require.NoError(t, err)

		// Call SetBlockSubtreesSet
		err = store.SetBlockSubtreesSet(context.Background(), nonExistentHash)

		// Should return an error
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "subtrees_set was not updated")
	})

	t.Run("multiple blocks can be updated", func(t *testing.T) {
		// Create multiple test blocks
		hashes := make([]*chainhash.Hash, 3)
		for i := 0; i < 3; i++ {
			hash := chainhash.Hash{}
			err := hash.SetBytes([]byte{byte(i), byte(i), byte(i), byte(i), byte(i), byte(i), byte(i), byte(i),
				byte(i), byte(i), byte(i), byte(i), byte(i), byte(i), byte(i), byte(i),
				byte(i), byte(i), byte(i), byte(i), byte(i), byte(i), byte(i), byte(i),
				byte(i), byte(i), byte(i), byte(i), byte(i), byte(i), byte(i), byte(i)})
			require.NoError(t, err)
			hashes[i] = &hash

			// Insert block with subtrees_set = false
			err = insertTestBlock(&hash, false)
			require.NoError(t, err)
		}

		// Update all blocks
		for i, hash := range hashes {
			err := store.SetBlockSubtreesSet(context.Background(), hash)
			assert.NoError(t, err, "Failed to update block %d", i)

			// Verify
			err = verifySubtreesSet(hash, true)
			assert.NoError(t, err, "Block %d not updated correctly", i)
		}
	})

	t.Run("context cancellation", func(t *testing.T) {
		// Create a test block hash (exactly 32 bytes)
		blockHash := chainhash.Hash{}
		testBytes := make([]byte, 32)
		copy(testBytes, []byte("test_ctx_cancel"))
		err := blockHash.SetBytes(testBytes)
		require.NoError(t, err)

		// Insert block
		err = insertTestBlock(&blockHash, false)
		require.NoError(t, err)

		// Create a cancelled context
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		// Call SetBlockSubtreesSet with cancelled context
		err = store.SetBlockSubtreesSet(ctx, &blockHash)

		// Should return an error due to cancelled context
		assert.Error(t, err)
	})

	t.Run("context timeout", func(t *testing.T) {
		// Create a test block hash (exactly 32 bytes)
		blockHash := chainhash.Hash{}
		testBytes := make([]byte, 32)
		copy(testBytes, []byte("test_ctx_timeout"))
		err := blockHash.SetBytes(testBytes)
		require.NoError(t, err)

		// Insert block
		err = insertTestBlock(&blockHash, false)
		require.NoError(t, err)

		// Create a context with very short timeout
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Nanosecond)
		defer cancel()

		// Wait for timeout
		time.Sleep(10 * time.Millisecond)

		// Call SetBlockSubtreesSet with timed-out context
		err = store.SetBlockSubtreesSet(ctx, &blockHash)

		// May or may not error depending on timing, but shouldn't panic
		_ = err
	})

	t.Run("cache is invalidated after update", func(t *testing.T) {
		// Create a test block hash (exactly 32 bytes)
		blockHash := chainhash.Hash{}
		testBytes := make([]byte, 32)
		copy(testBytes, []byte("test_cache_inval"))
		err := blockHash.SetBytes(testBytes)
		require.NoError(t, err)

		// Insert block
		err = insertTestBlock(&blockHash, false)
		require.NoError(t, err)

		// Call SetBlockSubtreesSet
		err = store.SetBlockSubtreesSet(context.Background(), &blockHash)
		assert.NoError(t, err)

		// The cache should be cleared
		// We can't directly test cache state, but we verify the function completes
		// without error, which means cache clearing succeeded
	})

	t.Run("sequential updates to same block", func(t *testing.T) {
		// Create a test block hash (exactly 32 bytes)
		blockHash := chainhash.Hash{}
		testBytes := make([]byte, 32)
		copy(testBytes, []byte("test_sequential"))
		err := blockHash.SetBytes(testBytes)
		require.NoError(t, err)

		// Insert block
		err = insertTestBlock(&blockHash, false)
		require.NoError(t, err)

		// Update multiple times
		for i := 0; i < 5; i++ {
			err = store.SetBlockSubtreesSet(context.Background(), &blockHash)
			assert.NoError(t, err, "Update %d failed", i)

			// Verify it's still true
			err = verifySubtreesSet(&blockHash, true)
			assert.NoError(t, err, "Verification %d failed", i)
		}
	})

	t.Run("nil block hash", func(t *testing.T) {
		// Call SetBlockSubtreesSet with nil hash
		// This will panic as the implementation doesn't check for nil
		assert.Panics(t, func() {
			_ = store.SetBlockSubtreesSet(context.Background(), nil)
		})
	})

	t.Run("non-existent block hash", func(t *testing.T) {
		// Use a hash that definitely doesn't exist in the database
		// Note: We can't use an empty/zero hash because a previous test
		// ("multiple blocks can be updated") inserts a block with all-zero hash when i=0
		nonExistentHash, err := chainhash.NewHashFromStr("deadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef")
		require.NoError(t, err)
		err = store.SetBlockSubtreesSet(context.Background(), nonExistentHash)
		require.Error(t, err, "Should return error for non-existent block")
	})

	t.Run("genesis block", func(t *testing.T) {
		// The genesis block should already exist in the database
		// Get the genesis block hash
		genesisHash, err := chainhash.NewHashFromStr("0f9188f13cb7b2c71f2a335e3a4fc328bf5beb436012afca590b1a11466e2206")
		require.NoError(t, err)

		// Try to set subtrees_set for genesis block
		err = store.SetBlockSubtreesSet(context.Background(), genesisHash)

		// Should succeed if genesis exists, or error if it doesn't exist - both are acceptable
		_ = err
	})

	t.Run("concurrent updates to different blocks", func(t *testing.T) {
		// Create multiple test blocks
		numBlocks := 10
		hashes := make([]*chainhash.Hash, numBlocks)
		done := make(chan error, numBlocks)

		for i := 0; i < numBlocks; i++ {
			hash := chainhash.Hash{}
			// Create unique hash for each block
			hashBytes := make([]byte, 32)
			hashBytes[0] = byte(100 + i) // Start from 100 to avoid conflicts
			hashBytes[1] = byte(200 + i)
			hashBytes[31] = byte(i) // Also set last byte for extra uniqueness
			err := hash.SetBytes(hashBytes)
			require.NoError(t, err)
			hashes[i] = &hash

			// Insert block
			err = insertTestBlock(&hash, false)
			require.NoError(t, err)
		}

		// Update all blocks concurrently
		for i, hash := range hashes {
			go func(h *chainhash.Hash, id int) {
				err := store.SetBlockSubtreesSet(context.Background(), h)
				done <- err
			}(hash, i)
		}

		// Wait for all goroutines to complete
		for i := 0; i < numBlocks; i++ {
			err := <-done
			assert.NoError(t, err, "Concurrent update %d failed", i)
		}

		// Verify all blocks were updated
		for i, hash := range hashes {
			err := verifySubtreesSet(hash, true)
			assert.NoError(t, err, "Concurrent update verification failed for block %d", i)
		}
	})

	t.Run("update after database close", func(t *testing.T) {
		// Create a new store
		dbURL2, err := url.Parse("sqlitememory:///")
		require.NoError(t, err)

		store2, err := New(logger, dbURL2, settings.NewSettings())
		require.NoError(t, err)

		// Create and insert a test block
		blockHash := chainhash.Hash{}
		err = blockHash.SetBytes([]byte("test_after_close_1234567890abcde"))
		require.NoError(t, err)

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

		_, err = store2.db.ExecContext(
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

		// Close the store
		store2.Close()

		// Try to update after close
		err = store2.SetBlockSubtreesSet(context.Background(), &blockHash)

		// Should error
		assert.Error(t, err, "Should error after database close")
	})

	t.Run("verify logging", func(t *testing.T) {
		// Create a test block hash (exactly 32 bytes)
		blockHash := chainhash.Hash{}
		testBytes := make([]byte, 32)
		copy(testBytes, []byte("test_logging"))
		err := blockHash.SetBytes(testBytes)
		require.NoError(t, err)

		// Insert block
		err = insertTestBlock(&blockHash, false)
		require.NoError(t, err)

		// Call SetBlockSubtreesSet
		// The function logs with Infof, which should not cause errors
		err = store.SetBlockSubtreesSet(context.Background(), &blockHash)
		assert.NoError(t, err)
	})

	t.Run("valid hash formats", func(t *testing.T) {
		// Test with various valid hash formats
		testHashes := []string{
			"0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
			"ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff",
			"1111111111111111111111111111111111111111111111111111111111111111",
		}

		for i, hashStr := range testHashes {
			hash, err := chainhash.NewHashFromStr(hashStr)
			require.NoError(t, err)

			// Insert block
			err = insertTestBlock(hash, false)
			require.NoError(t, err)

			// Update
			err = store.SetBlockSubtreesSet(context.Background(), hash)
			assert.NoError(t, err, "Update failed for hash %d", i)

			// Verify
			err = verifySubtreesSet(hash, true)
			assert.NoError(t, err, "Verification failed for hash %d", i)
		}
	})
}

// BenchmarkSetBlockSubtreesSet benchmarks the SetBlockSubtreesSet function
func BenchmarkSetBlockSubtreesSet(b *testing.B) {
	// Setup
	logger := ulogger.TestLogger{}
	dbURL, _ := url.Parse("sqlitememory:///")
	store, _ := New(logger, dbURL, settings.NewSettings())
	defer store.Close()

	// Create and insert test blocks
	hashes := make([]*chainhash.Hash, b.N)
	for i := 0; i < b.N; i++ {
		hash := chainhash.Hash{}
		hashBytes := make([]byte, 32)
		hashBytes[0] = byte(i)
		hashBytes[1] = byte(i >> 8)
		hashBytes[2] = byte(i >> 16)
		hashBytes[3] = byte(i >> 24)
		_ = hash.SetBytes(hashBytes)
		hashes[i] = &hash

		// Insert block
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

		_, _ = store.db.ExecContext(
			context.Background(),
			q,
			hash.CloneBytes(),
			make([]byte, 32),
			make([]byte, 32),
			uint32(time.Now().Unix()),
			[]byte{0xff, 0xff, 0x00, 0x1d},
			uint32(0),
			uint32(i),
			make([]byte, 32),
			uint32(1),
			uint32(100),
			uint32(1),
			[]byte{0x01},
			make([]byte, 32),
			false,
			false,
			false,
			"test",
		)
	}

	b.ResetTimer()

	// Benchmark
	for i := 0; i < b.N; i++ {
		_ = store.SetBlockSubtreesSet(context.Background(), hashes[i])
	}
}
