package state

import (
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

// TestGetLastBlockHeightEmptyFile validates behavior when reading from an empty state file
func TestGetLastBlockHeightEmptyFile(t *testing.T) {
	store := New(ulogger.NewVerboseTestLogger(t), t.TempDir()+"/blocks.dat")

	height, err := store.GetLastPersistedBlockHeight()
	require.NoError(t, err)
	require.Zero(t, height)
}

// TestGetLastBlockHeight validates the block height tracking functionality
func TestGetLastBlockHeight(t *testing.T) {
	store := New(ulogger.NewVerboseTestLogger(t), t.TempDir()+"/blocks.dat")

	err := store.AddBlock(1, "0000000000000000000000000000000000000000000000000000000000000001")
	require.NoError(t, err)

	height, err := store.GetLastPersistedBlockHeight()
	require.NoError(t, err)
	require.Equal(t, uint32(1), height)

	err = store.AddBlock(2, "0000000000000000000000000000000000000000000000000000000000000002")
	require.NoError(t, err)

	err = store.AddBlock(3, "0000000000000000000000000000000000000000000000000000000000000003")
	require.NoError(t, err)

	height, err = store.GetLastPersistedBlockHeight()
	require.NoError(t, err)
	require.Equal(t, uint32(3), height)
}

// TestRollbackToHash validates rolling back to a specific block hash
func TestRollbackToHash(t *testing.T) {
	store := New(ulogger.NewVerboseTestLogger(t), t.TempDir()+"/blocks.dat")

	// Add multiple blocks
	hash1, _ := chainhash.NewHashFromStr("0000000000000000000000000000000000000000000000000000000000000001")
	hash2, _ := chainhash.NewHashFromStr("0000000000000000000000000000000000000000000000000000000000000002")
	hash3, _ := chainhash.NewHashFromStr("0000000000000000000000000000000000000000000000000000000000000003")
	hash4, _ := chainhash.NewHashFromStr("0000000000000000000000000000000000000000000000000000000000000004")
	hash5, _ := chainhash.NewHashFromStr("0000000000000000000000000000000000000000000000000000000000000005")

	err := store.AddBlock(1, hash1.String())
	require.NoError(t, err)

	err = store.AddBlock(2, hash2.String())
	require.NoError(t, err)

	err = store.AddBlock(3, hash3.String())
	require.NoError(t, err)

	err = store.AddBlock(4, hash4.String())
	require.NoError(t, err)

	err = store.AddBlock(5, hash5.String())
	require.NoError(t, err)

	// Verify we have 5 blocks
	height, hash, err := store.GetLastPersistedBlock()
	require.NoError(t, err)
	require.Equal(t, uint32(5), height)
	require.Equal(t, hash5.String(), hash.String())

	// Rollback to hash at height 2
	err = store.RollbackToHash(hash2)
	require.NoError(t, err)

	// Verify last block is now height 2 with correct hash
	height, hash, err = store.GetLastPersistedBlock()
	require.NoError(t, err)
	require.Equal(t, uint32(2), height)
	require.Equal(t, hash2.String(), hash.String())

	// Verify we can add new blocks after rollback
	err = store.AddBlock(3, hash3.String())
	require.NoError(t, err)

	height, hash, err = store.GetLastPersistedBlock()
	require.NoError(t, err)
	require.Equal(t, uint32(3), height)
	require.Equal(t, hash3.String(), hash.String())
}

// TestRollbackToHashNotFound validates error when hash is not in state file
func TestRollbackToHashNotFound(t *testing.T) {
	store := New(ulogger.NewVerboseTestLogger(t), t.TempDir()+"/blocks.dat")

	// Add blocks
	err := store.AddBlock(1, "0000000000000000000000000000000000000000000000000000000000000001")
	require.NoError(t, err)

	err = store.AddBlock(2, "0000000000000000000000000000000000000000000000000000000000000002")
	require.NoError(t, err)

	// Try to rollback to hash that doesn't exist
	unknownHash, _ := chainhash.NewHashFromStr("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	err = store.RollbackToHash(unknownHash)
	require.Error(t, err)
	require.Contains(t, err.Error(), "hash not found in state file")

	// Verify state wasn't changed
	height, hash, err := store.GetLastPersistedBlock()
	require.NoError(t, err)
	require.Equal(t, uint32(2), height)
	require.Equal(t, "0000000000000000000000000000000000000000000000000000000000000002", hash.String())
}

// TestRollbackToHashWithDuplicateHeights validates rollback with duplicate heights
func TestRollbackToHashWithDuplicateHeights(t *testing.T) {
	store := New(ulogger.NewVerboseTestLogger(t), t.TempDir()+"/blocks.dat")

	// Simulate a scenario where state file somehow has duplicate heights
	// (This shouldn't happen in normal operation, but test defensive behavior)
	hash1, _ := chainhash.NewHashFromStr("0000000000000000000000000000000000000000000000000000000000000001")
	hash2, _ := chainhash.NewHashFromStr("0000000000000000000000000000000000000000000000000000000000000002")
	hash3A, _ := chainhash.NewHashFromStr("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	hash3B, _ := chainhash.NewHashFromStr("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")

	err := store.AddBlock(1, hash1.String())
	require.NoError(t, err)

	err = store.AddBlock(2, hash2.String())
	require.NoError(t, err)

	err = store.AddBlock(3, hash3A.String())
	require.NoError(t, err)

	err = store.AddBlock(3, hash3B.String()) // Duplicate height, different hash
	require.NoError(t, err)

	// Rollback to hash2 - should remove both entries at height 3
	err = store.RollbackToHash(hash2)
	require.NoError(t, err)

	// Verify last block is height 2 with correct hash
	height, hash, err := store.GetLastPersistedBlock()
	require.NoError(t, err)
	require.Equal(t, uint32(2), height)
	require.Equal(t, hash2.String(), hash.String())
}
