package sql

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	customtime "github.com/bsv-blockchain/teranode/model/time"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/usql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// createMockSQL creates a SQL instance with mocked database for testing GetBlockHeadersByHeight
func createMockSQL() (*SQL, sqlmock.Sqlmock, error) {
	db, mock, err := sqlmock.New()
	if err != nil {
		return nil, nil, err
	}

	// Wrap the sql.DB in usql.DB
	udb := &usql.DB{DB: db}

	tSettings := &settings.Settings{}

	s := &SQL{
		db:            udb,
		logger:        ulogger.TestLogger{},
		responseCache: NewGenerationalCache(),
		cacheTTL:      2 * time.Minute,
		chainParams:   tSettings.ChainCfgParams,
	}

	return s, mock, nil
}

// Test successful retrieval of headers within height range
func TestGetBlockHeadersByHeight_Success(t *testing.T) {
	s, mock, err := createMockSQL()
	require.NoError(t, err)
	defer s.db.Close()

	ctx := context.Background()

	// Setup mock expectations
	rows := sqlmock.NewRows([]string{
		"version", "block_time", "nonce", "previous_hash", "merkle_root", "n_bits",
		"id", "height", "tx_count", "size_in_bytes", "peer_id", "block_time", "inserted_at", "median_time_past",
	}).
		AddRow(1, int64(1729259727), uint32(0), hashPrevBlock.CloneBytes(), hashMerkleRoot.CloneBytes(), bits.CloneBytes(),
			uint32(1), uint32(1), int64(1), int64(1000), "test_peer", int64(1729259727), customtime.CustomTime{}, uint32(0)).
		AddRow(1, int64(1729259727), uint32(1), block2PrevBlockHash.CloneBytes(), block2MerkleRootHash.CloneBytes(), bits.CloneBytes(),
			uint32(2), uint32(2), int64(1), int64(1000), "test_peer", int64(1729259727), customtime.CustomTime{}, uint32(0))

	mock.ExpectQuery(`SELECT(.+)FROM blocks b(.+)`).
		WithArgs(uint32(1), uint32(2)).
		WillReturnRows(rows)

	// Test getting headers from height 1 to 2
	headers, metas, err := s.GetBlockHeadersByHeight(ctx, 1, 2)

	// Assertions
	require.NoError(t, err)
	assert.NotNil(t, headers)
	assert.NotNil(t, metas)
	assert.Equal(t, 2, len(headers))
	assert.Equal(t, len(headers), len(metas))

	// Verify headers are in ascending height order
	for i := 1; i < len(metas); i++ {
		assert.LessOrEqual(t, metas[i-1].Height, metas[i].Height)
	}

	// Verify all headers are within the requested range
	for _, meta := range metas {
		assert.GreaterOrEqual(t, meta.Height, uint32(1))
		assert.LessOrEqual(t, meta.Height, uint32(2))
	}

	// Verify header structure integrity
	for i, header := range headers {
		assert.NotNil(t, header.HashPrevBlock)
		assert.NotNil(t, header.HashMerkleRoot)
		assert.Greater(t, header.Version, uint32(0))

		meta := metas[i]
		assert.Greater(t, meta.Height, uint32(0))
		assert.Greater(t, meta.ID, uint32(0))
		assert.NotEmpty(t, meta.PeerID)
	}

	// Ensure all expectations were met
	require.NoError(t, mock.ExpectationsWereMet())
}

// Test empty result when no blocks exist in range
func TestGetBlockHeadersByHeight_EmptyResult(t *testing.T) {
	s, mock, err := createMockSQL()
	require.NoError(t, err)
	defer s.db.Close()

	ctx := context.Background()

	// Setup mock expectations - return empty result
	rows := sqlmock.NewRows([]string{
		"version", "block_time", "nonce", "previous_hash", "merkle_root", "n_bits",
		"id", "height", "tx_count", "size_in_bytes", "peer_id", "block_time", "inserted_at", "median_time_past",
	})

	mock.ExpectQuery(`SELECT(.+)FROM blocks b(.+)`).
		WithArgs(uint32(100), uint32(200)).
		WillReturnRows(rows)

	// Request headers from height 100-200 (should be empty)
	headers, metas, err := s.GetBlockHeadersByHeight(ctx, 100, 200)

	// Should return empty slices with no error
	require.NoError(t, err)
	assert.Empty(t, headers)
	assert.Empty(t, metas)

	require.NoError(t, mock.ExpectationsWereMet())
}

// Test with same start and end height
func TestGetBlockHeadersByHeight_SameHeight(t *testing.T) {
	s, mock, err := createMockSQL()
	require.NoError(t, err)
	defer s.db.Close()

	ctx := context.Background()

	// Setup mock expectations - return single block at height 2
	rows := sqlmock.NewRows([]string{
		"version", "block_time", "nonce", "previous_hash", "merkle_root", "n_bits",
		"id", "height", "tx_count", "size_in_bytes", "peer_id", "block_time", "inserted_at", "median_time_past",
	}).
		AddRow(1, int64(1729259727), uint32(1), block2PrevBlockHash.CloneBytes(), block2MerkleRootHash.CloneBytes(), bits.CloneBytes(),
			uint32(2), uint32(2), int64(1), int64(1000), "test_peer", int64(1729259727), customtime.CustomTime{}, uint32(0))

	mock.ExpectQuery(`SELECT(.+)FROM blocks b(.+)`).
		WithArgs(uint32(2), uint32(2)).
		WillReturnRows(rows)

	// Request headers for exactly height 2
	headers, metas, err := s.GetBlockHeadersByHeight(ctx, 2, 2)

	require.NoError(t, err)
	assert.NotEmpty(t, headers)
	assert.NotEmpty(t, metas)
	assert.Equal(t, 1, len(headers))
	assert.Equal(t, len(headers), len(metas))

	// All results should be at height 2
	for _, meta := range metas {
		assert.Equal(t, uint32(2), meta.Height)
	}

	require.NoError(t, mock.ExpectationsWereMet())
}

// Test with startHeight > endHeight (reverse range)
func TestGetBlockHeadersByHeight_ReverseRange(t *testing.T) {
	s, mock, err := createMockSQL()
	require.NoError(t, err)
	defer s.db.Close()

	ctx := context.Background()

	// Setup mock expectations - return empty result for impossible range
	rows := sqlmock.NewRows([]string{
		"version", "block_time", "nonce", "previous_hash", "merkle_root", "n_bits",
		"id", "height", "tx_count", "size_in_bytes", "peer_id", "block_time", "inserted_at", "median_time_past",
	})

	mock.ExpectQuery(`SELECT(.+)FROM blocks b(.+)`).
		WithArgs(uint32(2), uint32(1)).
		WillReturnRows(rows)

	// Request with startHeight > endHeight
	headers, metas, err := s.GetBlockHeadersByHeight(ctx, 2, 1)

	// Should return empty results (no blocks match the impossible range)
	require.NoError(t, err)
	assert.Empty(t, headers)
	assert.Empty(t, metas)

	require.NoError(t, mock.ExpectationsWereMet())
}

// Test context cancellation
func TestGetBlockHeadersByHeight_ContextCancellation(t *testing.T) {
	s, mock, err := createMockSQL()
	require.NoError(t, err)
	defer s.db.Close()

	// Create cancelled context
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	// Mock should expect a query but will be cancelled
	mock.ExpectQuery(`SELECT(.+)FROM blocks b(.+)`).
		WithArgs(uint32(1), uint32(10)).
		WillReturnError(context.Canceled)

	// Should return error due to cancelled context
	_, _, err = s.GetBlockHeadersByHeight(ctx, 1, 10)

	assert.Error(t, err)
}

// Test with zero height range (edge case)
func TestGetBlockHeadersByHeight_ZeroHeight(t *testing.T) {
	s, mock, err := createMockSQL()
	require.NoError(t, err)
	defer s.db.Close()

	ctx := context.Background()

	// Setup mock expectations - return genesis block
	genesisHash := &chainhash.Hash{}
	genesisMerkleRoot, _ := chainhash.NewHashFromStr("4a5e1e4baab89f3a32518a88c31bc87f618f76673e2cc77ab2127b7afdeda33b")

	rows := sqlmock.NewRows([]string{
		"version", "block_time", "nonce", "previous_hash", "merkle_root", "n_bits",
		"id", "height", "tx_count", "size_in_bytes", "peer_id", "block_time", "inserted_at", "median_time_past",
	}).
		AddRow(1, int64(1296688602), uint32(2), genesisHash.CloneBytes(), genesisMerkleRoot.CloneBytes(), bits.CloneBytes(),
			uint32(0), uint32(0), int64(1), int64(285), "", int64(1296688602), customtime.CustomTime{}, uint32(0))

	mock.ExpectQuery(`SELECT(.+)FROM blocks b(.+)`).
		WithArgs(uint32(0), uint32(0)).
		WillReturnRows(rows)

	// Request genesis block (height 0)
	headers, metas, err := s.GetBlockHeadersByHeight(ctx, 0, 0)

	require.NoError(t, err)
	assert.NotEmpty(t, headers) // Genesis block should exist
	assert.NotEmpty(t, metas)
	assert.Equal(t, len(headers), len(metas))

	// Verify genesis block properties
	for _, meta := range metas {
		assert.Equal(t, uint32(0), meta.Height)
	}

	require.NoError(t, mock.ExpectationsWereMet())
}

// Test with large height range
func TestGetBlockHeadersByHeight_LargeRange(t *testing.T) {
	s, mock, err := createMockSQL()
	require.NoError(t, err)
	defer s.db.Close()

	ctx := context.Background()

	// Setup mock expectations - return a few blocks out of the large range
	rows := sqlmock.NewRows([]string{
		"version", "block_time", "nonce", "previous_hash", "merkle_root", "n_bits",
		"id", "height", "tx_count", "size_in_bytes", "peer_id", "block_time", "inserted_at", "median_time_past",
	}).
		AddRow(1, int64(1729259727), uint32(0), hashPrevBlock.CloneBytes(), hashMerkleRoot.CloneBytes(), bits.CloneBytes(),
			uint32(1), uint32(1), int64(1), int64(1000), "test_peer", int64(1729259727), customtime.CustomTime{}, uint32(0)).
		AddRow(1, int64(1729259727), uint32(1), block2PrevBlockHash.CloneBytes(), block2MerkleRootHash.CloneBytes(), bits.CloneBytes(),
			uint32(2), uint32(2), int64(1), int64(1000), "test_peer", int64(1729259727), customtime.CustomTime{}, uint32(0))

	mock.ExpectQuery(`SELECT(.+)FROM blocks b(.+)`).
		WithArgs(uint32(0), uint32(1000000)).
		WillReturnRows(rows)

	// Request very large range
	headers, metas, err := s.GetBlockHeadersByHeight(ctx, 0, 1000000)

	require.NoError(t, err)
	assert.NotNil(t, headers)
	assert.NotNil(t, metas)
	assert.Equal(t, len(headers), len(metas))

	// Should only return blocks that exist
	assert.Greater(t, len(headers), 0)
	assert.LessOrEqual(t, len(headers), 10) // reasonable upper bound

	require.NoError(t, mock.ExpectationsWereMet())
}

// Test query error handling
func TestGetBlockHeadersByHeight_QueryError(t *testing.T) {
	s, mock, err := createMockSQL()
	require.NoError(t, err)
	defer s.db.Close()

	ctx := context.Background()

	// Setup mock expectations - return a database error
	mock.ExpectQuery(`SELECT(.+)FROM blocks b(.+)`).
		WithArgs(uint32(1), uint32(10)).
		WillReturnError(sql.ErrConnDone)

	// Should return storage error
	_, _, err = s.GetBlockHeadersByHeight(ctx, 1, 10)

	assert.Error(t, err)
	require.NoError(t, mock.ExpectationsWereMet())
}

// Test with maximum uint32 values
func TestGetBlockHeadersByHeight_MaxValues(t *testing.T) {
	s, mock, err := createMockSQL()
	require.NoError(t, err)
	defer s.db.Close()

	ctx := context.Background()

	// Setup mock expectations - return empty result for very high heights
	rows := sqlmock.NewRows([]string{
		"version", "block_time", "nonce", "previous_hash", "merkle_root", "n_bits",
		"id", "height", "tx_count", "size_in_bytes", "peer_id", "block_time", "inserted_at", "median_time_past",
	})

	mock.ExpectQuery(`SELECT(.+)FROM blocks b(.+)`).
		WithArgs(uint32(4294967290), uint32(4294967295)).
		WillReturnRows(rows)

	// Request with very high height values (should be empty)
	headers, metas, err := s.GetBlockHeadersByHeight(ctx, 4294967290, 4294967295) // near uint32 max

	require.NoError(t, err)
	assert.Empty(t, headers) // No blocks at such high heights
	assert.Empty(t, metas)

	require.NoError(t, mock.ExpectationsWereMet())
}

// Test data consistency between headers and metas
func TestGetBlockHeadersByHeight_DataConsistency(t *testing.T) {
	s, mock, err := createMockSQL()
	require.NoError(t, err)
	defer s.db.Close()

	ctx := context.Background()

	// Setup mock expectations
	rows := sqlmock.NewRows([]string{
		"version", "block_time", "nonce", "previous_hash", "merkle_root", "n_bits",
		"id", "height", "tx_count", "size_in_bytes", "peer_id", "block_time", "inserted_at", "median_time_past",
	}).
		AddRow(1, int64(1729259727), uint32(0), hashPrevBlock.CloneBytes(), hashMerkleRoot.CloneBytes(), bits.CloneBytes(),
			uint32(1), uint32(1), int64(1), int64(1000), "test_peer", int64(1729259727), customtime.CustomTime{}, uint32(0)).
		AddRow(1, int64(1729259727), uint32(1), block2PrevBlockHash.CloneBytes(), block2MerkleRootHash.CloneBytes(), bits.CloneBytes(),
			uint32(2), uint32(2), int64(1), int64(1000), "test_peer", int64(1729259727), customtime.CustomTime{}, uint32(0))

	mock.ExpectQuery(`SELECT(.+)FROM blocks b(.+)`).
		WithArgs(uint32(0), uint32(5)).
		WillReturnRows(rows)

	// Get headers in range
	headers, metas, err := s.GetBlockHeadersByHeight(ctx, 0, 5)

	require.NoError(t, err)
	assert.Equal(t, len(headers), len(metas))

	// Just verify that we get valid non-nil results
	for i, header := range headers {
		meta := metas[i]

		// Basic structure validation
		assert.NotNil(t, header, "Header should not be nil")
		assert.NotNil(t, meta, "Meta should not be nil")

		// Verify header hash fields are set
		assert.NotNil(t, header.HashPrevBlock, "HashPrevBlock should be set")
		assert.NotNil(t, header.HashMerkleRoot, "HashMerkleRoot should be set")
	}

	require.NoError(t, mock.ExpectationsWereMet())
}

// Test edge case with capacity calculation (reverse range)
func TestGetBlockHeadersByHeight_CapacityCalculation(t *testing.T) {
	s, mock, err := createMockSQL()
	require.NoError(t, err)
	defer s.db.Close()

	ctx := context.Background()

	// Setup mock expectations - empty result for reverse range
	rows := sqlmock.NewRows([]string{
		"version", "block_time", "nonce", "previous_hash", "merkle_root", "n_bits",
		"id", "height", "tx_count", "size_in_bytes", "peer_id", "block_time", "inserted_at", "median_time_past",
	})

	mock.ExpectQuery(`SELECT(.+)FROM blocks b(.+)`).
		WithArgs(uint32(10), uint32(5)).
		WillReturnRows(rows)

	// This tests the capacity calculation: should handle startHeight > endHeight without panic
	headers, metas, err := s.GetBlockHeadersByHeight(ctx, 10, 5)

	require.NoError(t, err)
	assert.Empty(t, headers)
	assert.Empty(t, metas)

	require.NoError(t, mock.ExpectationsWereMet())
}

// Test ordering verification
func TestGetBlockHeadersByHeight_Ordering(t *testing.T) {
	s, mock, err := createMockSQL()
	require.NoError(t, err)
	defer s.db.Close()

	ctx := context.Background()

	// Setup mock expectations - multiple blocks in order
	rows := sqlmock.NewRows([]string{
		"version", "block_time", "nonce", "previous_hash", "merkle_root", "n_bits",
		"id", "height", "tx_count", "size_in_bytes", "peer_id", "block_time", "inserted_at", "median_time_past",
	}).
		AddRow(1, int64(1729259727), uint32(0), hashPrevBlock.CloneBytes(), hashMerkleRoot.CloneBytes(), bits.CloneBytes(),
			uint32(1), uint32(1), int64(1), int64(1000), "test_peer", int64(1729259727), customtime.CustomTime{}, uint32(0)).
		AddRow(1, int64(1729259727), uint32(1), block2PrevBlockHash.CloneBytes(), block2MerkleRootHash.CloneBytes(), bits.CloneBytes(),
			uint32(2), uint32(2), int64(1), int64(1000), "test_peer", int64(1729259727), customtime.CustomTime{}, uint32(0)).
		AddRow(1, int64(1729259727), uint32(1), block3PrevBlockHash.CloneBytes(), block3MerkleRootHash.CloneBytes(), bits.CloneBytes(),
			uint32(3), uint32(3), int64(1), int64(1000), "test_peer", int64(1729259727), customtime.CustomTime{}, uint32(0))

	mock.ExpectQuery(`SELECT(.+)FROM blocks b(.+)`).
		WithArgs(uint32(0), uint32(10)).
		WillReturnRows(rows)

	// Get all blocks
	headers, metas, err := s.GetBlockHeadersByHeight(ctx, 0, 10)

	require.NoError(t, err)
	assert.Greater(t, len(headers), 1) // Should have multiple blocks

	// Verify strict ascending order
	for i := 1; i < len(metas); i++ {
		assert.LessOrEqual(t, metas[i-1].Height, metas[i].Height, "Headers should be ordered by height (ASC)")
	}

	require.NoError(t, mock.ExpectationsWereMet())
}

// Test scan error handling
func TestGetBlockHeadersByHeight_ScanError(t *testing.T) {
	s, mock, err := createMockSQL()
	require.NoError(t, err)
	defer s.db.Close()

	ctx := context.Background()

	// Setup mock expectations - return row with data that causes scan error
	// Use incorrect byte length for hash which will fail chainhash.NewHash
	invalidHash := []byte{0x01} // Too short for a valid hash (needs 32 bytes)
	rows := sqlmock.NewRows([]string{
		"version", "block_time", "nonce", "previous_hash", "merkle_root", "n_bits",
		"id", "height", "tx_count", "size_in_bytes", "peer_id", "block_time", "inserted_at", "median_time_past",
	}).
		AddRow(1, int64(1729259727), uint32(0), invalidHash, invalidHash, bits.CloneBytes(),
			uint32(1), uint32(1), int64(1), int64(1000), "test_peer", int64(1729259727), customtime.CustomTime{}, uint32(0))

	mock.ExpectQuery(`SELECT(.+)FROM blocks b(.+)`).
		WithArgs(uint32(1), uint32(10)).
		WillReturnRows(rows)

	// Should return error due to invalid hash length
	_, _, err = s.GetBlockHeadersByHeight(ctx, 1, 10)

	assert.Error(t, err)
	require.NoError(t, mock.ExpectationsWereMet())
}

// Test max helper function with different types and scenarios
func TestMax_Integer(t *testing.T) {
	// Test with integers
	assert.Equal(t, 5, max(3, 5))
	assert.Equal(t, 10, max(10, 7))
	assert.Equal(t, 42, max(42, 42)) // Equal values
	assert.Equal(t, 0, max(-5, 0))   // Negative and positive
}

func TestMax_Float(t *testing.T) {
	// Test with floats
	assert.Equal(t, 3.14, max(2.71, 3.14))
	assert.Equal(t, 1.0, max(1.0, 0.5))
	assert.Equal(t, 2.5, max(2.5, 2.5)) // Equal values
}

func TestMax_String(t *testing.T) {
	// Test with strings (lexicographical ordering)
	assert.Equal(t, "zebra", max("apple", "zebra"))
	assert.Equal(t, "hello", max("hello", "goodbye"))
	assert.Equal(t, "test", max("test", "test")) // Equal values
}

func TestMax_Uint32(t *testing.T) {
	// Test with uint32 (relevant for height values)
	assert.Equal(t, uint32(100), max(uint32(50), uint32(100)))
	assert.Equal(t, uint32(1000), max(uint32(1000), uint32(999)))
	assert.Equal(t, uint32(0), max(uint32(0), uint32(0))) // Both zero
}
