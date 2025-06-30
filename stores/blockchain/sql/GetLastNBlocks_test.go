package sql

import (
	"context"
	"net/url"
	"testing"
	"time"

	time2 "github.com/bitcoin-sv/teranode/model/time"
	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/bitcoin-sv/teranode/util/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockRows implements a subset of sql.Rows interface for testing error cases

func TestSQLGetLastNBlocks(t *testing.T) {
	tSettings := test.CreateBaseTestSettings()

	t.Run("get last blocks with empty chain", func(t *testing.T) {
		storeURL, err := url.Parse("sqlitememory:///")
		require.NoError(t, err)

		s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
		require.NoError(t, err)

		blocks, err := s.GetLastNBlocks(context.Background(), 10, false, 0)
		require.NoError(t, err)
		assert.Len(t, blocks, 1) // Genesis block
	})

	t.Run("get last blocks with chain", func(t *testing.T) {
		storeURL, err := url.Parse("sqlitememory:///")
		require.NoError(t, err)

		s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
		require.NoError(t, err)

		// Store blocks 1, 2, and 3
		_, _, err = s.StoreBlock(context.Background(), block1, "")
		require.NoError(t, err)
		_, _, err = s.StoreBlock(context.Background(), block2, "")
		require.NoError(t, err)
		_, _, err = s.StoreBlock(context.Background(), block3, "")
		require.NoError(t, err)

		// Get last 2 blocks
		blocks, err := s.GetLastNBlocks(context.Background(), 2, false, 0)
		require.NoError(t, err)
		assert.Len(t, blocks, 2)
		assert.Equal(t, block3.Header.Bytes(), blocks[0].BlockHeader)
		assert.Equal(t, block2.Header.Bytes(), blocks[1].BlockHeader)
	})

	t.Run("get last blocks with fromHeight", func(t *testing.T) {
		storeURL, err := url.Parse("sqlitememory:///")
		require.NoError(t, err)

		s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
		require.NoError(t, err)

		// Store blocks 1, 2, and 3
		_, _, err = s.StoreBlock(context.Background(), block1, "")
		require.NoError(t, err)
		_, _, err = s.StoreBlock(context.Background(), block2, "")
		require.NoError(t, err)
		_, _, err = s.StoreBlock(context.Background(), block3, "")
		require.NoError(t, err)

		// Get blocks from height 1
		blocks, err := s.GetLastNBlocks(context.Background(), 10, false, 2)
		require.NoError(t, err)
		assert.Len(t, blocks, 3)
		assert.Equal(t, uint32(2), blocks[0].Height)
		assert.Equal(t, uint32(1), blocks[1].Height)
		assert.Equal(t, uint32(0), blocks[2].Height)
	})

	t.Run("get last blocks with includeOrphans=true", func(t *testing.T) {
		storeURL, err := url.Parse("sqlitememory:///")
		require.NoError(t, err)

		s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
		require.NoError(t, err)

		// Store blocks 1, 2, and 3
		_, _, err = s.StoreBlock(context.Background(), block1, "")
		require.NoError(t, err)
		_, _, err = s.StoreBlock(context.Background(), block2, "")
		require.NoError(t, err)
		_, _, err = s.StoreBlock(context.Background(), block3, "")
		require.NoError(t, err)

		// Store an alternative block at height 2
		_, _, err = s.StoreBlock(context.Background(), blockAlternative2, "")
		require.NoError(t, err)

		// Get last blocks including orphans
		blocks, err := s.GetLastNBlocks(context.Background(), 10, true, 0)
		require.NoError(t, err)

		// Should include all blocks including orphans
		assert.GreaterOrEqual(t, len(blocks), 4) // Genesis + 3 blocks + alternative
	})

	t.Run("cache hit", func(t *testing.T) {
		storeURL, err := url.Parse("sqlitememory:///")
		require.NoError(t, err)

		s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
		require.NoError(t, err)

		// Store blocks 1, 2, and 3
		_, _, err = s.StoreBlock(context.Background(), block1, "")
		require.NoError(t, err)
		_, _, err = s.StoreBlock(context.Background(), block2, "")
		require.NoError(t, err)

		// First call will populate the cache
		blocks1, err := s.GetLastNBlocks(context.Background(), 2, false, 0)
		require.NoError(t, err)
		assert.Len(t, blocks1, 2)

		// Second call should hit the cache
		blocks2, err := s.GetLastNBlocks(context.Background(), 2, false, 0)
		require.NoError(t, err)
		assert.Len(t, blocks2, 2)

		// Verify both results are the same (from cache)
		assert.Equal(t, blocks1, blocks2)
	})

	t.Run("database error", func(t *testing.T) {
		storeURL, err := url.Parse("sqlitememory:///")
		require.NoError(t, err)

		s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
		require.NoError(t, err)

		// Close the database to force an error
		err = s.db.Close()
		require.NoError(t, err)

		// Call GetLastNBlocks and expect an error
		_, err = s.GetLastNBlocks(context.Background(), 10, false, 0)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "failed to get blocks")
	})

	t.Run("processBlockRows error", func(t *testing.T) {
		storeURL, err := url.Parse("sqlitememory:///")
		require.NoError(t, err)

		s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
		require.NoError(t, err)

		// Store a block first
		_, _, err = s.StoreBlock(context.Background(), block1, "")
		require.NoError(t, err)

		// Corrupt the database by executing an invalid SQL query
		// This will make the database return invalid data for the block query
		_, err = s.db.Exec("ALTER TABLE blocks ADD COLUMN corrupted TEXT")
		require.NoError(t, err)

		// Update the block to have invalid data that will cause scanBlockRow to fail
		_, err = s.db.Exec("UPDATE blocks SET previous_hash = 'invalid_hash', merkle_root = 'invalid_root'")
		require.NoError(t, err)

		// Call GetLastNBlocks which will use processBlockRows internally
		// The scanBlockRow function will fail when trying to convert the invalid hash values
		_, err = s.GetLastNBlocks(context.Background(), 10, false, 0)
		require.Error(t, err)
		// The error should be from processBlockRows
		assert.Contains(t, err.Error(), "failed to convert")
	})
}

func TestCustomTime(t *testing.T) {
	t.Run("scan time.Time value", func(t *testing.T) {
		now := time.Now()
		ct := &time2.CustomTime{}
		err := ct.Scan(now)
		require.NoError(t, err)
		assert.Equal(t, now, ct.Time)
	})

	t.Run("scan []byte value", func(t *testing.T) {
		timeStr := "2023-01-02 15:04:05"
		expected, _ := time.Parse(time2.SQLiteTimestampFormat, timeStr)
		ct := &time2.CustomTime{}
		err := ct.Scan([]byte(timeStr))
		require.NoError(t, err)
		assert.Equal(t, expected, ct.Time)
	})

	t.Run("scan string value", func(t *testing.T) {
		timeStr := "2023-01-02 15:04:05"
		expected, _ := time.Parse(time2.SQLiteTimestampFormat, timeStr)
		ct := &time2.CustomTime{}
		err := ct.Scan(timeStr)
		require.NoError(t, err)
		assert.Equal(t, expected, ct.Time)
	})

	t.Run("scan invalid []byte format", func(t *testing.T) {
		ct := &time2.CustomTime{}
		err := ct.Scan([]byte("invalid-time-format"))
		require.Error(t, err)
	})

	t.Run("scan invalid string format", func(t *testing.T) {
		ct := &time2.CustomTime{}
		err := ct.Scan("invalid-time-format")
		require.Error(t, err)
	})

	t.Run("scan unsupported type", func(t *testing.T) {
		ct := &time2.CustomTime{}
		err := ct.Scan(123) // integer is not supported
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unsupported type")
	})

	t.Run("value method", func(t *testing.T) {
		now := time.Now()
		ct := time2.CustomTime{Time: now}
		val, err := ct.Value()
		require.NoError(t, err)
		assert.Equal(t, now, val)
	})
}
