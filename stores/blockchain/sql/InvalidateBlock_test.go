package sql

import (
	"context"
	"net/url"
	"testing"

	"github.com/bitcoin-sv/teranode/stores/blockchain/options"
	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/bitcoin-sv/teranode/util/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSQLInvalidateBlock(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)

	t.Run("empty - error", func(t *testing.T) {
		storeURL, err := url.Parse("sqlitememory:///")
		require.NoError(t, err)

		s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
		require.NoError(t, err)

		hashes, err := s.InvalidateBlock(context.Background(), block2.Hash())
		require.Error(t, err)
		require.Nil(t, hashes)
	})

	t.Run("Block invalidated", func(t *testing.T) {
		storeURL, err := url.Parse("sqlitememory:///")
		require.NoError(t, err)

		s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
		require.NoError(t, err)

		err = s.insertGenesisTransaction(ulogger.TestLogger{})
		require.NoError(t, err)

		_, _, err = s.StoreBlock(context.Background(), block1, "", options.WithMinedSet(true))
		require.NoError(t, err)

		_, _, err = s.StoreBlock(context.Background(), block2, "", options.WithMinedSet(true))
		require.NoError(t, err)

		_, _, err = s.StoreBlock(context.Background(), block3, "", options.WithMinedSet(true))
		require.NoError(t, err)

		hashes, err := s.InvalidateBlock(context.Background(), block3.Hash())
		require.NoError(t, err)
		require.Len(t, hashes, 1)
		assert.Equal(t, block3.Hash().String(), hashes[0].String())

		var (
			id        int
			height    uint32
			invalid   bool
			mined_set bool
		)

		err = s.db.QueryRowContext(context.Background(),
			"SELECT id, height, invalid, mined_set FROM blocks WHERE hash = $1",
			block1.Hash().CloneBytes()).Scan(&id, &height, &invalid, &mined_set)
		require.NoError(t, err)

		assert.Equal(t, 1, id)
		assert.Equal(t, uint32(1), height)
		assert.False(t, invalid)
		assert.True(t, mined_set)

		err = s.db.QueryRowContext(context.Background(),
			"SELECT id, height, invalid, mined_set FROM blocks WHERE hash = $1",
			block2.Hash().CloneBytes()).Scan(&id, &height, &invalid, &mined_set)
		require.NoError(t, err)

		assert.Equal(t, 2, id)
		assert.Equal(t, uint32(2), height)
		assert.False(t, invalid)
		assert.True(t, mined_set)

		err = s.db.QueryRowContext(context.Background(),
			"SELECT id, height, invalid, mined_set FROM blocks WHERE hash = $1",
			block3.Hash().CloneBytes()).Scan(&id, &height, &invalid, &mined_set)
		require.NoError(t, err)

		assert.Equal(t, 3, id)
		assert.Equal(t, uint32(3), height)
		assert.True(t, invalid)
		assert.False(t, mined_set) // this should be false as we set mined_set to false when invalidating a block
	})

	t.Run("Blocks invalidated", func(t *testing.T) {
		storeURL, err := url.Parse("sqlitememory:///")
		require.NoError(t, err)

		s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
		require.NoError(t, err)

		err = s.insertGenesisTransaction(ulogger.TestLogger{})
		require.NoError(t, err)

		_, _, err = s.StoreBlock(context.Background(), block1, "", options.WithMinedSet(true))
		require.NoError(t, err)

		_, _, err = s.StoreBlock(context.Background(), block2, "", options.WithMinedSet(true))
		require.NoError(t, err)

		_, _, err = s.StoreBlock(context.Background(), block3, "", options.WithMinedSet(true))
		require.NoError(t, err)

		// err = dumpDBData(t, err, s)

		hashes, err := s.InvalidateBlock(context.Background(), block2.Hash())
		require.NoError(t, err)
		require.Len(t, hashes, 2)
		assert.Contains(t, []string{block2.Hash().String(), block3.Hash().String()}, hashes[0].String())
		assert.Contains(t, []string{block2.Hash().String(), block3.Hash().String()}, hashes[1].String())

		var (
			id        int
			height    uint32
			invalid   bool
			mined_set bool
		)

		err = s.db.QueryRowContext(context.Background(),
			"SELECT id, height, invalid, mined_set FROM blocks WHERE hash = $1",
			block1.Hash().CloneBytes()).Scan(&id, &height, &invalid, &mined_set)
		require.NoError(t, err)

		assert.Equal(t, 1, id)
		assert.Equal(t, uint32(1), height)
		assert.False(t, invalid)
		assert.True(t, mined_set)

		b2, block2Height, err := s.GetBlock(context.Background(), block2.Hash())
		require.NoError(t, err)
		assert.Equal(t, uint32(2), block2Height)
		assert.Equal(t, block2.Hash().String(), b2.Hash().String())

		err = s.db.QueryRowContext(context.Background(),
			"SELECT id, height, invalid, mined_set FROM blocks WHERE hash = $1",
			block2.Hash().CloneBytes()).Scan(&id, &height, &invalid, &mined_set)
		require.NoError(t, err)

		assert.Equal(t, 2, id)
		assert.Equal(t, uint32(2), height)
		assert.True(t, invalid)
		assert.False(t, mined_set)

		err = s.db.QueryRowContext(context.Background(),
			"SELECT id, height, invalid, mined_set FROM blocks WHERE hash = $1",
			block3.Hash().CloneBytes()).Scan(&id, &height, &invalid, &mined_set)
		require.NoError(t, err)

		assert.Equal(t, 3, id)
		assert.Equal(t, uint32(3), height)
		assert.True(t, invalid)
		assert.False(t, mined_set)
	})

	// Ensure best block header cache is invalidated when the tip is marked invalid
	t.Run("Invalidate tip updates best block header (cache invalidation)", func(t *testing.T) {
		storeURL, err := url.Parse("sqlitememory:///")
		require.NoError(t, err)

		s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
		require.NoError(t, err)

		// Build a short chain: genesis -> block1 -> block2 -> block3
		_, _, err = s.StoreBlock(context.Background(), block1, "peer")
		require.NoError(t, err)
		_, _, err = s.StoreBlock(context.Background(), block2, "peer")
		require.NoError(t, err)
		_, _, err = s.StoreBlock(context.Background(), block3, "peer")
		require.NoError(t, err)

		// Warm the cache so best header is served from cache
		head, meta, err := s.GetBestBlockHeader(context.Background())
		require.NoError(t, err)
		require.NotNil(t, head)
		require.NotNil(t, meta)
		assert.Equal(t, uint32(3), meta.Height)
		assert.Equal(t, block3.Hash().String(), head.Hash().String())

		// Invalidate the tip
		invalidateHashes, err := s.InvalidateBlock(context.Background(), block3.Hash())
		require.NoError(t, err)
		require.Len(t, invalidateHashes, 1)
		assert.Equal(t, block3.Hash().String(), invalidateHashes[0].String())

		// Now best header must move back to block2 (not the invalid block3)
		head2, meta2, err := s.GetBestBlockHeader(context.Background())
		require.NoError(t, err)
		require.NotNil(t, head2)
		require.NotNil(t, meta2)
		assert.Equal(t, uint32(2), meta2.Height)
		assert.Equal(t, block2.Hash().String(), head2.Hash().String())

		// Call again to ensure cached path is also correct
		head3, meta3, err := s.GetBestBlockHeader(context.Background())
		require.NoError(t, err)
		assert.Equal(t, uint32(2), meta3.Height)
		assert.Equal(t, head2.Hash().String(), head3.Hash().String())
	})

}

// func dumpDBData(t *testing.T, err error, s *SQL) error {
// 	// get and print all the data in the db
// 	rows, err := s.db.QueryContext(context.Background(), "SELECT id, hash, height, invalid FROM blocks")
// 	require.NoError(t, err)
// 	defer rows.Close()

// 	for rows.Next() {
// 		var id int
// 		var hash []byte
// 		var height uint32
// 		var invalid bool
// 		err = rows.Scan(&id, &hash, &height, &invalid)
// 		require.NoError(t, err)
// 		t.Logf("id: %d, hash: %x, height: %d, invalid: %t", id, hash, height, invalid)
// 	}
// 	return err
// }
