package sql

import (
	"context"
	"net/url"
	"testing"

	"github.com/bsv-blockchain/go-chaincfg"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/stores/blockchain/options"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSQL_RevalidateBlock(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)

	t.Run("empty - error", func(t *testing.T) {
		storeURL, err := url.Parse("sqlitememory:///")
		require.NoError(t, err)

		s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
		require.NoError(t, err)

		err = s.RevalidateBlock(context.Background(), block2.Hash())
		require.Error(t, err)
	})

	t.Run("Block revalidated", func(t *testing.T) {
		storeURL, err := url.Parse("sqlitememory:///")
		require.NoError(t, err)

		s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
		require.NoError(t, err)

		err = s.insertGenesisTransaction(ulogger.TestLogger{}, tSettings.ChainCfgParams)
		require.NoError(t, err)

		_, _, err = s.StoreBlock(context.Background(), block1, "", options.WithMinedSet(true))
		require.NoError(t, err)

		_, _, err = s.StoreBlock(context.Background(), block2, "", options.WithMinedSet(true))
		require.NoError(t, err)

		_, _, err = s.StoreBlock(context.Background(), block3, "", options.WithMinedSet(true))
		require.NoError(t, err)

		_, err = s.InvalidateBlock(context.Background(), block3.Hash())
		require.NoError(t, err)

		var (
			id        int
			height    uint32
			invalid   bool
			mined_set bool
		)

		err = s.db.QueryRowContext(context.Background(),
			"SELECT id, height, invalid, mined_set FROM blocks WHERE hash = $1",
			block3.Hash().CloneBytes()).Scan(&id, &height, &invalid, &mined_set)
		require.NoError(t, err)

		assert.Equal(t, 3, id)
		assert.Equal(t, uint32(3), height)
		assert.True(t, invalid)
		assert.False(t, mined_set) // mined_set is set to false when invalidating to trigger tx unsetting

		// reset the mined_set to true, the revalidation should set it back to false
		err = s.SetBlockMinedSet(t.Context(), block3.Hash())
		require.NoError(t, err)

		err = s.RevalidateBlock(context.Background(), block3.Hash())
		require.NoError(t, err)

		err = s.db.QueryRowContext(context.Background(),
			"SELECT id, height, invalid, mined_set FROM blocks WHERE hash = $1",
			block3.Hash().CloneBytes()).Scan(&id, &height, &invalid, &mined_set)
		require.NoError(t, err)

		assert.Equal(t, 3, id)
		assert.Equal(t, uint32(3), height)
		assert.False(t, invalid)
		assert.False(t, mined_set)
	})
}

// TestSQL_RevalidateBlock_RejectsForkBelowPassedCheckpoint pins the checkpoint fork
// rule on reconsideration: an invalid fork below a checkpoint the best chain has
// passed stays invalid.
func TestSQL_RevalidateBlock_RejectsForkBelowPassedCheckpoint(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)
	params := *tSettings.ChainCfgParams
	params.Checkpoints = []chaincfg.Checkpoint{{Height: 3, Hash: block3.Hash()}}
	tSettings.ChainCfgParams = &params

	storeURL, err := url.Parse("sqlitememory:///")
	require.NoError(t, err)

	s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
	require.NoError(t, err)

	for _, b := range []*model.Block{block1, block2, block3} {
		_, _, err = s.StoreBlock(context.Background(), b, "")
		require.NoError(t, err)
	}

	_, _, err = s.StoreBlock(context.Background(), blockAlternative2, "", options.WithInvalid(true))
	require.NoError(t, err)

	err = s.RevalidateBlock(context.Background(), blockAlternative2.Hash())
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.ErrBlockInvalid), "got: %v", err)

	_, meta, err := s.GetBlockHeader(context.Background(), blockAlternative2.Hash())
	require.NoError(t, err)
	require.True(t, meta.Invalid, "the fork must stay invalid")
}
