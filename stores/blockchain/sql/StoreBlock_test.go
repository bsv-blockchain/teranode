package sql

import (
	"context"
	"net/url"
	"testing"

	"github.com/bitcoin-sv/ubsv/model"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_StoreBlock(t *testing.T) {
	t.Run("store block 1", func(t *testing.T) {
		storeUrl, err := url.Parse("sqlitememory:///")
		require.NoError(t, err)

		s, err := New(ulogger.TestLogger{}, storeUrl)
		require.NoError(t, err)

		_, err = s.StoreBlock(context.Background(), block1, "")
		require.NoError(t, err)

		_, err = s.StoreBlock(context.Background(), block2, "")
		require.NoError(t, err)
	})

	t.Run("store invalid child", func(t *testing.T) {
		storeUrl, err := url.Parse("sqlitememory:///")
		require.NoError(t, err)

		s, err := New(ulogger.TestLogger{}, storeUrl)
		require.NoError(t, err)

		_, err = s.StoreBlock(context.Background(), block1, "")
		require.NoError(t, err)

		err = s.InvalidateBlock(context.Background(), block1.Hash())
		require.NoError(t, err)

		var blockInvalid bool
		err = s.db.QueryRow("SELECT invalid FROM blocks WHERE hash = ?", block1.Hash().CloneBytes()).Scan(&blockInvalid)
		require.NoError(t, err)
		assert.True(t, blockInvalid)

		_, err = s.StoreBlock(context.Background(), block2, "")
		require.NoError(t, err)

		// block 2 should be invalid
		err = s.db.QueryRow("SELECT invalid FROM blocks WHERE hash = ?", block2.Hash().CloneBytes()).Scan(&blockInvalid)
		require.NoError(t, err)
		assert.True(t, blockInvalid)
	})
}

func Test_getCumulativeChainWork(t *testing.T) {
	t.Run("block 1", func(t *testing.T) {
		work, err := chainhash.NewHashFromStr("0000000000000000000000000000000000000000000000000000000000000000")
		require.NoError(t, err)

		chainWork, err := getCumulativeChainWork(work, &model.Block{
			Header: &model.BlockHeader{
				Bits: bits,
			},
		})
		require.NoError(t, err)
		assert.Equal(t, "0000000000000000000000000000000000000000000000000000000100010001", chainWork.String())
	})

	t.Run("block 2", func(t *testing.T) {
		work, err := chainhash.NewHashFromStr("0000000000000000000000000000000000000000000000000000000100010001")
		require.NoError(t, err)

		chainWork, err := getCumulativeChainWork(work, &model.Block{
			Header: &model.BlockHeader{
				Bits: bits,
			},
		})
		require.NoError(t, err)
		assert.Equal(t, "0000000000000000000000000000000000000000000000000000000200020002", chainWork.String())
	})

	t.Run("block 796044", func(t *testing.T) {
		work, err := chainhash.NewHashFromStr("000000000000000000000000000000000000000001473b8614ab22c164d42204")
		require.NoError(t, err)

		chainWork, err := getCumulativeChainWork(work, &model.Block{
			Header: &model.BlockHeader{
				Bits: model.NewNBitFromString("1810b7f0"),
			},
		})
		require.NoError(t, err)
		assert.Equal(t, "000000000000000000000000000000000000000001473b9564a2d255e87e7e86", chainWork.String())
	})
}
