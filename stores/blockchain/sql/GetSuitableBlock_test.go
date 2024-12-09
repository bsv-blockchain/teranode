package sql

import (
	"context"
	"net/url"
	"testing"

	"github.com/bitcoin-sv/ubsv/model"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/bitcoin-sv/ubsv/util/test"
	"github.com/stretchr/testify/require"
)

func Test_getMedianBlock(t *testing.T) {
	var blocks []*model.SuitableBlock

	blocks = append(blocks, &model.SuitableBlock{
		Time: 1,
	})
	blocks = append(blocks, &model.SuitableBlock{
		Time: 3,
	})
	blocks = append(blocks, &model.SuitableBlock{
		Time: 2,
	})

	b := getMedianBlock(blocks)
	require.Equal(t, uint32(2), b.Time)
}

func TestSQLGetSuitableBlock(t *testing.T) {
	storeURL, err := url.Parse("sqlitememory:///")
	require.NoError(t, err)

	tSettings := test.CreateBaseTestSettings()

	s, err := New(ulogger.TestLogger{}, storeURL, tSettings.ChainCfgParams)
	require.NoError(t, err)

	_, _, err = s.StoreBlock(context.Background(), block1, "")
	require.NoError(t, err)

	_, _, err = s.StoreBlock(context.Background(), block2, "")
	require.NoError(t, err)

	_, _, err = s.StoreBlock(context.Background(), block3, "")
	require.NoError(t, err)

	suitableBlock, err := s.GetSuitableBlock(context.Background(), block3Hash)
	require.NoError(t, err)
	// suitable block should be block3
	require.Equal(t, block2.Hash()[:], suitableBlock.Hash)
}
