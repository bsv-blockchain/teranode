package sql

import (
	"context"
	"net/url"
	"testing"

	"github.com/bitcoin-sv/ubsv/chaincfg"
	"github.com/bitcoin-sv/ubsv/errors"
	"github.com/bitcoin-sv/ubsv/model"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/bitcoin-sv/ubsv/util/test"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/stretchr/testify/require"
)

func TestSQL_GetBlockHeadersFromTill(t *testing.T) {
	tSettings := test.CreateBaseTestSettings()

	t.Run("blocks not found", func(t *testing.T) {
		ctx := context.Background()
		storeURL, err := url.Parse("sqlitememory:///")
		require.NoError(t, err)

		s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
		require.NoError(t, err)

		headers, _, err := s.GetBlockHeadersFromTill(ctx, &chainhash.Hash{}, &chainhash.Hash{})
		require.ErrorIs(t, err, errors.ErrBlockNotFound)
		require.Len(t, headers, 0)
	})

	t.Run("", func(t *testing.T) {
		ctx := context.Background()
		storeURL, err := url.Parse("sqlitememory:///")
		require.NoError(t, err)

		s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
		require.NoError(t, err)

		generatedBlockHeaders := generateBlockHeaders(t, s, 25)

		blockHashFrom := generatedBlockHeaders[10].Hash()
		blockHashTill := generatedBlockHeaders[15].Hash()

		blockHeaders, blockHeaderMetas, err := s.GetBlockHeadersFromTill(ctx, blockHashFrom, blockHashTill)
		require.NoError(t, err)
		require.Len(t, blockHeaders, 6)
		require.Len(t, blockHeaderMetas, 6)

		require.Equal(t, blockHashTill.String(), blockHeaders[0].Hash().String())
		require.Equal(t, blockHashFrom.String(), blockHeaders[5].Hash().String())
	})
}

func generateBlockHeaders(t *testing.T, s *SQL, numberOfBlocks uint32) []*model.BlockHeader {
	prevBlockHash := chaincfg.RegressionNetParams.GenesisBlock.BlockHash()
	bits, _ = model.NewNBitFromString("207fffff")

	blockHeaders := make([]*model.BlockHeader, 0, numberOfBlocks)

	for i := uint32(0); i < numberOfBlocks; i++ {
		prevHash := prevBlockHash
		block := &model.Block{
			Header: &model.BlockHeader{
				Version:        i,
				Timestamp:      i,
				Nonce:          i,
				HashPrevBlock:  &prevHash,
				HashMerkleRoot: &chainhash.Hash{},
				Bits:           *bits,
			},
			CoinbaseTx:       coinbaseTx,
			TransactionCount: 1,
			Subtrees:         []*chainhash.Hash{},
		}

		_, _, err := s.StoreBlock(context.Background(), block, "test")

		blockHeaders = append(blockHeaders, block.Header)

		prevBlockHash = *block.Hash()

		require.NoError(t, err)
	}

	return blockHeaders
}
