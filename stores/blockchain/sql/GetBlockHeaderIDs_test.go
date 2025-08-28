package sql

import (
	"context"
	"net/url"
	"testing"

	"github.com/bitcoin-sv/teranode/model"
	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/bitcoin-sv/teranode/util/test"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSQLGetBlockHeaderIDs(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)

	t.Run("empty - no error", func(t *testing.T) {
		storeURL, err := url.Parse("sqlitememory:///")
		require.NoError(t, err)

		s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
		require.NoError(t, err)

		headerIDs, err := s.GetBlockHeaderIDs(context.Background(), &chainhash.Hash{}, 2)
		require.NoError(t, err)
		assert.Equal(t, 0, len(headerIDs))
	})

	t.Run("", func(t *testing.T) {
		storeURL, err := url.Parse("sqlitememory:///")
		require.NoError(t, err)

		s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
		require.NoError(t, err)

		_, _, err = s.StoreBlock(context.Background(), block1, "")
		require.NoError(t, err)

		_, _, err = s.StoreBlock(context.Background(), block2, "")
		require.NoError(t, err)

		_, _, err = s.StoreBlock(context.Background(), blockAlternative2, "")
		require.NoError(t, err)

		headerIDs, err := s.GetBlockHeaderIDs(context.Background(), &chainhash.Hash{}, 10)
		require.NoError(t, err)
		assert.Equal(t, 0, len(headerIDs))

		headerIDs, err = s.GetBlockHeaderIDs(context.Background(), block1.Hash(), 10)
		require.NoError(t, err)
		assert.Equal(t, 2, len(headerIDs))
		require.Equal(t, headerIDs, []uint32{1, 0})

		headerIDs, err = s.GetBlockHeaderIDs(context.Background(), block2.Hash(), 10)
		require.NoError(t, err)
		assert.Equal(t, 3, len(headerIDs))
		require.Equal(t, headerIDs, []uint32{2, 1, 0})

		headerIDs, err = s.GetBlockHeaderIDs(context.Background(), blockAlternative2.Hash(), 10)
		require.NoError(t, err)
		assert.Equal(t, 3, len(headerIDs))
		require.Equal(t, headerIDs, []uint32{3, 1, 0})

		_, _, err = s.StoreBlock(context.Background(), block3, "")
		require.NoError(t, err)

		headerIDs, err = s.GetBlockHeaderIDs(context.Background(), block3.Hash(), 10)
		require.NoError(t, err)
		assert.Equal(t, 4, len(headerIDs))
		require.Equal(t, headerIDs, []uint32{4, 2, 1, 0})
	})
}

func BenchmarkSQLGetBlockHeaderIDs(b *testing.B) {
	storeURL, err := url.Parse("sqlitememory:///")
	if err != nil {
		b.Fatal(err)
	}

	tSettings := test.CreateBaseTestSettings(b)

	s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
	if err != nil {
		b.Fatal(err)
	}

	var (
		ids           []uint32
		lastBlockHash *chainhash.Hash
	)

	block := &model.Block{
		Header: &model.BlockHeader{
			Version:        1,
			Timestamp:      1729259727,
			Nonce:          0,
			HashPrevBlock:  hashPrevBlock,
			HashMerkleRoot: hashMerkleRoot,
			Bits:           *bits,
		},
		Height:           1,
		CoinbaseTx:       coinbaseTx,
		TransactionCount: 1,
		Subtrees:         []*chainhash.Hash{},
	}

	for i := uint32(0); i < 100_000; i++ {
		block.Height = i + 1
		block.Header.Version = i + 1

		if i == 0 {
			block.Header.HashPrevBlock = tSettings.ChainCfgParams.GenesisHash
		} else {
			block.Header.HashPrevBlock = lastBlockHash
		}

		lastBlockHash = block.GetHash()

		if _, _, err = s.StoreBlock(b.Context(), block, "test"); err != nil {
			b.Fatal(err)
		}
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if ids, err = s.GetBlockHeaderIDs(b.Context(), lastBlockHash, 10_000); err != nil {
			b.Fatal(err)
		}

		if len(ids) != 10_000 {
			b.Fatalf("expected 10,000 IDs, got %d", len(ids))
		}
	}
}
