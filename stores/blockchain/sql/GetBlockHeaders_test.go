package sql

import (
	"context"
	"database/sql/driver"
	"fmt"
	"net/url"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/stores/blockchain/options"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSQLGetBlockHeaders(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)

	t.Run("empty - no error", func(t *testing.T) {
		storeURL, err := url.Parse("sqlitememory:///")
		require.NoError(t, err)

		s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
		require.NoError(t, err)

		headers, heights, err := s.GetBlockHeaders(context.Background(), block2.Hash(), 2)
		require.NoError(t, err)
		assert.Equal(t, 0, len(headers))
		assert.Equal(t, 0, len(heights))
	})

	t.Run("normal", func(t *testing.T) {
		storeURL, err := url.Parse("sqlitememory:///")
		require.NoError(t, err)

		s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
		require.NoError(t, err)

		_, _, err = s.StoreBlock(context.Background(), block1, "test_peer")
		require.NoError(t, err)

		_, _, err = s.StoreBlock(context.Background(), block2, "test_peer")
		require.NoError(t, err)

		block2Alt := &model.Block{
			Header: &model.BlockHeader{
				Version:        1,
				Timestamp:      1231469744,
				Nonce:          1639830026,
				HashPrevBlock:  block2PrevBlockHash,
				HashMerkleRoot: block2MerkleRootHash,
				Bits:           *bits,
			},
			CoinbaseTx:       coinbaseTx2,
			TransactionCount: 1,
			Subtrees: []*chainhash.Hash{
				subtree,
			},
		}

		_, _, err = s.StoreBlock(context.Background(), block2Alt, "test_peer")
		require.NoError(t, err)

		headers, metas, err := s.GetBlockHeaders(context.Background(), block2.Hash(), 2)
		require.NoError(t, err)
		assert.Equal(t, 2, len(headers))
		assert.Equal(t, block2.Header.Hash(), headers[0].Hash())
		assert.Equal(t, uint32(2), metas[0].Height)
		assert.Equal(t, "test_peer", metas[0].PeerID)
		assert.Equal(t, block1.Header.Hash(), headers[1].Hash())
		assert.Equal(t, uint32(1), metas[1].Height)
		assert.Equal(t, "test_peer", metas[1].PeerID)

		headers, metas, err = s.GetBlockHeaders(context.Background(), block1.Hash(), 2)
		require.NoError(t, err)
		assert.Equal(t, 2, len(headers))
		assert.Equal(t, block1.Header.Hash(), headers[0].Hash())
		assert.Equal(t, uint32(1), metas[0].Height)
		assert.Equal(t, "0f9188f13cb7b2c71f2a335e3a4fc328bf5beb436012afca590b1a11466e2206", headers[1].Hash().String())

		headers, metas, err = s.GetBlockHeaders(context.Background(), block1.Hash(), 1)
		require.NoError(t, err)
		assert.Equal(t, 1, len(headers))
		assert.Equal(t, block1.Header.Hash(), headers[0].Hash())
		assert.Equal(t, uint32(1), metas[0].Height)

		headers, metas, err = s.GetBlockHeaders(context.Background(), block1.Hash(), 10)
		require.NoError(t, err)
		assert.Equal(t, 2, len(headers)) // there are only 2 headers in the chain
		assert.Equal(t, block1.Header.Hash(), headers[0].Hash())
		assert.Equal(t, uint32(1), metas[0].Height)
		assert.Equal(t, "0f9188f13cb7b2c71f2a335e3a4fc328bf5beb436012afca590b1a11466e2206", headers[1].Hash().String())

		headers, metas, err = s.GetBlockHeaders(context.Background(), block2Alt.Hash(), 10)
		require.NoError(t, err)
		assert.Equal(t, 3, len(headers)) // there should be 3 headers in the chain
		assert.Equal(t, block2Alt.Header.Hash(), headers[0].Hash())
		assert.Equal(t, uint32(2), metas[0].Height)
		assert.Equal(t, block1.Header.Hash(), headers[1].Hash())
		assert.Equal(t, uint32(1), metas[1].Height)
		assert.Equal(t, "0f9188f13cb7b2c71f2a335e3a4fc328bf5beb436012afca590b1a11466e2206", headers[2].Hash().String())
	})

	t.Run("with options", func(t *testing.T) {
		storeURL, err := url.Parse("sqlitememory:///")
		require.NoError(t, err)

		s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
		require.NoError(t, err)

		_, _, err = s.StoreBlock(context.Background(), block1, "test_peer", options.WithMinedSet(true), options.WithSubtreesSet(true), options.WithInvalid(true))
		require.NoError(t, err)

		_, _, err = s.StoreBlock(context.Background(), block2, "test_peer")
		require.NoError(t, err)

		headers, metas, err := s.GetBlockHeaders(context.Background(), block2.Hash(), 2)
		require.NoError(t, err)
		assert.Equal(t, 2, len(headers))
		assert.Equal(t, block1.Header.Hash(), headers[1].Hash())
		assert.Equal(t, uint32(1), metas[1].Height)
		assert.Equal(t, true, metas[1].MinedSet)
		assert.Equal(t, true, metas[1].SubtreesSet)
		assert.Equal(t, true, metas[1].Invalid)

		assert.Equal(t, block2.Header.Hash(), headers[0].Hash())
		assert.Equal(t, uint32(2), metas[0].Height)
		assert.Equal(t, false, metas[0].MinedSet)
		assert.Equal(t, false, metas[0].SubtreesSet)
		assert.Equal(t, true, metas[0].Invalid)
	})
}

// TestProcessBlockHeadersRows_RowErrorIsReturned: an error part way through the rows is returned,
// not a short list of headers.
func TestProcessBlockHeadersRows_RowErrorIsReturned(t *testing.T) {
	s, mock, err := createMockSQL()
	require.NoError(t, err)
	defer s.db.Close()

	columns := make([]string, 19)
	values := make([]driver.Value, 19)

	for i := range columns {
		columns[i] = fmt.Sprintf("c%d", i)
	}

	mock.ExpectQuery("SELECT").WillReturnRows(sqlmock.NewRows(columns).AddRow(values...).RowError(0, errors.NewStorageError("connection reset")))

	rows, err := s.db.QueryContext(context.Background(), "SELECT")
	require.NoError(t, err)

	defer rows.Close()

	_, _, err = s.processBlockHeadersRows(rows, 10, false)
	require.ErrorContains(t, err, "connection reset")
}
