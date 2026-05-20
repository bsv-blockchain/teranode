package sql

import (
	"context"
	"encoding/json"
	"net/url"
	"testing"

	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStoreBlock_CoinbaseBUMP_RoundTrip(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)
	storeURL, err := url.Parse("sqlitememory:///")
	require.NoError(t, err)

	s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
	require.NoError(t, err)
	defer s.Close()

	// Create a block with CoinbaseBUMP set
	testBUMP := []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0xAA, 0xBB, 0xCC}
	blockWithBUMP := &model.Block{
		Header:           block1.Header,
		CoinbaseTx:       block1.CoinbaseTx,
		TransactionCount: block1.TransactionCount,
		Subtrees:         block1.Subtrees,
		CoinbaseBUMP:     testBUMP,
	}

	blockID, _, err := s.StoreBlock(context.Background(), blockWithBUMP, "test-peer")
	require.NoError(t, err)

	// Retrieve the block by ID and verify BUMP data is preserved
	retrieved, err := s.GetBlockByID(context.Background(), blockID)
	require.NoError(t, err)
	assert.Equal(t, testBUMP, []byte(retrieved.CoinbaseBUMP))
}

func TestStoreBlock_CoinbaseBUMP_NilBackwardCompat(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)
	storeURL, err := url.Parse("sqlitememory:///")
	require.NoError(t, err)

	s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
	require.NoError(t, err)
	defer s.Close()

	// Store a block WITHOUT CoinbaseBUMP
	blockID, _, err := s.StoreBlock(context.Background(), block1, "test-peer")
	require.NoError(t, err)

	// Retrieve and verify CoinbaseBUMP is nil
	retrieved, err := s.GetBlockByID(context.Background(), blockID)
	require.NoError(t, err)
	assert.Nil(t, retrieved.CoinbaseBUMP)
}

func TestBlock_CoinbaseBUMP_JSONSerialization(t *testing.T) {
	t.Run("hex encoded when present", func(t *testing.T) {
		block := &model.Block{
			Header:       &model.BlockHeader{},
			CoinbaseBUMP: model.HexBytes{0xDE, 0xAD, 0xBE, 0xEF},
		}

		data, err := json.Marshal(block)
		require.NoError(t, err)

		var parsed map[string]interface{}
		err = json.Unmarshal(data, &parsed)
		require.NoError(t, err)

		bumpVal, ok := parsed["coinbase_bump"]
		require.True(t, ok, "coinbase_bump field should be present in JSON")
		assert.Equal(t, "deadbeef", bumpVal)
	})

	t.Run("omitted when nil", func(t *testing.T) {
		block := &model.Block{
			Header: &model.BlockHeader{},
		}

		data, err := json.Marshal(block)
		require.NoError(t, err)

		var parsed map[string]interface{}
		err = json.Unmarshal(data, &parsed)
		require.NoError(t, err)

		_, ok := parsed["coinbase_bump"]
		assert.False(t, ok, "coinbase_bump field should NOT be present in JSON when nil")
	})

	t.Run("round-trip unmarshaling", func(t *testing.T) {
		original := model.HexBytes{0x01, 0x02, 0x03}

		data, err := json.Marshal(original)
		require.NoError(t, err)
		assert.Equal(t, `"010203"`, string(data))

		var decoded model.HexBytes
		err = json.Unmarshal(data, &decoded)
		require.NoError(t, err)
		assert.Equal(t, original, decoded)
	})
}
