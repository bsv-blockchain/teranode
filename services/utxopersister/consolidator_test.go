package utxopersister

import (
	"context"
	"net/url"
	"testing"
	"time"

	"github.com/bitcoin-sv/ubsv/chaincfg"
	"github.com/bitcoin-sv/ubsv/model"
	"github.com/bitcoin-sv/ubsv/settings"
	"github.com/bitcoin-sv/ubsv/stores/blob"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type mockHeaderIfc struct{}

func (m *mockHeaderIfc) GetBlockHeadersByHeight(ctx context.Context, startHeight, endHeight uint32) ([]*model.BlockHeader, []*model.BlockHeaderMeta, error) {
	nBits, err := model.NewNBitFromString("1d00ffff")
	if err != nil {
		return nil, nil, err
	}

	return []*model.BlockHeader{
			{ // block 169
				Version:        1,
				HashPrevBlock:  mustDecodeHash("00000000567e95797f93675ac23683ae3787b183bb36859c18d9220f3fa66a69"),
				HashMerkleRoot: mustDecodeHash("d7b9a9da6becbf47494c27e913241e5a2b85c5cceba4b2f0d8305e0a87b92d98"),
				Timestamp:      uint32(time.Unix(1231730523, 0).Unix()), // nolint:gosec
				Bits:           *nBits,
				Nonce:          3718213931,
			},
			{ // block 170
				Version:        1,
				HashPrevBlock:  mustDecodeHash("000000002a22cfee1f2c846adbd12b3e183d4f97683f85dad08a79780a84bd55"),
				HashMerkleRoot: mustDecodeHash("7dac2c5666815c17a3b36427de37bb9d2e2c5ccec3f8633eb91a4205cb4c10ff"),
				Timestamp:      uint32(time.Unix(1231731025, 0).Unix()), // nolint:gosec
				Bits:           *nBits,
				Nonce:          1889418792,
			},
		}, []*model.BlockHeaderMeta{
			{
				Height: 169,
			},
			{
				Height: 170,
			},
		}, nil
}

func TestConsolidateBlockRange(t *testing.T) {
	ctx := context.Background()
	// logger := ulogger.New("test")
	logger := ulogger.TestLogger{}

	storeURL, err := url.Parse("file://./data/blockstore")
	require.NoError(t, err)

	blockStore, err := blob.NewStore(logger, storeURL)
	require.NoError(t, err)

	tSettings := settings.NewSettings()
	tSettings.ChainCfgParams = &chaincfg.MainNetParams

	consolidator := NewConsolidator(logger, tSettings, &mockHeaderIfc{}, nil, blockStore, nil)

	err = consolidator.ConsolidateBlockRange(ctx, 169, 170)
	require.NoError(t, err)

	assert.Len(t, consolidator.additions, 4)
	assert.Len(t, consolidator.deletions, 1)
}

func mustDecodeHash(s string) *chainhash.Hash {
	h, err := chainhash.NewHashFromStr(s)
	if err != nil {
		panic(err)
	}

	return h
}

func TestMap(t *testing.T) {
	tx1 := chainhash.HashH([]byte("test1"))

	consolidator := &consolidator{
		additions: map[UTXODeletion]*Addition{
			{tx1, 1}: {
				Height:   1,
				Coinbase: false,
				Value:    100,
				Order:    1,
			},
		},
	}

	tx2 := chainhash.HashH([]byte("test1"))

	// Compare the actual hash values
	assert.Equal(t, tx1, tx2, "tx1 and tx2 should have the same hash value")

	// Check if the key exists in the map
	_, exists := consolidator.additions[UTXODeletion{TxID: tx2, Index: 1}]
	assert.True(t, exists, "The key should exist in the map")

	// Additional check to ensure the pointers are different but values are the same
	assert.NotSame(t, &tx1, &tx2, "tx1 and tx2 should be different pointers")
	assert.Equal(t, tx1.String(), tx2.String(), "tx1 and tx2 should have the same string representation")
}

func TestGetSortedUTXOWrappers(t *testing.T) {
	tx1 := chainhash.HashH([]byte("test1"))
	tx2 := chainhash.HashH([]byte("test2"))

	consolidator := &consolidator{
		additions: map[UTXODeletion]*Addition{
			{tx1, 1}: {
				Height:   1,
				Coinbase: false,
				Value:    100,
				Order:    1,
			},
			{tx1, 4}: {
				Height:   1,
				Coinbase: false,
				Value:    200,
				Order:    2,
			},
			{tx2, 1}: {
				Height:   1,
				Coinbase: false,
				Value:    300,
				Order:    6,
			},
			{tx1, 2}: {
				Height:   1,
				Coinbase: false,
				Value:    150,
				Order:    3,
			},
		},
	}

	wrappers := consolidator.getSortedUTXOWrappers()

	assert.Len(t, wrappers, 2)
	assert.Equal(t, wrappers[0].TxID, tx1)
	assert.Equal(t, wrappers[1].TxID, tx2)
	assert.Equal(t, wrappers[0].UTXOs[0].Index, uint32(1))
	assert.Equal(t, wrappers[0].UTXOs[1].Index, uint32(2))
	assert.Equal(t, wrappers[0].UTXOs[2].Index, uint32(4))
	assert.Equal(t, wrappers[1].UTXOs[0].Index, uint32(1))
}
