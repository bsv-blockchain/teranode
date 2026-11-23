package utxopersister

import (
	"context"
	"net/url"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-chaincfg"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/stores/blob"
	"github.com/bsv-blockchain/teranode/stores/blob/memory"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

type mockHeaderIfc struct{}

func (m *mockHeaderIfc) GetBlockHeadersByHeight(ctx context.Context, startHeight, endHeight uint32) ([]*model.BlockHeader, []*model.BlockHeaderMeta, error) {
	nBits, err := model.NewNBitFromString("1d00ffff")
	if err != nil {
		return nil, nil, err
	}

	headers := []*model.BlockHeader{
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
	}

	metas := []*model.BlockHeaderMeta{
		{
			Height: 169,
		},
		{
			Height: 170,
		},
	}

	return headers, metas, nil
}

func TestConsolidateBlockRange(t *testing.T) {
	ctx := context.Background()
	// logger := ulogger.New("test")
	logger := ulogger.NewVerboseTestLogger(t)

	storeURL, err := url.Parse("file://./data/blockstore")
	require.NoError(t, err)

	blockStore, err := blob.NewStore(logger, storeURL)
	require.NoError(t, err)

	tSettings := test.CreateBaseTestSettings(t)
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

// A roll-up reports progress once per block, so a liveness heartbeat can tell a
// long catch-up from a wedge (issue 1447).
func TestConsolidateBlockRange_ReportsProgressPerBlock(t *testing.T) {
	ctx := context.Background()
	store := memory.New()
	tSettings := test.CreateBaseTestSettings(t)

	headers, metas, byH := buildChainHeaders(t, tSettings.ChainCfgParams.GenesisHash, 3)

	for h := uint32(1); h <= 3; h++ {
		stageBlockDeltas(t, ctx, tSettings, store, byH[h], h, p2pkhTx(t, byte(h), 1000))
	}

	mockClient := &blockchain.Mock{}
	mockClient.On("GetBlockHeadersByHeight", mock.Anything, uint32(1), uint32(3)).Return(headers, metas, nil)

	c := NewConsolidator(ulogger.TestLogger{}, tSettings, nil, mockClient, store, tSettings.ChainCfgParams.GenesisHash)

	beats := 0
	c.progress = func() { beats++ }

	require.NoError(t, c.ConsolidateBlockRange(ctx, 1, 3))
	require.Equal(t, 3, beats)
}

// Records report progress once every progressBeatEvery, not on each one, so
// beating costs nothing next to the records themselves.
func TestConsolidatorRecordDone_ReportsEveryBatch(t *testing.T) {
	c := &consolidator{}

	beats := 0
	c.progress = func() { beats++ }

	for range 2*progressBeatEvery + 1 {
		c.recordDone()
	}

	require.Equal(t, 2, beats)

	// No progress callback, as in a consolidator nobody watches, is a no-op.
	(&consolidator{}).recordDone()
}

// A missing deletions ledger is never "no deletions": NewUTXOSet always writes
// the file, so a miss means the block persister has not finished the block.
// Treating it as zero spends would roll the block into the set with every
// spent output still present.
func TestConsumeDeletions_MissingLedgerPropagatesAsError(t *testing.T) {
	ctx := context.Background()
	logger := ulogger.NewVerboseTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)

	storeURL, err := url.Parse("file://" + t.TempDir())
	require.NoError(t, err)

	store, err := blob.NewStore(logger, storeURL)
	require.NoError(t, err)

	us := &UTXOSet{
		logger:           logger,
		settings:         tSettings,
		blockHash:        chainhash.HashH([]byte("additions-written-deletions-missing")),
		blockHeight:      200,
		blockHeightKnown: true,
		store:            store,
	}

	c := &consolidator{
		logger:    logger,
		settings:  tSettings,
		additions: map[UTXODeletion]*Addition{},
		deletions: map[UTXODeletion]struct{}{},
	}

	err = c.consumeDeletions(ctx, us)
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.ErrNotFound), "expected the store's not-found error to reach the caller, got %v", err)
	require.Empty(t, c.deletions)
}

func TestConsumeDeletions_PropagatesReaderErrors(t *testing.T) {
	ctx := context.Background()
	logger := ulogger.NewVerboseTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)

	us := &UTXOSet{
		logger:           logger,
		settings:         tSettings,
		blockHash:        chainhash.HashH([]byte("height-unknown-block")),
		blockHeightKnown: false,
	}

	c := &consolidator{
		logger:    logger,
		settings:  tSettings,
		additions: map[UTXODeletion]*Addition{},
		deletions: map[UTXODeletion]struct{}{},
	}

	err := c.consumeDeletions(ctx, us)
	require.Error(t, err)
	require.Contains(t, err.Error(), "requires the block height")
}
