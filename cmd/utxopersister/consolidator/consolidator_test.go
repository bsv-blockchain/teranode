package consolidator

import (
	"context"
	"net/url"
	"testing"
	"time"

	"github.com/bitcoin-sv/ubsv/model"
	"github.com/bitcoin-sv/ubsv/services/legacy/wire"
	"github.com/bitcoin-sv/ubsv/stores/blob"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/stretchr/testify/require"
)

type mockBlockHeader struct {
	hash string
}

func (m mockBlockHeader) Bytes() []byte {
	return []byte{}
}
func (m mockBlockHeader) HasMetTargetDifficulty() (bool, *chainhash.Hash, error) {
	return false, nil, nil
}
func (m mockBlockHeader) String() string {
	return ""
}
func (m mockBlockHeader) StringDump() string {
	return ""
}
func (m mockBlockHeader) ToWireBlockHeader() *wire.BlockHeader {
	return &wire.BlockHeader{}
}
func (m *mockBlockHeader) Hash() *chainhash.Hash {
	h, _ := chainhash.NewHashFromStr(m.hash)
	return h
}

type mockHeaderIfc struct{}

func (m *mockHeaderIfc) GetBlockHeadersFromHeight(ctx context.Context, height, limit uint32) ([]*model.BlockHeader, []*model.BlockHeaderMeta, error) {
	nBits, err := model.NewNBitFromString("1d00ffff")
	if err != nil {
		return nil, nil, err
	}

	return []*model.BlockHeader{
		{ // block 170
			Version:        1,
			HashPrevBlock:  mustDecodeHash("000000002a22cfee1f2c846adbd12b3e183d4f97683f85dad08a79780a84bd55"),
			HashMerkleRoot: mustDecodeHash("7dac2c5666815c17a3b36427de37bb9d2e2c5ccec3f8633eb91a4205cb4c10ff"),
			Timestamp:      uint32(time.Unix(1231731025, 0).Unix()),
			Bits:           *nBits,
			Nonce:          1889418792,
		},
		{ // block 169
			Version:        1,
			HashPrevBlock:  mustDecodeHash("00000000567e95797f93675ac23683ae3787b183bb36859c18d9220f3fa66a69"),
			HashMerkleRoot: mustDecodeHash("d7b9a9da6becbf47494c27e913241e5a2b85c5cceba4b2f0d8305e0a87b92d98"),
			Timestamp:      uint32(time.Unix(1231730523, 0).Unix()),
			Bits:           *nBits,
			Nonce:          3718213931,
		},
	}, nil, nil
}

func TestConsolidateBlockRange(t *testing.T) {
	ctx := context.Background()
	logger := ulogger.New("test")

	storeURL, err := url.Parse("file://./data/blockstore")
	require.NoError(t, err)

	blockStore, err := blob.NewStore(logger, storeURL)
	require.NoError(t, err)

	consolidator := NewConsolidator(logger, &mockHeaderIfc{}, nil, blockStore)

	err = consolidator.ConsolidateBlockRange(ctx, 169, 170)
	require.NoError(t, err)
}

func mustDecodeHash(s string) *chainhash.Hash {
	h, err := chainhash.NewHashFromStr(s)
	if err != nil {
		panic(err)
	}

	return h
}
