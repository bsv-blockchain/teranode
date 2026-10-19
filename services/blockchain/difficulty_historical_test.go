package blockchain

import (
	"context"
	"net/url"
	"os"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-chaincfg"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	blockchainstore "github.com/bsv-blockchain/teranode/stores/blockchain"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// Real headers include the first modern-DAA calculation at parent height 148,
// and continue past the first pinned testnet checkpoint at height 546.
func TestDifficultyHistoricalTestnet(t *testing.T) {
	ctx := context.Background()
	s := test.CreateBaseTestSettings(t)
	params := chaincfg.TestNetParams
	s.ChainCfgParams = &params
	store, err := blockchainstore.NewStore(ulogger.TestLogger{}, &url.URL{Scheme: "sqlitememory"}, s)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.(interface{ Close() error }).Close()) })
	difficulty, err := NewDifficulty(store, ulogger.TestLogger{}, s)
	require.NoError(t, err)
	data, err := os.ReadFile("testdata/testnet_headers_0_547.bin")
	require.NoError(t, err)
	require.Len(t, data, 548*80)
	genesis, err := model.NewBlockFromMsgBlock(params.GenesisBlock, nil)
	require.NoError(t, err)
	// The SQL store identifies genesis by its coinbase txid.
	genesis.CoinbaseTx.LockTime = 1
	parent := genesis.Header
	for height := uint32(1); height <= 547; height++ {
		header, err := model.NewBlockHeaderFromBytes(data[height*80 : (height+1)*80])
		require.NoError(t, err)
		require.Equal(t, parent.Hash(), header.HashPrevBlock)
		require.NoError(t, header.HasMetPowLimit(&params))
		met, _, err := header.HasMetTargetDifficulty()
		require.NoError(t, err)
		require.True(t, met)
		if height == 149 {
			require.Equal(t, "00000000291d8e6f5d0d2a59de8f0f206917f3e00ff53edc8f6dcaddd61f3fe9", header.Hash().String())
		}
		if height == 546 {
			require.Equal(t, params.Checkpoints[0].Hash, header.Hash())
		}
		bits, err := difficulty.CalcNextWorkRequired(ctx, parent, height-1, int64(header.Timestamp))
		require.NoError(t, err, "height %d", height)
		require.Equal(t, header.Bits, *bits, "height %d", height)
		// Difficulty consumes headers only; the store requires a coinbase field.
		_, _, err = store.StoreBlock(ctx, &model.Block{Header: header, CoinbaseTx: genesis.CoinbaseTx, TransactionCount: 1, Height: height}, "test")
		require.NoError(t, err)
		parent = header
	}
}

// Synthetic timestamps isolate retarget boundaries without mining a chain.
// All ancestor lookups still use the real SQLite store.
func historicalDifficultyChain(t *testing.T, params chaincfg.Params, count uint32, spacing uint32) (*Difficulty, []*model.BlockHeader) {
	t.Helper()
	s := test.CreateBaseTestSettings(t)
	s.ChainCfgParams = &params
	store, err := blockchainstore.NewStore(ulogger.TestLogger{}, &url.URL{Scheme: "sqlitememory"}, s)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.(interface{ Close() error }).Close()) })
	d, err := NewDifficulty(store, ulogger.TestLogger{}, s)
	require.NoError(t, err)
	genesis, err := model.NewBlockFromMsgBlock(params.GenesisBlock, nil)
	require.NoError(t, err)
	genesis.CoinbaseTx.LockTime = 1
	headers := []*model.BlockHeader{genesis.Header}
	bits, err := model.NewNBitFromString("1c0ffff0")
	require.NoError(t, err)
	for height := uint32(1); height <= count; height++ {
		header := *genesis.Header
		header.HashPrevBlock = headers[height-1].Hash()
		header.Timestamp += height * spacing
		if spacing == 0 {
			header.Timestamp -= height
		}
		header.Bits = *bits
		header.Nonce = height
		_, _, err := store.StoreBlock(context.Background(), &model.Block{Header: &header, CoinbaseTx: genesis.CoinbaseTx, TransactionCount: 1, Height: height}, "test")
		require.NoError(t, err)
		headers = append(headers, &header)
	}
	return d, headers
}

func TestDifficultyHistoricalRetarget(t *testing.T) {
	for _, tc := range []struct {
		name    string
		spacing uint32
		want    string
	}{
		{"ordinary retarget", 600, "1c0ffde7"},
		{"fast clamp", 1, "1c03fffc"},
		{"slow clamp", 3000, "1c3fffc0"},
		{"negative timespan", 0, "1c03fffc"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			params := chaincfg.TestNetParams
			d, headers := historicalDifficultyChain(t, params, 2015, tc.spacing)
			parent := *headers[2015]
			// A delayed testnet timestamp must not bypass the periodic retarget.
			bits, err := d.CalcNextWorkRequired(t.Context(), &parent, 2015, int64(parent.Timestamp)+1201)
			require.NoError(t, err)
			require.Equal(t, tc.want, bits.String())
		})
	}
}

// Count read volume while executing every query against the real SQLite store.
type historicalHeaderReadStore struct {
	blockchainstore.Store
	requestedHeaders uint64
}

func (s *historicalHeaderReadStore) GetBlockHeaders(ctx context.Context, hash *chainhash.Hash, count uint64) ([]*model.BlockHeader, []*model.BlockHeaderMeta, error) {
	s.requestedHeaders += count
	return s.Store.GetBlockHeaders(ctx, hash, count)
}

func TestDifficultyHistoricalTestnetRestoresTarget(t *testing.T) {
	for _, tc := range []struct {
		name       string
		height     uint32
		run        uint32
		want       string
		maxHeaders uint64
	}{
		{"last ordinary target", 20, 3, "1c0ffff0", 0},
		{"short run late in interval", 1800, 3, "1c0ffff0", 64},
		{"long minimum-difficulty run", 20, 70, "1c0ffff0", 0},
		{"retarget boundary", 2015, 70, "1d00ffff", 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			params := chaincfg.TestNetParams
			d, headers := historicalDifficultyChain(t, params, tc.height, 600)
			reads := &historicalHeaderReadStore{Store: d.store}
			d.store = reads
			genesis, err := model.NewBlockFromMsgBlock(params.GenesisBlock, nil)
			require.NoError(t, err)
			genesis.CoinbaseTx.LockTime = 1
			parent := headers[tc.height]
			// Persist a run of special blocks so both single and batch reads
			// see the same ancestry, including a minimum-difficulty boundary.
			for height := tc.height + 1; height <= tc.height+tc.run; height++ {
				header := *parent
				header.HashPrevBlock = parent.Hash()
				header.Timestamp += 600
				header.Bits = *d.powLimitnBits
				_, _, err := d.store.StoreBlock(t.Context(), &model.Block{Header: &header, CoinbaseTx: genesis.CoinbaseTx, TransactionCount: 1, Height: height}, "test")
				require.NoError(t, err)
				parent = &header
			}
			for _, delay := range []int64{1200, 1201} {
				bits, err := d.CalcNextWorkRequired(t.Context(), parent, tc.height+tc.run, int64(parent.Timestamp)+delay)
				require.NoError(t, err)
				want := tc.want
				if delay == 1201 {
					want = "1d00ffff"
				}
				require.Equal(t, want, bits.String())
			}
			if tc.maxHeaders != 0 {
				require.LessOrEqual(t, reads.requestedHeaders, tc.maxHeaders, "a short minimum-difficulty run must not fetch the whole retarget interval")
			}
		})
	}
}

func TestDifficultyHistoricalEDAActivation(t *testing.T) {
	params := chaincfg.MainNetParams
	params.UahfForkHeight = 20
	params.DaaForkHeight = 40
	for _, spacing := range []uint32{7199, 7200} {
		d, headers := historicalDifficultyChain(t, params, 20, spacing)
		for _, height := range []uint32{19, 20} {
			bits, err := d.CalcNextWorkRequired(t.Context(), headers[height], height, int64(headers[height].Timestamp)+600)
			require.NoError(t, err)
			want := "1c0ffff0"
			if height == 20 && spacing == 7200 {
				want = "1c13ffec"
			}
			require.Equal(t, want, bits.String(), "parent %d, spacing %d", height, spacing)
		}
	}
}

func TestDifficultyHistoricalDAAActivation(t *testing.T) {
	params := chaincfg.MainNetParams
	params.UahfForkHeight = 0
	params.DaaForkHeight = 150
	d, headers := historicalDifficultyChain(t, params, 150, 300)
	for _, tc := range []struct {
		height uint32
		want   string
	}{{149, "1c0ffff0"}, {150, "1c07fff8"}} {
		bits, err := d.CalcNextWorkRequired(t.Context(), headers[tc.height], tc.height, int64(headers[tc.height].Timestamp)+300)
		require.NoError(t, err)
		require.Equal(t, tc.want, bits.String(), "parent %d", tc.height)
	}
}

func TestDifficultyHistoricalMissingHistory(t *testing.T) {
	for _, tc := range []struct {
		name   string
		params chaincfg.Params
	}{{"testnet restoration", chaincfg.TestNetParams}, {"EDA window", chaincfg.MainNetParams}} {
		t.Run(tc.name, func(t *testing.T) {
			tc.params.UahfForkHeight = 0
			d, headers := historicalDifficultyChain(t, tc.params, 6, 600)
			parent := *headers[6]
			if tc.params.ReduceMinDifficulty {
				parent.Bits = *d.powLimitnBits
				// This hash has never been stored, independently of cache state.
				missing := *parent.HashPrevBlock
				missing[0] ^= 0xff
				parent.HashPrevBlock = &missing
			}
			// The EDA case has only seven headers where seventeen are required.
			bits, err := d.CalcNextWorkRequired(t.Context(), &parent, 20, int64(parent.Timestamp)+600)
			require.ErrorIs(t, err, errors.ErrNotFound)
			require.Nil(t, bits, "unavailable ancestry cannot silently grant minimum difficulty")
		})
	}
}

func TestDifficultySTNPreservesExistingRules(t *testing.T) {
	d, headers := historicalDifficultyChain(t, chaincfg.StnParams, 2200, 300)
	previousSettings := *d.settings
	previousParams := chaincfg.StnParams
	previousParams.DaaForkHeight = 0 // Before this fix STN always used this path.
	previousSettings.ChainCfgParams = &previousParams
	previous, err := NewDifficulty(d.store, ulogger.TestLogger{}, &previousSettings)
	require.NoError(t, err)
	for _, height := range []uint32{0, 147, 148, 2199, 2200} {
		parent := headers[height]
		want, err := previous.CalcNextWorkRequired(t.Context(), parent, height, int64(parent.Timestamp)+300)
		require.NoError(t, err)
		bits, err := d.CalcNextWorkRequired(t.Context(), parent, height, int64(parent.Timestamp)+300)
		require.NoError(t, err)
		require.Equal(t, *want, *bits, "STN parent %d", height)
	}
}

func TestDifficultyHistoricalRetargetPowLimit(t *testing.T) {
	params := chaincfg.TestNetParams
	params.PowLimitBits = 0x1c0ffff0
	params.PowLimit = CompactToBig(params.PowLimitBits)
	d, headers := historicalDifficultyChain(t, params, 2015, 3000)
	bits, err := d.CalcNextWorkRequired(t.Context(), headers[2015], 2015, int64(headers[2015].Timestamp)+600)
	require.NoError(t, err)
	require.Equal(t, "1c0ffff0", bits.String(), "a historical retarget must respect the network PoW limit")
}
