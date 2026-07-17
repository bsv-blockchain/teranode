package model

import (
	"context"
	"encoding/hex"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-chaincfg"
	txmap "github.com/bsv-blockchain/go-tx-map"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// TestCheckBlockVersion exercises the BIP34/66/65 version floor against the mainnet, regtest and
// teratestnet activation heights, mirroring bitcoin-sv ContextualCheckBlockHeader
// (validation.cpp:5918-5924): version < 2/3/4 is rejected at or after BIP34/66/65Height.
func TestCheckBlockVersion(t *testing.T) {
	// Copy the package params to avoid mutating the shared globals.
	mainParams := chaincfg.MainNetParams          // BIP34=227931, BIP66=363725, BIP65=388381
	regtestParams := chaincfg.RegressionNetParams // BIP34=100000000, BIP66=1251, BIP65=1351

	tests := []struct {
		name       string
		version    uint32
		height     uint32
		params     *chaincfg.Params
		wantReject bool
	}{
		// Mainnet boundary matrix.
		{"v1 below BIP34", 1, 227930, &mainParams, false},
		{"v1 at BIP34", 1, 227931, &mainParams, true},
		{"v2 at BIP34", 2, 227931, &mainParams, false},
		{"v2 below BIP66", 2, 363724, &mainParams, false},
		{"v2 at BIP66", 2, 363725, &mainParams, true},
		{"v3 at BIP66", 3, 363725, &mainParams, false},
		{"v3 below BIP65", 3, 388380, &mainParams, false},
		{"v3 at BIP65", 3, 388381, &mainParams, true},
		{"v4 at BIP65", 4, 388381, &mainParams, false},
		{"v4 high height", 4, 900000, &mainParams, false},
		{"0x20000000 high height", 0x20000000, 900000, &mainParams, false},

		// Signedness: high-bit versions are negative int32 and therefore below every floor.
		{"0xffffffff (signed -1) at 300000", 0xffffffff, 300000, &mainParams, true},
		{"0x80000000 (signed high bit) at 300000", 0x80000000, 300000, &mainParams, true},
		{"0x20000000 at 300000", 0x20000000, 300000, &mainParams, false},

		// Regtest: proves the OR across BIP34/66/65 (BIP66 engages at 1251).
		{"regtest v1 at 100", 1, 100, &regtestParams, false},
		{"regtest v1 at 1000", 1, 1000, &regtestParams, false},
		{"regtest v1 at BIP66", 1, 1251, &regtestParams, true},

		// Genesis is always exempt, even where BIP0034Height == 0.
		{"genesis v1", 1, 0, &mainParams, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := CheckBlockVersion(tt.version, tt.height, tt.params)
			if tt.wantReject {
				require.Error(t, err)
				require.Contains(t, err.Error(), "bad-version")
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// TestCheckBlockVersionErrorToken pins the exact svnode-style reject token, so the formatted
// message stays byte-compatible with bitcoin-sv's bad-version(0x%08x) / rejected nVersion=0x%08x
// text (the unsigned version is used for both hex fields).
func TestCheckBlockVersionErrorToken(t *testing.T) {
	mainParams := chaincfg.MainNetParams

	err := CheckBlockVersion(1, 227931, &mainParams) // v1 at BIP34
	require.Error(t, err)
	require.Contains(t, err.Error(), "bad-version(0x00000001) rejected nVersion=0x00000001 block")

	err = CheckBlockVersion(0xffffffff, 300000, &mainParams) // signed -1, below the floor
	require.Error(t, err)
	require.Contains(t, err.Error(), "bad-version(0xffffffff) rejected nVersion=0xffffffff block")
}

// TestCheckBlockVersionGenesisExemptTeraTestNet checks that a v1 genesis on a network whose
// BIP0034Height is 0 (teratestnet/tstn) is not rejected, matching svnode which never runs the
// header-version check on the genesis block.
func TestCheckBlockVersionGenesisExemptTeraTestNet(t *testing.T) {
	teraParams := chaincfg.TeraTestNetParams // BIP34=BIP65=BIP66=0
	require.NoError(t, CheckBlockVersion(1, 0, &teraParams))
	// A non-genesis v1 block on the same network must be rejected (floor active from height 1).
	require.Error(t, CheckBlockVersion(1, 1, &teraParams))
}

// TestBlockValidRejectsOutdatedVersion drives the full Block.Valid path and asserts that the
// version floor is enforced there and that a v1 block at/after BIP34 no longer bypasses
// validation but is rejected earlier with bad-version.
func TestBlockValidRejectsOutdatedVersion(t *testing.T) {
	blockHeaderBytes, err := hex.DecodeString(block1Header)
	require.NoError(t, err)

	coinbase, err := bt.NewTxFromString(CoinbaseHex)
	require.NoError(t, err)

	mainParams := chaincfg.MainNetParams

	t.Run("v1 at mainnet height 300000 is rejected as bad-version", func(t *testing.T) {
		tSettings := test.CreateBaseTestSettings(t)
		tSettings.ChainCfgParams = &mainParams

		blockHeader, err := NewBlockHeaderFromBytes(blockHeaderBytes)
		require.NoError(t, err)
		blockHeader.Version = 1

		block, err := NewBlock(blockHeader, coinbase, []*chainhash.Hash{}, 1, 123, 300000, 0)
		require.NoError(t, err)

		valid, err := block.Valid(context.Background(), ulogger.TestLogger{}, nil, createTestUTXOStore(t),
			txmap.NewSyncedMap[chainhash.Hash, []uint32](), []*BlockHeader{}, []uint32{}, tSettings, nil)
		require.False(t, valid)
		require.Error(t, err)
		require.Contains(t, err.Error(), "bad-version")
	})

	t.Run("v2 at mainnet height 300000 passes the version floor and reaches the coinbase-height check", func(t *testing.T) {
		tSettings := test.CreateBaseTestSettings(t)
		tSettings.ChainCfgParams = &mainParams

		blockHeader, err := NewBlockHeaderFromBytes(blockHeaderBytes)
		require.NoError(t, err)
		blockHeader.Version = 2

		block, err := NewBlock(blockHeader, coinbase, []*chainhash.Hash{}, 1, 123, 300000, 0)
		require.NoError(t, err)

		_, err = block.Valid(context.Background(), ulogger.TestLogger{}, nil, createTestUTXOStore(t),
			txmap.NewSyncedMap[chainhash.Hash, []uint32](), []*BlockHeader{}, []uint32{}, tSettings, nil)
		// A v2 block must never be rejected by the version floor at this height; if it fails a later
		// (e.g. coinbase-height) check that is fine, but the error must not be bad-version.
		if err != nil {
			require.NotContains(t, err.Error(), "bad-version")
		}
	})
}

// TestBlockValidGenesisExemption drives the full Block.Valid path for a height-0 block on a network
// whose BIP0034Height is 0 (teratestnet). It proves both the version exemption and the
// mandatory b.Height > 0 guard on the coinbase-height gate: without that guard,
// heightAtOrAfterActivation(0, 0) would be true and Block.Valid would attempt ExtractCoinbaseHeight
// and reject the genesis block.
func TestBlockValidGenesisExemption(t *testing.T) {
	blockHeaderBytes, err := hex.DecodeString(block1Header)
	require.NoError(t, err)

	coinbase, err := bt.NewTxFromString(CoinbaseHex)
	require.NoError(t, err)

	teraParams := chaincfg.TeraTestNetParams
	tSettings := test.CreateBaseTestSettings(t)
	tSettings.ChainCfgParams = &teraParams

	blockHeader, err := NewBlockHeaderFromBytes(blockHeaderBytes)
	require.NoError(t, err)
	blockHeader.Version = 1

	block, err := NewBlock(blockHeader, coinbase, []*chainhash.Hash{}, 1, 123, 0, 0)
	require.NoError(t, err)

	_, err = block.Valid(context.Background(), ulogger.TestLogger{}, nil, createTestUTXOStore(t),
		txmap.NewSyncedMap[chainhash.Hash, []uint32](), []*BlockHeader{}, []uint32{}, tSettings, nil)
	// Whether or not later body checks pass, the height-0 block must never be rejected for its
	// version or for a coinbase-height mismatch.
	if err != nil {
		require.NotContains(t, err.Error(), "bad-version")
		require.NotContains(t, err.Error(), "coinbase height")
		require.NotContains(t, err.Error(), "does not match block height")
	}
}
