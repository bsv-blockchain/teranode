package model

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-chaincfg"
	txmap "github.com/bsv-blockchain/go-tx-map"
	"github.com/bsv-blockchain/teranode/errors"
	blobmemory "github.com/bsv-blockchain/teranode/stores/blob/memory"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// coinbaseEncodingHeight builds a BIP34-compliant coinbase tx whose scriptSig encodes `height`.
func coinbaseEncodingHeight(t *testing.T, height uint32) *bt.Tx {
	t.Helper()

	c1, c2, err := GetCoinbaseParts(height, 5000000000, "/teranode/", []string{"1DkmRkb5iQFkDu4NBysog5bugnsyx7kwtn"})
	require.NoError(t, err)

	coinbase, err := bt.NewTxFromBytes(BuildCoinbase(c1, c2, "0000000000000000", "00000000"))
	require.NoError(t, err)
	require.True(t, coinbase.IsCoinbase())

	return coinbase
}

// minedHeaderVersion is minedHeader with a configurable version, needed because the BIP34
// coinbase-height check only runs at/after BIP0034Height — where CheckBlockVersion also enforces the
// version-2 floor, so a version-1 header would be rejected before BIP34 is ever reached.
func minedHeaderVersion(t *testing.T, version uint32, merkleRoot *chainhash.Hash) *BlockHeader {
	t.Helper()

	prevHash := chainhash.Hash{}
	nBits, err := NewNBitFromString("207fffff")
	require.NoError(t, err)

	hdr := &BlockHeader{
		Version:        version,
		HashPrevBlock:  &prevHash,
		HashMerkleRoot: merkleRoot,
		Timestamp:      1296688602,
		Bits:           *nBits,
		Nonce:          0,
	}

	for {
		ok, _, _ := hdr.HasMetTargetDifficulty()
		if ok {
			break
		}
		hdr.Nonce++
	}

	return hdr
}

// bip34ReorderSettings returns settings whose params activate BIP34 at height 1, so a modest test
// height exercises the coinbase-height check. Version 4 blocks satisfy every version floor.
func bip34ReorderSettings(t *testing.T) *chaincfg.Params {
	t.Helper()

	params := chaincfg.RegressionNetParams
	params.BIP0034Height = 1

	return &params
}

// TestBlock_Valid_BIP34Reorder is the freemans13 item 3 fix (bitcoin-sv/teranode#4692, C2/C3): the
// BIP34 coinbase-height check now runs AFTER the merkle binding, and is classified by whether the
// body was merkle-bound. A merkle-MATCHING body with a wrong BIP34 height is genuine consensus
// invalidity (condemn once, invalid=true); an UNBOUND body (coinbase-only / no subtrees, where
// CheckMerkleRoot never ran) can only be corrupt (re-download, never poison), preserving the
// bound-before-poisoning rule.
func TestBlock_Valid_BIP34Reorder(t *testing.T) {
	const (
		coinbaseHeight = uint32(100) // encoded in the coinbase scriptSig
		blockHeight    = uint32(200) // header height != coinbase height -> BIP34 fails
	)

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.ChainCfgParams = bip34ReorderSettings(t)

	txHash, err := chainhash.NewHashFromStr("0f9188f13cb7b2c71f2a335e3a4fc328bf5beb436012afca590b1a11466e2206")
	require.NoError(t, err)

	t.Run("merkle-bound bad-BIP34 height is CONDEMNED (invalid, never re-downloaded)", func(t *testing.T) {
		coinbase := coinbaseEncodingHeight(t, coinbaseHeight)

		// A merkle-MATCHING body: the header commits exactly this coinbase, so a wrong BIP34 height
		// is genuine consensus invalidity.
		st, merkleRoot := buildSubtreeAndMerkleRoot(t, coinbase, *txHash)
		hdr := minedHeaderVersion(t, 4, merkleRoot)

		block, err := NewBlock(hdr, coinbase, []*chainhash.Hash{st.RootHash()}, 2, 123, blockHeight, 0)
		require.NoError(t, err)

		blobStore := blobmemory.New()
		storeSubtree(t, blobStore, st)

		oldBlockIDs := txmap.NewSyncedMap[chainhash.Hash, []uint32]()

		valid, err := block.Valid(
			context.Background(), ulogger.TestLogger{}, blobStore,
			&panicTxMetaStore{}, oldBlockIDs, []*BlockHeader{}, []uint32{}, tSettings, nil,
		)
		require.False(t, valid)
		require.Error(t, err)
		require.True(t, errors.Is(err, errors.ErrBlockInvalid),
			"a merkle-bound wrong BIP34 height must be condemned invalid, got: %v", err)
		require.False(t, errors.IsBlockCorrupt(err), "a merkle-bound body must NOT be classified corrupt")
		require.Contains(t, err.Error(), "does not match block height")
	})

	t.Run("coinbase-only bad-BIP34 height is CORRUPT (re-download, never poisoned)", func(t *testing.T) {
		coinbase := coinbaseEncodingHeight(t, coinbaseHeight)

		// No subtrees: the merkle block is skipped, merkleRootChecked stays false, so BIP34 runs on
		// an UNBOUND body and must classify corrupt — never invalid=true.
		hdr := minedHeaderVersion(t, 4, coinbase.TxIDChainHash())

		block, err := NewBlock(hdr, coinbase, []*chainhash.Hash{}, 1, uint64(coinbase.Size()), blockHeight, 0) //nolint:gosec
		require.NoError(t, err)

		oldBlockIDs := txmap.NewSyncedMap[chainhash.Hash, []uint32]()

		// nil subtreeStore == the internal / no-subtree caller: the merkle block never runs.
		valid, err := block.Valid(
			context.Background(), ulogger.TestLogger{}, nil,
			&panicTxMetaStore{}, oldBlockIDs, []*BlockHeader{}, []uint32{}, tSettings, nil,
		)
		require.False(t, valid)
		require.Error(t, err)
		require.True(t, errors.IsBlockCorrupt(err),
			"an unbound (coinbase-only) wrong BIP34 height must be corrupt (re-download), got: %v", err)
		require.False(t, errors.Is(err, errors.ErrBlockInvalid),
			"an unbound body must NEVER be poisoned (invalid=true) on a BIP34 failure")
		require.Contains(t, err.Error(), "does not match block height")
	})
}
