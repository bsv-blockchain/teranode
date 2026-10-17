package clientfetch

import (
	"context"
	"net/url"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/model"
	blockchainsql "github.com/bsv-blockchain/teranode/stores/blockchain/sql"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/chainancestry"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// The real fetch: the blockchain SQL store on sqlitememory, through the ClientFetcher adapter,
// with a fork stored so the walk has a branch to ignore.
func TestBuildAgainstTheBlockchainStore(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)

	storeURL, err := url.Parse("sqlitememory:///")
	require.NoError(t, err)

	s, err := blockchainsql.New(ulogger.TestLogger{}, storeURL, tSettings)
	require.NoError(t, err)

	ctx := context.Background()

	genesisHeader, _, err := s.GetBlockHeader(ctx, tSettings.ChainCfgParams.GenesisHash)
	require.NoError(t, err)

	// Five blocks on the genesis, then a fork block on block 3 stored last, so the best tip is
	// block 5 and the fork tip is a branch the walk from block 5 must never take.
	main := testChain(t, genesisHeader.Hash(), 5, 1)
	for _, b := range main {
		_, _, err = s.StoreBlock(ctx, b, "test_peer")
		require.NoError(t, err)
	}

	fork := testChain(t, main[2].Hash(), 1, 99)[0]
	_, _, err = s.StoreBlock(ctx, fork, "test_peer")
	require.NoError(t, err)

	tipHeader, tipMeta, err := s.GetBestBlockHeader(ctx)
	require.NoError(t, err)
	require.Equal(t, main[4].Hash(), tipHeader.Hash(), "the longest branch is the best tip")

	anc, err := chainancestry.Build(ctx, ClientFetcher{Client: s}, *tipHeader.Hash(), tipMeta.Height, 0, 2)
	require.NoError(t, err)
	require.Equal(t, uint32(0), anc.Lo())
	require.Equal(t, tipMeta.Height, anc.Hi())

	for _, b := range main {
		_, meta, err := s.GetBlockHeader(ctx, b.Hash())
		require.NoError(t, err)

		id, ok := anc.BlockID(meta.Height)
		require.True(t, ok)
		require.Equal(t, meta.ID, id)
		require.True(t, anc.Contains(meta.ID))
	}

	_, forkMeta, err := s.GetBlockHeader(ctx, fork.Hash())
	require.NoError(t, err)
	require.False(t, anc.Contains(forkMeta.ID), "the fork block is not on the best branch")

	// Anchored on the fork tip instead, the walk follows the fork's own lineage.
	forkAnc, err := chainancestry.Build(ctx, ClientFetcher{Client: s}, *fork.Hash(), forkMeta.Height, 0, 0)
	require.NoError(t, err)
	require.True(t, forkAnc.Contains(forkMeta.ID))

	_, m4, err := s.GetBlockHeader(ctx, main[3].Hash())
	require.NoError(t, err)
	require.False(t, forkAnc.Contains(m4.ID), "block 4 of the main branch is not on the fork's lineage")
}

// testChain builds n blocks on top of prev, each with a unique nonce so their hashes differ,
// carrying the same throwaway coinbase. StoreBlock checks none of the proof of work here.
func testChain(t *testing.T, prev *chainhash.Hash, n int, nonceBase uint32) []*model.Block {
	t.Helper()

	coinbase, err := bt.NewTxFromString("01000000010000000000000000000000000000000000000000000000000000000000000000ffffffff17030100002f6d312d65752f29c267ffea1adb87f33b398fffffffff03ac505763000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88acaa505763000000001976a9143c22b6d9ba7b50b6d6e615c69d11ecb2ba3db14588acaa505763000000001976a914b7177c7deb43f3869eabc25cfd9f618215f34d5588ac00000000")
	require.NoError(t, err)

	merkle, err := chainhash.NewHashFromStr("6c487efd5e078c65988f78a52f6d9438a7a9eaf1b9446f78e692e81e8d593970")
	require.NoError(t, err)

	subtree, err := chainhash.NewHashFromStr("0e3e2357e806b6cdb1f70b54c3a3a17b6714ee1f0e68bebb44a74b1efd512098")
	require.NoError(t, err)

	bits, err := model.NewNBitFromString("207fffff")
	require.NoError(t, err)

	out := make([]*model.Block, 0, n)

	for i := 0; i < n; i++ {
		b := &model.Block{
			Header: &model.BlockHeader{
				Version:        1,
				Timestamp:      1729259727,
				Nonce:          nonceBase + uint32(i), //nolint:gosec // small test values
				HashPrevBlock:  prev,
				HashMerkleRoot: merkle,
				Bits:           *bits,
			},
			CoinbaseTx:       coinbase,
			TransactionCount: 1,
			Subtrees:         []*chainhash.Hash{subtree},
		}

		out = append(out, b)
		prev = b.Hash()
	}

	return out
}
