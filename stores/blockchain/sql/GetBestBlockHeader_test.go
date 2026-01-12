package sql

import (
	"context"
	"encoding/hex"
	"net/url"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSqlGetChainTip(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)

	t.Run("block 0 - genesis block", func(t *testing.T) {
		storeURL, err := url.Parse("sqlitememory:///")
		require.NoError(t, err)

		s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
		require.NoError(t, err)

		tip, meta, err := s.GetBestBlockHeader(context.Background())
		require.NoError(t, err)
		assert.Equal(t, uint32(0), meta.Height)
		assert.Equal(t, uint32(1), tip.Version)

		assertRegtestGenesis(t, tip)
	})

	t.Run("multiple blocks", func(t *testing.T) {
		storeURL, err := url.Parse("sqlitememory:///")
		require.NoError(t, err)

		s, err := New(ulogger.TestLogger{}, storeURL, tSettings)

		require.NoError(t, err)

		_, _, err = s.StoreBlock(context.Background(), block1, "test_peer")
		require.NoError(t, err)

		_, _, err = s.StoreBlock(context.Background(), block2, "test_peer")
		require.NoError(t, err)

		tip, meta, err := s.GetBestBlockHeader(context.Background())
		require.NoError(t, err)

		// block 2 should be the tip
		assert.Equal(t, uint32(2), meta.Height)
		assert.Equal(t, "484e58c7bf0208d787314710535ef7be8ca31748bc9fef5e1ee2de67ebda757a", tip.Hash().String())
		assert.Equal(t, uint32(1), tip.Version)
		assert.Equal(t, *block1.Header.Hash(), *tip.HashPrevBlock)
		assert.Equal(t, "b881339d3b500bcaceb5d2f1225f45edd77e846805ddffe27788fc06f218f177", tip.HashMerkleRoot.String())
		assert.Equal(t, uint32(0x671268cf), tip.Timestamp)
		assert.Equal(t, []byte{0xff, 0xff, 0x7f, 0x20}, tip.Bits.CloneBytes())
		assert.Equal(t, uint32(0x1), tip.Nonce)
		assert.Equal(t, "0000000000000000000000000000000000000000000000000000000000000006", hex.EncodeToString(meta.ChainWork))
		assert.Equal(t, "test_peer", meta.PeerID)
	})

	t.Run("multiple tips", func(t *testing.T) {
		storeURL, err := url.Parse("sqlitememory:///")
		require.NoError(t, err)

		s, err := New(ulogger.TestLogger{}, storeURL, tSettings)

		require.NoError(t, err)

		_, _, err = s.StoreBlock(context.Background(), block1, "test_peer")
		require.NoError(t, err)

		_, _, err = s.StoreBlock(context.Background(), block2, "test_peer")
		require.NoError(t, err)

		tip, meta, err := s.GetBestBlockHeader(context.Background())
		require.NoError(t, err)

		// block 2 (000000006a625f06636b8bb6ac7b960a8d03705d1ace08b1a19da3fdcc99ddbd) should be the tip
		assert.Equal(t, uint32(2), meta.Height)
		assert.Equal(t, "484e58c7bf0208d787314710535ef7be8ca31748bc9fef5e1ee2de67ebda757a", tip.Hash().String())

		// add a block that should not become the new tip
		_, _, err = s.StoreBlock(context.Background(), blockAlternative2, "test_peer")
		require.NoError(t, err)

		tip, meta, err = s.GetBestBlockHeader(context.Background())
		require.NoError(t, err)

		// block 2 (000000006a625f06636b8bb6ac7b960a8d03705d1ace08b1a19da3fdcc99ddbd) should still be the tip
		assert.Equal(t, uint32(2), meta.Height)
		assert.Equal(t, "484e58c7bf0208d787314710535ef7be8ca31748bc9fef5e1ee2de67ebda757a", tip.Hash().String())
		assert.Equal(t, uint32(1), tip.Version)
		assert.Equal(t, *block1.Header.Hash(), *tip.HashPrevBlock)
		assert.Equal(t, "b881339d3b500bcaceb5d2f1225f45edd77e846805ddffe27788fc06f218f177", tip.HashMerkleRoot.String())
		assert.Equal(t, uint32(0x671268cf), tip.Timestamp)
		assert.Equal(t, []byte{0xff, 0xff, 0x7f, 0x20}, tip.Bits.CloneBytes())
		assert.Equal(t, uint32(0x1), tip.Nonce)
		assert.Equal(t, "test_peer", meta.PeerID)
	})

	// TestEqualChainworkTiebreaker verifies that when two chains have equal chainwork,
	// the tiebreaker uses peer_id ASC (lower peer_id wins, representing "first-seen").
	// This tests the ORDER BY clause: chain_work DESC, peer_id ASC, id ASC
	t.Run("equal chainwork prefers lower peer_id (first-seen rule)", func(t *testing.T) {
		storeURL, err := url.Parse("sqlitememory:///")
		require.NoError(t, err)

		s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
		require.NoError(t, err)

		// Store block1 to establish the chain
		_, _, err = s.StoreBlock(context.Background(), block1, "test_peer")
		require.NoError(t, err)

		// Create two competing blocks at height 2, both building on block1
		// They will have the same chainwork but different peer_ids
		// Block from peer "zzz_peer" (alphabetically later)
		blockFromLaterPeer := &model.Block{
			Header: &model.BlockHeader{
				Version:        1,
				Timestamp:      1729259727,
				Nonce:          100, // Different nonce to get different hash
				HashPrevBlock:  block1.Header.Hash(),
				HashMerkleRoot: block2MerkleRootHash,
				Bits:           *bits,
			},
			Height:           2,
			CoinbaseTx:       coinbaseTx2,
			TransactionCount: 1,
			Subtrees:         []*chainhash.Hash{subtree},
		}

		// Block from peer "aaa_peer" (alphabetically earlier)
		blockFromEarlierPeer := &model.Block{
			Header: &model.BlockHeader{
				Version:        1,
				Timestamp:      1729259727,
				Nonce:          200, // Different nonce to get different hash
				HashPrevBlock:  block1.Header.Hash(),
				HashMerkleRoot: block2MerkleRootHash,
				Bits:           *bits,
			},
			Height:           2,
			CoinbaseTx:       coinbaseTx2,
			TransactionCount: 1,
			Subtrees:         []*chainhash.Hash{subtree},
		}

		// Store the block from the LATER peer first (zzz_peer)
		_, _, err = s.StoreBlock(context.Background(), blockFromLaterPeer, "zzz_peer")
		require.NoError(t, err)

		// Check tip - should be from zzz_peer since it's the only block at height 2
		tip, meta, err := s.GetBestBlockHeader(context.Background())
		require.NoError(t, err)
		assert.Equal(t, uint32(2), meta.Height)
		assert.Equal(t, "zzz_peer", meta.PeerID)
		assert.Equal(t, blockFromLaterPeer.Header.Hash().String(), tip.Hash().String())

		// Now store the block from the EARLIER peer (aaa_peer)
		// This block has equal chainwork but lower peer_id
		_, _, err = s.StoreBlock(context.Background(), blockFromEarlierPeer, "aaa_peer")
		require.NoError(t, err)

		// Check tip again - should now be from aaa_peer due to peer_id ASC tiebreaker
		tip, meta, err = s.GetBestBlockHeader(context.Background())
		require.NoError(t, err)
		assert.Equal(t, uint32(2), meta.Height, "Height should still be 2")
		assert.Equal(t, "aaa_peer", meta.PeerID,
			"With equal chainwork, the block from the peer with lower peer_id (aaa_peer) should be preferred")
		assert.Equal(t, blockFromEarlierPeer.Header.Hash().String(), tip.Hash().String(),
			"Best block should be the one from the earlier peer")
	})
}
