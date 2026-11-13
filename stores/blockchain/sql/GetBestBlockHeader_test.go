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

	// TestMalformedNBits verifies that a corrupted n_bits column (wrong byte length,
	// e.g. ASCII-encoded hex stored as bytea) makes GetBestBlockHeader return a clean
	// storage error instead of panicking with a nil-pointer dereference. See issue #884.
	t.Run("malformed n_bits returns error instead of panicking", func(t *testing.T) {
		storeURL, err := url.Parse("sqlitememory:///")
		require.NoError(t, err)

		s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
		require.NoError(t, err)

		_, _, err = s.StoreBlock(context.Background(), block1, "test_peer")
		require.NoError(t, err)

		_, _, err = s.StoreBlock(context.Background(), block2, "test_peer")
		require.NoError(t, err)

		// Corrupt the tip's n_bits to 8 bytes (the length the affected operator's bad
		// hex-as-ASCII restore produced), mirroring the malformed-bytea condition.
		_, err = s.db.ExecContext(context.Background(),
			"UPDATE blocks SET n_bits = ? WHERE height = 2",
			[]byte{0xff, 0xff, 0x7f, 0x20, 0x00, 0x00, 0x00, 0x00})
		require.NoError(t, err)

		// Invalidate the response cache so the corrupted row is actually read back.
		s.responseCache.DeleteAll()

		_, _, err = s.GetBestBlockHeader(context.Background())
		require.Error(t, err, "GetBestBlockHeader must return an error for malformed n_bits, not panic")
		require.Contains(t, err.Error(), "n_bits")
	})

	// Two blocks with equal chain work: the one stored first stays the tip, even when the second
	// comes from a peer whose id sorts lower. The relay chooses its own peer id, so it must play
	// no part in which of two equal blocks this node follows.
	t.Run("equal chainwork keeps the block stored first", func(t *testing.T) {
		storeURL, err := url.Parse("sqlitememory:///")
		require.NoError(t, err)

		s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
		require.NoError(t, err)

		_, _, err = s.StoreBlock(context.Background(), block1, "test_peer")
		require.NoError(t, err)

		// Two siblings at height 2 on block1, so equal chain work. Only the nonce differs.
		blockFromLaterPeer := &model.Block{
			Header: &model.BlockHeader{
				Version:        1,
				Timestamp:      1729259727,
				Nonce:          100,
				HashPrevBlock:  block1.Header.Hash(),
				HashMerkleRoot: block2MerkleRootHash,
				Bits:           *bits,
			},
			Height:           2,
			CoinbaseTx:       coinbaseTx2,
			TransactionCount: 1,
			Subtrees:         []*chainhash.Hash{subtree},
		}

		blockFromEarlierPeer := &model.Block{
			Header: &model.BlockHeader{
				Version:        1,
				Timestamp:      1729259727,
				Nonce:          200,
				HashPrevBlock:  block1.Header.Hash(),
				HashMerkleRoot: block2MerkleRootHash,
				Bits:           *bits,
			},
			Height:           2,
			CoinbaseTx:       coinbaseTx2,
			TransactionCount: 1,
			Subtrees:         []*chainhash.Hash{subtree},
		}

		// The block from zzz_peer is stored first.
		_, _, err = s.StoreBlock(context.Background(), blockFromLaterPeer, "zzz_peer")
		require.NoError(t, err)

		tip, meta, err := s.GetBestBlockHeader(context.Background())
		require.NoError(t, err)
		require.Equal(t, uint32(2), meta.Height)
		require.Equal(t, blockFromLaterPeer.Header.Hash().String(), tip.Hash().String())

		// The sibling arrives second, from aaa_peer, whose id sorts before zzz_peer.
		_, _, err = s.StoreBlock(context.Background(), blockFromEarlierPeer, "aaa_peer")
		require.NoError(t, err)

		tip, meta, err = s.GetBestBlockHeader(context.Background())
		require.NoError(t, err)
		require.Equal(t, uint32(2), meta.Height)
		require.Equal(t, "zzz_peer", meta.PeerID, "a later block from a lower-sorting peer id must not take the tip")
		require.Equal(t, blockFromLaterPeer.Header.Hash().String(), tip.Hash().String())

		// The main-chain flags and the best-height reads agree with the tip.
		atHeight, err := s.GetBlockByHeight(context.Background(), 2)
		require.NoError(t, err)
		require.Equal(t, blockFromLaterPeer.Header.Hash().String(), atHeight.Header.Hash().String())

		require.True(t, getOnMainChain(t, s, blockFromLaterPeer.Header.Hash().CloneBytes()))
		require.False(t, getOnMainChain(t, s, blockFromEarlierPeer.Header.Hash().CloneBytes()))
	})
}
