package blockchain

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockchain/blockchain_api"
	"github.com/stretchr/testify/require"
)

func addBlockRequestWithID(blk *model.Block, id uint64) *blockchain_api.AddBlockRequest {
	subtreeHashes := make([][]byte, len(blk.Subtrees))
	for i, hash := range blk.Subtrees {
		subtreeHashes[i] = hash[:]
	}

	return &blockchain_api.AddBlockRequest{
		Header:           blk.Header.Bytes(),
		CoinbaseTx:       blk.CoinbaseTx.Bytes(),
		SubtreeHashes:    subtreeHashes,
		TransactionCount: blk.TransactionCount,
		SizeInBytes:      blk.SizeInBytes,
		PeerId:           "test-peer",
		OptionID:         id,
	}
}

func requireBlockAbsent(t *testing.T, tc *testContext, blk *model.Block) {
	t.Helper()

	exists, err := tc.server.GetBlockExists(context.Background(), &blockchain_api.GetBlockRequest{Hash: blk.Hash().CloneBytes()})
	require.NoError(t, err)
	require.False(t, exists.Exists, "a refused AddBlock must not write a blocks row")
}

// The gRPC AddBlock handler used to pass OptionID straight to the store. A
// caller could then write a blocks row under an id reserved for a different
// block, whose transactions already carry that id in the UTXO store. These cases
// pin the end state at the service boundary: a forged id writes nothing, and the
// reserved id stores.
func TestAddBlock_CallerSuppliedIDMustBeReserved(t *testing.T) {
	ctx := context.Background()

	t.Run("an id the sequence never issued is refused", func(t *testing.T) {
		tc := setup(t)
		blk := mockBlock(tc, t)

		_, err := tc.server.AddBlock(ctx, addBlockRequestWithID(blk, 1_000_000))
		require.Error(t, err)
		require.True(t, errors.Is(errors.UnwrapGRPC(err), errors.ErrStorageError), "got %v", err)
		requireBlockAbsent(t, tc, blk)
	})

	t.Run("an id reserved for another block is refused", func(t *testing.T) {
		tc := setup(t)
		blk := mockBlock(tc, t)

		otherHash := blk.Header.HashMerkleRoot // any hash that is not blk's
		other, err := tc.server.AssignBlockID(ctx, &blockchain_api.AssignBlockIDRequest{BlockHash: otherHash[:]})
		require.NoError(t, err)

		_, err = tc.server.AddBlock(ctx, addBlockRequestWithID(blk, other.BlockId))
		require.Error(t, err)
		requireBlockAbsent(t, tc, blk)
	})

	t.Run("the reserved id stores and the row carries it", func(t *testing.T) {
		tc := setup(t)
		blk := mockBlock(tc, t)

		reserved, err := tc.server.AssignBlockID(ctx, &blockchain_api.AssignBlockIDRequest{BlockHash: blk.Hash()[:]})
		require.NoError(t, err)

		_, err = tc.server.AddBlock(ctx, addBlockRequestWithID(blk, reserved.BlockId))
		require.NoError(t, err)

		stored, err := tc.server.store.GetBlockByID(ctx, reserved.BlockId)
		require.NoError(t, err)
		require.Equal(t, blk.Hash().String(), stored.Hash().String())
	})
}
