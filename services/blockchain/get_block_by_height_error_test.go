package blockchain

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockchain/blockchain_api"
	blockchain_store "github.com/bsv-blockchain/teranode/stores/blockchain"
	"github.com/stretchr/testify/require"
)

// failingHeightStore answers GetBlockByHeight with a storage fault and delegates
// everything else to the real store.
type failingHeightStore struct {
	blockchain_store.Store
}

func (failingHeightStore) GetBlockByHeight(context.Context, uint32) (*model.Block, error) {
	return nil, errors.NewStorageError("failed to get block by height", errors.New(errors.ERR_ERROR, "connection refused"))
}

// TestGetBlockByHeight_KeepsTheStoreClassification pins that the server passes
// the store's own answer through. The store already separates a height with no
// block (BLOCK_NOT_FOUND) from a failing query (STORAGE_ERROR); the server
// rewrapped both as BLOCK_NOT_FOUND, so a database outage reached getblockbyheight
// as "no block at that height" and was logged as a caller fault.
func TestGetBlockByHeight_KeepsTheStoreClassification(t *testing.T) {
	t.Run("a failing query is a storage error", func(t *testing.T) {
		ctx := setup(t)
		ctx.server.store = failingHeightStore{Store: ctx.server.store}

		_, err := ctx.server.GetBlockByHeight(context.Background(), &blockchain_api.GetBlockByHeightRequest{Height: 1})
		require.Error(t, err)

		unwrapped := errors.UnwrapGRPC(err)
		require.ErrorIs(t, unwrapped, errors.ErrStorageError)
		require.NotErrorIs(t, unwrapped, errors.ErrBlockNotFound, "a failing query is not a missing block")
	})

	t.Run("a height above the tip is still not found", func(t *testing.T) {
		ctx := setup(t)

		_, err := ctx.server.GetBlockByHeight(context.Background(), &blockchain_api.GetBlockByHeightRequest{Height: 1_000_000})
		require.Error(t, err)

		unwrapped := errors.UnwrapGRPC(err)
		require.ErrorIs(t, unwrapped, errors.ErrBlockNotFound)
		// The asset service answers 404 rather than 500 by finding this phrase
		// (asset/httpimpl/GetBlock.go, Search.go).
		require.Contains(t, unwrapped.Error(), "not found")
	})
}
