package blockchain

import (
	"context"
	"net/url"
	"testing"

	"github.com/bsv-blockchain/teranode/errors"
	blockchain_store "github.com/bsv-blockchain/teranode/stores/blockchain"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// TestCommittedBlockHashLookup: the lookup ConfirmLeftovers relies on must say
// "no holder" for an id no committed block carries, and name the block for one
// that does. That other errors are not read as "no holder" rests on the
// errors.Is check, covered for the remote client's error mapping by
// TestBlockNotFoundSurvivesGRPC; this test does not inject one.
func TestCommittedBlockHashLookup(t *testing.T) {
	ctx := context.Background()
	tSettings := test.CreateBaseTestSettings(t)

	store, err := blockchain_store.NewStore(ulogger.TestLogger{}, &url.URL{Scheme: "sqlitememory"}, tSettings)
	require.NoError(t, err)

	client, err := NewLocalClient(ulogger.TestLogger{}, tSettings, store, nil, nil)
	require.NoError(t, err)

	lookup := CommittedBlockHashLookup(client)
	require.NotNil(t, lookup)

	genesis, genesisMeta, err := client.GetBestBlockHeader(ctx)
	require.NoError(t, err)

	holder, err := lookup(ctx, uint64(genesisMeta.ID))
	require.NoError(t, err)
	require.Equal(t, genesis.Hash(), holder, "a committed block is named as the holder of its id")

	holder, err = lookup(ctx, 1_000_000)
	require.NoError(t, err, "an id no committed block holds is an answer, not a failure")
	require.Nil(t, holder)

	require.Nil(t, CommittedBlockHashLookup(nil), "no client gives no lookup, which ConfirmLeftovers refuses")
}

// TestBlockNotFoundSurvivesGRPC: over the remote client, GetBlockByID returns
// errors.UnwrapGRPC of the server's error. The lookup reads "no holder" from
// ErrBlockNotFound, so that code must survive the round trip, and a storage
// error must not turn into it.
func TestBlockNotFoundSurvivesGRPC(t *testing.T) {
	notFound := errors.UnwrapGRPC(errors.WrapGRPC(errors.NewBlockNotFoundError("failed to get block by ID")))
	require.True(t, errors.Is(notFound, errors.ErrBlockNotFound))

	storage := errors.UnwrapGRPC(errors.WrapGRPC(errors.NewStorageError("failed to get block by ID")))
	require.False(t, errors.Is(storage, errors.ErrBlockNotFound))
}
