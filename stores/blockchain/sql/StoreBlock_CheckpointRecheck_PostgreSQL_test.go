package sql

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/go-chaincfg"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// TestStoreBlock_CheckpointRecheckUnderChainWriteLock_Postgres pins the second read of the best
// height that StoreBlock's fork path makes under the chain write lock. Another process moves
// the best chain past a checkpoint after the first read, which still sees it below; only the
// read under the lock can refuse the fork.
func TestStoreBlock_CheckpointRecheckUnderChainWriteLock_Postgres(t *testing.T) {
	t.Parallel() // a database of its own on the shared server

	ctx := context.Background()

	dbURL := postgresTestURL(t)

	tSettings := test.CreateBaseTestSettings(t)
	params := *tSettings.ChainCfgParams
	params.Checkpoints = []chaincfg.Checkpoint{{Height: 3, Hash: block3.Hash()}}
	tSettings.ChainCfgParams = &params

	// Two processes on one database.
	a, err := New(ulogger.TestLogger{}, dbURL, tSettings)
	require.NoError(t, err)
	defer a.Close(context.Background())

	b, err := New(ulogger.TestLogger{}, dbURL, tSettings)
	require.NoError(t, err)
	defer b.Close(context.Background())

	_, _, err = a.StoreBlock(ctx, block1, "")
	require.NoError(t, err)
	_, _, err = a.StoreBlock(ctx, block2, "")
	require.NoError(t, err)

	// After a's first read (best at 2), b extends the best chain to the checkpoint.
	a.forkBestReadHook = func() {
		a.forkBestReadHook = nil

		_, _, storeErr := b.StoreBlock(ctx, block3, "")
		require.NoError(t, storeErr)
	}

	_, _, err = a.StoreBlock(ctx, blockAlternative2, "")
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.ErrBlockInvalid), "got: %v", err)
	require.Contains(t, err.Error(), "bad-fork-prior-to-checkpoint")

	exists, err := a.GetBlockExists(ctx, blockAlternative2.Hash())
	require.NoError(t, err)
	require.False(t, exists, "the fork must not be stored")
}
