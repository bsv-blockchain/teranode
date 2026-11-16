package pruner

import (
	"context"
	"net/url"
	"testing"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	blockchainsql "github.com/bsv-blockchain/teranode/stores/blockchain/sql"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	utxologger "github.com/bsv-blockchain/teranode/stores/utxo/logger"
	"github.com/bsv-blockchain/teranode/stores/utxo/nullstore"
	utxosql "github.com/bsv-blockchain/teranode/stores/utxo/sql"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// newInitTestServer builds a pruner server on the given UTXO store with a local
// blockchain client on a sqlitememory blockchain store.
func newInitTestServer(t *testing.T, ctx context.Context, utxoStore utxo.Store) *Server {
	t.Helper()

	logger := ulogger.TestLogger{}
	tSettings := test.CreateBaseTestSettings(t)

	chainURL, err := url.Parse("sqlitememory:///")
	require.NoError(t, err)

	chainStore, err := blockchainsql.New(logger, chainURL, tSettings)
	require.NoError(t, err)
	t.Cleanup(func() { _ = chainStore.Close(context.Background()) })

	blockchainClient, err := blockchain.NewLocalClient(logger, tSettings, chainStore, nil, nil)
	require.NoError(t, err)

	return New(ctx, logger, tSettings, utxoStore, blockchainClient, nil)
}

func newSQLiteUTXOStore(t *testing.T, ctx context.Context) utxo.Store {
	t.Helper()

	utxosql.ResetPrunerServiceForTests()
	t.Cleanup(utxosql.ResetPrunerServiceForTests)

	utxoURL, err := url.Parse("sqlitememory:///" + t.Name())
	require.NoError(t, err)

	store, err := utxosql.New(ctx, ulogger.TestLogger{}, test.CreateBaseTestSettings(t), utxoURL)
	require.NoError(t, err)

	return store
}

// Turning on UTXO store logging (logging=true on the store URL) wraps the store
// in stores/utxo/logger. Init must still find the wrapped store's pruner, so
// logging stays about logging and does not stop the pruner service starting.
func TestInit_LoggedSQLStoreProvidesPrunerService(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	loggedStore := utxologger.New(ctx, ulogger.TestLogger{}, newSQLiteUTXOStore(t, ctx))

	server := newInitTestServer(t, ctx, loggedStore)

	require.NoError(t, server.Init(ctx))
	require.NotNil(t, server.prunerService)
}

// Init looks through every wrapper, not only the outermost one.
func TestInit_DoublyWrappedSQLStoreProvidesPrunerService(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	inner := utxologger.New(ctx, ulogger.TestLogger{}, newSQLiteUTXOStore(t, ctx))
	outer := utxologger.New(ctx, ulogger.TestLogger{}, inner)

	server := newInitTestServer(t, ctx, outer)

	require.NoError(t, server.Init(ctx))
	require.NotNil(t, server.prunerService)
}

// A wrapped store with no pruner still fails Init with the service error.
func TestInit_LoggedStoreWithoutPrunerFails(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	inner, err := nullstore.NewNullStore()
	require.NoError(t, err)

	loggedStore := utxologger.New(ctx, ulogger.TestLogger{}, inner)

	server := newInitTestServer(t, ctx, loggedStore)

	err = server.Init(ctx)
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.ErrServiceError))
	require.Contains(t, err.Error(), "UTXO store does not provide pruner service")
	require.Nil(t, server.prunerService)
}
