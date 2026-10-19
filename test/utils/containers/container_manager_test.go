package containers

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/teranode/stores/utxo/sql"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

func TestSQLiteURLCreatesUsableStore(t *testing.T) {
	manager, err := NewContainerManager(UTXOStoreSQLite)
	require.NoError(t, err)
	storeURL, err := manager.Initialize(t.Context())
	require.NoError(t, err)
	store, err := sql.New(t.Context(), ulogger.TestLogger{}, test.CreateBaseTestSettings(t), storeURL)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close(context.Background())) })
	require.NoError(t, store.RawDB().PingContext(t.Context()))
}
