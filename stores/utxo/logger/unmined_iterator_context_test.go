package logger

import (
	"context"
	"net/url"
	"testing"

	"github.com/bsv-blockchain/teranode/stores/utxo"
	utxosql "github.com/bsv-blockchain/teranode/stores/utxo/sql"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

func TestLoggingPreservesUnminedIteratorCancellation(t *testing.T) {
	storeURL, err := url.Parse("sqlitememory:///")
	require.NoError(t, err)
	log := ulogger.NewErrorTestLogger(t)
	store, err := utxosql.New(t.Context(), log, test.CreateBaseTestSettings(t), storeURL)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close(context.Background())) })
	wrapped := New(t.Context(), log, store)
	opener, ok := wrapped.(interface {
		GetUnminedTxIteratorContext(context.Context) (utxo.UnminedTxIterator, error)
	})
	require.True(t, ok, "logging must preserve cancellation of the SQL iterator query")
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	iterator, err := opener.GetUnminedTxIteratorContext(ctx)
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, iterator)
}
