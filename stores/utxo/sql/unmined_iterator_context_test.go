package sql

import (
	"context"
	"net/url"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

func TestUnminedIteratorContextCancelsWaitingForConnection(t *testing.T) {
	storeURL, err := url.Parse("sqlitememory:///")
	require.NoError(t, err)
	store, err := New(t.Context(), ulogger.NewErrorTestLogger(t), test.CreateBaseTestSettings(t), storeURL)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close(context.Background())) })
	opener, ok := any(store).(interface {
		GetUnminedTxIteratorContext(context.Context) (utxo.UnminedTxIterator, error)
	})
	require.True(t, ok, "online recovery needs cancellable iterator creation")
	store.db.SetMaxOpenConns(1)
	conn, err := store.db.Conn(t.Context())
	require.NoError(t, err)
	defer conn.Close()
	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	iterator, err := opener.GetUnminedTxIteratorContext(ctx)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Nil(t, iterator)
}

func TestUnminedIteratorContextCancelsOpenRows(t *testing.T) {
	storeURL, err := url.Parse("sqlitememory:///")
	require.NoError(t, err)
	store, err := New(t.Context(), ulogger.NewErrorTestLogger(t), test.CreateBaseTestSettings(t), storeURL)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close(context.Background())) })
	opener, ok := any(store).(interface {
		GetUnminedTxIteratorContext(context.Context) (utxo.UnminedTxIterator, error)
	})
	require.True(t, ok, "online recovery needs cancellable iterator rows")
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	iterator, err := opener.GetUnminedTxIteratorContext(ctx)
	require.NoError(t, err)
	defer iterator.Close()
	cancel()
	batch, err := iterator.Next(ctx)
	require.ErrorIs(t, err, context.Canceled)
	require.Empty(t, batch)
}
