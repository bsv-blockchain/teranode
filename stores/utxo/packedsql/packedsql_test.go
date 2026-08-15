package packedsql

import (
	"context"
	"net/http"
	"net/url"
	"sync"
	"testing"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/test/utils/postgres"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

func newTestStore(t *testing.T) *Store {
	dsn, cleanup, err := postgres.SetupTestPostgresContainer()
	require.NoError(t, err)

	t.Cleanup(func() { _ = cleanup() })

	storeURL, err := url.Parse(dsn)
	require.NoError(t, err)

	storeURL.Scheme = "packedsql"

	tSettings := settings.NewSettings()
	tSettings.UtxoStore.PackedSQLPartitions = 4

	store, err := New(context.Background(), ulogger.TestLogger{}, tSettings, storeURL)
	require.NoError(t, err)

	t.Cleanup(func() { _ = store.Close(context.Background()) })

	return store
}

func TestNewStoreHealth(t *testing.T) {
	store := newTestStore(t)

	status, msg, err := store.Health(context.Background(), true)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, status)
	require.NotEmpty(t, msg)
	require.True(t, store.SupportsOutpointOnlySpend())
}

func TestBlockStateAtomicity(t *testing.T) {
	store := newTestStore(t)

	require.NoError(t, store.SetBlockState(10, 999))

	state := store.GetBlockState()
	require.Equal(t, uint32(10), state.Height)
	require.Equal(t, uint32(999), state.MedianTime)
	require.Equal(t, uint32(10), store.GetBlockHeight())
	require.Equal(t, uint32(999), store.GetMedianBlockTime())

	err := store.SetBlockHeight(0)
	require.ErrorIs(t, err, errors.ErrInvalidArgument)

	err = store.SetBlockState(0, 5)
	require.ErrorIs(t, err, errors.ErrInvalidArgument)

	require.NoError(t, store.SetMedianBlockTime(1234))
	require.Equal(t, uint32(1234), store.GetMedianBlockTime())
	require.Equal(t, uint32(10), store.GetBlockHeight())

	require.NoError(t, store.SetBlockState(1, 1001))

	var wg sync.WaitGroup

	stop := make(chan struct{})

	wg.Add(1)

	go func() {
		defer wg.Done()

		for h := uint32(1); ; h++ {
			select {
			case <-stop:
				return
			default:
				_ = store.SetBlockState(h, h+1000)
			}
		}
	}()

	for i := 0; i < 10000; i++ {
		s := store.GetBlockState()
		require.Equal(t, s.Height+1000, s.MedianTime)
	}

	close(stop)
	wg.Wait()
}
