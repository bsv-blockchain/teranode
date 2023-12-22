package badger

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/bitcoin-sv/ubsv/stores/blob/options"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/dgraph-io/badger/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBadger_SetTTL(t *testing.T) {
	t.Run("TestBadger_SetTTL", func(t *testing.T) {
		defer func() {
			_ = os.RemoveAll("./test")
		}()

		_ = os.RemoveAll("./test")
		s, err := New(ulogger.TestLogger{}, "./test")
		require.NoError(t, err)

		key := []byte("key")
		value := []byte("value")
		err = s.Set(context.Background(), key, value, options.WithTTL(1*time.Hour))
		require.NoError(t, err)

		bEerr := s.store.View(func(tx *badger.Txn) error {
			data, err := tx.Get(key)
			require.NoError(t, err)

			expiresAt := data.ExpiresAt()
			assert.Greater(t, expiresAt, uint64(0))

			return nil
		})
		require.NoError(t, bEerr)

		err = s.SetTTL(context.Background(), []byte("key"), 0)
		require.NoError(t, err)

		bEerr = s.store.View(func(tx *badger.Txn) error {
			data, err := tx.Get(key)
			require.NoError(t, err)

			expiresAt := data.ExpiresAt()
			assert.Equal(t, expiresAt, uint64(0))

			return nil
		})
		require.NoError(t, bEerr)
	})
}
