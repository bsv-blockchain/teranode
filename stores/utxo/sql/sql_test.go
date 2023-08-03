package sql

import (
	"context"
	"net/url"
	"testing"
	"time"

	"github.com/TAAL-GmbH/ubsv/services/utxo/utxostore_api"
	utxostore "github.com/TAAL-GmbH/ubsv/stores/utxo"
	_ "github.com/lib/pq"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/stretchr/testify/require"
	_ "modernc.org/sqlite"
)

func Test_SQL(t *testing.T) {
	ctx := context.Background()
	sqliteURL, err := url.Parse("sqlitememory:///utxo")
	require.NoError(t, err)

	// ubsv db client
	var db *Store
	db, err = New(sqliteURL)
	require.NoError(t, err)

	var hash *chainhash.Hash
	hash, err = chainhash.NewHashFromStr("5e3bc5947f48cec766090aa17f309fd16259de029dcef5d306b514848c9687c7")
	require.NoError(t, err)

	var hash2 *chainhash.Hash
	hash2, err = chainhash.NewHashFromStr("5e3bc5947f48cec766090aa17f309fd16259de029dcef5d306b514848c9687c8")
	require.NoError(t, err)

	t.Cleanup(func() {
		err = db.delete(ctx, hash)
		require.NoError(t, err)
	})

	var resp *utxostore.UTXOResponse
	t.Run("sql store", func(t *testing.T) {
		_ = db.delete(ctx, hash)
		resp, err = db.Store(ctx, hash, 0)
		require.NoError(t, err)
		require.Equal(t, int(utxostore_api.Status_OK), resp.Status)

		resp, err = db.Get(ctx, hash)
		require.NoError(t, err)
		require.Nil(t, resp.SpendingTxID)
		require.Equal(t, uint32(0), resp.LockTime)

		resp, err = db.Store(ctx, hash, 0)
		require.NoError(t, err)
		require.Equal(t, int(utxostore_api.Status_OK), resp.Status)

		resp, err = db.Spend(ctx, hash, hash)
		require.NoError(t, err)
		require.Equal(t, int(utxostore_api.Status_OK), resp.Status)

		resp, err = db.Store(ctx, hash, 0)
		require.NoError(t, err)
		require.Equal(t, int(utxostore_api.Status_SPENT), resp.Status)
		require.Equal(t, hash, resp.SpendingTxID)
	})

	t.Run("sql spend", func(t *testing.T) {
		_ = db.delete(ctx, hash)
		resp, err = db.Store(ctx, hash, 0)
		require.NoError(t, err)
		require.Equal(t, int(utxostore_api.Status_OK), resp.Status)

		resp, err = db.Spend(ctx, hash, hash)
		require.NoError(t, err)

		resp, err = db.Get(ctx, hash)
		require.NoError(t, err)
		require.Equal(t, hash, resp.SpendingTxID)

		// try to spend with different txid
		resp, err = db.Spend(ctx, hash, hash2)
		require.NoError(t, err)
		require.Equal(t, int(utxostore_api.Status_SPENT), resp.Status)
	})

	t.Run("sql reset", func(t *testing.T) {
		_ = db.delete(ctx, hash)
		resp, err = db.Store(ctx, hash, 100)
		require.NoError(t, err)
		require.Equal(t, int(utxostore_api.Status_OK), resp.Status)

		_ = db.SetBlockHeight(101)

		resp, err = db.Spend(ctx, hash, hash)
		require.NoError(t, err)

		// try to reset the utxo
		resp, err = db.Reset(ctx, hash)
		require.NoError(t, err)

		resp, err = db.Get(ctx, hash)
		require.NoError(t, err)
		require.Nil(t, resp.SpendingTxID)
		require.Equal(t, uint32(100), resp.LockTime)
	})

	t.Run("sql block locktime", func(t *testing.T) {
		_ = db.delete(ctx, hash)
		resp, err = db.Store(ctx, hash, 1000)
		require.NoError(t, err)
		require.Equal(t, int(utxostore_api.Status_OK), resp.Status)

		resp, err = db.Get(ctx, hash)

		resp, err = db.Spend(ctx, hash, hash)
		require.NoError(t, err)
		require.Equal(t, int(utxostore_api.Status_LOCK_TIME), resp.Status)

		resp, err = db.Get(ctx, hash)
		require.NoError(t, err)
		require.Nil(t, resp.SpendingTxID)

		_ = db.SetBlockHeight(1001)

		resp, err = db.Spend(ctx, hash, hash)
		require.NoError(t, err)
		require.Equal(t, int(utxostore_api.Status_OK), resp.Status)

		resp, err = db.Get(ctx, hash)
		require.NoError(t, err)
		require.Equal(t, hash, resp.SpendingTxID)
	})

	t.Run("sql unix time locktime", func(t *testing.T) {
		_ = db.delete(ctx, hash)
		lockTime := uint32(time.Now().Unix() + 1)
		resp, err = db.Store(ctx, hash, lockTime) // valid in 1 second
		require.NoError(t, err)
		require.Equal(t, int(utxostore_api.Status_OK), resp.Status)

		resp, err = db.Get(ctx, hash)
		require.NoError(t, err)
		require.Equal(t, lockTime, resp.LockTime)

		resp, err = db.Spend(ctx, hash, hash)
		require.NoError(t, err)
		require.Equal(t, int(utxostore_api.Status_LOCK_TIME), resp.Status)

		resp, err = db.Get(ctx, hash)
		require.NoError(t, err)
		require.Nil(t, resp.SpendingTxID)

		time.Sleep(1 * time.Second)

		resp, err = db.Spend(ctx, hash, hash)
		require.NoError(t, err)
		require.Equal(t, int(utxostore_api.Status_OK), resp.Status)

		resp, err = db.Get(ctx, hash)
		require.NoError(t, err)
		require.Equal(t, hash, resp.SpendingTxID)
	})
}
