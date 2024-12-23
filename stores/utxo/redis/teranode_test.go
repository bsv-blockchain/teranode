package redis

import (
	"context"
	"net/url"
	"testing"
	"time"

	"github.com/bitcoin-sv/teranode/stores/utxo"
	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/libsv/go-bt/v2/chainhash"
	redis_db "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRegisterLuaIfNecessary(t *testing.T) {
	t.SkipNow()

	rdb := redis_db.NewClient(&redis_db.Options{
		Addr: "localhost:6379",
	})

	_, deferFn, err := registerLuaForTesting(rdb)
	require.NoError(t, err)

	defer func() {
		require.NoError(t, deferFn())
	}()
}

func TestDataTypes(t *testing.T) {
	t.SkipNow()

	rdb := redis_db.NewClient(&redis_db.Options{
		Addr: "localhost:6379",
	})

	ctx := context.Background()

	hash := chainhash.HashH([]byte("test"))

	err := rdb.Del(ctx, hash.String()).Err()
	require.NoError(t, err)

	fields := map[string]interface{}{
		"int":    1,
		"bool":   false,
		"string": "test",
		"time":   time.Now(),
		"float":  1.1,
		"bytes":  []byte("test"),
	}

	err = rdb.HSet(ctx, hash.String(), fields).Err()
	require.NoError(t, err)

	data, err := rdb.HGetAll(ctx, hash.String()).Result()
	require.NoError(t, err)

	for k, v := range data {
		assert.IsType(t, "string", k)
		assert.IsType(t, "string", v)
	}
}

func TestSpend(t *testing.T) {
	t.SkipNow()

	ctx := context.Background()

	redisURL, err := url.Parse("redis://localhost:6379")
	require.NoError(t, err)

	store, err := New(ctx, ulogger.TestLogger{}, redisURL)
	require.NoError(t, err)

	version, deferFn, err := registerLuaForTesting(store.client)
	require.NoError(t, err)

	store.setVersion(version)

	defer func() {
		require.NoError(t, deferFn())
	}()

	hash := chainhash.HashH([]byte("test"))

	err = store.client.Del(ctx, hash.String()).Err()
	require.NoError(t, err)

	fields := map[string]interface{}{
		"nrUtxos":    1,
		"isCoinbase": false,
		"spentUtxos": 0,
		"utxo:0":     hash.String(),
	}

	err = store.client.HSet(ctx, hash.String(), fields).Err()
	require.NoError(t, err)

	spends := []*utxo.Spend{
		{
			TxID:         &hash,
			Vout:         0,
			UTXOHash:     &hash,
			SpendingTxID: &hash,
		},
	}

	err = store.Spend(ctx, spends, 0)
	require.NoError(t, err)
}
