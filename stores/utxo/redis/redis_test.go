//go:build manual_tests

package redis

import (
	"context"
	"testing"

	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/ordishs/gocore"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRing(t *testing.T) {
	u, _, _ := gocore.Config().GetURL("utxostore")
	r, err := NewRedisRing(u)
	require.NoError(t, err)

	ctx := context.Background()

	txid := chainhash.HashH([]byte("test1"))

	_, err = r.Delete(ctx, &txid)
	require.NoError(t, err)

	// Store the txid
	res, err := r.Store(ctx, &txid, 0)
	require.NoError(t, err)
	assert.Equal(t, 0, res.Status)

	// Store it a second time
	res, err = r.Store(ctx, &txid, 0)
	require.NoError(t, err)
	assert.Equal(t, 4, res.Status) // Already exists

	spendingTxID1 := chainhash.HashH([]byte("test2"))

	// Spend txid with spendingTxID1
	res, err = r.Spend(ctx, &txid, &spendingTxID1)
	require.NoError(t, err)
	assert.Equal(t, 0, res.Status)

	// Spend txid with spendingTxID1 again
	res, err = r.Spend(ctx, &txid, &spendingTxID1)
	require.NoError(t, err)
	assert.Equal(t, 0, res.Status)

	spendingTxID2 := chainhash.HashH([]byte("test3"))

	// Spend txid with spendingTxID2 again
	res, err = r.Spend(ctx, &txid, &spendingTxID2)
	require.NoError(t, err)
	assert.Equal(t, 1, res.Status) // Already spent
	assert.Equal(t, spendingTxID1.String(), res.SpendingTxID.String())
}

func TestCluster(t *testing.T) {
	r, err := NewRedisCluster(nil)
	require.NoError(t, err)

	ctx := context.Background()

	txid := chainhash.HashH([]byte("test1"))

	_, err = r.Delete(ctx, &txid)
	require.NoError(t, err)

	// Store the txid
	res, err := r.Store(ctx, &txid, 0)
	require.NoError(t, err)
	assert.Equal(t, 0, res.Status)

	// Store it a second time
	res, err = r.Store(ctx, &txid, 0)
	require.NoError(t, err)
	assert.Equal(t, 4, res.Status) // Already exists

	spendingTxID1 := chainhash.HashH([]byte("test2"))

	// Spend txid with spendingTxID1
	res, err = r.Spend(ctx, &txid, &spendingTxID1)
	require.NoError(t, err)
	assert.Equal(t, 0, res.Status)

	// Spend txid with spendingTxID1 again
	res, err = r.Spend(ctx, &txid, &spendingTxID1)
	require.NoError(t, err)
	assert.Equal(t, 0, res.Status)

	spendingTxID2 := chainhash.HashH([]byte("test3"))

	// Spend txid with spendingTxID2 again
	res, err = r.Spend(ctx, &txid, &spendingTxID2)
	require.NoError(t, err)
	assert.Equal(t, 1, res.Status) // Already spent
	assert.Equal(t, spendingTxID1.String(), res.SpendingTxID.String())
}

func TestRingThreadSafe(t *testing.T) {
	rdb := redis.NewRing(&redis.RingOptions{
		Addrs: map[string]string{
			"shard1": "localhost:6379",
		},
	})

	ctx := context.Background()

	key := "test"

	rdb.Set(ctx, key, "INITIAL", 0)

	res := rdb.Get(ctx, key)
	assert.Equal(t, "INITIAL", res.Val())

	notifyCh1 := make(chan struct{})
	notifyCh2 := make(chan struct{})

	go func() {
		<-notifyCh1

		ctx := context.Background()
		_, err := rdb.Set(ctx, key, "HELLO", 0).Result()
		require.NoError(t, err)

		notifyCh2 <- struct{}{}
	}()

	err := rdb.Watch(ctx, func(tx *redis.Tx) error {
		res := tx.Get(ctx, key)

		if res.Err() != nil {
			return res.Err()
		}

		if res.Err() == redis.Nil {
			return errNotFound
		}

		notifyCh1 <- struct{}{}

		<-notifyCh2

		// Operation is committed only if the watched keys remain unchanged.
		_, err := tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
			res2 := pipe.Set(ctx, key, "GOODBYE", 0)
			return res2.Err()
		})
		if err != nil {
			return err
		}

		return nil
	}, key)

	res = rdb.Get(ctx, key)
	assert.Equal(t, "HELLO", res.Val())

	assert.Equal(t, "redis: transaction failed", err.Error())
}
