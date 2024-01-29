//go:build manual_tests

package redis

import (
	"context"
	"errors"
	"testing"
	"time"

	utxostore "github.com/bitcoin-sv/ubsv/stores/utxo"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/ordishs/gocore"
	"github.com/stretchr/testify/assert"

	"github.com/stretchr/testify/require"
)

var (
	tx1, _       = bt.NewTxFromString("010000000000000000ef0152a9231baa4e4b05dc30c8fbb7787bab5f460d4d33b039c39dd8cc006f3363e4020000006b483045022100ce3605307dd1633d3c14de4a0cf0df1439f392994e561b648897c4e540baa9ad02207af74878a7575a95c9599e9cdc7e6d73308608ee59abcd90af3ea1a5c0cca41541210275f8390df62d1e951920b623b8ef9c2a67c4d2574d408e422fb334dd1f3ee5b6ffffffff706b9600000000001976a914a32f7eaae3afd5f73a2d6009b93f91aa11d16eef88ac05404b4c00000000001976a914aabb8c2f08567e2d29e3a64f1f833eee85aaf74d88ac80841e00000000001976a914a4aff400bef2fa074169453e703c611c6b9df51588ac204e0000000000001976a9144669d92d46393c38594b2f07587f01b3e5289f6088ac204e0000000000001976a914a461497034343a91683e86b568c8945fb73aca0288ac99fe2a00000000001976a914de7850e419719258077abd37d4fcccdb0a659b9388ac00000000")
	utxoHash0, _ = util.UTXOHashFromOutput(tx1.TxIDChainHash(), tx1.Outputs[0], 0)
	spend1       = &utxostore.Spend{
		Vout:         0,
		Hash:         utxoHash0,
		SpendingTxID: tx1.TxIDChainHash(),
	}

	tx2 = chainhash.HashH([]byte("dummy"))

	spend1_2 = &utxostore.Spend{
		Vout:         0,
		Hash:         utxoHash0,
		SpendingTxID: &tx2,
	}

	utxoHash1, _ = util.UTXOHashFromOutput(tx1.TxIDChainHash(), tx1.Outputs[1], 1)

	spend2 = &utxostore.Spend{
		Vout:         1,
		Hash:         utxoHash1,
		SpendingTxID: tx1.TxIDChainHash(),
	}
)

func TestRedis(t *testing.T) {
	u, err, _ := gocore.Config().GetURL("utxostore")
	require.NoError(t, err)

	r, err := NewRedisClient(ulogger.TestLogger{}, u)
	// r, err := NewRedisRing(u)
	// r, err := NewRedisCluster(u)
	require.NoError(t, err)

	ctx := context.Background()

	err = r.Delete(ctx, tx1)
	require.NoError(t, err)

	// Store the txid
	err = r.Store(ctx, tx1)
	require.NoError(t, err)

	// Store it a second time
	err = r.Store(ctx, tx1)
	require.Error(t, err)
	assert.Equal(t, "0: utxo already exists", err.Error())

	// Spend txid with spend1
	err = r.Spend(ctx, []*utxostore.Spend{spend1})
	require.NoError(t, err)

	// Spend txid with spend1 again
	err = r.Spend(ctx, []*utxostore.Spend{spend1})
	require.NoError(t, err)

	// Spend txid with spend1_2
	err = r.Spend(ctx, []*utxostore.Spend{spend1_2})
	require.Error(t, err)
	assert.True(t, errors.Is(err, utxostore.ErrTypeSpent))

	// var errSpentExtra *utxostore.ErrSpent
	require.ErrorIs(t, err, utxostore.ErrTypeSpent)
	// ok := errors.As(err, &errSpentExtra)
	// require.True(t, ok)
	// assert.Equal(t, errSpentExtra.SpendingTxID.String(), spend1.SpendingTxID.String())
}

func TestRedisLockTime(t *testing.T) {
	u, err, _ := gocore.Config().GetURL("utxostore")
	require.NoError(t, err)

	r, err := NewRedisClient(ulogger.TestLogger{}, u)
	// r, err := NewRedisRing(u)
	// r, err := NewRedisCluster(u)
	require.NoError(t, err)

	ctx := context.Background()

	err = r.Delete(ctx, tx1)
	require.NoError(t, err)

	// Store the txid with locktime
	err = r.Store(ctx, tx1, 1000)
	require.NoError(t, err)

	err = r.SetBlockHeight(100)
	require.NoError(t, err)

	height := r.getBlockHeight()
	assert.Equal(t, uint32(100), height)

	// Spend txid with spend1
	err = r.Spend(ctx, []*utxostore.Spend{spend1})
	require.Error(t, err)
}

func TestRedisTTL(t *testing.T) {
	u, err, _ := gocore.Config().GetURL("utxostore")
	require.NoError(t, err)

	r, err := NewRedisClient(ulogger.TestLogger{}, u)
	// r, err := NewRedisRing(u)
	// r, err := NewRedisCluster(u)
	require.NoError(t, err)

	ctx := context.Background()

	err = r.Delete(ctx, tx1)
	require.NoError(t, err)

	// Store the txid
	err = r.Store(ctx, tx1)
	require.NoError(t, err)

	var val *utxostore.Response

	val, err = r.Get(ctx, spend1)
	require.NoError(t, err)
	assert.Equal(t, int(utxostore.Status_OK), val.Status)
	assert.Nil(t, val.SpendingTxID)
	assert.Equal(t, uint32(0), val.LockTime)

	r.spentUtxoTtl = 1

	// Spend txid with spend1
	err = r.Spend(ctx, []*utxostore.Spend{spend1})
	require.NoError(t, err)

	// check the ttl
	dur := r.rdb.TTL(ctx, utxoHash0.String())
	assert.Greater(t, dur.Val(), time.Duration(0)*time.Second)

	val, err = r.Get(ctx, spend1)
	require.NoError(t, err)
	assert.Equal(t, int(utxostore.Status_SPENT), val.Status)
	assert.Equal(t, val.SpendingTxID, spend1.SpendingTxID)
	assert.Equal(t, uint32(0), val.LockTime)

	time.Sleep(1100 * time.Millisecond)

	val, err = r.Get(ctx, spend1)
	require.NoError(t, err)
	assert.Equal(t, int(utxostore.Status_NOT_FOUND), val.Status)
	assert.Nil(t, val.SpendingTxID)
	assert.Equal(t, uint32(0), val.LockTime)
}

func TestRollbackSpend(t *testing.T) {
	u, err, _ := gocore.Config().GetURL("utxostore")
	require.NoError(t, err)

	r, err := NewRedisClient(ulogger.TestLogger{}, u)
	// r, err := NewRedisRing(u)
	// r, err := NewRedisCluster(u)
	require.NoError(t, err)

	ctx := context.Background()

	err = r.Delete(ctx, tx1)
	require.NoError(t, err)

	// Store the txid
	err = r.Store(ctx, tx1)
	require.NoError(t, err)

	// Spend txid with spend1
	err = r.Spend(ctx, []*utxostore.Spend{spend1})
	require.NoError(t, err)

	// Spend txid with spend2 and spend1_2
	err = r.Spend(ctx, []*utxostore.Spend{spend2, spend1_2})
	require.Error(t, err)
	assert.True(t, errors.Is(err, utxostore.ErrTypeSpent))

	var errSpentExtra *utxostore.ErrSpent
	ok := errors.As(err, &errSpentExtra)
	require.True(t, ok)
	assert.Equal(t, errSpentExtra.SpendingTxID.String(), spend1.SpendingTxID.String())
}
