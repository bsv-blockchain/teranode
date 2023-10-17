//go:build manual_tests

package redis

import (
	"context"
	"testing"

	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/ordishs/gocore"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRing(t *testing.T) {
	u, _, _ := gocore.Config().GetURL("utxostore")
	r, err := NewRedis(u)
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
