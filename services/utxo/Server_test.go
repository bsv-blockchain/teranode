package utxo

import (
	"context"
	"testing"

	"github.com/TAAL-GmbH/ubsv/services/utxo/utxostore_api"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStore(t *testing.T) {
	hash := []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32}

	s, err := New(nil)
	require.NoError(t, err)

	res, err := s.Store(context.Background(), &utxostore_api.StoreRequest{
		UxtoHash: hash,
	})

	assert.NoError(t, err)
	assert.Equal(t, utxostore_api.Status_OK, res.Status)

	res, err = s.Store(context.Background(), &utxostore_api.StoreRequest{
		UxtoHash: hash[:],
	})

	assert.NoError(t, err)
	assert.Equal(t, utxostore_api.Status_OK, res.Status)
}

func TestStoreAndSpend(t *testing.T) {
	hash := []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32}
	spendingHash := []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32}

	s, err := New(nil)
	require.NoError(t, err)

	res, err := s.Store(context.Background(), &utxostore_api.StoreRequest{
		UxtoHash: hash,
	})

	assert.NoError(t, err)
	assert.Equal(t, utxostore_api.Status_OK, res.Status)

	res2, err := s.Spend(context.Background(), &utxostore_api.SpendRequest{
		UxtoHash:     hash,
		SpendingTxid: spendingHash,
	})

	assert.NoError(t, err)
	assert.Equal(t, utxostore_api.Status_OK, res2.Status)

	res2, err = s.Spend(context.Background(), &utxostore_api.SpendRequest{
		UxtoHash:     hash,
		SpendingTxid: spendingHash,
	})

	assert.NoError(t, err)
	assert.Equal(t, utxostore_api.Status_OK, res2.Status)

	spendingHash[0] = 2

	res2, err = s.Spend(context.Background(), &utxostore_api.SpendRequest{
		UxtoHash:     hash,
		SpendingTxid: spendingHash,
	})

	assert.NoError(t, err)
	assert.Equal(t, utxostore_api.Status_SPENT, res2.Status)
	assert.Equal(t, hash[:], res2.SpendingTxid)
}
