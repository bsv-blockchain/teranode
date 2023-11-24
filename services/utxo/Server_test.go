package utxo

import (
	"context"
	"testing"

	"github.com/bitcoin-sv/ubsv/services/utxo/utxostore_api"
	"github.com/bitcoin-sv/ubsv/stores/utxo"
	"github.com/bitcoin-sv/ubsv/stores/utxo/memory"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/libsv/go-bt/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStore(t *testing.T) {
	tx := bt.NewTx()
	_ = tx.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", uint64(1000))

	s := New(ulogger.TestLogger{}, memory.New(false))
	err := s.Init(context.Background())
	require.NoError(t, err)

	_, err = s.Store(context.Background(), &utxostore_api.StoreRequest{
		Tx: tx.ExtendedBytes(),
	})
	assert.NoError(t, err)

	_, err = s.Store(context.Background(), &utxostore_api.StoreRequest{
		Tx: tx.ExtendedBytes(),
	})

	assert.Error(t, err, utxo.ErrAlreadyExists)
}

func TestStoreAndSpend(t *testing.T) {
	tx := bt.NewTx()
	_ = tx.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", uint64(1000))
	hash, err := util.UTXOHashFromOutput(tx.TxIDChainHash(), tx.Outputs[0], 0)
	require.NoError(t, err)

	spendingHash := []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32}

	s := New(ulogger.TestLogger{}, memory.New(false))
	err = s.Init(context.Background())
	require.NoError(t, err)

	_, err = s.Store(context.Background(), &utxostore_api.StoreRequest{
		Tx: tx.ExtendedBytes(),
	})

	assert.NoError(t, err)

	_, err = s.Spend(context.Background(), &utxostore_api.Request{
		TxId:         tx.TxIDChainHash().CloneBytes(),
		Vout:         0,
		UxtoHash:     hash.CloneBytes(),
		SpendingTxid: spendingHash,
	})

	assert.NoError(t, err)

	_, err = s.Spend(context.Background(), &utxostore_api.Request{
		TxId:         tx.TxIDChainHash().CloneBytes(),
		Vout:         0,
		UxtoHash:     hash.CloneBytes(),
		SpendingTxid: spendingHash,
	})

	assert.NoError(t, err)

	spendingHash[0] = 2

	_, err = s.Spend(context.Background(), &utxostore_api.Request{
		TxId:         tx.TxIDChainHash().CloneBytes(),
		Vout:         0,
		UxtoHash:     hash.CloneBytes(),
		SpendingTxid: spendingHash,
	})

	assert.Error(t, err, utxo.ErrTypeSpent)
}
