package utxo

import (
	"testing"

	"github.com/bitcoin-sv/ubsv/errors"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestErrTxNotFound(t *testing.T) {
	err := errors.NewTxNotFoundError("tx not found")
	require.Error(t, err)

	assert.True(t, errors.Is(err, errors.ErrTxNotFound))
}

func TestErrSpent(t *testing.T) {
	txID := chainhash.HashH([]byte("test"))
	vOut := uint32(0)
	utxoHash := chainhash.HashH([]byte("utxo"))
	spendingTxID := chainhash.Hash{}

	err := errors.NewUtxoSpentError(txID, vOut, utxoHash, spendingTxID)
	require.NotNil(t, err)

	err = errors.NewProcessingError("processing error 1", err)
	err = errors.NewProcessingError("processing error 2", err)
	err = errors.NewProcessingError("processing error 3", err)
	err = errors.NewProcessingError("processing error 4", err)

	var uErr *errors.Error
	ok := errors.As(err, &uErr)
	require.True(t, ok)

	var usErr *errors.UtxoSpentErrData
	ok = errors.AsData(uErr, &usErr)
	require.True(t, ok)

	assert.Equal(t, txID.String(), usErr.Hash.String())
	assert.Equal(t, vOut, usErr.Vout)
	assert.Equal(t, spendingTxID.String(), usErr.SpendingTxHash.String())
}
