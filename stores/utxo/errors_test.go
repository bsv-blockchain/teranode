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
	var txID *chainhash.Hash
	vOut := uint32(0)
	var utxoHash *chainhash.Hash
	var spendingTxID *chainhash.Hash
	err := NewErrSpent(txID, vOut, utxoHash, spendingTxID)
	t.Log(err.Error())
	require.NotNil(t, err)
}
