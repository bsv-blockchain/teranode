package utxo

import (
	"testing"

	"github.com/bitcoin-sv/ubsv/errors"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestErrNotExist(t *testing.T) {
	err := NewErrTxmetaAlreadyExists(&chainhash.Hash{})
	require.Error(t, err)

	assert.True(t, errors.Is(err, errors.ErrTxmetaAlreadyExists))

	assert.Contains(t, err.Error(), "TXMETA_ALREADY_EXISTS")

}
