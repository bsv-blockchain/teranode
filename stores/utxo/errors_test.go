package utxo

import (
	"testing"

	"github.com/bitcoin-sv/ubsv/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestErrTxNotFound(t *testing.T) {
	err := errors.New(errors.ERR_TX_NOT_FOUND, "tx not found")
	require.Error(t, err)

	assert.True(t, errors.Is(err, errors.ErrTxNotFound))
}
