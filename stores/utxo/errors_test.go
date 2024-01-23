package utxo_test

import (
	"testing"

	"github.com/bitcoin-sv/ubsv/ubsverrors"
	"github.com/stretchr/testify/assert"
)

func TestIs(t *testing.T) {
	err1 := ubsverrors.New(ubsverrors.ErrorConstants_NOT_FOUND, "utxo not found")
	err2 := ubsverrors.New(ubsverrors.ErrorConstants_NOT_FOUND, "meta not found")

	assert.ErrorIs(t, err1, err2)

}
