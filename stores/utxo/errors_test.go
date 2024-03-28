package utxo_test

import (
	"testing"

	"github.com/bitcoin-sv/ubsv/errors"
	"github.com/stretchr/testify/assert"
)

func TestIs(t *testing.T) {
	err1 := errors.New(errors.ERR_NOT_FOUND, "utxo not found")
	err2 := errors.New(errors.ERR_NOT_FOUND, "meta not found")

	assert.ErrorIs(t, err1, err2)

}
