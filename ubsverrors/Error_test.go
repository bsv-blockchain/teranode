package ubsverrors_test

import (
	"errors"
	"fmt"
	"testing"

	"github.com/bitcoin-sv/ubsv/ubsverrors"
	"github.com/stretchr/testify/assert"
)

func TestErr(t *testing.T) {
	err := ubsverrors.New(0, "test")
	t.Log(err)

}

func TestErrorWrapping(t *testing.T) {
	ErrConst := errors.New("const")

	err1 := fmt.Errorf("level 1: %w", ErrConst)
	assert.Equal(t, "level 1: const", err1.Error())
	assert.ErrorIs(t, err1, ErrConst)

	err2 := ubsverrors.New(0, "test", ErrConst)
	assert.Equal(t, "0: test: const", err2.Error())
	assert.ErrorIs(t, err2, ErrConst)

}
