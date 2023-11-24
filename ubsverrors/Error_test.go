package ubsverrors_test

import (
	"io"
	"testing"

	"github.com/bitcoin-sv/ubsv/ubsverrors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	ErrOne   = ubsverrors.New("ErrorOne")
	ErrTwo   = ubsverrors.New("ErrorTwo")
	ErrThree = ubsverrors.New("ErrorThree")
)

func One() error {
	if err := Two(); err != nil {
		return ubsverrors.Wrap(ErrOne, err)
	}
	return nil
}

func Two() error {
	if err := Three(); err != nil {
		return ubsverrors.Wrap(ErrTwo, err)
	}
	return nil
}

func Three() error {
	return ubsverrors.Wrap(ErrThree, ubsverrors.Wrap(io.EOF, io.ErrClosedPipe))
}

func TestWrap(t *testing.T) {
	err := One()
	require.Error(t, err)

	assert.Equal(t, "ErrorOne: ErrorTwo: ErrorThree: EOF: io: read/write on closed pipe", err.Error())
	assert.ErrorIs(t, err, ErrOne)
}
