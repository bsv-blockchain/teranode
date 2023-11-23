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
		return ErrOne.Wrap(err)
	}
	return nil
}

func Two() error {
	if err := Three(); err != nil {
		return ErrTwo.Wrap(err)
	}
	return nil
}

func Three() error {
	return ErrThree.Wrap(io.EOF, io.ErrClosedPipe)
}

func TestWrap(t *testing.T) {
	err := One()
	require.Error(t, err)

	assert.Equal(t, "ErrorOne: ErrorTwo: ErrorThree: EOF: io: read/write on closed pipe", err.Error())
	assert.ErrorIs(t, err, ErrOne)
}
