package errorchain_test

import (
	"fmt"
	"testing"

	"github.com/bitcoin-sv/ubsv/playground/errors/errorchain"
)

var (
	ErrOne   = errorchain.New("Service1", fmt.Errorf("error from one"))
	ErrTwo   = errorchain.New("Service 2", fmt.Errorf("error from two"))
	ErrThree = errorchain.New("Service 3", fmt.Errorf("error from three"))
)

func TestMError(t *testing.T) {
	err := one()
	if err != nil {
		t.Logf("ERROR: %s\n", err)
	}

	// For simplicity, just checking if the string representation contains
	// a substring related to each error type
	if err != nil && (err.Error() == ErrOne.Error()) {
		t.Log("Was error 1")
	}

	if err != nil && (err.Error() == ErrTwo.Error()) {
		t.Log("Was error 2")
	}

	// No unwrap functionality in our custom error type
	// log.Println(errors.Unwrap(err))
}

func one() error {
	err := two()
	return errorchain.Wrap("Service 1", err)
}

func two() error {
	return errorchain.Wrap("Service 2", three())
}
func three() error {
	return errorchain.Wrap("Service 3", ErrThree)
}
