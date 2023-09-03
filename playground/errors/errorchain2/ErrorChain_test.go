package errorchain2_test

import (
	"errors"
	"strings"
	"testing"
)

func Test(t *testing.T) {
	err := one()
	if err != nil {
		print(t, err)
	}
}

func print(t *testing.T, err error) {
	errs := make([]error, 0)
	for err != nil {
		errs = append(errs, err)
		err = errors.Unwrap(err) // Unwrap returns the next error in the chain
	}

	t.Logf("There are %d errors\n", len(errs))
	// Display errors on a single line
	t.Log(singleLineErrors(errs))
}

func singleLineErrors(errs []error) string {
	var errStrings []string
	for _, err := range errs {
		errStrings = append(errStrings, err.Error())
	}
	return strings.Join(errStrings, ", ")
}

var (
	ErrOne   = errors.Join(errors.New("error from one"))
	ErrTwo   = errors.Join(errors.New("error from two"))
	ErrThree = errors.Join(errors.New("error from three"))
)

func one() error {
	return errors.Join(ErrOne, two())
}

func two() error {
	return errors.Join(ErrTwo, three())
}

func three() error {
	return ErrThree
}
