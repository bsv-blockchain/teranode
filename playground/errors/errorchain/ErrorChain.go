package errorchain

import "fmt"

type chainedError struct {
	service string
	err     error
	prev    error
}

func (ce *chainedError) Error() string {
	if ce.prev != nil {
		return fmt.Sprintf("%s: %s - %v", ce.service, ce.err, ce.prev.Error())
	}
	return fmt.Sprintf("%s: %v", ce.service, ce.err)
}

func (ce *chainedError) Unwrap() error {
	return ce.prev
}

// New creates a new error with the given message.
func New(service string, err error) error {
	return &chainedError{service: service, err: err}
}

// Wrap wraps a series of errors with a given message.
// The first error in the list has the next error added to its chain, and so on.
func Wrap(service string, err error, errs ...error) error {
	var currentErr error = &chainedError{service: service, err: err}

	for _, err := range errs {
		if err != nil {
			currentErr = &chainedError{
				service: service,
				err:     err,
				prev:    currentErr,
			}
		}
	}

	return currentErr
}
