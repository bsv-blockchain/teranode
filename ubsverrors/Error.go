package ubsverrors

import (
	"errors"
	"fmt"
	"reflect"
)

type Error struct {
	Sentinel               error
	ImplementationSpecific error
}

func (e *Error) Error() string {
	if e.ImplementationSpecific == nil {
		return e.Sentinel.Error()
	}
	return fmt.Sprintf("%s: %s", e.Sentinel, e.ImplementationSpecific)
}

func (e *Error) As(target interface{}) bool {
	if target == nil {
		return false
	}

	if e.Sentinel != nil {
		if errors.As(e.Sentinel, target) {
			return true
		}
	}

	if e.ImplementationSpecific != nil {
		return errors.As(e.ImplementationSpecific, target)
	}

	return false
}

func (e *Error) Is(target error) bool {
	if target == nil {
		return e.Sentinel == nil
	}

	err := e.Sentinel
	doTestString := false

	if t, ok := err.(*Error); ok {
		err = t.Sentinel
	} else if t, ok := err.(errString); ok {
		err = t
		doTestString = true
	}

	targetType := reflect.TypeOf(target)
	sentinelType := reflect.TypeOf(err)

	if sentinelType == targetType {
		if doTestString {
			return err.Error() == target.Error()
		}
		return true
	}

	if e.ImplementationSpecific != nil {
		return errors.Is(e.ImplementationSpecific, target)
	}
	return false
}

func (e *Error) Unwrap() error {
	return e.ImplementationSpecific
}

func Wrap(sentinel error, implementationSpecific ...error) *Error {
	var wrappedErr error
	if len(implementationSpecific) > 0 {
		wrappedErr = implementationSpecific[0]
	}

	return &Error{
		Sentinel:               sentinel,
		ImplementationSpecific: wrappedErr,
	}
}

type errString string

func (e errString) Error() string {
	return string(e)
}

func NewErrString(text string) error {
	return errString(text)
}
