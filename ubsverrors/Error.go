package ubsverrors

import (
	"errors"
	"fmt"
	"reflect"
)

// Error struct defines a custom error type with two components.
type Error struct {
	Sentinel               error // Base error
	ImplementationSpecific error // Additional error providing more context
}

// Error method formats and returns the error message.
// If ImplementationSpecific is nil, it returns the Sentinel error message.
// Otherwise, it combines both Sentinel and ImplementationSpecific messages.
func (e *Error) Error() string {
	if e.ImplementationSpecific == nil {
		return e.Sentinel.Error()
	}
	return fmt.Sprintf("%s: %s", e.Sentinel, e.ImplementationSpecific)
}

// As method checks if the custom error can be assigned to the target interface.
// It returns true if either Sentinel or ImplementationSpecific errors can be
// assigned to the target.
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

// Is method determines if the custom error is equivalent to the target error.
// It compares types and, in some cases, the error messages to check equivalence.
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

// Unwrap method returns the ImplementationSpecific error.
// This allows compatibility with Go's error unwrapping functionality.
func (e *Error) Unwrap() error {
	return e.ImplementationSpecific
}

// Wrap function creates a new Error instance with a sentinel error
// and an optional implementation-specific error.
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

// errString type defines a custom error type as a string.
type errString string

// Error method for errString returns the string itself as the error message.
func (e errString) Error() string {
	return string(e)
}

// New function creates a new errString error with the given text.
func New(text string) error {
	return errString(text)
}
