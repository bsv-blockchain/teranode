package ubsverrors

import "strings"

type Error struct {
	errorMessage  string
	wrappedErrors []error
}

func New(errorMessage string, wrappedErrors ...error) *Error {
	return &Error{
		wrappedErrors: wrappedErrors,
		errorMessage:  errorMessage,
	}
}

func (e *Error) Error() string {
	var sb strings.Builder

	sb.WriteString(e.errorMessage)

	for _, err := range e.wrappedErrors {
		sb.WriteString(": ")
		sb.WriteString(err.Error())
	}

	return sb.String()
}

func (e *Error) Wrap(err ...error) *Error {
	e.wrappedErrors = append(e.wrappedErrors, err...)
	return e
}

func (e *Error) Unwrap() []error {
	return e.wrappedErrors
}
