package merrors

import (
	"fmt"
	"strings"
)

type ServiceError struct {
	Err         error
	ServiceName string
}

func (se *ServiceError) Error() string {
	return fmt.Sprintf("%s: %v", se.ServiceName, se.Err)
}

type MultiError struct {
	errors []ServiceError
}

func Wrap(serviceName string, errors ...error) error {
	// Create a function to flatten errors
	flattenErrors := func(err error) []ServiceError {
		if m, ok := err.(*MultiError); ok {
			return m.errors
		}
		return []ServiceError{{ServiceName: serviceName, Err: err}}
	}

	m := &MultiError{}
	for _, err := range errors {
		m.errors = append(m.errors, flattenErrors(err)...)
	}

	if len(m.errors) == 0 {
		return nil
	}
	return m
}

func (m *MultiError) Error() string {
	var sb strings.Builder
	for i, err := range m.errors {
		if i > 0 {
			sb.WriteString(" - ")
		}
		sb.WriteString(fmt.Sprintf("%s: %v", err.ServiceName, err.Err))
	}

	return sb.String()
}
