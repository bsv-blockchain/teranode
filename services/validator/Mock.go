package validator

import (
	"context"

	"github.com/libsv/go-bt/v2"
)

type MockValidatorClient struct {
	Errors []error
}

func (m *MockValidatorClient) Validate(_ context.Context, _ *bt.Tx) error {
	if len(m.Errors) > 0 {
		// return error and pop of stack
		err := m.Errors[0]
		m.Errors = m.Errors[1:]

		return err
	}

	return nil
}
