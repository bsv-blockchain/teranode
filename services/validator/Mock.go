package validator

import (
	"context"

	"github.com/bitcoin-sv/ubsv/stores/txmeta"
	"github.com/libsv/go-bt/v2"
)

type MockValidatorClient struct {
	Errors      []error
	TxMetaStore txmeta.Store
}

func (m *MockValidatorClient) Health(ctx context.Context) (int, string, error) {
	return 0, "MockValidator", nil
}

func (m *MockValidatorClient) Validate(_ context.Context, tx *bt.Tx) error {
	if len(m.Errors) > 0 {
		// return error and pop of stack
		err := m.Errors[0]
		m.Errors = m.Errors[1:]

		return err
	}

	if _, err := m.TxMetaStore.Create(context.Background(), tx); err != nil {
		return err
	}

	return nil
}
