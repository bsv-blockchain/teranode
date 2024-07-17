package validator

import (
	"context"

	"github.com/bitcoin-sv/ubsv/stores/utxo"
	"github.com/libsv/go-bt/v2"
)

type MockValidatorClient struct {
	BlockHeight uint32
	Errors      []error
	TxMetaStore utxo.Store
}

func (m *MockValidatorClient) Health(ctx context.Context) (int, string, error) {
	return 0, "MockValidator", nil
}

func (m *MockValidatorClient) SetBlockHeight(blockHeight uint32) error {
	m.BlockHeight = blockHeight
	return nil
}

func (m *MockValidatorClient) GetBlockHeight() (uint32, error) {
	return m.BlockHeight, nil
}

func (m *MockValidatorClient) Validate(_ context.Context, tx *bt.Tx, blockHeight uint32) error {
	if len(m.Errors) > 0 {
		// return error and pop of stack
		err := m.Errors[0]
		m.Errors = m.Errors[1:]

		return err
	}

	if _, err := m.TxMetaStore.Create(context.Background(), tx, 0); err != nil {
		return err
	}

	return nil
}
