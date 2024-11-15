/*
Package validator implements Bitcoin SV transaction validation functionality.

This package provides comprehensive transaction validation for Bitcoin SV nodes,
including script verification, UTXO management, and policy enforcement. It supports
multiple script interpreters (GoBT, GoSDK, GoBDK) and implements the full Bitcoin
transaction validation ruleset.

Key features:
  - Transaction validation against Bitcoin consensus rules
  - UTXO spending and creation
  - Script verification using multiple interpreters
  - Policy enforcement
  - Block assembly integration
  - Kafka integration for transaction metadata

Usage:

	validator := NewTxValidator(logger, policy, params)
	err := validator.ValidateTransaction(tx, blockHeight)
*/
package validator

import (
	"context"

	"github.com/bitcoin-sv/ubsv/stores/utxo"
	"github.com/libsv/go-bt/v2"
)

type MockValidatorClient struct {
	BlockHeight     uint32
	MedianBlockTime uint32
	Errors          []error
	TxMetaStore     utxo.Store
}

func (m *MockValidatorClient) Health(ctx context.Context, checkLiveness bool) (int, string, error) {
	return 0, "MockValidator", nil
}

func (m *MockValidatorClient) SetBlockHeight(blockHeight uint32) error {
	m.BlockHeight = blockHeight
	return nil
}

func (m *MockValidatorClient) GetBlockHeight() uint32 {
	return m.BlockHeight
}

func (m *MockValidatorClient) SetMedianBlockTime(medianTime uint32) error {
	m.MedianBlockTime = medianTime
	return nil
}

func (m *MockValidatorClient) GetMedianBlockTime() uint32 {
	return m.MedianBlockTime
}

func (m *MockValidatorClient) Validate(_ context.Context, tx *bt.Tx, blockHeight uint32, opts ...Option) error {
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
func (m *MockValidatorClient) TriggerBatcher() {}
