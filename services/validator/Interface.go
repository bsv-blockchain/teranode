package validator

import (
	"context"

	"github.com/libsv/go-bt/v2"
)

type Interface interface {
	Health(ctx context.Context) (int, string, error)
	Validate(ctx context.Context, tx *bt.Tx, blockHeight uint32) error
	GetBlockHeight() (uint32, error)
}

var _ Interface = &MockValidator{}

type MockValidator struct{}

func (mv *MockValidator) Health(ctx context.Context) (int, string, error) {
	return 0, "Mock Validator", nil
}

func (mv *MockValidator) Validate(ctx context.Context, tx *bt.Tx, blockHeight uint32) error {
	return nil
}

func (mv *MockValidator) GetBlockHeight() (uint32, error) {
	return 0, nil
}
