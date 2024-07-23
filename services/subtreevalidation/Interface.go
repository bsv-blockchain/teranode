package subtreevalidation

import (
	"context"

	"github.com/libsv/go-bt/v2/chainhash"
)

type Interface interface {
	Health(ctx context.Context) (int, string, error)
	CheckSubtree(ctx context.Context, hash chainhash.Hash, baseUrl string, blockHeight uint32) error
}

var _ Interface = &MockSubtreeValidation{}

type MockSubtreeValidation struct{}

func (mv *MockSubtreeValidation) Health(ctx context.Context) (int, string, error) {
	return 0, "MockValidator", nil
}

func (mv *MockSubtreeValidation) CheckSubtree(ctx context.Context, hash chainhash.Hash, baseUrl string, blockHeight uint32) error {
	return nil
}
