package subtreevalidation

import (
	"context"

	"github.com/libsv/go-bt/v2/chainhash"
)

type Interface interface {
	Health(ctx context.Context, checkLiveness bool) (int, string, error)
	CheckSubtree(ctx context.Context, hash chainhash.Hash, baseURL string, blockHeight uint32, blockHash *chainhash.Hash) error
}

var _ Interface = &MockSubtreeValidation{}

type MockSubtreeValidation struct{}

func (mv *MockSubtreeValidation) Health(ctx context.Context, checkLiveness bool) (int, string, error) {
	return 0, "MockValidator", nil
}

func (mv *MockSubtreeValidation) CheckSubtree(ctx context.Context, hash chainhash.Hash, baseURL string, blockHeight uint32, blockHash *chainhash.Hash) error {
	return nil
}
