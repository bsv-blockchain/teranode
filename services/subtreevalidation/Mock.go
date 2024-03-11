package subtreevalidation

import (
	"context"
	"github.com/libsv/go-bt/v2/chainhash"
)

type MockSubtreeValidationClient struct {
}

func (m *MockSubtreeValidationClient) Health(ctx context.Context) (int, string, error) {
	return 0, "MockValidator", nil
}

func (m *MockSubtreeValidationClient) ProcessSubtree(ctx context.Context, subtreeHash chainhash.Hash, baseURL string) error {
	return nil
}
