package subtreevalidation

import (
	"context"

	"github.com/libsv/go-bt/v2/chainhash"
)

type Interface interface {
	Health(ctx context.Context) (int, string, error)
	CheckSubtree(ctx context.Context, hash chainhash.Hash, baseUrl string, blockHeight uint32) error
}
