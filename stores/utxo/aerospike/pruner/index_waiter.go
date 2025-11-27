package pruner

import (
	"context"
)

// IndexWaiter defines an interface for components that can wait for an index to be built
type IndexWaiter interface {
	// WaitForIndexReady polls until the index is ready or times out
	WaitForIndexReady(ctx context.Context, indexName string) error
}
