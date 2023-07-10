package blob

import (
	"context"
	"time"

	"github.com/TAAL-GmbH/ubsv/stores/blob/options"
)

type Store interface {
	Get(ctx context.Context, key []byte) ([]byte, error)
	Set(ctx context.Context, key []byte, value []byte, opts ...options.Options) error
	SetTTL(ctx context.Context, key []byte, ttl time.Duration) error
	Del(ctx context.Context, key []byte) error
}
