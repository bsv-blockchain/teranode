package blob

import (
	"context"
	"io"
	"time"

	"github.com/bitcoin-sv/ubsv/stores/blob/options"
)

type Store interface {
	Health(ctx context.Context) (int, string, error)
	Exists(ctx context.Context, key []byte, opts ...options.Options) (bool, error)
	Get(ctx context.Context, key []byte, opts ...options.Options) ([]byte, error)
	GetHead(ctx context.Context, key []byte, nrOfBytes int, opts ...options.Options) ([]byte, error)
	GetIoReader(ctx context.Context, key []byte, opts ...options.Options) (io.ReadCloser, error)
	Set(ctx context.Context, key []byte, value []byte, opts ...options.Options) error
	SetFromReader(ctx context.Context, key []byte, value io.ReadCloser, opts ...options.Options) error
	SetTTL(ctx context.Context, key []byte, ttl time.Duration, opts ...options.Options) error
	Del(ctx context.Context, key []byte, opts ...options.Options) error
	Close(ctx context.Context) error
}
