package blob

import (
	"context"
	"io"
	"time"

	"github.com/bitcoin-sv/ubsv/stores/blob/options"
)

type Store interface {
	Health(ctx context.Context, checkLiveness bool) (int, string, error)
	Exists(ctx context.Context, key []byte, opts ...options.FileOption) (bool, error)
	Get(ctx context.Context, key []byte, opts ...options.FileOption) ([]byte, error)
	GetHead(ctx context.Context, key []byte, nrOfBytes int, opts ...options.FileOption) ([]byte, error)
	GetIoReader(ctx context.Context, key []byte, opts ...options.FileOption) (io.ReadCloser, error)
	Set(ctx context.Context, key []byte, value []byte, opts ...options.FileOption) error
	SetFromReader(ctx context.Context, key []byte, value io.ReadCloser, opts ...options.FileOption) error
	SetTTL(ctx context.Context, key []byte, ttl time.Duration, opts ...options.FileOption) error
	Del(ctx context.Context, key []byte, opts ...options.FileOption) error
	Close(ctx context.Context) error
}
