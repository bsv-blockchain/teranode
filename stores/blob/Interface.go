package blob

import (
	"context"
)

type Store interface {
	Get(ctx context.Context, key []byte) ([]byte, error)
	Set(ctx context.Context, key []byte, value []byte) error
	Del(ctx context.Context, key []byte) error
}
