package blob

import (
	"context"
	"time"
)

type Options func(s *SetOptions)

type SetOptions struct {
	TTL time.Duration
}

type Store interface {
	Get(ctx context.Context, key []byte) ([]byte, error)
	Set(ctx context.Context, key []byte, value []byte, opts ...Options) error
	SetTTL(ctx context.Context, key []byte, ttl time.Duration) error
	Del(ctx context.Context, key []byte) error
}

func NewSetOptions(opts ...Options) *SetOptions {
	options := &SetOptions{}

	for _, opt := range opts {
		opt(options)
	}

	return options
}

func WithTTL(ttl time.Duration) Options {
	return func(s *SetOptions) {
		s.TTL = ttl
	}
}
