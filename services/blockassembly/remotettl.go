package blockassembly

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/bitcoin-sv/ubsv/stores/blob"
	"github.com/bitcoin-sv/ubsv/stores/blob/options"
)

type WrapperInterface interface {
	Get(ctx context.Context, key []byte) ([]byte, error)
	Set(ctx context.Context, key []byte, value []byte, opts ...options.Options) error
	SetTTL(ctx context.Context, key []byte, ttl time.Duration) error
}

type Wrapper struct {
	store                 blob.Store
	blobServerClient      WrapperInterface
	blockValidationClient WrapperInterface
}

func NewRemoteTTLWrapper(store blob.Store, blobServerClient, blockValidationClient WrapperInterface) (*Wrapper, error) {
	return &Wrapper{
		store:                 store,
		blobServerClient:      blobServerClient,
		blockValidationClient: blockValidationClient,
	}, nil
}

func (r Wrapper) Health(ctx context.Context) (int, string, error) {
	return r.store.Health(ctx)
}

func (r Wrapper) Exists(ctx context.Context, key []byte) (bool, error) {
	return r.store.Exists(ctx, key)
}

func (r Wrapper) GetIoReader(ctx context.Context, key []byte) (io.ReadCloser, error) {
	return r.store.GetIoReader(ctx, key)
}

func (r Wrapper) Get(ctx context.Context, key []byte) ([]byte, error) {
	subtreeBytes, _ := r.blockValidationClient.Get(ctx, key)
	if subtreeBytes == nil {
		return r.store.Get(ctx, key)
	}

	return subtreeBytes, nil
}

func (r Wrapper) SetFromReader(ctx context.Context, key []byte, value io.ReadCloser, opts ...options.Options) error {
	b, err := io.ReadAll(value)
	if err != nil {
		return fmt.Errorf("failed to read data from reader: %w", err)
	}

	return r.Set(ctx, key, b, opts...)
}

func (r Wrapper) Set(ctx context.Context, key []byte, value []byte, opts ...options.Options) error {
	return r.blobServerClient.Set(ctx, key, value, opts...)
}

func (r Wrapper) SetTTL(ctx context.Context, key []byte, ttl time.Duration) error {
	return r.blobServerClient.SetTTL(ctx, key, ttl)
}

func (r Wrapper) Del(_ context.Context, key []byte) error {
	return fmt.Errorf("not implemented")
}

func (r Wrapper) Close(ctx context.Context) error {
	return r.store.Close(ctx)
}
