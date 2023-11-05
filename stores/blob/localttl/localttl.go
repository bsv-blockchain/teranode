package localttl

import (
	"context"
	"time"

	"github.com/bitcoin-sv/ubsv/stores/blob/options"
	"github.com/ordishs/go-utils"
)

type LocalTTL struct {
	logger    utils.Logger
	ttlStore  BlobStore
	blobStore BlobStore
}

type BlobStore interface {
	Health(ctx context.Context) (int, string, error)
	Exists(ctx context.Context, key []byte) (bool, error)
	Get(ctx context.Context, key []byte) ([]byte, error)
	Set(ctx context.Context, key []byte, value []byte, opts ...options.Options) error
	SetTTL(ctx context.Context, key []byte, ttl time.Duration) error
	Del(ctx context.Context, key []byte) error
	Close(ctx context.Context) error
}

func New(logger utils.Logger, ttlStore, blobStore BlobStore) (*LocalTTL, error) {
	b := &LocalTTL{
		logger:    logger,
		ttlStore:  ttlStore,
		blobStore: blobStore,
	}

	return b, nil
}

func (b *LocalTTL) Health(ctx context.Context) (int, string, error) {
	n, resp, err := b.ttlStore.Health(ctx)
	if err != nil || n <= 0 {
		return n, resp, err
	}

	return b.blobStore.Health(ctx)
}

func (b *LocalTTL) Close(_ context.Context) error {
	return nil
}

func (b *LocalTTL) Set(ctx context.Context, key []byte, value []byte, opts ...options.Options) error {
	setOptions := options.NewSetOptions(opts...)

	if setOptions.TTL > 0 {
		// set the value in the ttl store
		return b.ttlStore.Set(ctx, key, value, opts...)
	}

	// set the value in the blob store
	return b.blobStore.Set(ctx, key, value, opts...)
}

func (b *LocalTTL) SetTTL(ctx context.Context, key []byte, duration time.Duration) error {
	if duration <= 0 {
		// move the file from the TTL store to the blob store
		value, err := b.ttlStore.Get(ctx, key)
		if err != nil {
			if found, _ := b.blobStore.Exists(ctx, key); found {
				// already there
				return nil
			}
			return err
		}

		return b.blobStore.Set(ctx, key, value)
	}

	// we are setting a ttl, if it's already in the ttl store, reset the ttl, if it is not in the ttl store, move it there
	found, _ := b.ttlStore.Exists(ctx, key)
	if found {
		return b.ttlStore.SetTTL(ctx, key, duration)
	}

	// move the file from the blob store to the TTL store
	value, err := b.blobStore.Get(ctx, key)
	if err != nil {
		return err
	}

	if err = b.ttlStore.Set(ctx, key, value, options.WithTTL(duration)); err != nil {
		return err
	}

	// delete from the blob store ?
	return b.blobStore.Del(ctx, key)
}

func (b *LocalTTL) Get(ctx context.Context, key []byte) ([]byte, error) {
	value, err := b.ttlStore.Get(ctx, key)
	if err != nil {
		// couldn't find it in the ttl store, try the blob store
		return b.blobStore.Get(ctx, key)
	}

	return value, nil
}

func (b *LocalTTL) Exists(ctx context.Context, key []byte) (bool, error) {
	found, err := b.ttlStore.Exists(ctx, key)
	if err != nil || !found {
		// couldn't find it in the ttl store, try the blob store
		return b.blobStore.Exists(ctx, key)
	}

	return found, nil
}

func (b *LocalTTL) Del(ctx context.Context, key []byte) error {
	_ = b.ttlStore.Del(ctx, key)
	return b.blobStore.Del(ctx, key)
}
