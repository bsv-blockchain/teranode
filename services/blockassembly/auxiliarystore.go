package blockassembly

import (
	"context"
	"errors"
	"io"
	"time"

	"github.com/bitcoin-sv/ubsv/stores/blob"
	"github.com/bitcoin-sv/ubsv/stores/blob/options"
	"github.com/bitcoin-sv/ubsv/ubsverrors"
	"github.com/bitcoin-sv/ubsv/ulogger"
)

type AuxiliaryStore struct {
	logger         ulogger.Logger
	store          blob.Store
	auxiliaryStore blob.Store
}

// NewAuxiliaryStore creates a new AuxiliaryStore.
// The AuxiliaryStore is a wrapper around a blob.Store that will also check a secondary blob.Store for the existence of a key.
// If the key exists in the secondary blob.Store, the AuxiliaryStore will return the value from the secondary blob.Store.
// If the key does not exist in the secondary blob.Store, the AuxiliaryStore will return the value from the primary blob.Store.
// The AuxiliaryStore will only write to the primary blob.Store.
func NewAuxiliaryStore(logger ulogger.Logger, store, auxiliaryStore blob.Store) (*AuxiliaryStore, error) {
	w := &AuxiliaryStore{
		logger:         logger,
		store:          store,
		auxiliaryStore: auxiliaryStore,
	}

	return w, nil
}

func (as *AuxiliaryStore) Health(ctx context.Context) (int, string, error) {
	return as.store.Health(ctx)
}

func (as *AuxiliaryStore) TryLockIfNotExists(ctx context.Context, hash []byte, opts ...options.Options) (bool, error) {
	// TODO - implement
	return true, nil
}

func (as *AuxiliaryStore) Exists(ctx context.Context, key []byte, opts ...options.Options) (bool, error) {
	if ok, err := as.auxiliaryStore.Exists(ctx, key); ok && err == nil {
		return true, nil
	}

	return as.store.Exists(ctx, key)
}

func (as *AuxiliaryStore) GetIoReader(ctx context.Context, key []byte, opts ...options.Options) (io.ReadCloser, error) {
	reader, err := as.auxiliaryStore.GetIoReader(ctx, key)
	if err == nil && reader != nil {
		return reader, nil
	}
	if err != nil && !errors.Is(err, ubsverrors.ErrNotFound) {
		as.logger.Warnf("error reading from auxiliary store: %s", err)
	}

	return as.store.GetIoReader(ctx, key)
}

func (as *AuxiliaryStore) Get(ctx context.Context, key []byte, opts ...options.Options) ([]byte, error) {
	data, err := as.auxiliaryStore.Get(ctx, key)
	if err == nil && data != nil {
		return data, nil
	}
	if err != nil && !errors.Is(err, ubsverrors.ErrNotFound) {
		as.logger.Warnf("error reading from auxiliary store: %s", err)
	}

	return as.store.Get(ctx, key)
}

func (as *AuxiliaryStore) GetHead(ctx context.Context, key []byte, nrOfBytes int, opts ...options.Options) ([]byte, error) {
	data, err := as.auxiliaryStore.GetHead(ctx, key, nrOfBytes)
	if err == nil && data != nil {
		return data, nil
	}
	if err != nil && !errors.Is(err, ubsverrors.ErrNotFound) {
		as.logger.Warnf("error reading from auxiliary store: %s", err)
	}

	return as.store.GetHead(ctx, key, nrOfBytes)
}

func (as *AuxiliaryStore) SetFromReader(ctx context.Context, key []byte, value io.ReadCloser, opts ...options.Options) error {
	// we only write to our main store, never to our auxiliary store
	return as.store.SetFromReader(ctx, key, value, opts...)
}

func (as *AuxiliaryStore) Set(ctx context.Context, key []byte, value []byte, opts ...options.Options) error {
	// we only write to our main store, never to our auxiliary store
	return as.store.Set(ctx, key, value, opts...)
}

func (as *AuxiliaryStore) SetTTL(ctx context.Context, key []byte, ttl time.Duration, opts ...options.Options) error {
	// we only write to our main store, never to our auxiliary store
	return as.store.SetTTL(ctx, key, ttl)
}

func (as *AuxiliaryStore) Del(_ context.Context, key []byte, opts ...options.Options) error {
	// we only write to our main store, never to our auxiliary store
	return as.store.Del(context.Background(), key)
}

func (as *AuxiliaryStore) Close(ctx context.Context) error {
	_ = as.auxiliaryStore.Close(ctx)
	return as.store.Close(ctx)
}
