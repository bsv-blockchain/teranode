package localttl

import (
	"context"
	"io"
	"time"

	"github.com/bitcoin-sv/ubsv/stores/blob/options"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/ordishs/go-utils"
)

type LocalTTL struct {
	logger    ulogger.Logger
	ttlStore  BlobStore
	blobStore BlobStore
}

type BlobStore interface {
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

func New(logger ulogger.Logger, ttlStore, blobStore BlobStore) (*LocalTTL, error) {
	b := &LocalTTL{
		logger:    logger,
		ttlStore:  ttlStore,
		blobStore: blobStore,
	}

	return b, nil
}

func (l *LocalTTL) Health(ctx context.Context) (int, string, error) {
	n, resp, err := l.ttlStore.Health(ctx)
	if err != nil || n <= 0 {
		return n, resp, err
	}

	return l.blobStore.Health(ctx)
}

func (l *LocalTTL) Close(_ context.Context) error {
	return nil
}

func (l *LocalTTL) SetFromReader(ctx context.Context, key []byte, reader io.ReadCloser, opts ...options.Options) error {
	setOptions := options.NewSetOptions(nil, opts...)

	if setOptions.TTL > 0 {
		// set the value in the ttl store
		return l.ttlStore.SetFromReader(ctx, key, reader, opts...)
	}

	// set the value in the blob store
	return l.blobStore.SetFromReader(ctx, key, reader, opts...)
}

func (l *LocalTTL) Set(ctx context.Context, key []byte, value []byte, opts ...options.Options) error {
	// l.logger.Debugf("[localTTL] Set called %v\n%s\n%s\n", utils.ReverseAndHexEncodeSlice(key), stack.Stack(), ctx.Value("stack"))
	setOptions := options.NewSetOptions(nil, opts...)

	if setOptions.TTL > 0 {
		// set the value in the ttl store
		return l.ttlStore.Set(ctx, key, value, opts...)
	}

	// set the value in the blob store
	return l.blobStore.Set(ctx, key, value, opts...)
}

func (l *LocalTTL) SetTTL(ctx context.Context, key []byte, duration time.Duration, opts ...options.Options) error {
	// l.logger.Debugf("[localTTL] SetTTL called %v\n%s\n%s\n", utils.ReverseAndHexEncodeSlice(key), stack.Stack(), ctx.Value("stack"))
	if duration <= 0 {
		// move the file from the TTL store to the blob store
		reader, err := l.ttlStore.GetIoReader(ctx, key, opts...)
		if err != nil {
			if found, _ := l.blobStore.Exists(ctx, key, opts...); found {
				// already there
				return nil
			}
			return err
		}

		return l.blobStore.SetFromReader(ctx, key, reader, opts...)
	}

	// we are setting a ttl, if it's already in the ttl store, reset the ttl, if it is not in the ttl store, move it there
	found, _ := l.ttlStore.Exists(ctx, key, opts...)
	if found {
		return l.ttlStore.SetTTL(ctx, key, duration, opts...)
	}

	// move the file from the blob store to the TTL store
	reader, err := l.blobStore.GetIoReader(ctx, key, opts...)
	if err != nil {
		return err
	}

	if err = l.ttlStore.SetFromReader(ctx, key, reader, options.WithTTL(duration)); err != nil {
		return err
	}

	// delete from the blob store ?
	return l.blobStore.Del(ctx, key, opts...)
}

func (l *LocalTTL) GetIoReader(ctx context.Context, key []byte, opts ...options.Options) (io.ReadCloser, error) {
	ioReader, err := l.ttlStore.GetIoReader(ctx, key, opts...)
	if err != nil {
		// couldn't find it in the ttl store, try the blob store
		l.logger.Errorf("LocalTTL.GetIoReader miss for %s", utils.ReverseAndHexEncodeSlice(key))
		return l.blobStore.GetIoReader(ctx, key, opts...)
	}

	return ioReader, nil
}

func (l *LocalTTL) Get(ctx context.Context, key []byte, opts ...options.Options) ([]byte, error) {
	value, err := l.ttlStore.Get(ctx, key, opts...)
	if err != nil {
		// couldn't find it in the ttl store, try the blob store
		l.logger.Errorf("LocalTTL.Get miss for %s", utils.ReverseAndHexEncodeSlice(key))
		return l.blobStore.Get(ctx, key, opts...)
	}

	return value, nil
}

func (l *LocalTTL) GetHead(ctx context.Context, key []byte, nrOfBytes int, opts ...options.Options) ([]byte, error) {
	value, err := l.ttlStore.GetHead(ctx, key, nrOfBytes, opts...)
	if err != nil {
		// couldn't find it in the ttl store, try the blob store
		l.logger.Errorf("LocalTTL.Get miss for %s", utils.ReverseAndHexEncodeSlice(key))
		return l.blobStore.GetHead(ctx, key, nrOfBytes, opts...)
	}

	return value, nil
}

func (l *LocalTTL) Exists(ctx context.Context, key []byte, opts ...options.Options) (bool, error) {
	found, err := l.ttlStore.Exists(ctx, key, opts...)
	if err != nil || !found {
		// couldn't find it in the ttl store, try the blob store
		// hash, _ := chainhash.NewHash(key)
		// l.logger.Warnf("LocalTTL.Exists miss for %s", hash.String())
		return l.blobStore.Exists(ctx, key, opts...)
	}

	return found, nil
}

func (l *LocalTTL) Del(ctx context.Context, key []byte, opts ...options.Options) error {
	_ = l.ttlStore.Del(ctx, key, opts...)
	return l.blobStore.Del(ctx, key, opts...)
}
