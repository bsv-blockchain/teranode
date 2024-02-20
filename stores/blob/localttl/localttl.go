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
	Exists(ctx context.Context, key []byte) (bool, error)
	Get(ctx context.Context, key []byte) ([]byte, error)
	GetHead(ctx context.Context, key []byte, nrOfBytes int) ([]byte, error)
	GetIoReader(ctx context.Context, key []byte) (io.ReadCloser, error)
	Set(ctx context.Context, key []byte, value []byte, opts ...options.Options) error
	SetFromReader(ctx context.Context, key []byte, value io.ReadCloser, opts ...options.Options) error
	SetTTL(ctx context.Context, key []byte, ttl time.Duration) error
	Del(ctx context.Context, key []byte) error
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
	setOptions := options.NewSetOptions(opts...)

	if setOptions.TTL > 0 {
		// set the value in the ttl store
		return l.ttlStore.SetFromReader(ctx, key, reader, opts...)
	}

	// set the value in the blob store
	return l.blobStore.SetFromReader(ctx, key, reader, opts...)
}

func (l *LocalTTL) Set(ctx context.Context, key []byte, value []byte, opts ...options.Options) error {
	// l.logger.Debugf("[localTTL] Set called %v\n%s\n%s\n", utils.ReverseAndHexEncodeSlice(key), stack.Stack(), ctx.Value("stack"))
	setOptions := options.NewSetOptions(opts...)

	if setOptions.TTL > 0 {
		// set the value in the ttl store
		return l.ttlStore.Set(ctx, key, value, opts...)
	}

	// set the value in the blob store
	return l.blobStore.Set(ctx, key, value, opts...)
}

func (l *LocalTTL) SetTTL(ctx context.Context, key []byte, duration time.Duration) error {
	// l.logger.Debugf("[localTTL] SetTTL called %v\n%s\n%s\n", utils.ReverseAndHexEncodeSlice(key), stack.Stack(), ctx.Value("stack"))
	if duration <= 0 {
		// move the file from the TTL store to the blob store
		reader, err := l.ttlStore.GetIoReader(ctx, key)
		if err != nil {
			if found, _ := l.blobStore.Exists(ctx, key); found {
				// already there
				return nil
			}
			return err
		}

		return l.blobStore.SetFromReader(ctx, key, reader)
	}

	// we are setting a ttl, if it's already in the ttl store, reset the ttl, if it is not in the ttl store, move it there
	found, _ := l.ttlStore.Exists(ctx, key)
	if found {
		return l.ttlStore.SetTTL(ctx, key, duration)
	}

	// move the file from the blob store to the TTL store
	reader, err := l.blobStore.GetIoReader(ctx, key)
	if err != nil {
		return err
	}

	if err = l.ttlStore.SetFromReader(ctx, key, reader, options.WithTTL(duration)); err != nil {
		return err
	}

	// delete from the blob store ?
	return l.blobStore.Del(ctx, key)
}

func (l *LocalTTL) GetIoReader(ctx context.Context, key []byte) (io.ReadCloser, error) {
	ioReader, err := l.ttlStore.GetIoReader(ctx, key)
	if err != nil {
		// couldn't find it in the ttl store, try the blob store
		l.logger.Errorf("LocalTTL.GetIoReader miss for %s", utils.ReverseAndHexEncodeSlice(key))
		return l.blobStore.GetIoReader(ctx, key)
	}

	return ioReader, nil
}

func (l *LocalTTL) Get(ctx context.Context, key []byte) ([]byte, error) {
	value, err := l.ttlStore.Get(ctx, key)
	if err != nil {
		// couldn't find it in the ttl store, try the blob store
		l.logger.Errorf("LocalTTL.Get miss for %s", utils.ReverseAndHexEncodeSlice(key))
		return l.blobStore.Get(ctx, key)
	}

	return value, nil
}

func (l *LocalTTL) GetHead(ctx context.Context, key []byte, nrOfBytes int) ([]byte, error) {
	value, err := l.ttlStore.GetHead(ctx, key, nrOfBytes)
	if err != nil {
		// couldn't find it in the ttl store, try the blob store
		l.logger.Errorf("LocalTTL.Get miss for %s", utils.ReverseAndHexEncodeSlice(key))
		return l.blobStore.GetHead(ctx, key, nrOfBytes)
	}

	return value, nil
}

func (l *LocalTTL) Exists(ctx context.Context, key []byte) (bool, error) {
	found, err := l.ttlStore.Exists(ctx, key)
	if err != nil || !found {
		// couldn't find it in the ttl store, try the blob store
		// hash, _ := chainhash.NewHash(key)
		// l.logger.Warnf("LocalTTL.Exists miss for %s", hash.String())
		return l.blobStore.Exists(ctx, key)
	}

	return found, nil
}

func (l *LocalTTL) Del(ctx context.Context, key []byte) error {
	_ = l.ttlStore.Del(ctx, key)
	return l.blobStore.Del(ctx, key)
}
