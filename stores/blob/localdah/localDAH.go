package localdah

import (
	"context"
	"io"

	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/stores/blob/options"
	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/ordishs/go-utils"
)

type blobStore interface {
	Health(ctx context.Context, checkLiveness bool) (int, string, error)
	Exists(ctx context.Context, key []byte, opts ...options.FileOption) (bool, error)
	Get(ctx context.Context, key []byte, opts ...options.FileOption) ([]byte, error)
	GetHead(ctx context.Context, key []byte, nrOfBytes int, opts ...options.FileOption) ([]byte, error)
	GetIoReader(ctx context.Context, key []byte, opts ...options.FileOption) (io.ReadCloser, error)
	Set(ctx context.Context, key []byte, value []byte, opts ...options.FileOption) error
	SetFromReader(ctx context.Context, key []byte, value io.ReadCloser, opts ...options.FileOption) error
	SetDAH(ctx context.Context, key []byte, newDAH uint32, opts ...options.FileOption) error
	GetDAH(ctx context.Context, key []byte, opts ...options.FileOption) (uint32, error)
	Del(ctx context.Context, key []byte, opts ...options.FileOption) error
	GetHeader(ctx context.Context, key []byte, opts ...options.FileOption) ([]byte, error)
	GetFooterMetaData(ctx context.Context, key []byte, opts ...options.FileOption) ([]byte, error)
	Close(ctx context.Context) error
}

type LocalDAH struct {
	logger    ulogger.Logger
	dahStore  blobStore
	blobStore blobStore
	options   *options.Options
}

func New(logger ulogger.Logger, dahStore, blobStore blobStore, opts ...options.StoreOption) (*LocalDAH, error) {
	options := options.NewStoreOptions(opts...)

	b := &LocalDAH{
		logger:    logger,
		dahStore:  dahStore,
		blobStore: blobStore,
		options:   options,
	}

	return b, nil
}

func (l *LocalDAH) Health(ctx context.Context, checkLiveness bool) (int, string, error) {
	n, resp, err := l.dahStore.Health(ctx, checkLiveness)
	if err != nil || n <= 0 {
		return n, resp, err
	}

	return l.blobStore.Health(ctx, checkLiveness)
}

func (l *LocalDAH) Close(_ context.Context) error {
	return nil
}

func (l *LocalDAH) SetFromReader(ctx context.Context, key []byte, reader io.ReadCloser, opts ...options.FileOption) error {
	merged := options.MergeOptions(l.options, opts)

	if merged.DAH > 0 || merged.BlockHeightRetention > 0 {
		// set the value in the DAH store
		return l.dahStore.SetFromReader(ctx, key, reader, opts...)
	}

	// set the value in the blob store
	return l.blobStore.SetFromReader(ctx, key, reader, opts...)
}

func (l *LocalDAH) Set(ctx context.Context, key []byte, value []byte, opts ...options.FileOption) error {
	// l.logger.Debugf("[localDAH] Set called %v\n%s\n%s\n", utils.ReverseAndHexEncodeSlice(key), stack.Stack(), ctx.Value("stack"))
	merged := options.MergeOptions(l.options, opts)

	if merged.DAH > 0 || merged.BlockHeightRetention > 0 {
		// set the value in the DAH store
		return l.dahStore.Set(ctx, key, value, opts...)
	}

	// set the value in the blob store
	return l.blobStore.Set(ctx, key, value, opts...)
}

func (l *LocalDAH) SetDAH(ctx context.Context, key []byte, newDAH uint32, opts ...options.FileOption) error {
	// l.logger.Debugf("[localDAH] SetDAH called %v\n%s\n%s\n", utils.ReverseAndHexEncodeSlice(key), stack.Stack(), ctx.Value("stack"))
	if newDAH == 0 {
		// move the file from the DAH store to the blob store
		reader, err := l.dahStore.GetIoReader(ctx, key, opts...)
		if err != nil {
			if found, _ := l.blobStore.Exists(ctx, key, opts...); found {
				// already there
				return nil
			}

			return err
		}

		defer reader.Close()

		err = l.blobStore.SetFromReader(ctx, key, reader, opts...)
		if err == nil {
			err = l.dahStore.Del(ctx, key, opts...)
		}

		return err
	}

	// we are setting a DAH, if it's already in the DAH store, reset the DAH, if it is not in the DAH store, move it there
	found, _ := l.dahStore.Exists(ctx, key, opts...)
	if found {
		return l.dahStore.SetDAH(ctx, key, newDAH, opts...)
	}

	// move the file from the blob store to the DAH store
	reader, err := l.blobStore.GetIoReader(ctx, key, opts...)
	if err != nil {
		return err
	}

	defer reader.Close()

	if err = l.dahStore.SetFromReader(ctx, key, reader, options.WithDeleteAt(newDAH)); err != nil {
		return err
	}

	return nil
}

func (l *LocalDAH) GetDAH(ctx context.Context, key []byte, opts ...options.FileOption) (uint32, error) {
	dah, err := l.dahStore.GetDAH(ctx, key, opts...)
	if err != nil {
		// couldn't find it in the DAH store, try the blob store
		l.logger.Errorf("LocalDAH.GetDAH miss for %s", utils.ReverseAndHexEncodeSlice(key))
		return l.blobStore.GetDAH(ctx, key, opts...)
	}

	return dah, nil
}

func (l *LocalDAH) GetIoReader(ctx context.Context, key []byte, opts ...options.FileOption) (io.ReadCloser, error) {
	ioReader, err := l.dahStore.GetIoReader(ctx, key, opts...)
	if err != nil {
		// couldn't find it in the DAH store, try the blob store
		l.logger.Errorf("LocalDAH.GetIoReader miss for %s", utils.ReverseAndHexEncodeSlice(key))
		return l.blobStore.GetIoReader(ctx, key, opts...)
	}

	return ioReader, nil
}

func (l *LocalDAH) Get(ctx context.Context, key []byte, opts ...options.FileOption) ([]byte, error) {
	value, err := l.dahStore.Get(ctx, key, opts...)
	if err != nil {
		if errors.Is(err, errors.ErrNotFound) {
			value, err = l.dahStore.Get(ctx, key, options.ReplaceExtention(opts, "subtree", "subtreeToCheck")...)
		}

		if err != nil {
			// couldn't find it in the DAH store, try the blob store
			l.logger.Errorf("LocalDAH.Get miss for %s", utils.ReverseAndHexEncodeSlice(key))
			return l.blobStore.Get(ctx, key, opts...)
		}
	}

	return value, nil
}

func (l *LocalDAH) GetHead(ctx context.Context, key []byte, nrOfBytes int, opts ...options.FileOption) ([]byte, error) {
	value, err := l.dahStore.GetHead(ctx, key, nrOfBytes, opts...)
	if err != nil {
		// couldn't find it in the DAH store, try the blob store
		l.logger.Errorf("LocalDAH.GetHead miss for %s", utils.ReverseAndHexEncodeSlice(key))
		return l.blobStore.GetHead(ctx, key, nrOfBytes, opts...)
	}

	return value, nil
}

func (l *LocalDAH) Exists(ctx context.Context, key []byte, opts ...options.FileOption) (bool, error) {
	found, err := l.dahStore.Exists(ctx, key, opts...)
	if err != nil || !found {
		// couldn't find it in the DAH store, try the blob store
		// hash, _ := chainhash.NewHash(key)
		// l.logger.Warnf("LocalDAH.Exists miss for %s", hash.String())
		return l.blobStore.Exists(ctx, key, opts...)
	}

	return found, nil
}

func (l *LocalDAH) Del(ctx context.Context, key []byte, opts ...options.FileOption) error {
	_ = l.dahStore.Del(ctx, key, opts...)
	return l.blobStore.Del(ctx, key, opts...)
}

func (l *LocalDAH) GetHeader(ctx context.Context, key []byte, opts ...options.FileOption) ([]byte, error) {
	return l.blobStore.GetHeader(ctx, key, opts...)
}

func (l *LocalDAH) GetFooterMetaData(ctx context.Context, key []byte, opts ...options.FileOption) ([]byte, error) {
	return l.blobStore.GetFooterMetaData(ctx, key, opts...)
}
