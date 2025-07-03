package localdah

import (
	"context"
	"io"

	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/pkg/fileformat"
	"github.com/bitcoin-sv/teranode/stores/blob/options"
	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/ordishs/go-utils"
)

type blobStore interface {
	Health(ctx context.Context, checkLiveness bool) (int, string, error)
	Exists(ctx context.Context, key []byte, fileType fileformat.FileType, opts ...options.FileOption) (bool, error)
	Get(ctx context.Context, key []byte, fileType fileformat.FileType, opts ...options.FileOption) ([]byte, error)
	GetIoReader(ctx context.Context, key []byte, fileType fileformat.FileType, opts ...options.FileOption) (io.ReadCloser, error)
	Set(ctx context.Context, key []byte, fileType fileformat.FileType, value []byte, opts ...options.FileOption) error
	SetFromReader(ctx context.Context, key []byte, fileType fileformat.FileType, value io.ReadCloser, opts ...options.FileOption) error
	SetDAH(ctx context.Context, key []byte, fileType fileformat.FileType, newDAH uint32, opts ...options.FileOption) error
	GetDAH(ctx context.Context, key []byte, fileType fileformat.FileType, opts ...options.FileOption) (uint32, error)
	Del(ctx context.Context, key []byte, fileType fileformat.FileType, opts ...options.FileOption) error
	Close(ctx context.Context) error
	SetCurrentBlockHeight(height uint32)
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

func (l *LocalDAH) SetFromReader(ctx context.Context, key []byte, fileType fileformat.FileType, reader io.ReadCloser, opts ...options.FileOption) error {
	merged := options.MergeOptions(l.options, opts)

	if merged.DAH > 0 || merged.BlockHeightRetention > 0 {
		// set the value in the DAH store
		return l.dahStore.SetFromReader(ctx, key, fileType, reader, opts...)
	}

	// set the value in the blob store
	return l.blobStore.SetFromReader(ctx, key, fileType, reader, opts...)
}

func (l *LocalDAH) Set(ctx context.Context, key []byte, fileType fileformat.FileType, value []byte, opts ...options.FileOption) error {
	// l.logger.Debugf("[localDAH] Set called %v\n%s\n%s\n", utils.ReverseAndHexEncodeSlice(key), stack.Stack(), ctx.Value("stack"))
	merged := options.MergeOptions(l.options, opts)

	if merged.DAH > 0 || merged.BlockHeightRetention > 0 {
		// set the value in the DAH store
		return l.dahStore.Set(ctx, key, fileType, value, opts...)
	}

	// set the value in the blob store
	return l.blobStore.Set(ctx, key, fileType, value, opts...)
}

func (l *LocalDAH) SetDAH(ctx context.Context, key []byte, fileType fileformat.FileType, newDAH uint32, opts ...options.FileOption) error {
	// l.logger.Debugf("[localDAH] SetDAH called %v\n%s\n%s\n", utils.ReverseAndHexEncodeSlice(key), stack.Stack(), ctx.Value("stack"))
	if newDAH == 0 {
		// move the file from the DAH store to the blob store
		reader, err := l.dahStore.GetIoReader(ctx, key, fileType, opts...)
		if err != nil {
			if found, _ := l.blobStore.Exists(ctx, key, fileType, opts...); found {
				// already there
				return nil
			}

			return err
		}

		defer reader.Close()

		err = l.blobStore.SetFromReader(ctx, key, fileType, reader, opts...)
		if err == nil {
			err = l.dahStore.Del(ctx, key, fileType, opts...)
		}

		return err
	}

	// we are setting a DAH, if it's already in the DAH store, reset the DAH, if it is not in the DAH store, move it there
	found, _ := l.dahStore.Exists(ctx, key, fileType, opts...)
	if found {
		return l.dahStore.SetDAH(ctx, key, fileType, newDAH, opts...)
	}

	// move the file from the blob store to the DAH store
	reader, err := l.blobStore.GetIoReader(ctx, key, fileType, opts...)
	if err != nil {
		return err
	}

	defer reader.Close()

	if err = l.dahStore.SetFromReader(ctx, key, fileType, reader, options.WithDeleteAt(newDAH)); err != nil {
		return err
	}

	return nil
}

func (l *LocalDAH) GetDAH(ctx context.Context, key []byte, fileType fileformat.FileType, opts ...options.FileOption) (uint32, error) {
	dah, err := l.dahStore.GetDAH(ctx, key, fileType, opts...)
	if err != nil {
		// couldn't find it in the DAH store, try the blob store
		l.logger.Errorf("LocalDAH.GetDAH miss for %s", utils.ReverseAndHexEncodeSlice(key))
		return l.blobStore.GetDAH(ctx, key, fileType, opts...)
	}

	return dah, nil
}

func (l *LocalDAH) GetIoReader(ctx context.Context, key []byte, fileType fileformat.FileType, opts ...options.FileOption) (io.ReadCloser, error) {
	ioReader, err := l.dahStore.GetIoReader(ctx, key, fileType, opts...)
	if err != nil {
		if errors.Is(err, errors.ErrNotFound) && fileType == fileformat.FileTypeSubtree {
			ioReader, err = l.dahStore.GetIoReader(ctx, key, fileformat.FileTypeSubtreeToCheck, opts...)
		}

		if err != nil {
			// couldn't find it in the DAH store, try the blob store
			l.logger.Errorf("LocalDAH.GetIoReader miss for %s", utils.ReverseAndHexEncodeSlice(key))
			return l.blobStore.GetIoReader(ctx, key, fileType, opts...)
		}
	}

	return ioReader, nil
}

func (l *LocalDAH) Get(ctx context.Context, key []byte, fileType fileformat.FileType, opts ...options.FileOption) ([]byte, error) {
	value, err := l.dahStore.Get(ctx, key, fileType, opts...)
	if err != nil {
		if errors.Is(err, errors.ErrNotFound) && fileType == fileformat.FileTypeSubtree {
			value, err = l.dahStore.Get(ctx, key, fileformat.FileTypeSubtreeToCheck, opts...)
		}

		if err != nil {
			// couldn't find it in the DAH store, try the blob store
			l.logger.Errorf("LocalDAH.Get miss for %s", utils.ReverseAndHexEncodeSlice(key))
			return l.blobStore.Get(ctx, key, fileType, opts...)
		}
	}

	return value, nil
}

func (l *LocalDAH) Exists(ctx context.Context, key []byte, fileType fileformat.FileType, opts ...options.FileOption) (bool, error) {
	found, err := l.dahStore.Exists(ctx, key, fileType, opts...)
	if err != nil || !found {
		if errors.Is(err, errors.ErrNotFound) && fileType == fileformat.FileTypeSubtree {
			found, err = l.dahStore.Exists(ctx, key, fileformat.FileTypeSubtreeToCheck, opts...)
		}

		if err != nil {
			// couldn't find it in the DAH store, try the blob store
			// hash, _ := chainhash.NewHash(key)
			// l.logger.Warnf("LocalDAH.Exists miss for %s", hash.String())
			return l.blobStore.Exists(ctx, key, fileType, opts...)
		}
	}

	return found, nil
}

func (l *LocalDAH) Del(ctx context.Context, key []byte, fileType fileformat.FileType, opts ...options.FileOption) error {
	_ = l.dahStore.Del(ctx, key, fileType, opts...)
	return l.blobStore.Del(ctx, key, fileType, opts...)
}

func (l *LocalDAH) SetCurrentBlockHeight(height uint32) {
	l.dahStore.SetCurrentBlockHeight(height)
	l.blobStore.SetCurrentBlockHeight(height)
}
