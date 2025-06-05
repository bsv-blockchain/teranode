package null

import (
	"context"
	"io"
	"net/http"

	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/pkg/fileformat"
	"github.com/bitcoin-sv/teranode/stores/blob/options"
	"github.com/bitcoin-sv/teranode/ulogger"
)

type Null struct {
	logger  ulogger.Logger
	options *options.Options
}

func New(logger ulogger.Logger, opts ...options.StoreOption) (*Null, error) {
	logger = logger.New("null")

	return &Null{
		options: options.NewStoreOptions(opts...),
		logger:  logger,
	}, nil
}

func (n *Null) Health(_ context.Context, _ bool) (int, string, error) {
	return http.StatusOK, "Null Store", nil
}

func (n *Null) Close(_ context.Context) error {
	return nil
}

func (n *Null) SetFromReader(_ context.Context, _ []byte, _ fileformat.FileType, _ io.ReadCloser, _ ...options.FileOption) error {
	return nil
}

func (n *Null) Set(_ context.Context, _ []byte, _ fileformat.FileType, _ []byte, _ ...options.FileOption) error {
	return nil
}

func (n *Null) SetDAH(_ context.Context, _ []byte, _ fileformat.FileType, _ uint32, opts ...options.FileOption) error {
	return nil
}

func (n *Null) GetDAH(_ context.Context, _ []byte, _ fileformat.FileType, opts ...options.FileOption) (uint32, error) {
	return 0, nil
}

func (n *Null) GetIoReader(_ context.Context, key []byte, fileType fileformat.FileType, opts ...options.FileOption) (io.ReadCloser, error) {
	merged := options.MergeOptions(n.options, opts)

	fileName, err := merged.ConstructFilename("", key, fileType)
	if err != nil {
		return nil, err
	}

	return nil, errors.NewStorageError("failed to read data from file: no such file or directory: %s", fileName)
}

func (n *Null) Get(ctx context.Context, key []byte, fileType fileformat.FileType, opts ...options.FileOption) ([]byte, error) {
	_, err := n.GetIoReader(ctx, key, fileType, opts...)
	return nil, err
}

func (n *Null) Exists(_ context.Context, _ []byte, _ fileformat.FileType, opts ...options.FileOption) (bool, error) {
	return false, nil
}

func (n *Null) Del(_ context.Context, _ []byte, _ fileformat.FileType, opts ...options.FileOption) error {
	return nil
}

func (n *Null) SetCurrentBlockHeight(height uint32) {
	// noop
}
