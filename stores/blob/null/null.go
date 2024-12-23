package null

import (
	"context"
	"io"
	"net/http"
	"time"

	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/stores/blob/options"
	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/libsv/go-bt/v2"
)

type Null struct {
	logger ulogger.Logger
}

func New(logger ulogger.Logger, opts ...options.StoreOption) (*Null, error) {
	logger = logger.New("null")

	return &Null{
		logger: logger,
	}, nil
}

func (n *Null) Health(_ context.Context, _ bool) (int, string, error) {
	return http.StatusOK, "Null Store", nil
}

func (n *Null) Close(_ context.Context) error {
	return nil
}

func (n *Null) SetFromReader(_ context.Context, _ []byte, _ io.ReadCloser, _ ...options.FileOption) error {
	return nil
}

func (n *Null) Set(_ context.Context, _ []byte, _ []byte, _ ...options.FileOption) error {
	return nil
}

func (n *Null) SetTTL(_ context.Context, _ []byte, _ time.Duration, opts ...options.FileOption) error {
	return nil
}

func (n *Null) GetTTL(_ context.Context, _ []byte, opts ...options.FileOption) (time.Duration, error) {
	return 0, nil
}

func (n *Null) GetIoReader(_ context.Context, _ []byte, opts ...options.FileOption) (io.ReadCloser, error) {
	return nil, errors.NewStorageError("failed to read data from file: no such file or directory")
}

func (n *Null) Get(_ context.Context, hash []byte, opts ...options.FileOption) ([]byte, error) {
	return nil, errors.NewStorageError("failed to read data from file: no such file or directory: %x", bt.ReverseBytes(hash))
}

func (n *Null) GetHead(_ context.Context, hash []byte, nrOfBytes int, opts ...options.FileOption) ([]byte, error) {
	return nil, errors.NewStorageError("failed to read data from file: no such file or directory: %x", bt.ReverseBytes(hash))
}

func (n *Null) Exists(_ context.Context, _ []byte, opts ...options.FileOption) (bool, error) {
	return false, nil
}

func (n *Null) Del(_ context.Context, _ []byte, opts ...options.FileOption) error {
	return nil
}
