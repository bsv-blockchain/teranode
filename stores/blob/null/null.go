package null

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/bitcoin-sv/ubsv/stores/blob/options"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/libsv/go-bt/v2"
)

type Null struct {
	logger ulogger.Logger
}

func New(logger ulogger.Logger) (*Null, error) {
	logger = logger.New("null")

	return &Null{
		logger: logger,
	}, nil
}

func (n *Null) Health(_ context.Context) (int, string, error) {
	return 0, "Null Store", nil
}

func (n *Null) Close(_ context.Context) error {
	return nil
}

func (n *Null) SetFromReader(_ context.Context, _ []byte, _ io.ReadCloser, _ ...options.Options) error {
	return nil
}

func (n *Null) Set(_ context.Context, _ []byte, _ []byte, _ ...options.Options) error {
	return nil
}

func (n *Null) SetTTL(_ context.Context, _ []byte, _ time.Duration) error {
	return nil
}

func (n *Null) GetIoReader(_ context.Context, _ []byte) (io.ReadCloser, error) {
	return nil, fmt.Errorf("failed to read data from file: no such file or directory")
}

func (n *Null) Get(_ context.Context, hash []byte) ([]byte, error) {
	return nil, fmt.Errorf("failed to read data from file: no such file or directory: %x", bt.ReverseBytes(hash))
}

func (n *Null) Exists(_ context.Context, _ []byte) (bool, error) {
	return false, nil
}

func (n *Null) Del(_ context.Context, _ []byte) error {
	return nil
}
