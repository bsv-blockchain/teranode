package null

import (
	"context"
	"fmt"
	"time"

	"github.com/TAAL-GmbH/ubsv/stores/blob"
	"github.com/libsv/go-bt/v2"
	"github.com/ordishs/go-utils"
	"github.com/ordishs/gocore"
)

type Null struct {
	logger utils.Logger
}

func New() (*Null, error) {
	logLevel, _ := gocore.Config().Get("logLevel")
	logger := gocore.Log("null", gocore.NewLogLevelFromString(logLevel))

	return &Null{
		logger: logger,
	}, nil
}

func (n *Null) Close(_ context.Context) error {
	return nil
}

func (n *Null) Set(_ context.Context, _ []byte, _ []byte, _ ...blob.Options) error {
	return nil
}

func (n *Null) SetTTL(_ context.Context, _ []byte, _ time.Duration) error {
	return nil
}

func (n *Null) Get(_ context.Context, hash []byte) ([]byte, error) {
	return nil, fmt.Errorf("failed to read data from file: no such file or directory: %x", bt.ReverseBytes(hash))
}

func (n *Null) Del(_ context.Context, hash []byte) error {
	return nil
}
