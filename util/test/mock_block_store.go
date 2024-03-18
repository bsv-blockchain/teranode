package test

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/bitcoin-sv/ubsv/stores/blob/options"
	"github.com/bitcoin-sv/ubsv/ulogger"
)

type BlobStoreStub struct {
	logger ulogger.Logger
}

func New(logger ulogger.Logger) (*BlobStoreStub, error) {
	logger = logger.New("null")

	return &BlobStoreStub{
		logger: logger,
	}, nil
}

func (n *BlobStoreStub) Health(_ context.Context) (int, string, error) {
	return 0, "BlobStoreStub Store", nil
}

func (n *BlobStoreStub) Close(_ context.Context) error {
	return nil
}

func (n *BlobStoreStub) SetFromReader(_ context.Context, _ []byte, _ io.ReadCloser, _ ...options.Options) error {
	return nil
}

func (n *BlobStoreStub) Set(_ context.Context, _ []byte, _ []byte, _ ...options.Options) error {
	return nil
}

func (n *BlobStoreStub) SetTTL(_ context.Context, _ []byte, _ time.Duration) error {
	return nil
}

func (n *BlobStoreStub) GetIoReader(_ context.Context, _ []byte) (io.ReadCloser, error) {
	path := filepath.Join("testdata", "testSubtreeHex.bin")

	// read the file
	subtreeReader, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read file: %s", err)
	}

	return subtreeReader, nil
}

func (n *BlobStoreStub) Get(_ context.Context, hash []byte) ([]byte, error) {
	path := filepath.Join("testdata", "testSubtreeHex.bin")

	// read the file
	subtreeBytes, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read file: %s", err)
	}

	return subtreeBytes, nil
}

func (n *BlobStoreStub) Exists(_ context.Context, _ []byte) (bool, error) {
	return false, nil
}

func (n *BlobStoreStub) Del(_ context.Context, _ []byte) error {
	return nil
}
