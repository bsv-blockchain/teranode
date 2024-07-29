package test

import (
	"context"
	"fmt"
	"github.com/bitcoin-sv/ubsv/errors"
	"io"
	"os"
	"time"

	"github.com/bitcoin-sv/ubsv/stores/blob/options"
	"github.com/libsv/go-bt/v2/chainhash"
)

func NewLocalSubtreeStore() *TestSubtreeStore {
	return &TestSubtreeStore{
		Files: make(map[chainhash.Hash]int),
	}
}

func (l TestSubtreeStore) Health(_ context.Context) (int, string, error) {
	return 0, "", nil
}

func (l TestSubtreeStore) Exists(_ context.Context, key []byte, _ ...options.Options) (bool, error) {
	_, ok := l.Files[chainhash.Hash(key)]
	return ok, nil
}

func (l TestSubtreeStore) Get(_ context.Context, key []byte, _ ...options.Options) ([]byte, error) {
	file, ok := l.Files[chainhash.Hash(key)]
	if !ok {
		return nil, errors.NewProcessingError("file not found")
	}

	subtreeBytes, err := os.ReadFile(fmt.Sprintf(FileNameTemplate, file))
	if err != nil {
		return nil, err
	}

	return subtreeBytes, nil
}

func (l TestSubtreeStore) GetIoReader(_ context.Context, key []byte, opts ...options.Options) (io.ReadCloser, error) {
	file, ok := l.Files[chainhash.Hash(key)]
	if !ok {
		return nil, errors.NewProcessingError("file not found")
	}

	subtreeFile, err := os.Open(fmt.Sprintf(FileNameTemplate, file))
	if err != nil {
		return nil, err
	}

	return subtreeFile, nil
}

func (l TestSubtreeStore) GetHead(_ context.Context, _ []byte, _ int, opts ...options.Options) ([]byte, error) {
	panic("not implemented")
}

func (l TestSubtreeStore) Set(_ context.Context, _ []byte, _ []byte, _ ...options.Options) error {
	panic("not implemented")
}

func (l TestSubtreeStore) SetFromReader(_ context.Context, _ []byte, _ io.ReadCloser, _ ...options.Options) error {
	panic("not implemented")
}

func (l TestSubtreeStore) SetTTL(_ context.Context, _ []byte, _ time.Duration, opts ...options.Options) error {
	panic("not implemented")
}

func (l TestSubtreeStore) Del(_ context.Context, _ []byte, _ ...options.Options) error {
	panic("not implemented")
}

func (l TestSubtreeStore) Close(_ context.Context) error {
	return nil
}
