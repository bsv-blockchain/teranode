package test

import (
	"context"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/stores/blob/options"
	"github.com/libsv/go-bt/v2/chainhash"
)

func NewLocalSubtreeStore() *TestSubtreeStore {
	return &TestSubtreeStore{
		Files: make(map[chainhash.Hash]int),
	}
}

func (l TestSubtreeStore) Health(_ context.Context, _ bool) (int, string, error) {
	return 0, "", nil
}

func (l TestSubtreeStore) Exists(_ context.Context, key []byte, _ ...options.FileOption) (bool, error) {
	_, ok := l.Files[chainhash.Hash(key)]
	return ok, nil
}

func (l TestSubtreeStore) Get(_ context.Context, key []byte, _ ...options.FileOption) ([]byte, error) {
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

func (l TestSubtreeStore) GetIoReader(_ context.Context, key []byte, opts ...options.FileOption) (io.ReadCloser, error) {
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

func (l TestSubtreeStore) GetHead(_ context.Context, _ []byte, _ int, opts ...options.FileOption) ([]byte, error) {
	panic("not implemented")
}

func (l TestSubtreeStore) Set(_ context.Context, _ []byte, _ []byte, _ ...options.FileOption) error {
	panic("not implemented")
}

func (l TestSubtreeStore) SetFromReader(_ context.Context, _ []byte, _ io.ReadCloser, _ ...options.FileOption) error {
	panic("not implemented")
}

func (l TestSubtreeStore) SetTTL(_ context.Context, _ []byte, _ time.Duration, opts ...options.FileOption) error {
	panic("not implemented")
}

func (l TestSubtreeStore) GetTTL(_ context.Context, _ []byte, opts ...options.FileOption) (time.Duration, error) {
	panic("not implemented")
}

func (l TestSubtreeStore) Del(_ context.Context, _ []byte, _ ...options.FileOption) error {
	panic("not implemented")
}

func (l TestSubtreeStore) GetHeader(_ context.Context, _ []byte, _ ...options.FileOption) ([]byte, error) {
	panic("not implemented")
}

func (l TestSubtreeStore) GetFooterMetaData(_ context.Context, _ []byte, _ ...options.FileOption) ([]byte, error) {
	panic("not implemented")
}

func (l TestSubtreeStore) Close(_ context.Context) error {
	return nil
}
