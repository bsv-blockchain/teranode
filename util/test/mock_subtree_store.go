package test

import (
	"context"
	"fmt"
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

func (l TestSubtreeStore) Exists(_ context.Context, key []byte) (bool, error) {
	_, ok := l.Files[chainhash.Hash(key)]
	return ok, nil
}

func (l TestSubtreeStore) Get(_ context.Context, key []byte) ([]byte, error) {
	file, ok := l.Files[chainhash.Hash(key)]
	if !ok {
		return nil, fmt.Errorf("file not found")
	}

	subtreeBytes, err := os.ReadFile(fmt.Sprintf(FileNameTemplate, file))
	if err != nil {
		return nil, err
	}

	return subtreeBytes, nil
}

func (l TestSubtreeStore) GetIoReader(_ context.Context, key []byte) (io.ReadCloser, error) {
	file, ok := l.Files[chainhash.Hash(key)]
	if !ok {
		return nil, fmt.Errorf("file not found")
	}

	subtreeFile, err := os.Open(fmt.Sprintf(FileNameTemplate, file))
	if err != nil {
		return nil, err
	}

	return subtreeFile, nil
}

func (l TestSubtreeStore) Set(_ context.Context, _ []byte, _ []byte, _ ...options.Options) error {
	panic("not implemented")
}

func (l TestSubtreeStore) SetFromReader(_ context.Context, _ []byte, _ io.ReadCloser, _ ...options.Options) error {
	panic("not implemented")
}

func (l TestSubtreeStore) SetTTL(_ context.Context, _ []byte, _ time.Duration) error {
	panic("not implemented")
}

func (l TestSubtreeStore) Del(_ context.Context, _ []byte) error {
	panic("not implemented")
}

func (l TestSubtreeStore) Close(_ context.Context) error {
	return nil
}
