package memory

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/bitcoin-sv/ubsv/stores/blob/options"
	"github.com/libsv/go-bt/v2/chainhash"
)

type Memory struct {
	mu    sync.Mutex
	blobs map[chainhash.Hash][]byte
}

func New() *Memory {
	return &Memory{
		blobs: make(map[chainhash.Hash][]byte),
	}
}

func (m *Memory) Health(ctx context.Context) (int, string, error) {
	return 0, "Memory Store", nil
}

func (m *Memory) Close(_ context.Context) error {
	// noop
	return nil
}

func (m *Memory) SetFromReader(ctx context.Context, key []byte, reader io.ReadCloser, opts ...options.Options) error {
	defer reader.Close()

	b, err := io.ReadAll(reader)
	if err != nil {
		return fmt.Errorf("failed to read data from reader: %w", err)
	}

	return m.Set(ctx, key, b, opts...)
}

func (m *Memory) Set(_ context.Context, hash []byte, value []byte, opts ...options.Options) error {
	// hash should have been a chainhash.Hash
	key := chainhash.Hash(hash)

	m.mu.Lock()
	defer m.mu.Unlock()

	m.blobs[key] = value

	return nil
}

func (m *Memory) SetTTL(_ context.Context, hash []byte, ttl time.Duration) error {
	// not supported in memory store yet
	return errors.New("TTL is not supported in a memory store")
}

func (m *Memory) GetIoReader(ctx context.Context, key []byte) (io.ReadCloser, error) {
	b, err := m.Get(ctx, key)
	if err != nil {
		return nil, err
	}

	return io.NopCloser(bytes.NewBuffer(b)), nil
}

func (m *Memory) Get(_ context.Context, hash []byte) ([]byte, error) {
	// hash should have been a chainhash.Hash
	key := chainhash.Hash(hash)

	m.mu.Lock()
	defer m.mu.Unlock()

	bytes, ok := m.blobs[key]
	if !ok {
		return nil, fmt.Errorf("not found")
	}

	return bytes, nil
}

func (m *Memory) GetHead(_ context.Context, hash []byte, nrOfBytes int) ([]byte, error) {
	b, err := m.Get(context.Background(), hash)
	if err != nil {
		return nil, err
	}

	if nrOfBytes > len(b) {
		return b, nil
	}

	return b[:nrOfBytes], nil
}

func (m *Memory) Exists(_ context.Context, hash []byte) (bool, error) {
	// hash should have been a chainhash.Hash
	key := chainhash.Hash(hash)

	m.mu.Lock()
	defer m.mu.Unlock()

	_, ok := m.blobs[key]
	return ok, nil
}

func (m *Memory) Del(_ context.Context, hash []byte) error {
	// hash should have been a chainhash.Hash
	key := chainhash.Hash(hash)

	m.mu.Lock()
	defer m.mu.Unlock()

	delete(m.blobs, key)

	return nil
}
