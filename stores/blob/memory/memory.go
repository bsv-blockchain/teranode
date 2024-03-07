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
)

type Memory struct {
	mu    sync.Mutex
	blobs map[string][]byte
}

func New() *Memory {
	return &Memory{
		blobs: make(map[string][]byte),
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
	setOptions := options.NewSetOptions(opts...)

	storeKey := hash
	if setOptions.Extension != "" {
		storeKey = append(storeKey, []byte(setOptions.Extension)...)
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	m.blobs[string(storeKey)] = value

	return nil
}

func (m *Memory) SetTTL(_ context.Context, hash []byte, ttl time.Duration, opts ...options.Options) error {
	// not supported in memory store yet
	return errors.New("TTL is not supported in a memory store")
}

func (m *Memory) GetIoReader(ctx context.Context, key []byte, opts ...options.Options) (io.ReadCloser, error) {
	b, err := m.Get(ctx, key)
	if err != nil {
		return nil, err
	}

	return io.NopCloser(bytes.NewBuffer(b)), nil
}

func (m *Memory) Get(_ context.Context, hash []byte, opts ...options.Options) ([]byte, error) {
	setOptions := options.NewSetOptions(opts...)

	storeKey := hash
	if setOptions.Extension != "" {
		storeKey = append(storeKey, []byte(setOptions.Extension)...)
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	bytes, ok := m.blobs[string(storeKey)]
	if !ok {
		return nil, fmt.Errorf("not found")
	}

	return bytes, nil
}

func (m *Memory) GetHead(_ context.Context, hash []byte, nrOfBytes int, opts ...options.Options) ([]byte, error) {
	b, err := m.Get(context.Background(), hash)
	if err != nil {
		return nil, err
	}

	if nrOfBytes > len(b) {
		return b, nil
	}

	return b[:nrOfBytes], nil
}

func (m *Memory) TryLockIfNotExists(ctx context.Context, hash []byte, opts ...options.Options) (bool, error) {
	// TODO - implement
	return true, nil
}

func (m *Memory) Exists(_ context.Context, hash []byte, opts ...options.Options) (bool, error) {
	setOptions := options.NewSetOptions(opts...)

	storeKey := hash
	if setOptions.Extension != "" {
		storeKey = append(storeKey, []byte(setOptions.Extension)...)
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	_, ok := m.blobs[string(storeKey)]
	return ok, nil
}

func (m *Memory) Del(_ context.Context, hash []byte, opts ...options.Options) error {
	setOptions := options.NewSetOptions(opts...)

	storeKey := hash
	if setOptions.Extension != "" {
		storeKey = append(storeKey, []byte(setOptions.Extension)...)
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	delete(m.blobs, string(storeKey))

	return nil
}
