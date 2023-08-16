package memory

import (
	"context"
	"fmt"
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

func (m *Memory) Close(_ context.Context) error {
	// noop
	return nil
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
	return nil
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
