package store

import (
	"context"
	"sync"
)

type MemoryStore struct {
	mu sync.Mutex
	m  map[string]string
}

func NewMemoryStore() Store {
	m := make(map[string]string)

	return &MemoryStore{
		m: m,
	}
}

func (m *MemoryStore) SetAndGet(ctx context.Context, key string, value string) (string, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if existing, ok := m.m[key]; ok {
		if existing == value {
			return value, true, nil
		}
		return existing, false, nil
	}

	m.m[key] = value
	return value, true, nil
}

func (m *MemoryStore) Delete(ctx context.Context, key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	delete(m.m, key)

	return nil
}
