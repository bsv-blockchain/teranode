package memory

import (
	"context"
	"errors"
	"sync"

	"github.com/TAAL-GmbH/ubsv/services/seeder/store"
)

type memorySeederStore struct {
	mu           sync.Mutex
	transactions []*store.SpendableTransaction
}

func NewMemorySeederStore() store.SeederStore {
	return &memorySeederStore{}
}

func (s *memorySeederStore) Push(ctx context.Context, tx *store.SpendableTransaction) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.transactions = append(s.transactions, tx)
	return nil
}

func (s *memorySeederStore) Pop(ctx context.Context) (*store.SpendableTransaction, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if len(s.transactions) == 0 {
		return nil, errors.New("store is empty")
	}

	tx := s.transactions[0]
	s.transactions = s.transactions[1:]
	return tx, nil
}

func (s *memorySeederStore) Iterator() store.Iterator {
	return &memoryIterator{store: s}
}

type memoryIterator struct {
	store        *memorySeederStore
	currentIndex int
}

func (it *memoryIterator) Next() (*store.SpendableTransaction, error) {
	it.store.mu.Lock()
	defer it.store.mu.Unlock()

	if it.currentIndex >= len(it.store.transactions) {
		return nil, errors.New("no more transactions")
	}

	tx := it.store.transactions[it.currentIndex]
	it.currentIndex++
	return tx, nil
}
