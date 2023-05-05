package memory

import (
	"context"
	"errors"
	"sync"

	"github.com/TAAL-GmbH/ubsv/services/seeder/store"
	"github.com/ordishs/go-utils"
	"github.com/ordishs/gocore"
)

type Memory struct {
	mu     sync.Mutex
	s      []*store.SpendableTransaction
	logger utils.Logger
}

func New() *Memory {
	logLevel, _ := gocore.Config().Get("logLevel")
	logger := gocore.Log("seederStore", gocore.NewLogLevelFromString(logLevel))

	s := make([]*store.SpendableTransaction, 0)
	return &Memory{
		s:      s,
		logger: logger,
	}
}

func (m *Memory) Push(ctx context.Context, st *store.SpendableTransaction) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.s = append(m.s, st)
	return nil
}

func (m *Memory) Pop(ctx context.Context) (*store.SpendableTransaction, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Pop the first element
	if len(m.s) > 0 {
		st := m.s[0]
		m.s = m.s[1:]
		return st, nil
	}

	return nil, errors.New("Memory store is empty")
}
