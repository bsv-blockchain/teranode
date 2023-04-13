package memory

import (
	"context"
	"sync"

	"github.com/TAAL-GmbH/ubsv/services/utxo/store"
	"github.com/libsv/go-p2p/chaincfg/chainhash"
)

type Memory struct {
	mu sync.Mutex
	m  map[chainhash.Hash]chainhash.Hash
}

func New() store.UTXOStore {
	return &Memory{
		m: make(map[chainhash.Hash]chainhash.Hash),
	}
}

func (m *Memory) Get(_ context.Context, hash *chainhash.Hash) (*store.UTXOResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, ok := m.m[*hash]; ok {
		return &store.UTXOResponse{
			Status: 1,
		}, nil
	}

	return &store.UTXOResponse{
		Status: 0,
	}, nil
}

func (m *Memory) Store(_ context.Context, hash *chainhash.Hash) (*store.UTXOResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.m[*hash] = chainhash.Hash{}

	return &store.UTXOResponse{
		Status: 1,
	}, nil
}

func (m *Memory) Spend(_ context.Context, hash *chainhash.Hash, txID *chainhash.Hash) (*store.UTXOResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.m[*hash] = *txID

	return &store.UTXOResponse{
		Status: 1,
	}, nil
}

func (m *Memory) Reset(_ context.Context, hash *chainhash.Hash) (*store.UTXOResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	delete(m.m, *hash)

	return &store.UTXOResponse{
		Status: 1,
	}, nil
}
