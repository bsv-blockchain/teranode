package memory

import (
	"context"
	"sync"
	"time"

	"github.com/TAAL-GmbH/ubsv/stores/txstatus"
	"github.com/libsv/go-p2p/chaincfg/chainhash"
)

type Memory struct {
	mu       sync.Mutex
	txStatus map[chainhash.Hash]txstatus.Status
}

func New() *Memory {
	return &Memory{
		txStatus: make(map[chainhash.Hash]txstatus.Status),
	}
}

func (m *Memory) Get(_ context.Context, hash *chainhash.Hash) (*txstatus.Status, error) {
	m.mu.Lock()
	status, ok := m.txStatus[*hash]
	m.mu.Unlock()

	if !ok {
		return nil, txstatus.ErrNotFound
	}

	return &status, nil
}

func (m *Memory) Set(_ context.Context, hash *chainhash.Hash, fee uint64, parentTxHashes []*chainhash.Hash, utxoHashes []*chainhash.Hash) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	_, ok := m.txStatus[*hash]
	if ok {
		return txstatus.ErrAlreadyExists
	}

	s := txstatus.Status{
		Status:         txstatus.Unconfirmed,
		Fee:            fee,
		FirstSeen:      time.Now(),
		ParentTxHashes: parentTxHashes,
		UtxoHashes:     utxoHashes,
	}

	m.txStatus[*hash] = s

	return nil
}

func (m *Memory) SetMined(_ context.Context, hash *chainhash.Hash, blockHash *chainhash.Hash) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	s, ok := m.txStatus[*hash]
	if !ok {
		s = txstatus.Status{}
	}

	s.Status = txstatus.Confirmed

	if s.BlockHashes == nil {
		s.BlockHashes = make([]*chainhash.Hash, 0)
	}

	s.Status = txstatus.Confirmed
	s.BlockHashes = append(s.BlockHashes, blockHash)

	m.txStatus[*hash] = s

	return nil
}

func (m *Memory) Delete(_ context.Context, hash *chainhash.Hash) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	delete(m.txStatus, *hash)

	return nil
}
