package memory

import (
	"context"
	"sync"
	"time"

	"github.com/bitcoin-sv/ubsv/stores/txmeta"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/chainhash"
)

type Memory struct {
	mu       sync.Mutex
	txStatus map[chainhash.Hash]txmeta.Data
}

func New() *Memory {
	return &Memory{
		txStatus: make(map[chainhash.Hash]txmeta.Data),
	}
}

func (m *Memory) Get(_ context.Context, hash *chainhash.Hash) (*txmeta.Data, error) {
	m.mu.Lock()
	status, ok := m.txStatus[*hash]
	m.mu.Unlock()

	if !ok {
		return nil, txmeta.ErrNotFound
	}

	return &status, nil
}

func (m *Memory) Create(_ context.Context, tx *bt.Tx, hash *chainhash.Hash, fee uint64, sizeInBytes uint64, parentTxHashes []*chainhash.Hash,
	utxoHashes []*chainhash.Hash, nLockTime uint32) error {

	m.mu.Lock()
	defer m.mu.Unlock()

	_, ok := m.txStatus[*hash]
	if ok {
		return txmeta.ErrAlreadyExists
	}

	s := txmeta.Data{
		Tx:             tx,
		Status:         txmeta.Validated,
		Fee:            fee,
		SizeInBytes:    sizeInBytes,
		FirstSeen:      uint32(time.Now().Unix()),
		ParentTxHashes: parentTxHashes,
		UtxoHashes:     utxoHashes,
		LockTime:       nLockTime,
	}

	m.txStatus[*hash] = s

	return nil
}

func (m *Memory) SetMined(_ context.Context, hash *chainhash.Hash, blockHash *chainhash.Hash) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	s, ok := m.txStatus[*hash]
	if !ok {
		s = txmeta.Data{}
	}

	s.Status = txmeta.Confirmed

	if s.BlockHashes == nil {
		s.BlockHashes = make([]*chainhash.Hash, 0)
	}

	s.Status = txmeta.Confirmed
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
