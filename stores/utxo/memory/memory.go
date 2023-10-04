package memory

import (
	"context"
	"sync"

	"github.com/bitcoin-sv/ubsv/services/utxo/utxostore_api"
	utxostore "github.com/bitcoin-sv/ubsv/stores/utxo"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/libsv/go-bt/v2/chainhash"
)

// var (
// 	empty = &chainhash.Hash{}
// )

type UTXO struct {
	Hash     *chainhash.Hash
	LockTime uint32
}

type Memory struct {
	mu               sync.Mutex
	m                map[chainhash.Hash]UTXO // needs to be able to be variable length
	BlockHeight      uint32
	DeleteSpentUtxos bool
}

func New(deleteSpends bool) *Memory {
	return &Memory{
		m:                make(map[chainhash.Hash]UTXO),
		DeleteSpentUtxos: deleteSpends,
	}
}

func (m *Memory) SetBlockHeight(height uint32) error {
	m.BlockHeight = height
	return nil
}

func (m *Memory) Health(ctx context.Context) (int, string, error) {
	return 0, "Memory Store", nil
}

func (m *Memory) Get(_ context.Context, hash *chainhash.Hash) (*utxostore.UTXOResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if utxo, ok := m.m[*hash]; ok {
		if utxo.Hash == nil {
			return &utxostore.UTXOResponse{
				Status:   int(utxostore_api.Status_OK),
				LockTime: utxo.LockTime,
			}, nil
		}
		return &utxostore.UTXOResponse{
			Status:       int(utxostore_api.Status_SPENT),
			SpendingTxID: utxo.Hash,
			LockTime:     utxo.LockTime,
		}, nil
	}

	return &utxostore.UTXOResponse{
		Status: int(utxostore_api.Status_NOT_FOUND),
	}, nil
}

func (m *Memory) Store(_ context.Context, hash *chainhash.Hash, nLockTime uint32) (*utxostore.UTXOResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	utxo, found := m.m[*hash]
	if found {
		if utxo.Hash == nil {
			return &utxostore.UTXOResponse{
				Status:   int(utxostore_api.Status_OK),
				LockTime: utxo.LockTime,
			}, nil
		}

		return &utxostore.UTXOResponse{
			Status:       int(utxostore_api.Status_SPENT),
			SpendingTxID: utxo.Hash,
			LockTime:     utxo.LockTime,
		}, nil
	}

	m.m[*hash] = UTXO{
		Hash:     nil,
		LockTime: nLockTime,
	}

	return &utxostore.UTXOResponse{
		Status:   int(utxostore_api.Status_OK),
		LockTime: nLockTime,
	}, nil
}

func (m *Memory) BatchStore(ctx context.Context, hashes []*chainhash.Hash, nLockTime uint32) (*utxostore.BatchResponse, error) {
	var h *chainhash.Hash
	for _, h = range hashes {
		_, err := m.Store(ctx, h, nLockTime)
		if err != nil {
			return nil, err
		}
	}

	return &utxostore.BatchResponse{
		Status: 0,
	}, nil
}

func (m *Memory) Spend(_ context.Context, hash *chainhash.Hash, txID *chainhash.Hash) (*utxostore.UTXOResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if utxo, found := m.m[*hash]; found {
		if utxo.Hash == nil {
			if util.ValidLockTime(utxo.LockTime, m.BlockHeight) {
				if m.DeleteSpentUtxos {
					delete(m.m, *hash)
				} else {
					m.m[*hash] = UTXO{
						Hash:     txID,
						LockTime: utxo.LockTime,
					}
				}
				return &utxostore.UTXOResponse{
					Status:       int(utxostore_api.Status_OK),
					SpendingTxID: txID,
				}, nil
			} else {
				return &utxostore.UTXOResponse{
					Status:   int(utxostore_api.Status_LOCK_TIME),
					LockTime: utxo.LockTime,
				}, nil
			}
		} else {
			if utxo.Hash.IsEqual(txID) {
				return &utxostore.UTXOResponse{
					Status:       int(utxostore_api.Status_OK),
					SpendingTxID: txID,
				}, nil
			} else {
				return &utxostore.UTXOResponse{
					Status:       int(utxostore_api.Status_SPENT),
					SpendingTxID: utxo.Hash,
				}, nil
			}
		}
	}

	return &utxostore.UTXOResponse{
		Status: int(utxostore_api.Status_NOT_FOUND),
	}, nil
}

func (m *Memory) Reset(ctx context.Context, hash *chainhash.Hash) (*utxostore.UTXOResponse, error) {
	m.mu.Lock()
	utxo, ok := m.m[*hash]
	delete(m.m, *hash)
	m.mu.Unlock()

	nLockTime := uint32(0)
	if ok {
		nLockTime = utxo.LockTime
	}

	return m.Store(ctx, hash, nLockTime)
}

func (m *Memory) Delete(_ context.Context, hash *chainhash.Hash) (*utxostore.UTXOResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	delete(m.m, *hash)

	return &utxostore.UTXOResponse{
		Status: int(utxostore_api.Status_OK),
	}, nil
}

func (m *Memory) delete(hash *chainhash.Hash) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	delete(m.m, *hash)

	return nil
}

func (m *Memory) DeleteSpends(deleteSpends bool) {
	m.DeleteSpentUtxos = deleteSpends
}
