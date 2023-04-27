package memory

import (
	"context"
	"sync"

	"github.com/TAAL-GmbH/ubsv/services/utxo/store"
	"github.com/TAAL-GmbH/ubsv/services/utxo/utxostore_api"
	"github.com/libsv/go-p2p/chaincfg/chainhash"
)

var (
	empty = &chainhash.Hash{}
)

type Memory struct {
	mu               sync.Mutex
	m                map[chainhash.Hash]chainhash.Hash
	DeleteSpentUtxos bool
}

func New(deleteSpends bool) *Memory {
	return &Memory{
		m:                make(map[chainhash.Hash]chainhash.Hash),
		DeleteSpentUtxos: deleteSpends,
	}
}

func (m *Memory) Get(_ context.Context, hash *chainhash.Hash) (*store.UTXOResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if txID, ok := m.m[*hash]; ok {
		if txID.IsEqual(empty) {
			return &store.UTXOResponse{
				Status: int(utxostore_api.Status_OK),
			}, nil
		}
		return &store.UTXOResponse{
			Status:       int(utxostore_api.Status_SPENT),
			SpendingTxID: &txID,
		}, nil
	}

	return &store.UTXOResponse{
		Status: int(utxostore_api.Status_NOT_FOUND),
	}, nil
}

func (m *Memory) Store(_ context.Context, hash *chainhash.Hash) (*store.UTXOResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	spendingTxid, found := m.m[*hash]
	if found {
		if spendingTxid.IsEqual(empty) {
			return &store.UTXOResponse{
				Status: int(utxostore_api.Status_OK),
			}, nil
		}

		return &store.UTXOResponse{
			Status:       int(utxostore_api.Status_SPENT),
			SpendingTxID: &spendingTxid,
		}, nil

	}

	m.m[*hash] = *empty

	return &store.UTXOResponse{
		Status: int(utxostore_api.Status_OK),
	}, nil
}

func (m *Memory) Spend(_ context.Context, hash *chainhash.Hash, txID *chainhash.Hash) (*store.UTXOResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if existingHash, found := m.m[*hash]; found {
		if existingHash.IsEqual(&chainhash.Hash{}) {
			if m.DeleteSpentUtxos {
				delete(m.m, *hash)
			} else {
				m.m[*hash] = *txID
			}
			return &store.UTXOResponse{
				Status:       int(utxostore_api.Status_OK),
				SpendingTxID: txID,
			}, nil
		} else {
			if existingHash.IsEqual(txID) {
				return &store.UTXOResponse{
					Status:       int(utxostore_api.Status_OK),
					SpendingTxID: txID,
				}, nil
			} else {
				return &store.UTXOResponse{
					Status:       int(utxostore_api.Status_SPENT),
					SpendingTxID: &existingHash,
				}, nil
			}
		}
	}

	return &store.UTXOResponse{
		Status: int(utxostore_api.Status_NOT_FOUND),
	}, nil
}

func (m *Memory) Reset(ctx context.Context, hash *chainhash.Hash) (*store.UTXOResponse, error) {
	m.mu.Lock()
	delete(m.m, *hash)
	m.mu.Unlock()

	return m.Store(ctx, hash)
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
