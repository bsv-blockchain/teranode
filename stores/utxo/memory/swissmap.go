package memory

import (
	"context"
	"sync"

	"github.com/TAAL-GmbH/ubsv/services/utxo/utxostore_api"
	utxostore "github.com/TAAL-GmbH/ubsv/stores/utxo"
	"github.com/dolthub/swiss"
	"github.com/libsv/go-p2p/chaincfg/chainhash"
)

type SwissMap struct {
	mu               sync.Mutex
	m                *swiss.Map[chainhash.Hash, *chainhash.Hash]
	DeleteSpentUtxos bool
}

func NewSwissMap(deleteSpends bool) *SwissMap {
	// the swiss map uses a lot less memory than the standard map
	swissMap := swiss.NewMap[chainhash.Hash, *chainhash.Hash](1_000_000)

	return &SwissMap{
		m:                swissMap,
		DeleteSpentUtxos: deleteSpends,
	}
}

func (m *SwissMap) Get(_ context.Context, hash *chainhash.Hash) (*utxostore.UTXOResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if txID, ok := m.m.Get(*hash); ok {
		if *txID == emptyHash {
			return &utxostore.UTXOResponse{
				Status: int(utxostore_api.Status_OK),
			}, nil
		}

		return &utxostore.UTXOResponse{
			Status:       int(utxostore_api.Status_SPENT),
			SpendingTxID: txID,
		}, nil
	}

	return &utxostore.UTXOResponse{
		Status: 0,
	}, nil
}

func (m *SwissMap) Store(_ context.Context, hash *chainhash.Hash) (*utxostore.UTXOResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if txID, ok := m.m.Get(*hash); ok {
		if *txID != emptyHash {
			return &utxostore.UTXOResponse{
				Status:       int(utxostore_api.Status_SPENT),
				SpendingTxID: txID,
			}, nil
		}
	} else {
		m.m.Put(*hash, &emptyHash)
	}

	return &utxostore.UTXOResponse{
		Status: int(utxostore_api.Status_OK),
	}, nil
}

func (m *SwissMap) BatchStore(ctx context.Context, hashes []*chainhash.Hash) (*utxostore.BatchResponse, error) {
	var h *chainhash.Hash
	for _, h = range hashes {
		_, err := m.Store(ctx, h)
		if err != nil {
			return nil, err
		}
	}

	return &utxostore.BatchResponse{
		Status: 0,
	}, nil
}

func (m *SwissMap) Spend(_ context.Context, hash *chainhash.Hash, txID *chainhash.Hash) (*utxostore.UTXOResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if existingTxID, ok := m.m.Get(*hash); ok {
		if existingTxID.IsEqual(&chainhash.Hash{}) {
			m.m.Put(*hash, txID)
			return &utxostore.UTXOResponse{
				Status:       int(utxostore_api.Status_OK),
				SpendingTxID: txID,
			}, nil
		} else {
			if existingTxID.IsEqual(txID) {
				return &utxostore.UTXOResponse{
					Status:       int(utxostore_api.Status_SPENT),
					SpendingTxID: existingTxID,
				}, nil
			} else {
				return &utxostore.UTXOResponse{
					Status:       int(utxostore_api.Status_SPENT),
					SpendingTxID: existingTxID,
				}, nil
			}
		}
	}

	return &utxostore.UTXOResponse{
		Status: int(utxostore_api.Status_NOT_FOUND),
	}, nil
}

func (m *SwissMap) Reset(ctx context.Context, hash *chainhash.Hash) (*utxostore.UTXOResponse, error) {
	m.mu.Lock()
	m.m.Delete(*hash)
	m.mu.Unlock()

	return m.Store(ctx, hash)
}

func (m *SwissMap) delete(hash *chainhash.Hash) error {
	m.m.Delete(*hash)
	return nil
}

func (m *SwissMap) DeleteSpends(deleteSpends bool) {
	m.DeleteSpentUtxos = deleteSpends
}
