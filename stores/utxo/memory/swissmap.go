package memory

import (
	"context"
	"sync"

	"github.com/bitcoin-sv/ubsv/services/utxo/utxostore_api"
	utxostore "github.com/bitcoin-sv/ubsv/stores/utxo"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/dolthub/swiss"
	"github.com/libsv/go-bt/v2/chainhash"
)

type SwissMap struct {
	mu               sync.Mutex
	m                *swiss.Map[chainhash.Hash, UTXO]
	BlockHeight      uint32
	DeleteSpentUtxos bool
}

func NewSwissMap(deleteSpends bool) *SwissMap {
	// the swiss map uses a lot less memory than the standard map
	swissMap := swiss.NewMap[chainhash.Hash, UTXO](1024 * 1024)

	return &SwissMap{
		m:                swissMap,
		DeleteSpentUtxos: deleteSpends,
	}
}

func (m *SwissMap) SetBlockHeight(height uint32) error {
	m.BlockHeight = height
	return nil
}

func (m *SwissMap) Health(ctx context.Context) (int, string, error) {
	return 0, "SwissMap Store", nil
}

func (m *SwissMap) Get(_ context.Context, hash *chainhash.Hash) (*utxostore.UTXOResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if utxo, ok := m.m.Get(*hash); ok {
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

func (m *SwissMap) Store(_ context.Context, hash *chainhash.Hash, nLockTime uint32) (*utxostore.UTXOResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if utxo, ok := m.m.Get(*hash); ok {
		if utxo.Hash != nil {
			return &utxostore.UTXOResponse{
				Status:       int(utxostore_api.Status_SPENT),
				SpendingTxID: utxo.Hash,
			}, nil
		}
	} else {
		m.m.Put(*hash, UTXO{
			Hash:     nil,
			LockTime: nLockTime,
		})
	}

	return &utxostore.UTXOResponse{
		Status: int(utxostore_api.Status_OK),
	}, nil
}

func (m *SwissMap) BatchStore(ctx context.Context, hashes []*chainhash.Hash, nLockTime uint32) (*utxostore.BatchResponse, error) {
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

func (m *SwissMap) Spend(_ context.Context, hash *chainhash.Hash, txID *chainhash.Hash) (*utxostore.UTXOResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if utxo, ok := m.m.Get(*hash); ok {
		if utxo.Hash == nil {
			if util.ValidLockTime(utxo.LockTime, m.BlockHeight) {
				if m.DeleteSpentUtxos {
					m.m.Delete(*hash)
				} else {
					m.m.Put(*hash, UTXO{
						Hash:     txID,
						LockTime: utxo.LockTime,
					})
				}
			} else {
				return &utxostore.UTXOResponse{
					Status:   int(utxostore_api.Status_LOCKED),
					LockTime: utxo.LockTime,
				}, nil
			}

			return &utxostore.UTXOResponse{
				Status:       int(utxostore_api.Status_OK),
				SpendingTxID: txID,
				LockTime:     utxo.LockTime,
			}, nil
		} else {
			if utxo.Hash == nil {
				return &utxostore.UTXOResponse{
					Status:       int(utxostore_api.Status_OK),
					SpendingTxID: txID,
					LockTime:     utxo.LockTime,
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

func (m *SwissMap) Reset(ctx context.Context, hash *chainhash.Hash) (*utxostore.UTXOResponse, error) {
	m.mu.Lock()
	utxo, ok := m.m.Get(*hash)
	m.m.Delete(*hash)
	m.mu.Unlock()

	nLockTime := uint32(0)
	if ok {
		nLockTime = utxo.LockTime
	}

	return m.Store(ctx, hash, nLockTime)
}

func (m *SwissMap) Delete(_ context.Context, hash *chainhash.Hash) (*utxostore.UTXOResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.m.Delete(*hash)

	return &utxostore.UTXOResponse{
		Status: int(utxostore_api.Status_OK),
	}, nil
}

func (m *SwissMap) delete(hash *chainhash.Hash) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.m.Delete(*hash)
	return nil
}

func (m *SwissMap) DeleteSpends(deleteSpends bool) {
	m.DeleteSpentUtxos = deleteSpends
}
