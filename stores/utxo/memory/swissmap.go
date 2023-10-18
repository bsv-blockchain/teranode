package memory

import (
	"context"
	"fmt"
	"sync"

	"github.com/bitcoin-sv/ubsv/services/utxo/utxostore_api"
	utxostore "github.com/bitcoin-sv/ubsv/stores/utxo"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/dolthub/swiss"
	"github.com/libsv/go-bt/v2"
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

func (m *SwissMap) Health(_ context.Context) (int, string, error) {
	return 0, "SwissMap Store", nil
}

func (m *SwissMap) Get(_ context.Context, spend *utxostore.Spend) (*utxostore.Response, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if utxo, ok := m.m.Get(*spend.Hash); ok {
		if utxo.Hash == nil || utxo.Hash.IsEqual(spend.SpendingTxID) {
			return &utxostore.Response{
				Status:       int(utxostore_api.Status_OK),
				SpendingTxID: utxo.Hash,
				LockTime:     utxo.LockTime,
			}, nil
		}

		return &utxostore.Response{
			Status:       int(utxostore_api.Status_SPENT),
			SpendingTxID: utxo.Hash,
			LockTime:     utxo.LockTime,
		}, nil
	}

	return &utxostore.Response{
		Status: int(utxostore_api.Status_NOT_FOUND),
	}, nil
}

func (m *SwissMap) Store(_ context.Context, tx *bt.Tx) error {
	_, utxoHashes, err := utxostore.GetFeesAndUtxoHashes(tx)
	if err != nil {
		return err
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	for _, hash := range utxoHashes {
		if _, ok := m.m.Get(*hash); ok {
			return utxostore.ErrAlreadyExists
		} else {
			m.m.Put(*hash, UTXO{
				Hash:     nil,
				LockTime: tx.LockTime,
			})
		}
	}

	return nil
}

func (m *SwissMap) Spend(ctx context.Context, spends []*utxostore.Spend) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	for idx, spend := range spends {
		err := m.spendUtxo(spend.Hash, spend.SpendingTxID)
		if err != nil {
			for i := 0; i < idx; i++ {
				err = m.Reset(ctx, spends[i])
				if err != nil {
					fmt.Printf("ERROR: could not reset utxo %s: %s\n", spends[i].Hash, err)
				}
			}
			return err
		}
	}

	return nil
}

func (m *SwissMap) spendUtxo(hash *chainhash.Hash, txID *chainhash.Hash) error {
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
				return utxostore.ErrLockTime
			}

			return nil
		} else {
			if utxo.Hash == nil {
				return nil
			} else {
				return utxostore.ErrSpent
			}
		}
	}

	return nil
}

func (m *SwissMap) Reset(_ context.Context, spend *utxostore.Spend) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	utxo, ok := m.m.Get(*spend.Hash)
	if ok {
		m.m.Put(*spend.Hash, UTXO{
			Hash:     nil,
			LockTime: utxo.LockTime,
		})
	}

	return nil
}

func (m *SwissMap) Delete(_ context.Context, spend *utxostore.Spend) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.m.Delete(*spend.Hash)

	return nil
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
