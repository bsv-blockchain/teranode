package memory

import (
	"context"
	"sync"

	"github.com/bitcoin-sv/ubsv/services/utxo/utxostore_api"
	utxostore "github.com/bitcoin-sv/ubsv/stores/utxo"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/libsv/go-bt/v2"
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

func (m *Memory) Health(_ context.Context) (int, string, error) {
	return 0, "Memory Store", nil
}

func (m *Memory) Get(_ context.Context, spend *utxostore.Spend) (*utxostore.Response, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if utxo, ok := m.m[*spend.Hash]; ok {
		if utxo.Hash == nil {
			return &utxostore.Response{
				Status:   int(utxostore_api.Status_OK),
				LockTime: utxo.LockTime,
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

// Store stores the utxos of the tx in aerospike
// the lockTime optional argument is needed for coinbase transactions that do not contain the lock time
func (m *Memory) Store(_ context.Context, tx *bt.Tx, lockTime ...uint32) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	_, utxoHashes, err := utxostore.GetFeesAndUtxoHashes(tx)
	if err != nil {
		return err
	}

	storeLockTime := tx.LockTime
	if len(lockTime) > 0 {
		storeLockTime = lockTime[0]
	}

	for _, hash := range utxoHashes {
		_, found := m.m[*hash]
		if found {
			return utxostore.ErrAlreadyExists
		}

		m.m[*hash] = UTXO{
			Hash:     nil,
			LockTime: storeLockTime,
		}
	}

	return nil
}

func (m *Memory) Spend(_ context.Context, spends []*utxostore.Spend) (err error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	for _, spend := range spends {
		err = m.spendUtxo(spend)
		if err != nil {
			return err
		}
	}

	return nil
}

func (m *Memory) spendUtxo(spend *utxostore.Spend) error {
	if utxo, found := m.m[*spend.Hash]; found {
		if utxo.Hash == nil {
			if util.ValidLockTime(utxo.LockTime, m.BlockHeight) {
				if m.DeleteSpentUtxos {
					delete(m.m, *spend.Hash)
				} else {
					m.m[*spend.Hash] = UTXO{
						Hash:     spend.SpendingTxID,
						LockTime: utxo.LockTime,
					}
				}
				return nil
			}

			return utxostore.ErrLockTime
		} else {
			if utxo.Hash.IsEqual(spend.SpendingTxID) {
				return nil
			}

			return utxostore.ErrSpent
		}
	}

	return utxostore.ErrNotFound
}

func (m *Memory) UnSpend(_ context.Context, spends []*utxostore.Spend) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	for _, spend := range spends {
		utxo, ok := m.m[*spend.Hash]
		if !ok {
			return utxostore.ErrNotFound
		}

		utxo.Hash = nil
		m.m[*spend.Hash] = utxo
	}

	return nil
}

func (m *Memory) Delete(_ context.Context, spend *utxostore.Spend) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	delete(m.m, *spend.Hash)

	return nil
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
