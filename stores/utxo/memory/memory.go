package memory

import (
	"context"
	"sync"
	"time"

	utxostore "github.com/bitcoin-sv/ubsv/stores/utxo"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/chainhash"
)

// var (
// 	empty = &chainhash.Hash{}
// )

type UTXO struct {
	SpendingTxID *chainhash.Hash
	LockTime     uint32
}

type Memory struct {
	mu               sync.Mutex
	m                map[chainhash.Hash]UTXO // needs to be able to be variable length
	BlockHeight      uint32
	DeleteSpentUtxos bool
	timeout          time.Duration
}

func New(deleteSpends bool) *Memory {
	return &Memory{
		m:                make(map[chainhash.Hash]UTXO),
		DeleteSpentUtxos: deleteSpends,
		timeout:          5000 * time.Millisecond,
	}
}

func (m *Memory) SetBlockHeight(height uint32) error {
	m.BlockHeight = height
	return nil
}

func (m *Memory) GetBlockHeight() (uint32, error) {
	return m.BlockHeight, nil
}

func (m *Memory) Health(_ context.Context) (int, string, error) {
	return 0, "Memory Store", nil
}

func (m *Memory) Get(_ context.Context, spend *utxostore.Spend) (*utxostore.Response, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if utxo, ok := m.m[*spend.Hash]; ok {
		if utxo.SpendingTxID == nil {
			return &utxostore.Response{
				Status:   int(utxostore.Status_OK),
				LockTime: utxo.LockTime,
			}, nil
		}
		return &utxostore.Response{
			Status:       int(utxostore.Status_SPENT),
			SpendingTxID: utxo.SpendingTxID,
			LockTime:     utxo.LockTime,
		}, nil
	}

	return &utxostore.Response{
		Status: int(utxostore.Status_NOT_FOUND),
	}, nil
}

// Store stores the utxos of the tx in aerospike
// the lockTime optional argument is needed for coinbase transactions that do not contain the lock time
func (m *Memory) Store(ctx context.Context, tx *bt.Tx, lockTime ...uint32) error {
	if m.timeout > 0 {
		var cancel context.CancelFunc
		_, cancel = context.WithTimeout(ctx, m.timeout)
		defer cancel()
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	utxoHashes, err := utxostore.GetUtxoHashes(tx)
	if err != nil {
		return err
	}

	storeLockTime := tx.LockTime
	if len(lockTime) > 0 {
		storeLockTime = lockTime[0]
	}

	for _, hash := range utxoHashes {
		_, found := m.m[hash]
		if found {
			return utxostore.ErrAlreadyExists
		}

		m.m[hash] = UTXO{
			SpendingTxID: nil,
			LockTime:     storeLockTime,
		}
	}

	return nil
}

func (m *Memory) StoreFromHashes(_ context.Context, _ chainhash.Hash, hashes []chainhash.Hash, lockTime uint32) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	for _, hash := range hashes {
		_, found := m.m[hash]
		if found {
			return utxostore.ErrAlreadyExists
		}

		m.m[hash] = UTXO{
			SpendingTxID: nil,
			LockTime:     lockTime,
		}
	}

	return nil
}

func (m *Memory) Spend(_ context.Context, spends []*utxostore.Spend) (err error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	for idx, spend := range spends {
		if spend == nil {
			continue
		}

		err = m.spendUtxo(spend)
		if err != nil {
			for i := 0; i < idx; i++ {
				err = m.unSpend(spends[i])
				if err != nil {
					return err
				}
			}
			return err
		}
	}

	return nil
}

func (m *Memory) spendUtxo(spend *utxostore.Spend) error {
	if utxo, found := m.m[*spend.Hash]; found {
		if utxo.SpendingTxID == nil {
			if util.ValidLockTime(utxo.LockTime, m.BlockHeight) {
				if m.DeleteSpentUtxos {
					delete(m.m, *spend.Hash)
				} else {
					m.m[*spend.Hash] = UTXO{
						SpendingTxID: spend.SpendingTxID,
						LockTime:     utxo.LockTime,
					}
				}
				return nil
			}

			return utxostore.NewErrLockTime(utxo.LockTime, m.BlockHeight)
		} else {
			if utxo.SpendingTxID.IsEqual(spend.SpendingTxID) {
				return nil
			}

			return utxostore.NewErrSpent(utxo.SpendingTxID)
		}
	}

	return utxostore.ErrNotFound
}

func (m *Memory) UnSpend(_ context.Context, spends []*utxostore.Spend) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	for _, spend := range spends {
		err := m.unSpend(spend)
		if err != nil {
			return err
		}
	}

	return nil
}

func (m *Memory) unSpend(spend *utxostore.Spend) error {
	utxo, ok := m.m[*spend.Hash]
	if !ok {
		return utxostore.ErrNotFound
	}

	utxo.SpendingTxID = nil
	m.m[*spend.Hash] = utxo

	return nil
}

func (m *Memory) Delete(_ context.Context, tx *bt.Tx) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	delete(m.m, *tx.TxIDChainHash())

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
