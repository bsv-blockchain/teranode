package memory

import (
	"context"
	"fmt"
	"sync"
	"time"

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
	timeout          time.Duration
}

func NewSwissMap(deleteSpends bool) *SwissMap {
	// the swiss map uses a lot less memory than the standard map
	swissMap := swiss.NewMap[chainhash.Hash, UTXO](1024 * 1024)

	return &SwissMap{
		m:                swissMap,
		DeleteSpentUtxos: deleteSpends,
		timeout:          5000 * time.Millisecond,
	}
}

func (m *SwissMap) SetBlockHeight(height uint32) error {
	m.BlockHeight = height
	return nil
}

func (m *SwissMap) GetBlockHeight() (uint32, error) {
	return m.BlockHeight, nil
}

func (m *SwissMap) Health(_ context.Context) (int, string, error) {
	return 0, "SwissMap Store", nil
}

func (m *SwissMap) Get(_ context.Context, spend *utxostore.Spend) (*utxostore.Response, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if utxo, ok := m.m.Get(*spend.Hash); ok {
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
func (m *SwissMap) Store(ctx context.Context, tx *bt.Tx, lockTime ...uint32) error {
	if m.timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, m.timeout)
		defer cancel()
	}

	_, utxoHashes, err := utxostore.GetFeesAndUtxoHashes(ctx, tx)
	if err != nil {
		return err
	}

	storeLockTime := tx.LockTime
	if len(lockTime) > 0 {
		storeLockTime = lockTime[0]
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	for i, hash := range utxoHashes {
		select {
		case <-ctx.Done():
			return fmt.Errorf("timeout storing %d of %d swissmp utxos", i, len(utxoHashes))
		default:
			if _, ok := m.m.Get(*hash); ok {
				return utxostore.ErrAlreadyExists
			} else {
				m.m.Put(*hash, UTXO{
					Hash:     nil,
					LockTime: storeLockTime,
				})
			}
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
				m.unSpend(spends[i])
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
				return utxostore.NewErrLockTime(utxo.LockTime, m.BlockHeight)
			}

			return nil
		} else {
			if utxo.Hash == nil {
				return nil
			} else {
				return utxostore.NewErrSpent(utxo.Hash)
			}
		}
	}

	return nil
}

func (m *SwissMap) UnSpend(_ context.Context, spends []*utxostore.Spend) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	for _, spend := range spends {
		m.unSpend(spend)
	}

	return nil
}

func (m *SwissMap) unSpend(spend *utxostore.Spend) {
	utxo, ok := m.m.Get(*spend.Hash)
	if ok {
		m.m.Put(*spend.Hash, UTXO{
			Hash:     nil,
			LockTime: utxo.LockTime,
		})
	}
}

func (m *SwissMap) Delete(_ context.Context, tx *bt.Tx) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.m.Delete(*tx.TxIDChainHash())

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
