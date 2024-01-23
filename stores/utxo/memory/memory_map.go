package memory

import (
	"sync"

	"github.com/bitcoin-sv/ubsv/services/utxo/utxostore_api"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/libsv/go-bt/v2/chainhash"
)

type MapWithLocking struct {
	mu           sync.RWMutex
	m            map[chainhash.Hash]UTXO
	BlockHeight  uint32
	DeleteSpends bool
}

func NewMemoryMap(deleteSpends bool) *MapWithLocking {
	return &MapWithLocking{
		m:            make(map[chainhash.Hash]UTXO),
		DeleteSpends: deleteSpends,
	}
}

func (mm *MapWithLocking) SetBlockHeight(height uint32) error {
	mm.BlockHeight = height
	return nil
}

func (mm *MapWithLocking) GetBlockHeight() (uint32, error) {
	return mm.BlockHeight, nil
}

func (mm *MapWithLocking) Get(hash *chainhash.Hash) (*UTXO, bool) {
	mm.mu.RLock()
	defer mm.mu.RUnlock()

	if utxo, ok := mm.m[*hash]; ok {
		return &utxo, true
	}

	return nil, false
}

func (mm *MapWithLocking) Set(hash *chainhash.Hash, txID *chainhash.Hash) {
	mm.mu.Lock()
	defer mm.mu.Unlock()

	mm.m[*hash] = UTXO{
		SpendingTxID: txID,
	}
}

func (mm *MapWithLocking) Delete(hash *chainhash.Hash) {
	mm.mu.Lock()
	defer mm.mu.Unlock()

	delete(mm.m, *hash)
}

func (mm *MapWithLocking) Store(hash *chainhash.Hash, nLockTime uint32) (int, error) {
	mm.mu.Lock()
	defer mm.mu.Unlock()

	if utxo, ok := mm.m[*hash]; ok {
		if utxo.SpendingTxID != nil {
			return int(utxostore_api.Status_SPENT), nil
		}
	}

	mm.m[*hash] = UTXO{
		SpendingTxID: nil,
		LockTime:     nLockTime,
	}

	return int(utxostore_api.Status_OK), nil
}

func (mm *MapWithLocking) Spend(hash *chainhash.Hash, txID *chainhash.Hash) (int, uint32, *chainhash.Hash, error) {
	mm.mu.Lock()
	defer mm.mu.Unlock()

	if utxo, ok := mm.m[*hash]; ok {
		// if utxo exists, it has not been spent yet
		if utxo.SpendingTxID != nil {
			if utxo.SpendingTxID.IsEqual(txID) {
				return int(utxostore_api.Status_OK), utxo.LockTime, utxo.SpendingTxID, nil
			} else {
				return int(utxostore_api.Status_SPENT), utxo.LockTime, utxo.SpendingTxID, nil
			}
		}

		if util.ValidLockTime(utxo.LockTime, mm.BlockHeight) {
			if mm.DeleteSpends {
				delete(mm.m, *hash)
			} else {
				mm.m[*hash] = UTXO{
					SpendingTxID: txID,
					LockTime:     utxo.LockTime,
				}
			}
		} else {
			return int(utxostore_api.Status_LOCKED), utxo.LockTime, nil, nil
		}

		return int(utxostore_api.Status_OK), utxo.LockTime, nil, nil
	}

	return int(utxostore_api.Status_NOT_FOUND), 0, nil, nil
}
