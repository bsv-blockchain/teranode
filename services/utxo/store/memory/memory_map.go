package memory

import (
	"sync"

	"github.com/TAAL-GmbH/ubsv/services/utxo/utxostore_api"
	"github.com/libsv/go-p2p/chaincfg/chainhash"
)

type MapWithLocking struct {
	mu           sync.RWMutex
	m            map[chainhash.Hash]*chainhash.Hash
	DeleteSpends bool
}

func NewMemoryMap(deleteSpends bool) *MapWithLocking {
	return &MapWithLocking{
		m:            make(map[chainhash.Hash]*chainhash.Hash),
		DeleteSpends: deleteSpends,
	}
}

func (mm *MapWithLocking) Get(hash *chainhash.Hash) (*chainhash.Hash, bool) {
	mm.mu.RLock()
	defer mm.mu.RUnlock()

	if txID, ok := mm.m[*hash]; ok {
		if txID == nil {
			return nil, true
		}
		return txID, true
	}

	return nil, false
}

func (mm *MapWithLocking) Set(hash *chainhash.Hash, txID *chainhash.Hash) {
	mm.mu.Lock()
	defer mm.mu.Unlock()

	if txID == nil {
		mm.m[*hash] = nil
	} else {
		mm.m[*hash] = txID
	}
}

func (mm *MapWithLocking) Delete(hash *chainhash.Hash) {
	mm.mu.Lock()
	defer mm.mu.Unlock()

	delete(mm.m, *hash)
}

func (mm *MapWithLocking) Store(hash *chainhash.Hash) (int, error) {
	mm.mu.Lock()
	defer mm.mu.Unlock()

	if existingTxID, ok := mm.m[*hash]; ok {
		if existingTxID != nil {
			return int(utxostore_api.Status_SPENT), nil
		}
	}

	mm.m[*hash] = nil

	return int(utxostore_api.Status_OK), nil
}

func (mm *MapWithLocking) Spend(hash *chainhash.Hash, txID *chainhash.Hash) (int, error) {
	mm.mu.Lock()
	defer mm.mu.Unlock()

	if existingTxID, ok := mm.m[*hash]; ok {
		// if utxo exists, it has not been spent yet
		if existingTxID != nil {
			if *existingTxID == *txID {
				return int(utxostore_api.Status_OK), nil
			} else {
				return int(utxostore_api.Status_SPENT), nil
			}
		}

		if mm.DeleteSpends {
			delete(mm.m, *hash)
		} else {
			mm.m[*hash] = txID
		}

		return int(utxostore_api.Status_OK), nil
	}

	return int(utxostore_api.Status_NOT_FOUND), nil
}
