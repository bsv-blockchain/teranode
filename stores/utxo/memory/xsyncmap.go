package memory

import (
	"context"
	"encoding/binary"
	"fmt"
	"hash/maphash"

	"github.com/bitcoin-sv/ubsv/services/utxo/utxostore_api"
	utxostore "github.com/bitcoin-sv/ubsv/stores/utxo"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/puzpuzpuz/xsync/v2"
)

// var emptyHash = chainhash.Hash{}

type XsyncMap struct {
	m                *xsync.MapOf[chainhash.Hash, UTXO]
	BlockHeight      uint32
	DeleteSpentUtxos bool
}

func NewXSyncMap(deleteSpends bool) *XsyncMap {
	// the xsync map uses a lot less memory than the standard map
	// and has locking built in
	xsyncMap := xsync.NewTypedMapOf[chainhash.Hash, UTXO](func(seed maphash.Seed, hash chainhash.Hash) uint64 {
		var h maphash.Hash
		h.SetSeed(seed)
		_ = binary.Write(&h, binary.LittleEndian, hash[:16])
		hh := h.Sum64()
		h.Reset()
		_ = binary.Write(&h, binary.LittleEndian, hash[16:32])
		return 31*hh + h.Sum64()
	})

	return &XsyncMap{
		m:                xsyncMap,
		DeleteSpentUtxos: deleteSpends,
	}
}

func (m *XsyncMap) SetBlockHeight(height uint32) error {
	m.BlockHeight = height
	return nil
}

func (m *XsyncMap) Health(_ context.Context) (int, string, error) {
	return 0, "XsyncMap Store", nil
}

func (m *XsyncMap) Get(_ context.Context, spend *utxostore.Spend) (*utxostore.Response, error) {
	if utxo, ok := m.m.Load(*spend.Hash); ok {
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
func (m *XsyncMap) Store(_ context.Context, tx *bt.Tx, lockTime ...uint32) error {
	_, utxoHashes, err := utxostore.GetFeesAndUtxoHashes(tx)
	if err != nil {
		return err
	}

	storeLockTime := tx.LockTime
	if len(lockTime) > 0 {
		storeLockTime = lockTime[0]
	}

	for _, hash := range utxoHashes {
		if utxo, ok := m.m.Load(*hash); ok {
			if utxo.Hash != nil {
				return utxostore.ErrSpent
			} else {
				return utxostore.ErrAlreadyExists
			}
		} else {
			m.m.Store(*hash, UTXO{
				Hash:     nil,
				LockTime: storeLockTime,
			})
		}
	}

	return nil
}

func (m *XsyncMap) Spend(ctx context.Context, spends []*utxostore.Spend) (err error) {
	for idx, spend := range spends {
		err = m.spendUtxo(spend.Hash, spend.SpendingTxID)
		if err != nil {
			for i := 0; i < idx; i++ {
				err = m.Reset(ctx, spends[i])
				if err != nil {
					fmt.Printf("error resetting utxo: %s\n", err.Error())
				}
			}
			return err
		}
	}

	return nil
}

func (m *XsyncMap) spendUtxo(hash *chainhash.Hash, txID *chainhash.Hash) error {
	if utxo, ok := m.m.Load(*hash); ok {
		if utxo.Hash == nil {
			if util.ValidLockTime(utxo.LockTime, m.BlockHeight) {
				m.m.Store(*hash, UTXO{
					Hash:     txID,
					LockTime: utxo.LockTime,
				})
				return nil
			}

			return utxostore.ErrLockTime
		} else {
			if utxo.Hash.IsEqual(txID) {
				return nil
			} else {
				return utxostore.ErrSpent
			}
		}
	}

	return utxostore.ErrNotFound
}

func (m *XsyncMap) Reset(_ context.Context, spend *utxostore.Spend) error {
	utxo, ok := m.m.Load(*spend.Hash)
	if !ok {
		return utxostore.ErrNotFound
	}

	m.m.Store(*spend.Hash, UTXO{
		Hash:     nil,
		LockTime: utxo.LockTime,
	})

	return nil
}

func (m *XsyncMap) Delete(_ context.Context, spend *utxostore.Spend) error {
	m.m.Delete(*spend.Hash)

	return nil
}

func (m *XsyncMap) delete(hash *chainhash.Hash) error {
	m.m.Delete(*hash)
	return nil
}

func (m *XsyncMap) DeleteSpends(deleteSpends bool) {
	m.DeleteSpentUtxos = deleteSpends
}
