package memory

import (
	"context"
	"encoding/binary"
	"fmt"
	"hash/maphash"
	"time"

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
	timeout          time.Duration
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
		timeout:          5000 * time.Millisecond,
	}
}

func (m *XsyncMap) SetBlockHeight(height uint32) error {
	m.BlockHeight = height
	return nil
}

func (m *XsyncMap) GetBlockHeight() (uint32, error) {
	return m.BlockHeight, nil
}

func (m *XsyncMap) Health(_ context.Context) (int, string, error) {
	return 0, "XsyncMap Store", nil
}

func (m *XsyncMap) Get(_ context.Context, spend *utxostore.Spend) (*utxostore.Response, error) {
	if utxo, ok := m.m.Load(*spend.Hash); ok {
		if utxo.SpendingTxID == nil {
			return &utxostore.Response{
				Status:   int(utxostore_api.Status_OK),
				LockTime: utxo.LockTime,
			}, nil
		}

		return &utxostore.Response{
			Status:       int(utxostore_api.Status_SPENT),
			SpendingTxID: utxo.SpendingTxID,
			LockTime:     utxo.LockTime,
		}, nil
	}

	return &utxostore.Response{
		Status: int(utxostore_api.Status_NOT_FOUND),
	}, nil
}

// Store stores the utxos of the tx in aerospike
// the lockTime optional argument is needed for coinbase transactions that do not contain the lock time
func (m *XsyncMap) Store(ctx context.Context, tx *bt.Tx, lockTime ...uint32) error {
	if m.timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, m.timeout)
		defer cancel()
	}

	utxoHashes, err := utxostore.GetUtxoHashes(tx)
	if err != nil {
		return err
	}

	storeLockTime := tx.LockTime
	if len(lockTime) > 0 {
		storeLockTime = lockTime[0]
	}

	for i, hash := range utxoHashes {
		select {
		case <-ctx.Done():
			return fmt.Errorf("timeout storing %d of %d xsyncmap utxos", i, len(utxoHashes))
		default:
			if utxo, ok := m.m.Load(*hash); ok {
				if utxo.SpendingTxID != nil {
					return utxostore.NewErrSpent(utxo.SpendingTxID)
				} else {
					return utxostore.ErrAlreadyExists
				}
			} else {
				m.m.Store(*hash, UTXO{
					SpendingTxID: nil,
					LockTime:     storeLockTime,
				})
			}
		}
	}

	return nil
}

func (m *XsyncMap) Spend(ctx context.Context, spends []*utxostore.Spend) (err error) {
	for idx, spend := range spends {
		err = m.spendUtxo(spend.Hash, spend.SpendingTxID)
		if err != nil {
			for i := 0; i < idx; i++ {
				err = m.unSpend(spends[i])
				if err != nil {
					fmt.Printf("error unspending utxo: %s\n", err.Error())
				}
			}
			return err
		}
	}

	return nil
}

func (m *XsyncMap) spendUtxo(hash *chainhash.Hash, txID *chainhash.Hash) error {
	if utxo, ok := m.m.Load(*hash); ok {
		if utxo.SpendingTxID == nil {
			if util.ValidLockTime(utxo.LockTime, m.BlockHeight) {
				m.m.Store(*hash, UTXO{
					SpendingTxID: txID,
					LockTime:     utxo.LockTime,
				})
				return nil
			}

			return utxostore.NewErrLockTime(utxo.LockTime, m.BlockHeight)
		} else {
			if utxo.SpendingTxID.IsEqual(txID) {
				return nil
			} else {
				return utxostore.NewErrSpent(utxo.SpendingTxID)
			}
		}
	}

	return utxostore.ErrNotFound
}

func (m *XsyncMap) UnSpend(_ context.Context, spends []*utxostore.Spend) error {
	for _, spend := range spends {
		err := m.unSpend(spend)
		if err != nil {
			return err
		}
	}

	return nil
}

func (m *XsyncMap) unSpend(spend *utxostore.Spend) error {
	utxo, ok := m.m.Load(*spend.Hash)
	if !ok {
		return utxostore.ErrNotFound
	}

	m.m.Store(*spend.Hash, UTXO{
		SpendingTxID: nil,
		LockTime:     utxo.LockTime,
	})

	return nil
}

func (m *XsyncMap) Delete(_ context.Context, tx *bt.Tx) error {
	m.m.Delete(*tx.TxIDChainHash())

	return nil
}

func (m *XsyncMap) delete(hash *chainhash.Hash) error {
	m.m.Delete(*hash)
	return nil
}

func (m *XsyncMap) DeleteSpends(deleteSpends bool) {
	m.DeleteSpentUtxos = deleteSpends
}
