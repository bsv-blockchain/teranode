package memory

import (
	"context"

	"github.com/bitcoin-sv/ubsv/services/utxo/utxostore_api"
	utxostore "github.com/bitcoin-sv/ubsv/stores/utxo"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/chainhash"
)

type SplitByHash struct {
	m                map[[1]byte]*MapWithLocking
	DeleteSpentUtxos bool
}

func NewSplitByHash(deleteSpends bool) *SplitByHash {
	db := &SplitByHash{
		m:                make(map[[1]byte]*MapWithLocking),
		DeleteSpentUtxos: deleteSpends,
	}

	for i := 0; i <= 255; i++ {
		db.m[[1]byte{uint8(i)}] = NewMemoryMap(deleteSpends)
	}

	return db
}

func (m *SplitByHash) SetBlockHeight(height uint32) error {
	for i := 0; i <= 255; i++ {
		_ = m.m[[1]byte{uint8(i)}].SetBlockHeight(height)
	}
	return nil
}

func (m *SplitByHash) Health(ctx context.Context) (int, string, error) {
	return 0, "SplitByHash Store", nil
}

func (m *SplitByHash) Get(_ context.Context, spend *utxostore.Spend) (*utxostore.Response, error) {
	memMap := m.m[[1]byte{spend.Hash[0]}]

	if utxo, ok := memMap.Get(spend.Hash); ok {
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

func (m *SplitByHash) Store(_ context.Context, tx *bt.Tx) error {
	_, utxoHashes, err := utxostore.GetFeesAndUtxoHashes(tx)
	if err != nil {
		return err
	}

	var ok bool
	for _, hash := range utxoHashes {
		_, ok = m.m[[1]byte{hash[0]}].Get(hash)
		if ok {
			return utxostore.ErrAlreadyExists
		}

		_, err = m.m[[1]byte{hash[0]}].Store(hash, tx.LockTime)
		if err != nil {
			return err
		}
	}

	return nil
}

func (m *SplitByHash) Spend(_ context.Context, spends []*utxostore.Spend) error {
	for idx, spend := range spends {
		memMap := m.m[[1]byte{spend.Hash[0]}]

		status, _, _ := memMap.Spend(spend.Hash, spend.SpendingTxID)
		var statusErr error
		if status == int(utxostore_api.Status_NOT_FOUND) {
			statusErr = utxostore.ErrNotFound
		} else if status == int(utxostore_api.Status_SPENT) {
			statusErr = utxostore.ErrSpent
		} else if status == int(utxostore_api.Status_LOCKED) {
			statusErr = utxostore.ErrLockTime
		}

		if statusErr != nil {
			for i := 0; i < idx; i++ {
				m.m[[1]byte{spends[i].Hash[0]}].Set(spends[i].Hash, nil)
			}
			return statusErr
		}
	}

	return nil
}

func (m *SplitByHash) Reset(_ context.Context, spend *utxostore.Spend) error {
	memMap := m.m[[1]byte{spend.Hash[0]}]
	_, ok := memMap.Get(spend.Hash)

	if ok {
		memMap.Set(spend.Hash, nil)
	}

	return nil
}

func (m *SplitByHash) Delete(_ context.Context, spend *utxostore.Spend) error {
	memMap := m.m[[1]byte{spend.Hash[0]}]
	memMap.Delete(spend.Hash)

	return nil
}

// only used for testing
func (m *SplitByHash) delete(hash *chainhash.Hash) error {
	memMap := m.m[[1]byte{hash[0]}]
	memMap.Delete(hash)
	return nil
}

func (m *SplitByHash) DeleteSpends(deleteSpends bool) {
	m.DeleteSpentUtxos = deleteSpends
}
