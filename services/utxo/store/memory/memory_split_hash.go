package memory

import (
	"context"

	"github.com/TAAL-GmbH/ubsv/services/utxo/store"
	"github.com/TAAL-GmbH/ubsv/services/utxo/utxostore_api"
	"github.com/libsv/go-p2p/chaincfg/chainhash"
)

type SplitByHash struct {
	m            map[[1]byte]*MapWithLocking
	DeleteSpends bool
}

func NewSplitByHash(deleteSpends bool) *SplitByHash {
	db := &SplitByHash{
		m:            make(map[[1]byte]*MapWithLocking),
		DeleteSpends: deleteSpends,
	}

	for i := 0; i <= 255; i++ {
		db.m[[1]byte{uint8(i)}] = NewMemoryMap(deleteSpends)
	}

	return db
}

func (m *SplitByHash) Get(_ context.Context, hash *chainhash.Hash) (*store.UTXOResponse, error) {
	memMap := m.m[[1]byte{hash[0]}]

	if txID, ok := memMap.Get(hash); ok {
		if txID == nil {
			return &store.UTXOResponse{
				Status: int(utxostore_api.Status_OK),
			}, nil
		}
		return &store.UTXOResponse{
			Status:       int(utxostore_api.Status_SPENT),
			SpendingTxID: txID,
		}, nil
	}

	return &store.UTXOResponse{
		Status: int(utxostore_api.Status_NOT_FOUND),
	}, nil
}

func (m *SplitByHash) Store(_ context.Context, hash *chainhash.Hash) (*store.UTXOResponse, error) {
	memMap := m.m[[1]byte{hash[0]}]

	status, err := memMap.Store(hash)
	if err != nil {
		return nil, err
	}

	return &store.UTXOResponse{
		Status: status,
	}, nil
}

func (m *SplitByHash) Spend(_ context.Context, hash *chainhash.Hash, txID *chainhash.Hash) (*store.UTXOResponse, error) {
	memMap := m.m[[1]byte{hash[0]}]

	status, err := memMap.Spend(hash, txID)
	if err != nil {
		return nil, err
	}

	return &store.UTXOResponse{
		Status:       status,
		SpendingTxID: txID,
	}, nil
}

func (m *SplitByHash) Reset(ctx context.Context, hash *chainhash.Hash) (*store.UTXOResponse, error) {
	memMap := m.m[[1]byte{hash[0]}]
	memMap.Delete(hash)

	return m.Store(ctx, hash)
}

// only used for testing
func (m *SplitByHash) delete(hash *chainhash.Hash) error {
	memMap := m.m[[1]byte{hash[0]}]
	memMap.Delete(hash)
	return nil
}
