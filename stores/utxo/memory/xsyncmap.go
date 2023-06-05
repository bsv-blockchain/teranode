package memory

import (
	"context"
	"encoding/binary"
	"hash/maphash"

	"github.com/TAAL-GmbH/ubsv/services/utxo/utxostore_api"
	utxostore "github.com/TAAL-GmbH/ubsv/stores/utxo"
	"github.com/libsv/go-p2p/chaincfg/chainhash"
	"github.com/puzpuzpuz/xsync/v2"
)

var emptyHash = chainhash.Hash{}

type XsyncMap struct {
	m                *xsync.MapOf[chainhash.Hash, *chainhash.Hash]
	DeleteSpentUtxos bool
}

func NewXSyncMap(deleteSpends bool) *XsyncMap {
	// the xsync map uses a lot less memory than the standard map
	// and has locking built in
	xsyncMap := xsync.NewTypedMapOf[chainhash.Hash, *chainhash.Hash](func(seed maphash.Seed, hash chainhash.Hash) uint64 {
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

func (m *XsyncMap) Get(_ context.Context, hash *chainhash.Hash) (*utxostore.UTXOResponse, error) {
	if txID, ok := m.m.Load(*hash); ok {
		if *txID == emptyHash {
			return &utxostore.UTXOResponse{
				Status: int(utxostore_api.Status_OK),
			}, nil
		}

		return &utxostore.UTXOResponse{
			Status:       int(utxostore_api.Status_SPENT),
			SpendingTxID: txID,
		}, nil
	}

	return &utxostore.UTXOResponse{
		Status: 0,
	}, nil
}

func (m *XsyncMap) Store(_ context.Context, hash *chainhash.Hash) (*utxostore.UTXOResponse, error) {
	if txID, ok := m.m.Load(*hash); ok {
		if *txID != emptyHash {
			return &utxostore.UTXOResponse{
				Status:       int(utxostore_api.Status_SPENT),
				SpendingTxID: txID,
			}, nil
		}
	} else {
		m.m.Store(*hash, &emptyHash)
	}

	return &utxostore.UTXOResponse{
		Status: int(utxostore_api.Status_OK),
	}, nil
}

func (m *XsyncMap) Spend(_ context.Context, hash *chainhash.Hash, txID *chainhash.Hash) (*utxostore.UTXOResponse, error) {
	if existingTxID, ok := m.m.Load(*hash); ok {
		if existingTxID.IsEqual(&chainhash.Hash{}) {
			m.m.Store(*hash, txID)
			return &utxostore.UTXOResponse{
				Status:       int(utxostore_api.Status_OK),
				SpendingTxID: txID,
			}, nil
		} else {
			if existingTxID.IsEqual(txID) {
				return &utxostore.UTXOResponse{
					Status:       int(utxostore_api.Status_SPENT),
					SpendingTxID: existingTxID,
				}, nil
			} else {
				return &utxostore.UTXOResponse{
					Status:       int(utxostore_api.Status_SPENT),
					SpendingTxID: existingTxID,
				}, nil
			}
		}
	}

	return &utxostore.UTXOResponse{
		Status: int(utxostore_api.Status_NOT_FOUND),
	}, nil
}

func (m *XsyncMap) Reset(ctx context.Context, hash *chainhash.Hash) (*utxostore.UTXOResponse, error) {
	m.m.Delete(*hash)
	return m.Store(ctx, hash)
}

func (m *XsyncMap) delete(hash *chainhash.Hash) error {
	m.m.Delete(*hash)
	return nil
}

func (m *XsyncMap) DeleteSpends(deleteSpends bool) {
	m.DeleteSpentUtxos = deleteSpends
}
