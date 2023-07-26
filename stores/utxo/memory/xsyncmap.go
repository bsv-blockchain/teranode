package memory

import (
	"context"
	"encoding/binary"
	"hash/maphash"

	"github.com/TAAL-GmbH/ubsv/services/utxo/utxostore_api"
	utxostore "github.com/TAAL-GmbH/ubsv/stores/utxo"
	"github.com/TAAL-GmbH/ubsv/util"
	"github.com/libsv/go-p2p/chaincfg/chainhash"
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

func (m *XsyncMap) Get(_ context.Context, hash *chainhash.Hash) (*utxostore.UTXOResponse, error) {
	if utxo, ok := m.m.Load(*hash); ok {
		if utxo.Hash == nil {
			return &utxostore.UTXOResponse{
				Status:   int(utxostore_api.Status_OK),
				LockTime: utxo.LockTime,
			}, nil
		}

		return &utxostore.UTXOResponse{
			Status:       int(utxostore_api.Status_SPENT),
			SpendingTxID: utxo.Hash,
			LockTime:     utxo.LockTime,
		}, nil
	}

	return &utxostore.UTXOResponse{
		Status: int(utxostore_api.Status_NOT_FOUND),
	}, nil
}

func (m *XsyncMap) Store(_ context.Context, hash *chainhash.Hash, nLockTime uint32) (*utxostore.UTXOResponse, error) {
	if utxo, ok := m.m.Load(*hash); ok {
		if utxo.Hash != nil {
			return &utxostore.UTXOResponse{
				Status:       int(utxostore_api.Status_SPENT),
				SpendingTxID: utxo.Hash,
				LockTime:     utxo.LockTime,
			}, nil
		}
	} else {
		m.m.Store(*hash, UTXO{
			Hash:     nil,
			LockTime: nLockTime,
		})
	}

	return &utxostore.UTXOResponse{
		Status: int(utxostore_api.Status_OK),
	}, nil
}

func (m *XsyncMap) BatchStore(ctx context.Context, hashes []*chainhash.Hash) (*utxostore.BatchResponse, error) {
	var h *chainhash.Hash
	for _, h = range hashes {
		_, err := m.Store(ctx, h, 0)
		if err != nil {
			return nil, err
		}
	}

	return &utxostore.BatchResponse{
		Status: 0,
	}, nil
}

func (m *XsyncMap) Spend(_ context.Context, hash *chainhash.Hash, txID *chainhash.Hash) (*utxostore.UTXOResponse, error) {
	if utxo, ok := m.m.Load(*hash); ok {
		if utxo.Hash == nil {
			if util.ValidLockTime(utxo.LockTime, m.BlockHeight) {
				m.m.Store(*hash, UTXO{
					Hash:     txID,
					LockTime: utxo.LockTime,
				})
				return &utxostore.UTXOResponse{
					Status:       int(utxostore_api.Status_OK),
					SpendingTxID: txID,
				}, nil
			} else {
				return &utxostore.UTXOResponse{
					Status:   int(utxostore_api.Status_LOCK_TIME),
					LockTime: utxo.LockTime,
				}, nil
			}
		} else {
			if utxo.Hash.IsEqual(txID) {
				return &utxostore.UTXOResponse{
					Status:       int(utxostore_api.Status_OK),
					SpendingTxID: txID,
					LockTime:     utxo.LockTime,
				}, nil
			} else {
				return &utxostore.UTXOResponse{
					Status:       int(utxostore_api.Status_SPENT),
					SpendingTxID: utxo.Hash,
					LockTime:     utxo.LockTime,
				}, nil
			}
		}
	}

	return &utxostore.UTXOResponse{
		Status: int(utxostore_api.Status_NOT_FOUND),
	}, nil
}

func (m *XsyncMap) Reset(ctx context.Context, hash *chainhash.Hash) (*utxostore.UTXOResponse, error) {
	utxo, ok := m.m.Load(*hash)
	m.m.Delete(*hash)

	nLockTime := uint32(0)
	if ok {
		nLockTime = utxo.LockTime
	}

	return m.Store(ctx, hash, nLockTime)
}

func (m *XsyncMap) delete(hash *chainhash.Hash) error {
	m.m.Delete(*hash)
	return nil
}

func (m *XsyncMap) DeleteSpends(deleteSpends bool) {
	m.DeleteSpentUtxos = deleteSpends
}
