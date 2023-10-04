package memory

import (
	"context"

	"github.com/bitcoin-sv/ubsv/services/utxo/utxostore_api"
	utxostore "github.com/bitcoin-sv/ubsv/stores/utxo"
	"github.com/libsv/go-bt/v2/chainhash"
	"golang.org/x/sync/errgroup"
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

func (m *SplitByHash) Get(_ context.Context, hash *chainhash.Hash) (*utxostore.UTXOResponse, error) {
	memMap := m.m[[1]byte{hash[0]}]

	if utxo, ok := memMap.Get(hash); ok {
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

func (m *SplitByHash) Store(_ context.Context, hash *chainhash.Hash, nLockTime uint32) (*utxostore.UTXOResponse, error) {
	status, err := m.m[[1]byte{hash[0]}].Store(hash, nLockTime)
	if err != nil {
		return nil, err
	}

	return &utxostore.UTXOResponse{
		Status: status,
	}, nil
}

func (m *SplitByHash) BatchStore(ctx context.Context, hashes []*chainhash.Hash, nLockTime uint32) (*utxostore.BatchResponse, error) {
	var hash *chainhash.Hash
	g, _ := errgroup.WithContext(ctx)
	for _, hash = range hashes {
		hash := hash
		g.Go(func() error {
			_, err := m.m[[1]byte{hash[0]}].Store(hash, nLockTime)
			if err != nil {
				return err
			}
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return nil, err
	}

	return &utxostore.BatchResponse{
		Status: 0,
	}, nil
}

func (m *SplitByHash) Spend(_ context.Context, hash *chainhash.Hash, txID *chainhash.Hash) (*utxostore.UTXOResponse, error) {
	memMap := m.m[[1]byte{hash[0]}]

	status, nLockTime, err := memMap.Spend(hash, txID)
	if err != nil {
		return nil, err
	}

	return &utxostore.UTXOResponse{
		Status:       status,
		SpendingTxID: txID,
		LockTime:     nLockTime,
	}, nil
}

func (m *SplitByHash) Reset(ctx context.Context, hash *chainhash.Hash) (*utxostore.UTXOResponse, error) {
	memMap := m.m[[1]byte{hash[0]}]
	utxo, ok := memMap.Get(hash)
	memMap.Delete(hash)

	nLockTime := uint32(0)
	if ok {
		nLockTime = utxo.LockTime
	}

	return m.Store(ctx, hash, nLockTime)
}

func (m *SplitByHash) Delete(_ context.Context, hash *chainhash.Hash) (*utxostore.UTXOResponse, error) {
	memMap := m.m[[1]byte{hash[0]}]
	memMap.Delete(hash)

	return &utxostore.UTXOResponse{
		Status: int(utxostore_api.Status_OK),
	}, nil
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
