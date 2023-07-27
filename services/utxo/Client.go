package utxo

import (
	"context"

	"github.com/TAAL-GmbH/ubsv/services/utxo/utxostore_api"
	utxostore "github.com/TAAL-GmbH/ubsv/stores/utxo"
	"github.com/libsv/go-bt/v2/chainhash"
)

type Store struct {
	db               utxostore_api.UtxoStoreAPIClient
	BlockHeight      uint32
	DeleteSpentUtxos bool
}

func NewClient(db utxostore_api.UtxoStoreAPIClient) (*Store, error) {
	return &Store{
		db:          db,
		BlockHeight: 0,
	}, nil
}

func (s *Store) SetBlockHeight(height uint32) error {
	s.BlockHeight = height
	return nil
}

func (s *Store) Get(_ context.Context, hash *chainhash.Hash) (*utxostore.UTXOResponse, error) {
	response, err := s.db.Get(context.Background(), &utxostore_api.GetRequest{
		UxtoHash: hash[:],
	})
	if err != nil {
		return nil, err
	}

	txid, err := chainhash.NewHash(response.SpendingTxid)
	if err != nil {
		return nil, err
	}

	return &utxostore.UTXOResponse{
		Status:       int(response.Status.Number()),
		SpendingTxID: txid,
	}, nil
}

func (s *Store) Store(ctx context.Context, hash *chainhash.Hash, nLockTime uint32) (*utxostore.UTXOResponse, error) {
	response, err := s.db.Store(ctx, &utxostore_api.StoreRequest{
		UxtoHash: hash[:],
		LockTime: nLockTime,
	})
	if err != nil {
		return nil, err
	}

	return &utxostore.UTXOResponse{
		Status: int(response.Status),
	}, nil
}

func (s *Store) BatchStore(ctx context.Context, hash []*chainhash.Hash) (*utxostore.BatchResponse, error) {
	for _, h := range hash {
		_, err := s.Store(ctx, h, 0)
		if err != nil {
			return nil, err
		}
	}

	return &utxostore.BatchResponse{
		Status: 0,
	}, nil
}

func (s *Store) Spend(ctx context.Context, hash *chainhash.Hash, txID *chainhash.Hash) (*utxostore.UTXOResponse, error) {
	response, err := s.db.Spend(ctx, &utxostore_api.SpendRequest{
		UxtoHash:     hash[:],
		SpendingTxid: txID[:],
	})
	if err != nil {
		return nil, err
	}

	return &utxostore.UTXOResponse{
		Status: int(response.Status),
	}, nil
}

func (s *Store) Reset(ctx context.Context, hash *chainhash.Hash) (*utxostore.UTXOResponse, error) {
	response, err := s.db.Reset(ctx, &utxostore_api.ResetRequest{
		UxtoHash: hash[:],
	})
	if err != nil {
		return nil, err
	}

	return &utxostore.UTXOResponse{
		Status: int(response.Status),
	}, nil
}

func (s *Store) DeleteSpends(deleteSpends bool) {
	// do nothing
}
