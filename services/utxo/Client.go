package utxo

import (
	"context"

	"github.com/TAAL-GmbH/ubsv/services/utxo/store"
	"github.com/TAAL-GmbH/ubsv/services/utxo/utxostore_api"
	"github.com/libsv/go-p2p/chaincfg/chainhash"
)

type Store struct {
	db               utxostore_api.UtxoStoreAPIClient
	DeleteSpentUtxos bool
}

func NewClient(db utxostore_api.UtxoStoreAPIClient) (*Store, error) {
	return &Store{
		db: db,
	}, nil
}

func (s *Store) Get(_ context.Context, hash *chainhash.Hash) (*store.UTXOResponse, error) {
	return nil, nil
}

func (s *Store) Store(ctx context.Context, hash *chainhash.Hash) (*store.UTXOResponse, error) {
	response, err := s.db.Store(ctx, &utxostore_api.StoreRequest{
		UxtoHash: hash[:],
	})
	if err != nil {
		return nil, err
	}

	return &store.UTXOResponse{
		Status: int(response.Status),
	}, nil
}

func (s *Store) Spend(ctx context.Context, hash *chainhash.Hash, txID *chainhash.Hash) (*store.UTXOResponse, error) {
	response, err := s.db.Spend(ctx, &utxostore_api.SpendRequest{
		UxtoHash:     hash[:],
		SpendingTxid: txID[:],
	})
	if err != nil {
		return nil, err
	}

	return &store.UTXOResponse{
		Status: int(response.Status),
	}, nil
}

func (s *Store) Reset(ctx context.Context, hash *chainhash.Hash) (*store.UTXOResponse, error) {
	response, err := s.db.Reset(ctx, &utxostore_api.ResetRequest{
		UxtoHash: hash[:],
	})
	if err != nil {
		return nil, err
	}

	return &store.UTXOResponse{
		Status: int(response.Status),
	}, nil
}

func (s *Store) DeleteSpends(deleteSpends bool) {
	// do nothing
}
