package utxostore

import (
	"context"

	"github.com/TAAL-GmbH/ubsv/services/utxostore/utxostore_api"
	store "github.com/TAAL-GmbH/ubsv/services/validator/utxostore"
	"github.com/libsv/go-p2p/chaincfg/chainhash"
)

type Store struct {
	db utxostore_api.UtxoStoreAPIClient
}

func New(db utxostore_api.UtxoStoreAPIClient) *Store {
	return &Store{
		db: db,
	}
}

func (s Store) Store(ctx context.Context, hash *chainhash.Hash) (*store.UTXOResponse, error) {
	_, err := s.db.Store(ctx, &utxostore_api.StoreRequest{
		UxtoHash: hash[:],
	})
	if err != nil {
		return nil, err
	}

	return &store.UTXOResponse{
		Status: 200,
	}, nil
}

func (s Store) Spend(ctx context.Context, hash *chainhash.Hash, txID *chainhash.Hash) (*store.UTXOResponse, error) {
	_, err := s.db.Spend(ctx, &utxostore_api.SpendRequest{
		UxtoHash:     hash[:],
		SpendingTxid: txID[:],
	})
	if err != nil {
		return nil, err
	}

	return &store.UTXOResponse{
		Status: 200,
	}, nil
}

func (s Store) Reset(ctx context.Context, hash *chainhash.Hash) (*store.UTXOResponse, error) {
	_, err := s.db.Reset(ctx, &utxostore_api.ResetRequest{
		UxtoHash: hash[:],
	})
	if err != nil {
		return nil, err
	}

	return &store.UTXOResponse{
		Status: 200,
	}, nil
}
