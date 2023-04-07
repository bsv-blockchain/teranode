package utxostore

import (
	"context"

	"github.com/TAAL-GmbH/ubsv/services/utxostore/utxostore_api"
	"github.com/TAAL-GmbH/ubsv/services/validator/utxostore"
	"github.com/libsv/go-p2p/chaincfg/chainhash"
)

type Store struct {
	db utxostore_api.UtxoStoreAPIClient
}

func New(db utxostore_api.UtxoStoreAPIClient) (*Store, error) {
	return &Store{
		db: db,
	}, nil
}

func (s Store) Store(ctx context.Context, hash *chainhash.Hash) (*utxostore.UTXOResponse, error) {
	response, err := s.db.Store(ctx, &utxostore_api.StoreRequest{
		UxtoHash: hash[:],
	})
	if err != nil {
		return nil, err
	}

	return &utxostore.UTXOResponse{
		Status: int(response.Status),
	}, nil
}

func (s Store) Spend(ctx context.Context, hash *chainhash.Hash, txID *chainhash.Hash) (*utxostore.UTXOResponse, error) {
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

func (s Store) Reset(ctx context.Context, hash *chainhash.Hash) (*utxostore.UTXOResponse, error) {
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
