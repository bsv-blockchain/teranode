package utxo

import (
	"context"

	"github.com/bitcoin-sv/ubsv/services/utxo/utxostore_api"
	utxostore "github.com/bitcoin-sv/ubsv/stores/utxo"
	"github.com/libsv/go-bt/v2/chainhash"
	"google.golang.org/protobuf/types/known/emptypb"
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

func (s *Store) Health(ctx context.Context) (int, string, error) {
	resp, err := s.db.Health(ctx, &emptypb.Empty{})
	if err != nil {
		return -1, resp.Details, err
	}

	return 0, resp.Details, nil
}

func (s *Store) Get(ctx context.Context, hash *chainhash.Hash) (*utxostore.UTXOResponse, error) {
	response, err := s.db.Get(ctx, &utxostore_api.GetRequest{
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

func (s *Store) Delete(ctx context.Context, hash *chainhash.Hash) (*utxostore.UTXOResponse, error) {
	response, err := s.db.Delete(ctx, &utxostore_api.DeleteRequest{
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
