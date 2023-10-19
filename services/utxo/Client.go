package utxo

import (
	"context"

	"github.com/bitcoin-sv/ubsv/services/utxo/utxostore_api"
	utxostore "github.com/bitcoin-sv/ubsv/stores/utxo"
	"github.com/libsv/go-bt/v2"
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

func (s *Store) Get(ctx context.Context, spend *utxostore.Spend) (*utxostore.Response, error) {
	response, err := s.db.Get(ctx, &utxostore_api.Request{
		TxId:     spend.TxID.CloneBytes(),
		Vout:     spend.Vout,
		UxtoHash: spend.Hash.CloneBytes(),
	})
	if err != nil {
		return nil, err
	}

	txid, err := chainhash.NewHash(response.SpendingTxid)
	if err != nil {
		return nil, err
	}

	return &utxostore.Response{
		Status:       int(response.Status.Number()),
		SpendingTxID: txid,
	}, nil
}

func (s *Store) Store(ctx context.Context, tx *bt.Tx, lockTime ...uint32) error {
	storeLockTime := tx.LockTime
	if len(lockTime) > 0 {
		storeLockTime = lockTime[0]
	}

	_, err := s.db.Store(ctx, &utxostore_api.StoreRequest{
		Tx:       tx.Bytes(),
		LockTime: storeLockTime,
	})
	if err != nil {
		return err
	}

	return nil
}

func (s *Store) Spend(ctx context.Context, spends []*utxostore.Spend) error {
	for idx, spend := range spends {
		err := s.spend(ctx, spend)
		if err != nil {
			for i := 0; i < idx; i++ {
				// revert the created utxos
				_ = s.Reset(ctx, spends[i])
			}
			return err
		}
	}

	return nil
}

func (s *Store) spend(ctx context.Context, spend *utxostore.Spend) error {
	_, err := s.db.Spend(ctx, &utxostore_api.Request{
		TxId:         spend.TxID.CloneBytes(),
		Vout:         spend.Vout,
		UxtoHash:     spend.Hash.CloneBytes(),
		SpendingTxid: spend.SpendingTxID.CloneBytes(),
	})
	if err != nil {
		return err
	}

	return nil
}

func (s *Store) Reset(ctx context.Context, spend *utxostore.Spend) error {
	_, err := s.db.Reset(ctx, &utxostore_api.Request{
		TxId:     spend.TxID.CloneBytes(),
		Vout:     spend.Vout,
		UxtoHash: spend.Hash.CloneBytes(),
	})
	if err != nil {
		return err
	}

	return nil
}

func (s *Store) Delete(ctx context.Context, spend *utxostore.Spend) error {
	_, err := s.db.Delete(ctx, &utxostore_api.Request{
		TxId:     spend.TxID.CloneBytes(),
		Vout:     spend.Vout,
		UxtoHash: spend.Hash.CloneBytes(),
	})
	if err != nil {
		return err
	}

	return nil
}

func (s *Store) DeleteSpends(deleteSpends bool) {
	// do nothing
}
