//go:build foundationdb

package foundationdb

import (
	"context"
	"fmt"

	"github.com/TAAL-GmbH/ubsv/services/utxo/store"
	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/libsv/go-p2p/chaincfg/chainhash"
)

var emptyHash [32]byte

type Store struct {
	db fdb.Database
}

func New(host string, port int, user, password string) (*Store, error) {
	fdb.MustAPIVersion(720)
	// TODO add connection options etc.
	db := fdb.MustOpenDefault()

	return &Store{
		db: db,
	}, nil
}

func (s *Store) Store(_ context.Context, hash *chainhash.Hash) (*store.UTXOResponse, error) {
	// Database reads and writes happen inside transactions
	if _, err := s.db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		utxo, err := tr.Get(fdb.Key(hash[:])).Get()
		if err != nil {
			return nil, err
		}
		if utxo != nil {
			return nil, fmt.Errorf("utxo already exists")
		}

		// only store if utxo does not exist
		tr.Set(fdb.Key(hash[:]), emptyHash[:])

		return nil, nil
	}); err != nil {
		return nil, err
	}

	return &store.UTXOResponse{}, nil
}

func (s *Store) Spend(_ context.Context, hash *chainhash.Hash, txID *chainhash.Hash) (*store.UTXOResponse, error) {
	// Database reads and writes happen inside transactions
	_, err := s.db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		utxo := tr.Get(fdb.Key(hash[:])).MustGet()
		if utxo == nil {
			return nil, fmt.Errorf("utxo not found")
		}
		if [32]byte(utxo) != emptyHash && [32]byte(utxo) != [32]byte(txID[:]) {
			return nil, fmt.Errorf("utxo already spent")
		}
		return nil, nil
	})
	if err != nil {
		return nil, err
	}

	return &store.UTXOResponse{}, nil
}

func (s *Store) Reset(_ context.Context, hash *chainhash.Hash) (*store.UTXOResponse, error) {
	// Database reads and writes happen inside transactions
	_, err := s.db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		tr.Set(fdb.Key(hash[:]), emptyHash[:])
		return nil, nil
	})
	if err != nil {
		return nil, err
	}

	return &store.UTXOResponse{}, nil
}
