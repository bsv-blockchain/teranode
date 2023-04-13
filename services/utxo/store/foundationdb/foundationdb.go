//go:build foundationdb

package foundationdb

import (
	"context"
	"fmt"

	"github.com/TAAL-GmbH/ubsv/services/utxo/store"
	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/libsv/go-p2p/chaincfg/chainhash"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var emptyHash [32]byte

var (
	emptyHash                [32]byte
	prometheusUtxoStore      prometheus.Counter
	prometheusUtxoReStore    prometheus.Counter
	prometheusUtxoStoreSpent prometheus.Counter
	prometheusUtxoSpend      prometheus.Counter
	prometheusUtxoReSpend    prometheus.Counter
	prometheusUtxoSpendSpent prometheus.Counter
	prometheusUtxoReset      prometheus.Counter
)

func init() {
	prometheusUtxoGet = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "utxostore_utxo_get",
			Help: "Number of utxo get calls done to utxostore",
		},
	)
	prometheusUtxoStore = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "utxostore_utxo_store",
			Help: "Number of utxo store calls done to utxostore",
		},
	)
	prometheusUtxoStoreSpent = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "utxostore_utxo_store_spent",
			Help: "Number of utxo store calls that were already spent to utxostore",
		},
	)
	prometheusUtxoReStore = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "utxostore_utxo_restore",
			Help: "Number of utxo restore calls done to utxostore",
		},
	)
	prometheusUtxoSpend = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "utxostore_utxo_spend",
			Help: "Number of utxo spend calls done to utxostore",
		},
	)
	prometheusUtxoReSpend = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "utxostore_utxo_respend",
			Help: "Number of utxo respend calls done to utxostore",
		},
	)
	prometheusUtxoSpendSpent = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "utxostore_utxo_spend_spent",
			Help: "Number of utxo spend calls that were already spent done to utxostore",
		},
	)
	prometheusUtxoReset = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "utxostore_utxo_reset",
			Help: "Number of utxo reset calls done to utxostore",
		},
	)
}

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
		prometheusUtxoGet.Inc()
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

	prometheusUtxoStore.Inc()

	return &store.UTXOResponse{}, nil
}

func (s *Store) Spend(_ context.Context, hash *chainhash.Hash, txID *chainhash.Hash) (*store.UTXOResponse, error) {
	// Database reads and writes happen inside transactions
	_, err := s.db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		prometheusUtxoGet.Inc()
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

	prometheusUtxoSpend.Inc()

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

	prometheusUtxoReset.Inc()

	return &store.UTXOResponse{}, nil
}
