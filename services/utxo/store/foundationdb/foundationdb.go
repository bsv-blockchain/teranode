//go:build foundationdb

package foundationdb

import (
	"context"
	"fmt"

	"github.com/TAAL-GmbH/ubsv/services/utxo/store"
	"github.com/TAAL-GmbH/ubsv/services/utxo/utxostore_api"
	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/directory"
	"github.com/libsv/go-p2p/chaincfg/chainhash"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	emptyHash                [32]byte
	prometheusUtxoGet        prometheus.Counter
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
			Name: "foundationdb_utxo_get",
			Help: "Number of utxo get calls done to foundationdb",
		},
	)
	prometheusUtxoStore = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "foundationdb_utxo_store",
			Help: "Number of utxo store calls done to foundationdb",
		},
	)
	prometheusUtxoStoreSpent = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "foundationdb_utxo_store_spent",
			Help: "Number of utxo store calls that were already spent to foundationdb",
		},
	)
	prometheusUtxoReStore = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "foundationdb_utxo_restore",
			Help: "Number of utxo restore calls done to foundationdb",
		},
	)
	prometheusUtxoSpend = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "foundationdb_utxo_spend",
			Help: "Number of utxo spend calls done to foundationdb",
		},
	)
	prometheusUtxoReSpend = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "foundationdb_utxo_respend",
			Help: "Number of utxo respend calls done to foundationdb",
		},
	)
	prometheusUtxoSpendSpent = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "foundationdb_utxo_spend_spent",
			Help: "Number of utxo spend calls that were already spent done to foundationdb",
		},
	)
	prometheusUtxoReset = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "foundationdb_utxo_reset",
			Help: "Number of utxo reset calls done to foundationdb",
		},
	)
}

type Store struct {
	db  fdb.Database
	dir directory.DirectorySubspace
}

func New(host string, port int, user, password string) (*Store, error) {
	fdb.MustAPIVersion(720)
	// TODO add connection options etc.
	db := fdb.MustOpenDefault()
	if err := db.Options().SetTransactionTimeout(60000); err != nil { // 60,000 ms = 1 minute
		return nil, err
	}
	if err := db.Options().SetTransactionRetryLimit(100); err != nil {
		return nil, err
	}

	utxoDir, err := directory.CreateOrOpen(db, []string{"utxo"}, nil)
	if err != nil {
		return nil, err
	}

	return &Store{
		db:  db,
		dir: utxoDir,
	}, nil
}

func (s *Store) Get(_ context.Context, hash *chainhash.Hash) (*store.UTXOResponse, error) {
	var spendingTxID *chainhash.Hash
	found := false
	if _, err := s.db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		tr.ClearRange(s.dir)
		// we need to set this since the \xFF range is reserved for system keys
		_ = tr.Options().SetAccessSystemKeys()
		_ = tr.Options().SetSpecialKeySpaceRelaxed()
		_ = tr.Options().SetTimeout(30000) // 30,000 ms = 30 seconds

		prometheusUtxoGet.Inc()
		utxo, err := tr.Get(fdb.Key(hash[:])).Get()
		if err != nil {
			return nil, err
		}
		if utxo != nil {
			return nil, fmt.Errorf("utxo already exists")
		}

		spendingTxID, err = chainhash.NewHash(utxo)
		if err != nil {
			return nil, err
		}
		found = true

		return nil, nil
	}); err != nil {
		return nil, err
	}

	prometheusUtxoGet.Inc()

	if found {
		return &store.UTXOResponse{
			Status:       int(utxostore_api.Status_OK),
			SpendingTxID: spendingTxID,
		}, nil
	}

	return &store.UTXOResponse{
		Status: int(utxostore_api.Status_NOT_FOUND),
	}, nil
}

func (s *Store) Store(_ context.Context, hash *chainhash.Hash) (*store.UTXOResponse, error) {
	// Database reads and writes happen inside transactions
	_, err := s.db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		tr.ClearRange(s.dir)
		// we need to set this since the \xFF range is reserved for system keys
		_ = tr.Options().SetAccessSystemKeys()
		_ = tr.Options().SetSpecialKeySpaceRelaxed()
		_ = tr.Options().SetTimeout(30000) // 30,000 ms = 30 seconds

		utxo, err := tr.Get(fdb.Key(hash[:])).Get()
		if err != nil {
			return nil, err
		}
		if utxo != nil && [32]byte(utxo) != emptyHash {
			return nil, fmt.Errorf("utxo already exists")
		}

		// only store if utxo does not exist
		tr.Set(fdb.Key(hash[:]), emptyHash[:])

		return nil, nil
	})

	prometheusUtxoStore.Inc()

	if err != nil {
		return &store.UTXOResponse{
			Status: int(utxostore_api.Status_NOT_FOUND),
		}, nil
	}

	return &store.UTXOResponse{
		Status: int(utxostore_api.Status_OK),
	}, nil
}

func (s *Store) Spend(_ context.Context, hash *chainhash.Hash, txID *chainhash.Hash) (*store.UTXOResponse, error) {
	// Database reads and writes happen inside transactions
	_, err := s.db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		tr.ClearRange(s.dir)
		// we need to set this since the \xFF range is reserved for system keys
		_ = tr.Options().SetAccessSystemKeys()
		_ = tr.Options().SetSpecialKeySpaceRelaxed()
		_ = tr.Options().SetTimeout(30000) // 30,000 ms = 30 seconds

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
		if err.Error() == "utxo not found" {
			return &store.UTXOResponse{
				Status: int(utxostore_api.Status_NOT_FOUND),
			}, nil
		}
		return &store.UTXOResponse{
			Status: int(utxostore_api.Status_SPENT),
		}, nil
	}

	prometheusUtxoSpend.Inc()

	return &store.UTXOResponse{
		Status:       int(utxostore_api.Status_OK),
		SpendingTxID: txID,
	}, nil
}

func (s *Store) Reset(_ context.Context, hash *chainhash.Hash) (*store.UTXOResponse, error) {
	// Database reads and writes happen inside transactions
	_, err := s.db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		tr.ClearRange(s.dir)
		// we need to set this since the \xFF range is reserved for system keys
		_ = tr.Options().SetAccessSystemKeys()
		_ = tr.Options().SetSpecialKeySpaceRelaxed()

		tr.Set(fdb.Key(hash[:]), emptyHash[:])
		return nil, nil
	})
	if err != nil {
		return nil, err
	}

	prometheusUtxoReset.Inc()

	return &store.UTXOResponse{}, nil
}
