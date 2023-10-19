package scylla

import (
	"context"
	"log"
	"net/url"
	"sync"
	"time"

	"github.com/bitcoin-sv/ubsv/services/utxo/utxostore_api"
	utxostore "github.com/bitcoin-sv/ubsv/stores/utxo"
	"github.com/gocql/gocql"
	"github.com/libsv/go-bt/v2/chainhash"
)

type UTXO struct {
	Hash         []byte
	SpendingTxId []byte
	LockTime     uint32
}

type Scylla struct {
	url                *url.URL
	session            *gocql.Session
	heightMutex        sync.RWMutex
	currentBlockHeight uint32
}

func NewScylla(u *url.URL) (*Scylla, error) {
	log.Printf("newScylla with host %s", u.Host)
	cluster := gocql.NewCluster(u.Host)
	cluster.Keyspace = "utxo_keyspace"

	session, err := cluster.CreateSession()
	if err != nil {
		return nil, err
	}

	err = session.Query(`CREATE TABLE IF NOT EXISTS utxos (hash blob, spendingTxId blob, lockTime bigint, PRIMARY KEY (hash))`).Exec()
	if err != nil {
		return nil, err
	}

	return &Scylla{
		url:     u,
		session: session,
	}, nil
}

func (s *Scylla) SetBlockHeight(height uint32) error {
	s.heightMutex.Lock()
	defer s.heightMutex.Unlock()

	s.currentBlockHeight = height
	return nil
}

func (s *Scylla) getBlockHeight() uint32 {
	s.heightMutex.RLock()
	defer s.heightMutex.RUnlock()

	return s.currentBlockHeight
}

func (s *Scylla) Health(ctx context.Context) (int, string, error) {
	return 0, "Scylla Cluster", nil
}

func (s *Scylla) Get(ctx context.Context, hash *chainhash.Hash) (*utxostore.UTXOResponse, error) {
	var res *UTXO

	if err := s.session.Query(`SELECT * FROM utxos WHERE hash = ? LIMIT 1`, hash[:]).Scan(res.Hash, res.SpendingTxId, res.LockTime); err != nil {
		return nil, err
	}

	if res == nil {
		return &utxostore.UTXOResponse{
			Status: int(utxostore_api.Status_NOT_FOUND),
		}, nil
	}

	status := utxostore_api.Status_OK
	if res.SpendingTxId != nil {
		status = utxostore_api.Status_SPENT
	} else if res.LockTime > 500000000 && int64(res.LockTime) > time.Now().UTC().Unix() {
		status = utxostore_api.Status_LOCKED
	} else if res.LockTime > 0 && res.LockTime < s.getBlockHeight() {
		status = utxostore_api.Status_LOCKED
	}

	h, err := chainhash.NewHash(res.SpendingTxId)
	if err != nil {
		return nil, err
	}
	return &utxostore.UTXOResponse{
		Status:       int(status),
		LockTime:     res.LockTime,
		SpendingTxID: h,
	}, nil
}

func (s *Scylla) Store(ctx context.Context, hash *chainhash.Hash, nLockTime uint32) (*utxostore.UTXOResponse, error) {

	if err := s.session.Query(`INSERT INTO utxos (hash) VALUES (?)`,
		hash[:]).Exec(); err != nil {
		return &utxostore.UTXOResponse{
			Status: int(utxostore_api.Status_ALREADY_EXISTS),
		}, nil
	}

	return &utxostore.UTXOResponse{
		Status:   int(utxostore_api.Status_OK),
		LockTime: nLockTime,
	}, nil
}

func (s *Scylla) BatchStore(ctx context.Context, hashes []*chainhash.Hash, nLockTime uint32) (*utxostore.BatchResponse, error) {
	batch := s.session.NewBatch(gocql.LoggedBatch)
	for _, hash := range hashes {
		batch.Query(`INSERT INTO utxos (hash) VALUES (?)`, hash[:])
		if err := s.session.ExecuteBatch(batch); err != nil {
			return nil, err
		}
		batch = s.session.NewBatch(gocql.LoggedBatch)
	}

	return &utxostore.BatchResponse{
		Status: 0,
	}, nil
}

func (s *Scylla) Spend(ctx context.Context, hash *chainhash.Hash, spendingTxId *chainhash.Hash) (*utxostore.UTXOResponse, error) {
	var res UTXO

	if err := s.session.Query(`SELECT * FROM utxos WHERE hash = ? LIMIT 1`, hash[:]).Scan(&res.Hash, &res.SpendingTxId, &res.LockTime); err != nil {
		return &utxostore.UTXOResponse{
			Status: int(utxostore_api.Status_NOT_FOUND),
		}, nil
	}

	if res.LockTime > 500000000 && int64(res.LockTime) > time.Now().UTC().Unix() {
		return &utxostore.UTXOResponse{
			Status:       int(utxostore_api.Status_LOCKED),
			SpendingTxID: spendingTxId,
		}, nil
	}

	if res.LockTime > 0 && res.LockTime < s.getBlockHeight() {
		return &utxostore.UTXOResponse{
			Status:       int(utxostore_api.Status_LOCKED),
			SpendingTxID: spendingTxId,
		}, nil
	}
	// spent by us
	if string(res.SpendingTxId) == string(spendingTxId[:]) {
		h, err := chainhash.NewHash(res.SpendingTxId)
		if err != nil {
			return nil, err
		}
		return &utxostore.UTXOResponse{
			Status:       int(utxostore_api.Status_SPENT),
			SpendingTxID: h,
		}, nil
	}
	// spent by someone else
	if string(res.SpendingTxId) != string(spendingTxId[:]) {
		h, err := chainhash.NewHash(res.SpendingTxId)
		if err != nil {
			return nil, err
		}
		return &utxostore.UTXOResponse{
			Status:       int(utxostore_api.Status_SPENT),
			SpendingTxID: h,
		}, nil
	}

	query := `UPDATE utxos SET spendingTxId = ? WHERE hash = ? IF spendingTxId = null`
	var applied bool
	iter := s.session.Query(query, spendingTxId, hash).Iter()
	iter.Scan(&applied)
	if err := iter.Close(); err != nil {
		return nil, err
	}

	if applied {
		log.Printf("Successfully set spendingTxId for UTXO with hash: %x", hash)
		return &utxostore.UTXOResponse{
			Status: int(utxostore_api.Status_OK),
		}, nil
	} else {
		log.Printf("couldn't set spendingTxId for UTXO with hash: %x", hash)
		return &utxostore.UTXOResponse{
			Status: int(utxostore_api.Status_SPENT),
		}, nil
	}
}

func (s *Scylla) Reset(ctx context.Context, hash *chainhash.Hash) (*utxostore.UTXOResponse, error) {

	if err := s.session.Query(`UPDATE utxos SET spendingTxId = null WHERE hash = ?`, hash[:]).Exec(); err != nil {
		return nil, err
	}

	return &utxostore.UTXOResponse{
		Status: int(utxostore_api.Status_OK),
	}, nil
}

func (s *Scylla) Delete(ctx context.Context, hash *chainhash.Hash) (*utxostore.UTXOResponse, error) {
	if err := s.session.Query(`DELETE FROM utxos WHERE hash = ?`, hash[:]).Exec(); err != nil {
		return nil, err
	}

	return &utxostore.UTXOResponse{
		Status: int(utxostore_api.Status_OK),
	}, nil
}

func (s *Scylla) DeleteSpends(deleteSpends bool) {
}
