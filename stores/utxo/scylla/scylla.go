package scylla

import (
	"context"
	"log"
	"net/url"
	"sync"
	"time"

	"github.com/bitcoin-sv/ubsv/services/utxo/utxostore_api"
	utxostore "github.com/bitcoin-sv/ubsv/stores/utxo"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/gocql/gocql"
	"github.com/libsv/go-bt/v2"
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

func (s *Scylla) Get(ctx context.Context, spend *utxostore.Spend) (*utxostore.Response, error) {
	var res *UTXO

	if err := s.session.Query(`SELECT * FROM utxos WHERE hash = ? LIMIT 1`, spend.Hash[:]).Scan(res.Hash, res.SpendingTxId, res.LockTime); err != nil {
		return nil, err
	}

	if res == nil {
		return &utxostore.Response{
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

	return &utxostore.Response{
		Status:       int(status),
		LockTime:     res.LockTime,
		SpendingTxID: h,
	}, nil
}

// Store stores the utxos of the tx in aerospike
// the lockTime optional argument is needed for coinbase transactions that do not contain the lock time
func (s *Scylla) Store(ctx context.Context, tx *bt.Tx, lockTime ...uint32) error {
	storeLockTime := tx.LockTime
	if len(lockTime) > 0 {
		storeLockTime = lockTime[0]
	}

	txIDHash := tx.TxIDChainHash()

	utxoHashes := make([]*chainhash.Hash, len(tx.Outputs))
	for i, output := range tx.Outputs {
		if output.Satoshis > 0 { // only do outputs with value
			hash, err := util.UTXOHashFromOutput(txIDHash, output, uint32(i))
			if err != nil {
				return err
			}

			utxoHashes[i] = hash
		}
	}

	for outputIdx, hash := range utxoHashes {
		err := s.storeUtxo(ctx, hash, storeLockTime)
		if err != nil {
			for i := 0; i < outputIdx; i++ {
				// revert the created utxos
				_ = s.Delete(ctx, &utxostore.Spend{
					TxID: txIDHash,
					Vout: uint32(i),
					Hash: hash,
				})
			}
			return err
		}
	}

	return nil
}

func (s *Scylla) storeUtxo(ctx context.Context, hash *chainhash.Hash, nLockTime uint32) error {

	// TODO lock time
	if err := s.session.Query(`INSERT INTO utxos (hash) VALUES (?)`,
		hash[:]).Exec(); err != nil {
		return utxostore.ErrAlreadyExists
	}

	return nil
}

func (s *Scylla) Spend(ctx context.Context, spends []*utxostore.Spend) (err error) {
	for idx, spend := range spends {
		if err = s.spendUtxo(ctx, spend.Hash, spend.SpendingTxID); err != nil {
			for i := 0; i < idx; i++ {
				// revert the created utxos
				_ = s.Reset(ctx, spends[i])
			}
			return err
		}
	}

	return nil
}

// TODO this function is not atomic / concurrent safe
func (s *Scylla) spendUtxo(_ context.Context, hash *chainhash.Hash, spendingTxId *chainhash.Hash) error {
	var res UTXO

	if err := s.session.Query(`SELECT * FROM utxos WHERE hash = ? LIMIT 1`, hash[:]).Scan(&res.Hash, &res.SpendingTxId, &res.LockTime); err != nil {
		return utxostore.ErrNotFound
	}

	if res.LockTime > 500000000 && int64(res.LockTime) > time.Now().UTC().Unix() {
		return utxostore.ErrLockTime
	}

	if res.LockTime > 0 && res.LockTime < s.getBlockHeight() {
		return utxostore.ErrLockTime
	}
	// spent by us
	if string(res.SpendingTxId) == string(spendingTxId[:]) {
		return nil
	}

	// spent by someone else
	if string(res.SpendingTxId) != string(spendingTxId[:]) {
		return utxostore.ErrSpent
	}

	query := `UPDATE utxos SET spendingTxId = ? WHERE hash = ? IF spendingTxId = null`
	var applied bool
	iter := s.session.Query(query, spendingTxId, hash).Iter()
	iter.Scan(&applied)
	if err := iter.Close(); err != nil {
		return err
	}

	if applied {
		log.Printf("Successfully set spendingTxId for UTXO with hash: %x", hash)
		return nil
	} else {
		log.Printf("couldn't set spendingTxId for UTXO with hash: %x", hash)
		return utxostore.ErrSpent
	}
}

func (s *Scylla) Reset(_ context.Context, spend *utxostore.Spend) error {

	if err := s.session.Query(`UPDATE utxos SET spendingTxId = null WHERE hash = ?`, spend.Hash[:]).Exec(); err != nil {
		return err
	}

	return nil
}

func (s *Scylla) Delete(_ context.Context, spend *utxostore.Spend) error {
	if err := s.session.Query(`DELETE FROM utxos WHERE hash = ?`, spend.Hash[:]).Exec(); err != nil {
		return err
	}

	return nil
}

func (s *Scylla) DeleteSpends(deleteSpends bool) {
}
