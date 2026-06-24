package aerospike

import (
	"context"
	"time"

	"github.com/bsv-blockchain/aerospike-client-go/v8"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/util"
)

// This file implements the conflict-resolution write-ahead log (WAL) for the
// Aerospike backend — crash safety for ProcessConflicting /
// ReverseProcessConflicting (see #861). Intents are stored in a dedicated set
// (one per store, derived from the txmeta set name) so they never appear in the
// txmeta scan/iterator paths. Records are written with TTLDontExpire — an
// interrupted intent must survive until the next startup replay, however long
// that takes.

const conflictWALSetSuffix = "_conflictwal"

// WAL record bin names. These live in a dedicated set, so they cannot collide
// with the txmeta field bins.
const (
	walBinKind        = "kind"
	walBinBlockHeight = "blockHeight"
	walBinBlockHash   = "blockHash"
	walBinTxHashes    = "txHashes"
	walBinStartedAt   = "startedAt"
)

// conflictWALSet returns the dedicated set name for this store's WAL.
func (s *Store) conflictWALSet() string {
	return s.setName + conflictWALSetSuffix
}

// encodeIntentHashes flattens the intent's tx hashes into a single byte slice
// (32 bytes each) for storage in the txHashes bin.
func encodeIntentHashes(hashes []chainhash.Hash) []byte {
	buf := make([]byte, 0, len(hashes)*chainhash.HashSize)
	for i := range hashes {
		buf = append(buf, hashes[i][:]...)
	}

	return buf
}

// decodeIntentHashes splits a stored txHashes blob back into chainhash values.
func decodeIntentHashes(buf []byte) ([]chainhash.Hash, error) {
	if len(buf)%chainhash.HashSize != 0 {
		return nil, errors.NewStorageError("conflict WAL txHashes blob length %d is not a multiple of %d", len(buf), chainhash.HashSize)
	}

	hashes := make([]chainhash.Hash, 0, len(buf)/chainhash.HashSize)
	for off := 0; off < len(buf); off += chainhash.HashSize {
		var h chainhash.Hash
		copy(h[:], buf[off:off+chainhash.HashSize])
		hashes = append(hashes, h)
	}

	return hashes, nil
}

// BeginConflictIntent durably records an intent before the operation's first
// state mutation. The default write policy uses RecordExistsAction=UPDATE and
// TTLDontExpire, so re-recording the same deterministic intent id is an
// idempotent overwrite (never a duplicate, never expires).
func (s *Store) BeginConflictIntent(ctx context.Context, intent utxo.ConflictIntent) error {
	intentID := intent.IntentID()

	key, err := aerospike.NewKey(s.namespace, s.conflictWALSet(), intentID[:])
	if err != nil {
		return errors.NewStorageError("[BeginConflictIntent] failed to build key for intent %s", intentID.String(), err)
	}

	blockHash := intent.BlockHash

	bins := []*aerospike.Bin{
		aerospike.NewBin(walBinKind, string(intent.Kind)),
		aerospike.NewBin(walBinBlockHeight, int(intent.BlockHeight)),
		aerospike.NewBin(walBinBlockHash, blockHash[:]),
		aerospike.NewBin(walBinTxHashes, encodeIntentHashes(intent.TxHashes)),
		aerospike.NewBin(walBinStartedAt, int(intent.StartedAt)),
	}

	if err := s.client.PutBins(util.GetAerospikeWritePolicy(s.settings, 0), key, bins...); err != nil {
		return errors.NewStorageError("[BeginConflictIntent] failed to record intent %s", intentID.String(), err)
	}

	return nil
}

// CompleteConflictIntent removes the intent record once the terminal step
// committed. Deleting an absent key is idempotent (no error).
func (s *Store) CompleteConflictIntent(ctx context.Context, intentID chainhash.Hash) error {
	key, err := aerospike.NewKey(s.namespace, s.conflictWALSet(), intentID[:])
	if err != nil {
		return errors.NewStorageError("[CompleteConflictIntent] failed to build key for intent %s", intentID.String(), err)
	}

	if _, err := s.client.Delete(util.GetAerospikeWritePolicy(s.settings, 0), key); err != nil {
		return errors.NewStorageError("[CompleteConflictIntent] failed to remove intent %s", intentID.String(), err)
	}

	return nil
}

// PendingConflictIntents scans the WAL set and returns every begun-but-not-
// completed intent. Called once at BlockAssembler startup, so a full set scan
// is acceptable.
func (s *Store) PendingConflictIntents(ctx context.Context) ([]utxo.ConflictIntent, error) {
	stmt := aerospike.NewStatement(s.namespace, s.conflictWALSet())
	stmt.BinNames = []string{walBinKind, walBinBlockHeight, walBinBlockHash, walBinTxHashes, walBinStartedAt}

	queryPolicy := aerospike.NewQueryPolicy()
	queryPolicy.MaxRetries = 3
	queryPolicy.SocketTimeout = 30 * time.Second
	queryPolicy.TotalTimeout = 120 * time.Second

	recordset, err := s.client.Query(queryPolicy, stmt)
	if err != nil {
		return nil, errors.NewStorageError("[PendingConflictIntents] failed to query WAL set", err)
	}
	defer recordset.Close()

	var intents []utxo.ConflictIntent

	for res := range recordset.Results() {
		if res.Err != nil {
			return nil, errors.NewStorageError("[PendingConflictIntents] error reading WAL record", res.Err)
		}

		record := res.Record
		if record == nil {
			continue
		}

		kind, ok := record.Bins[walBinKind].(string)
		if !ok {
			return nil, errors.NewStorageError("[PendingConflictIntents] WAL record missing or invalid %s bin", walBinKind)
		}

		blockHeight, ok := record.Bins[walBinBlockHeight].(int)
		if !ok {
			return nil, errors.NewStorageError("[PendingConflictIntents] WAL record missing or invalid %s bin", walBinBlockHeight)
		}

		blockHashBytes, ok := record.Bins[walBinBlockHash].([]byte)
		if !ok {
			return nil, errors.NewStorageError("[PendingConflictIntents] WAL record missing or invalid %s bin", walBinBlockHash)
		}

		txHashesBytes, ok := record.Bins[walBinTxHashes].([]byte)
		if !ok {
			return nil, errors.NewStorageError("[PendingConflictIntents] WAL record missing or invalid %s bin", walBinTxHashes)
		}

		startedAt, ok := record.Bins[walBinStartedAt].(int)
		if !ok {
			return nil, errors.NewStorageError("[PendingConflictIntents] WAL record missing or invalid %s bin", walBinStartedAt)
		}

		bh, err := chainhash.NewHash(blockHashBytes)
		if err != nil {
			return nil, errors.NewStorageError("[PendingConflictIntents] corrupt blockHash (kind=%s height=%d startedAt=%d)", kind, blockHeight, startedAt, err)
		}

		hashes, err := decodeIntentHashes(txHashesBytes)
		if err != nil {
			return nil, errors.NewStorageError("[PendingConflictIntents] corrupt intent record (kind=%s height=%d startedAt=%d)", kind, blockHeight, startedAt, err)
		}

		intents = append(intents, utxo.ConflictIntent{
			Kind:        utxo.ConflictIntentKind(kind),
			BlockHeight: uint32(blockHeight),
			BlockHash:   *bh,
			TxHashes:    hashes,
			StartedAt:   int64(startedAt),
		})
	}

	return intents, nil
}
