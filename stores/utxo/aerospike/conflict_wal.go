package aerospike

import (
	"context"
	"math"
	"time"

	"github.com/bsv-blockchain/aerospike-client-go/v8"
	"github.com/bsv-blockchain/aerospike-client-go/v8/types"
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
	// walBinKeyFormula marks a record whose key this build derives from its fields. A record
	// without it was written by an older build, or under an explicit key, and keeps that key.
	walBinKeyFormula = "keyFormula"
)

// walKeyFormulaVersion is the version of IntentID's derivation that this build writes.
const walKeyFormulaVersion = 2

const errWALRecordMissingBin = "[PendingConflictIntents] WAL record missing or invalid %s bin"

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
// idempotent insert (preserves the existing checklist and never expires).
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

	if intent.ID == (chainhash.Hash{}) {
		bins = append(bins, aerospike.NewBin(walBinKeyFormula, walKeyFormulaVersion))
	}

	if intent.Recovery != nil {
		if intent.Recovery.Phase != utxo.ConflictRecoveryPlanning {
			return errors.NewStorageError("Begin requires a planning checklist")
		}
		bins = append(bins, aerospike.NewBin("recoveryPhase", utxo.ConflictRecoveryPlanning))
	}
	policy := util.GetAerospikeWritePolicy(s.settings, 0)
	policy.RecordExistsAction = aerospike.CREATE_ONLY
	if err := s.client.PutBins(policy, key, bins...); err != nil {
		if err.Matches(types.KEY_EXISTS_ERROR) {
			return nil
		}
		return errors.NewStorageError("[BeginConflictIntent] failed to record intent %s", intentID.String(), err)
	}

	return nil
}

// CompleteConflictIntent removes the intent record once the terminal step
// committed. Deleting an absent key is idempotent (no error).
func (s *Store) CompleteConflictIntent(ctx context.Context, intentID chainhash.Hash) error {
	if err := s.deleteRecoveryChunks(ctx, intentID); err != nil {
		return err
	}

	key, err := aerospike.NewKey(s.namespace, s.conflictWALSet(), intentID[:])
	if err != nil {
		return errors.NewStorageError("[CompleteConflictIntent] failed to build key for intent %s", intentID.String(), err)
	}

	if _, err := s.client.Delete(util.GetAerospikeWritePolicy(s.settings, 0), key); err != nil {
		return errors.NewStorageError("[CompleteConflictIntent] failed to remove intent %s", intentID.String(), err)
	}

	return nil
}

// conflictWALScanTimeout bounds one fallback scan of the WAL set. Without the WAL index the
// server walks every record of the namespace to find the few in this set, and sends nothing while
// it walks other sets' records, so the scan has no socket timeout and only this overall bound.
const conflictWALScanTimeout = 5 * time.Minute

// conflictWALIndexName is the secondary index over the startedAt bin of the WAL set. Every intent
// record has that bin, whichever build wrote it, so the index holds every pending intent. It is
// named after the set because an index name is unique within a namespace.
func (s *Store) conflictWALIndexName() string {
	return s.conflictWALSet() + "_" + walBinStartedAt
}

// conflictWALBins are the root bins PendingConflictIntents reads.
var conflictWALBins = []string{walBinKind, walBinBlockHeight, walBinBlockHash, walBinTxHashes, walBinStartedAt, walBinKeyFormula, "recoveryDone"}

// ensureConflictWALIndex creates the WAL index when it is missing. The server builds it in the
// background over every existing record, so intents written before it existed are found through it
// once it is built. A failure only costs startup speed: PendingConflictIntents scans the set until
// the index is readable.
func (s *Store) ensureConflictWALIndex() {
	if s.client == nil || s.client.Client == nil {
		return
	}

	indexName := s.conflictWALIndexName()

	s.indexMutex.Lock()
	defer s.indexMutex.Unlock()

	exists, err := s.indexExistsInSet(indexName, s.conflictWALSet())
	if err != nil {
		s.logger.Warnf("[ensureConflictWALIndex] could not check whether %s exists; Block Assembly startup will scan the namespace for conflict intents until it does: %v", indexName, err)
		return
	}

	if exists {
		return
	}

	s.logger.Infof("[ensureConflictWALIndex] creating index %s:%s:%s", s.namespace, s.conflictWALSet(), indexName)

	if _, err := s.client.CreateIndex(aerospike.NewWritePolicy(0, 0), s.namespace, s.conflictWALSet(), indexName, walBinStartedAt, aerospike.NUMERIC); err != nil {
		if err.Matches(types.INDEX_FOUND) {
			return
		}

		s.logger.Warnf("[ensureConflictWALIndex] failed to create %s; Block Assembly startup will scan the namespace for conflict intents: %v", indexName, err)
	}
}

// PendingConflictIntents returns every begun-but-not-completed intent. It reads only the root
// bins, never the checklist chunks: callers load a checklist with LoadConflictRecovery when they
// need it.
//
// It reads the WAL set through its secondary index, which costs the size of the set. While the
// index is missing or still building it falls back to a scan of the set, which without a set index
// walks the whole namespace. Either read is attempted once: an error is returned for the caller to
// retry.
func (s *Store) PendingConflictIntents(ctx context.Context) ([]utxo.ConflictIntent, error) {
	records, readable, err := s.queryConflictWALIndex(ctx)
	if err != nil {
		return nil, err
	}

	if !readable {
		s.logger.Warnf("[PendingConflictIntents] index %s is missing or not built yet; scanning the WAL set, which walks the whole namespace", s.conflictWALIndexName())

		if records, err = s.scanConflictWAL(ctx); err != nil {
			return nil, err
		}
	}

	var (
		intents []utxo.ConflictIntent
		done    []chainhash.Hash
	)

	for _, record := range records {
		intent, completed, err := decodeConflictWALRecord(record)
		if err != nil {
			return nil, err
		}

		// A completed intent whose chunk cleanup was interrupted is not pending. Finish its
		// deletion below rather than expose it as a new legacy operation.
		if completed {
			done = append(done, intent.IntentID())
			continue
		}

		intents = append(intents, intent)
	}

	for _, id := range done {
		if err := utxo.CompleteConflictIntent(ctx, s, id); err != nil {
			return nil, err
		}
	}

	return intents, nil
}

// queryConflictWALIndex reads every WAL root through the WAL index. readable is false when the
// server reports the index missing or not built on some node.
func (s *Store) queryConflictWALIndex(ctx context.Context) (records []*aerospike.Record, readable bool, err error) {
	if ctxErr := ctx.Err(); ctxErr != nil {
		return nil, false, ctxErr
	}

	stmt := aerospike.NewStatement(s.namespace, s.conflictWALSet(), conflictWALBins...)
	if err := stmt.SetFilter(aerospike.NewRangeFilter(walBinStartedAt, math.MinInt64, math.MaxInt64)); err != nil {
		return nil, false, errors.NewProcessingError("[PendingConflictIntents] could not build the index filter", err)
	}

	recordset, aErr := s.client.Query(util.GetAerospikeQueryPolicy(s.settings), stmt)
	if aErr != nil {
		if conflictingIndexNotReadable(aErr) {
			return nil, false, nil
		}

		return nil, false, errors.NewStorageError("[PendingConflictIntents] WAL index query failed", aErr)
	}

	_, readable, err = readQueryResults(ctx, "[PendingConflictIntents] WAL index query failed", recordset.Results(), func() { _ = recordset.Close() }, func(record *aerospike.Record) bool {
		records = append(records, record)
		return false
	})
	if err != nil || !readable {
		return nil, readable, err
	}

	return records, true, nil
}

// scanConflictWAL reads every WAL root with a scan of the set.
func (s *Store) scanConflictWAL(ctx context.Context) ([]*aerospike.Record, error) {
	if ctxErr := ctx.Err(); ctxErr != nil {
		return nil, ctxErr
	}

	stmt := aerospike.NewStatement(s.namespace, s.conflictWALSet(), conflictWALBins...)

	policy := aerospike.NewQueryPolicy()
	policy.MaxRetries = 2
	policy.SocketTimeout = 0
	policy.TotalTimeout = conflictWALScanTimeout

	recordset, aErr := s.client.Query(policy, stmt)
	if aErr != nil {
		return nil, errors.NewStorageError("[PendingConflictIntents] failed to scan WAL set", aErr)
	}

	var records []*aerospike.Record

	_, readable, err := readQueryResults(ctx, "[PendingConflictIntents] error reading WAL record", recordset.Results(), func() { _ = recordset.Close() }, func(record *aerospike.Record) bool {
		records = append(records, record)
		return false
	})
	if err != nil {
		return nil, err
	}

	if !readable {
		// A scan names no index. A server that answers it with an index error is misbehaving.
		return nil, errors.NewStorageError("[PendingConflictIntents] WAL set scan returned an index error")
	}

	return records, nil
}

// decodeConflictWALRecord turns a WAL root into its intent. completed reports a root whose
// completion was interrupted after it was marked done.
func decodeConflictWALRecord(record *aerospike.Record) (intent utxo.ConflictIntent, completed bool, err error) {
	kind, ok := record.Bins[walBinKind].(string)
	if !ok {
		return intent, false, errors.NewStorageError(errWALRecordMissingBin, walBinKind)
	}

	blockHeight, ok := record.Bins[walBinBlockHeight].(int)
	if !ok {
		return intent, false, errors.NewStorageError(errWALRecordMissingBin, walBinBlockHeight)
	}

	blockHashBytes, ok := record.Bins[walBinBlockHash].([]byte)
	if !ok {
		return intent, false, errors.NewStorageError(errWALRecordMissingBin, walBinBlockHash)
	}

	txHashesBytes, ok := record.Bins[walBinTxHashes].([]byte)
	if !ok {
		return intent, false, errors.NewStorageError(errWALRecordMissingBin, walBinTxHashes)
	}

	startedAt, ok := record.Bins[walBinStartedAt].(int)
	if !ok {
		return intent, false, errors.NewStorageError(errWALRecordMissingBin, walBinStartedAt)
	}

	bh, err := chainhash.NewHash(blockHashBytes)
	if err != nil {
		return intent, false, errors.NewStorageError("[PendingConflictIntents] corrupt blockHash (kind=%s height=%d startedAt=%d)", kind, blockHeight, startedAt, err)
	}

	hashes, err := decodeIntentHashes(txHashesBytes)
	if err != nil {
		return intent, false, errors.NewStorageError("[PendingConflictIntents] corrupt intent record (kind=%s height=%d startedAt=%d)", kind, blockHeight, startedAt, err)
	}

	intent = utxo.ConflictIntent{
		Kind:        utxo.ConflictIntentKind(kind),
		BlockHeight: uint32(blockHeight),
		BlockHash:   *bh,
		TxHashes:    hashes,
		StartedAt:   int64(startedAt),
	}

	// The query returns no user keys. A record that this build keyed by its fields carries the
	// key formula. One without it was written by an older build, under that build's formula.
	if _, derived := record.Bins[walBinKeyFormula]; !derived {
		intent.ID = utxo.LegacyConflictIntentID(intent.Kind, intent.BlockHash, intent.BlockHeight, intent.TxHashes)
	}

	return intent, record.Bins["recoveryDone"] == true, nil
}
