package aerospike

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"slices"
	"strings"
	"time"

	"github.com/bsv-blockchain/aerospike-client-go/v8"
	"github.com/bsv-blockchain/aerospike-client-go/v8/types"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/util"
)

// conflictingIndexName is the secondary index over records whose conflicting
// bin is true. Block validation reads it once per block to learn which
// transactions of its already-validated subtrees are conflicting, instead of
// reading every transaction's record.
const conflictingIndexName = "conflictingIndex"

// States of the conflicting index for this process.
const (
	// conflictingIndexUnchecked: the startup self-test has not finished.
	conflictingIndexUnchecked int32 = iota
	// conflictingIndexUsable: the self-test saw the index follow the flag.
	conflictingIndexUsable
	// conflictingIndexUnusable: the self-test saw the index miss a flag change,
	// so it is never trusted by this process.
	conflictingIndexUnusable
)

// conflictingIndexProbeMissLimit is how many probes the index has to fail,
// without passing one in between, before this process stops trusting it. A
// probe that errors counts neither way. A miss can be transient, for example
// while partitions migrate, and giving up is permanent.
const conflictingIndexProbeMissLimit = 3

// The startup self-test first checks whether the index is built after
// conflictingIndexSelfTestInterval, doubling the wait each time up to
// conflictingIndexSelfTestMaxDelay, which is also its wait after a failed probe.
const (
	conflictingIndexSelfTestInterval = time.Second
	conflictingIndexSelfTestMaxDelay = 30 * time.Second
)

// conflictingIndexExpression indexes 1 for a record whose conflicting bin is
// true. Every other record, including one without the bin, evaluates to unknown
// and is left out, so the index holds only the conflicting records. Being an
// expression index, the server maintains it on every write, whichever path set
// the bin: Lua, the native operate path, or a create. It needs Aerospike 8.1+.
func conflictingIndexExpression() *aerospike.Expression {
	return aerospike.ExpCond(
		aerospike.ExpBoolBin(fields.Conflicting.String()),
		aerospike.ExpIntVal(1),
		aerospike.ExpUnknown(),
	)
}

// createExpressionIndexIfNotExists is CreateIndexIfNotExists for an index over
// an expression rather than a bin. Like it, it starts the build and returns
// without waiting for it.
func (s *Store) createExpressionIndexIfNotExists(indexName string, exp *aerospike.Expression) error {
	if s.client.Client == nil {
		return nil // For unit tests, we don't have a client
	}

	s.indexMutex.Lock()
	defer s.indexMutex.Unlock()

	exists, err := s.indexExists(indexName)
	if err != nil || exists {
		return err
	}

	s.logger.Infof("Creating expression index %s:%s:%s", s.namespace, s.setName, indexName)

	if _, err := s.client.CreateIndexWithExpression(aerospike.NewWritePolicy(0, 0), s.namespace, s.setName, indexName, aerospike.NUMERIC, aerospike.ICT_DEFAULT, exp); err != nil {
		return err
	}

	return nil
}

// ConflictingTxHashes returns the hash of every transaction whose record is
// flagged conflicting, read from the conflicting index.
//
// indexed is false, with no error, until the startup self-test has seen the
// index follow a flag change, for the rest of the process once it has seen the
// index miss one, and while the index is not readable. The caller then has to
// find the conflicting transactions another way.
func (s *Store) ConflictingTxHashes(ctx context.Context) ([]chainhash.Hash, bool, error) {
	if s.client == nil || s.client.Client == nil || s.conflictingIndexState.Load() != conflictingIndexUsable {
		return nil, false, nil
	}

	hashes, readable, err := s.queryConflictingIndex(ctx)
	if err != nil || !readable {
		return nil, false, err
	}

	return hashes, true, nil
}

// queryConflictingIndex reads every master record in the conflicting index.
// readable is false when the server reports the index missing or not yet built.
func (s *Store) queryConflictingIndex(ctx context.Context) (hashes []chainhash.Hash, readable bool, err error) {
	_, readable, err = s.conflictingIndexRecords(ctx, []string{fields.TxID.String()}, func(record *aerospike.Record) bool {
		txIDBytes, ok := record.Bins[fields.TxID.String()].([]byte)
		if !ok || len(txIDBytes) != chainhash.HashSize {
			return false
		}

		hash := chainhash.Hash(txIDBytes)

		// A page of a paginated transaction carries the same txID: only the
		// record keyed by the transaction hash itself is the transaction.
		key, keyErr := aerospike.NewKey(s.namespace, s.setName, hash[:])
		if keyErr != nil || !bytes.Equal(key.Digest(), record.Key.Digest()) {
			return false
		}

		hashes = append(hashes, hash)

		return false
	})

	return hashes, readable, err
}

// conflictingIndexRecords runs the index query and hands each record to visit,
// with the named bins, or with its key alone when binNames is empty. It returns
// whether visit answered true for any record.
func (s *Store) conflictingIndexRecords(ctx context.Context, binNames []string, visit func(*aerospike.Record) bool) (found, readable bool, err error) {
	if ctxErr := ctx.Err(); ctxErr != nil {
		return false, false, ctxErr
	}

	stmt := aerospike.NewStatement(s.namespace, s.setName, binNames...)
	if err := stmt.SetFilter(aerospike.NewEqualWithIndexNameFilter(conflictingIndexName, int64(1))); err != nil {
		return false, false, errors.NewProcessingError("[ConflictingTxHashes] could not build the index filter", err)
	}

	policy := util.GetAerospikeQueryPolicy(s.settings)
	if len(binNames) == 0 {
		// A projection the record does not match would make the server leave the
		// record out, so a keys-only read asks for no bin data instead.
		keysOnly := *policy
		keysOnly.IncludeBinData = false
		policy = &keysOnly
	}

	recordset, aErr := s.client.Query(policy, stmt)
	if aErr != nil {
		if conflictingIndexNotReadable(aErr) {
			return false, false, nil
		}

		return false, false, errors.NewStorageError("[ConflictingTxHashes] index query failed", aErr)
	}

	defer func() {
		_ = recordset.Close()
	}()

	results := recordset.Results()

	for {
		var (
			res  *aerospike.Result
			more bool
		)

		// A stalled node would otherwise hold the caller until the query
		// policy's socket timeouts and retries run out.
		select {
		case <-ctx.Done():
			return false, false, ctx.Err()
		case res, more = <-results:
		}

		if !more {
			return found, true, nil
		}

		if res.Err != nil {
			if conflictingIndexNotReadable(res.Err) {
				return false, false, nil
			}

			return false, false, errors.NewStorageError("[ConflictingTxHashes] index query failed", res.Err)
		}

		if res.Record != nil && visit(res.Record) {
			found = true
		}
	}
}

// conflictingIndexBuiltOnAllNodes reports whether every node of the cluster
// lists the conflicting index with state RW, i.e. built and queryable.
func (s *Store) conflictingIndexBuiltOnAllNodes() (bool, error) {
	nodes := s.client.Client.Cluster().GetNodes()
	if len(nodes) == 0 {
		return false, nil
	}

	command := "sindex-list:ns=" + s.namespace
	entryPrefix := fmt.Sprintf("ns=%s:indexname=%s:set=%s:", s.namespace, conflictingIndexName, s.setName)

	for _, node := range nodes {
		info, err := node.RequestInfo(aerospike.NewInfoPolicy(), command)
		if err != nil {
			return false, errors.NewStorageError("[ConflictingTxHashes] could not read the index list from node %s", node.GetName(), err)
		}

		built := false

		for _, entry := range strings.Split(info[command], ";") {
			if strings.HasPrefix(entry, entryPrefix) && slices.Contains(strings.Split(entry, ":"), "state=RW") {
				built = true

				break
			}
		}

		if !built {
			return false, nil
		}
	}

	return true, nil
}

// conflictingIndexNotReadable reports the result codes a server gives for an
// index that does not exist or has not finished building.
func conflictingIndexNotReadable(err error) bool {
	return hasResultCode(err, types.INDEX_NOTFOUND) || hasResultCode(err, types.INDEX_NOTREADABLE)
}

// startConflictingIndexSelfTest runs the self-test in the background until it
// reaches a verdict or Close stops it. Until then ConflictingTxHashes reports
// not indexed, so block validation never waits on it.
func (s *Store) startConflictingIndexSelfTest(ctx context.Context) {
	if s.client == nil || s.client.Client == nil {
		return
	}

	ctx, s.conflictingIndexCancel = context.WithCancel(ctx)
	s.conflictingIndexDone = make(chan struct{})

	go func() {
		defer close(s.conflictingIndexDone)

		wait := conflictingIndexSelfTestInterval

		for {
			done, probeErr := s.verifyConflictingIndex(ctx)
			if done {
				return
			}

			if probeErr {
				wait = conflictingIndexSelfTestMaxDelay
			}

			select {
			case <-ctx.Done():
				return
			case <-time.After(wait):
			}

			wait = min(2*wait, conflictingIndexSelfTestMaxDelay)
		}
	}()
}

// stopConflictingIndexSelfTest stops the background self-test and waits for it.
func (s *Store) stopConflictingIndexSelfTest() {
	if s.conflictingIndexCancel == nil {
		return
	}

	s.conflictingIndexCancel()
	<-s.conflictingIndexDone
}

// verifyConflictingIndex runs the self-test once the index is built on every
// node and records the verdict. done reports whether a verdict was reached: an
// index still building, or a store error, leaves the state unchecked for the
// next attempt. probeErr reports that the probe itself failed.
func (s *Store) verifyConflictingIndex(ctx context.Context) (done, probeErr bool) {
	// A server without the index (8.0) answers a query naming it with a bare
	// PARAMETER_ERROR rather than INDEX_NOTFOUND, so the probe only runs once
	// every node lists the index built.
	ready, err := s.conflictingIndexBuiltOnAllNodes()
	if err != nil {
		s.logger.Debugf("[ConflictingTxHashes] could not check whether %s is built: %v", conflictingIndexName, err)

		return false, false
	}

	if !ready {
		return false, false
	}

	followsTheFlag, err := s.probeConflictingIndex(ctx)
	if err != nil {
		if ctx.Err() == nil {
			s.logger.Warnf("[ConflictingTxHashes] could not run the conflicting index self-test, retrying in %s: %v", conflictingIndexSelfTestMaxDelay, err)
		}

		return false, true
	}

	return s.recordConflictingIndexProbe(followsTheFlag), false
}

// recordConflictingIndexProbe records one probe result and reports whether it
// settles the verdict: a pass trusts the index, and conflictingIndexProbeMissLimit
// misses give it up for the rest of the process. Only the self-test goroutine
// calls it.
func (s *Store) recordConflictingIndexProbe(followsTheFlag bool) bool {
	if followsTheFlag {
		s.conflictingIndexState.Store(conflictingIndexUsable)
		s.logger.Infof("[ConflictingTxHashes] conflicting index verified, block validation reads it instead of every transaction of an already-validated subtree")

		return true
	}

	s.conflictingIndexProbeMisses++

	if s.conflictingIndexProbeMisses < conflictingIndexProbeMissLimit {
		s.logger.Warnf("[ConflictingTxHashes] the conflicting index did not follow a conflicting flag change (%d of %d), probing again", s.conflictingIndexProbeMisses, conflictingIndexProbeMissLimit)

		return false
	}

	s.conflictingIndexState.Store(conflictingIndexUnusable)
	s.logger.Errorf("[ConflictingTxHashes] the conflicting index did not follow a conflicting flag change on this server in %d probes; block validation reads every transaction of an already-validated subtree for the rest of this process", conflictingIndexProbeMissLimit)

	return true
}

// probeConflictingIndex flips the conflicting flag on a throwaway record through
// the same write SetConflicting issues (the native op or the Lua function,
// whichever this store uses) and checks that the index adds and then drops the
// record. It guards against a server build that maintains the flag on a path
// the index does not see.
//
// The record carries no txID bin, so a concurrent ConflictingTxHashes never
// reports it. It is written without a TTL, like every other record of the store:
// a namespace with expiration disabled (nsup-period=0) rejects a write that
// carries one, and setConflicting would reset it anyway. The deferred delete
// removes it; a process that dies between the write and the delete leaves the
// record behind, harmless but permanent. The flag is set with a block height retention of 0, so the record
// never gets a deleteAtHeight and the pruner never reads it.
func (s *Store) probeConflictingIndex(ctx context.Context) (bool, error) {
	key, keyErr := aerospike.NewKey(s.namespace, s.setName, fmt.Sprintf("_teranode-conflicting-index-probe-%d-%d", os.Getpid(), time.Now().UnixNano()))
	if keyErr != nil {
		return false, errors.NewProcessingError("could not create the probe key", keyErr)
	}

	// Deleted even when the write reports an error, which may have applied.
	defer func() {
		if _, delErr := s.client.Delete(nil, key); delErr != nil {
			s.logger.Warnf("[ConflictingTxHashes] could not delete the conflicting index probe record %s, delete it by hand: %v", key.Value(), delErr)
		}
	}()

	if putErr := s.client.PutBins(aerospike.NewWritePolicy(0, aerospike.TTLDontExpire), key, aerospike.NewBin("_cfIdxProbe", true)); putErr != nil {
		return false, errors.NewStorageError("could not write the probe record", putErr)
	}

	setFlag := func(value bool) error {
		record := s.teranodeBatchRecord(aerospike.NewBatchUDFPolicy(), LuaPackage, key, subOpSetConflicting, "setConflicting",
			value, int(s.GetBlockHeight()), 0)

		if opErr := s.client.BatchOperate(util.GetAerospikeBatchPolicy(s.settings), []aerospike.BatchRecordIfc{record}); opErr != nil {
			return errors.NewStorageError("could not set the probe flag", opErr)
		}

		resp, respErr := s.teranodeBatchRecordResponse("[conflictingIndex probe]", record)
		if respErr != nil {
			return respErr
		}

		if resp.Status != LuaStatusOK {
			return errors.NewStorageError("setConflicting on the probe record answered %s: %s", resp.Status, resp.Message)
		}

		return nil
	}

	probeInIndex := func() (bool, error) {
		found, readable, qErr := s.conflictingIndexRecords(ctx, nil, func(record *aerospike.Record) bool {
			return bytes.Equal(record.Key.Digest(), key.Digest())
		})
		if qErr == nil && !readable {
			qErr = errors.NewStorageError("the conflicting index stopped being readable during the self-test")
		}

		return found, qErr
	}

	if err := setFlag(true); err != nil {
		return false, err
	}

	listed, err := probeInIndex()
	if err != nil {
		return false, err
	}

	if !listed {
		return false, nil
	}

	if err := setFlag(false); err != nil {
		return false, err
	}

	listed, err = probeInIndex()
	if err != nil {
		return false, err
	}

	return !listed, nil
}
