package aerospike

import (
	"context"
	"math"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	as "github.com/bsv-blockchain/aerospike-client-go/v8"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/util"
)

// errAerospikeClientNotInit is the message used when an entry point is
// reached before the Aerospike client has been initialised. Surfaces in
// repair tooling and consistency scans; tests assert on the exact text.
const errAerospikeClientNotInit = "aerospike client not initialized"

// unminedTxIterator implements utxo.UnminedTxIterator for Aerospike
// It scans all records in the set and yields those that are not mined (i.e., unmined/mempool)
// Uses multiple workers to read from Aerospike in parallel for improved throughput
type unminedTxIterator struct {
	store            *Store
	prunerMode       bool   // when true, uses pruner-specific bins and filter
	prunerCutoff     uint32 // cutoff block height for pruner mode
	conflictingMode  bool   // when true, emits records where conflicting=true regardless of unminedSince
	err              error
	done             bool
	recordset        *as.Recordset
	resultChan       chan []*utxo.UnminedTransaction
	errorChan        chan error
	cancelWorkers    context.CancelFunc
	wg               sync.WaitGroup
	queryIdleTimeout time.Duration // idle timeout for detecting stalled Aerospike connections
}

// newUnminedTxIterator creates a new iterator for scanning unmined transactions in Aerospike.
// The iterator uses a scan operation to traverse all records in the set and filters
// for transactions that don't have block IDs (indicating they are unmined/mempool transactions).
// Multiple workers are spawned to process records in parallel for improved throughput.
//
// Parameters:
//   - store: The Aerospike store instance to iterate over
//
// Returns:
//   - *unminedTxIterator: A new iterator instance ready for use
//   - error: Any error encountered during iterator initialization
func newUnminedTxIterator(store *Store) (*unminedTxIterator, error) {
	numPartitionQueries, err := calculatePartitionQueries(store)
	if err != nil {
		return nil, err
	}

	store.logger.Infof("[newUnminedTxIterator] Using %d parallel Aerospike partition queries for unmined transactions", numPartitionQueries)

	return launchPartitionIterator(store, numPartitionQueries, false, 0)
}

// calculatePartitionQueries determines the optimal number of parallel partition queries
// based on CPU cores and Aerospike's query-threads-limit configuration.
func calculatePartitionQueries(store *Store) (int, error) {
	queryThreadsLimitStr, err := getConfigValue(store, "query-threads-limit")
	if err != nil {
		return 0, err
	}

	queryThreadsLimit, err := strconv.ParseInt(queryThreadsLimitStr, 10, 64)
	if err != nil {
		return 0, errors.NewProcessingError("failed to parse query-threads-limit: %v", err)
	}

	// Check that queryThreadsLimit fits in int before conversion
	if queryThreadsLimit > int64(math.MaxInt) || queryThreadsLimit < int64(math.MinInt) {
		return 0, errors.NewProcessingError("query-threads-limit value %d out of range for int type", queryThreadsLimit)
	}

	numPartitionQueries := runtime.NumCPU()

	// Ensure we don't exceed query-threads-limit, assuming each partition query uses up to 4 threads
	// nolint:gosec // bounds checked above
	queryLimits := int(queryThreadsLimit) / 4
	if queryThreadsLimit > 0 && numPartitionQueries > queryLimits {
		numPartitionQueries = queryLimits
	}

	return numPartitionQueries, nil
}

func getConfigValue(store *Store, configParam string) (string, error) {
	// determine number of partition queries based on CPU cores and query-threads-limit
	// Get the first node in the cluster
	nodes := store.client.GetNodes()
	if len(nodes) == 0 {
		return "", errors.NewProcessingError("no Aerospike nodes available")
	}
	node := nodes[0]

	// Request the service context configuration
	info, err := node.RequestInfo(as.NewInfoPolicy(), "get-config:context=service")
	if err != nil {
		return "", errors.NewProcessingError("failed to get info")
	}

	// The response is a map; the value for our key is a semicolon-separated string
	configStr, ok := info["get-config:context=service"]
	if !ok {
		return "", errors.NewProcessingError("service config not found in response")
	}

	// Parse the config string to find query-threads-limit
	configPairs := strings.Split(configStr, ";")
	for _, pair := range configPairs {
		kv := strings.SplitN(pair, "=", 2)
		if len(kv) == 2 && kv[0] == configParam {
			return kv[1], nil
		}
	}

	return "", errors.NewProcessingError("config parameter %s not found", configParam)
}

// launchPartitionIterator creates and starts a partition-parallel iterator.
// Both the block assembly and pruner iterators use this shared setup.
func launchPartitionIterator(store *Store, numPartitionQueries int, prunerMode bool, prunerCutoff uint32) (*unminedTxIterator, error) {
	return launchPartitionIteratorWithMode(store, numPartitionQueries, prunerMode, prunerCutoff, false)
}

// launchPartitionIteratorWithMode is the generalised form that supports the
// conflicting-scan mode used by repair tooling.
func launchPartitionIteratorWithMode(store *Store, numPartitionQueries int, prunerMode bool, prunerCutoff uint32, conflictingMode bool) (*unminedTxIterator, error) {
	const totalPartitions = 4096 // Aerospike has 4096 partitions

	if numPartitionQueries < 1 {
		numPartitionQueries = 1
	}
	if numPartitionQueries > totalPartitions {
		numPartitionQueries = totalPartitions
	}

	partitionsPerQuery := totalPartitions / numPartitionQueries
	remainingPartitions := totalPartitions % numPartitionQueries

	policy := partitionScanPolicy(util.GetAerospikeQueryPolicy(store.settings))

	workerCtx, cancel := context.WithCancel(context.Background())
	resultChanSize := numPartitionQueries * 2

	queryIdleTimeout := time.Duration(store.settings.UtxoStore.QueryIdleTimeoutSeconds) * time.Second

	// With no TotalTimeout on the scan, these are the only ways a dead
	// connection is detected.
	if queryIdleTimeout <= 0 && policy.SocketTimeout <= 0 {
		store.logger.Warnf("[launchPartitionIterator] utxostore_queryIdleTimeoutSeconds and the query policy SocketTimeout are both 0: a stalled Aerospike connection will hang this scan indefinitely")
	}

	it := &unminedTxIterator{
		store:            store,
		prunerMode:       prunerMode,
		prunerCutoff:     prunerCutoff,
		conflictingMode:  conflictingMode,
		resultChan:       make(chan []*utxo.UnminedTransaction, resultChanSize),
		errorChan:        make(chan error, numPartitionQueries),
		cancelWorkers:    cancel,
		queryIdleTimeout: queryIdleTimeout,
	}

	partitionStart := 0
	for i := 0; i < numPartitionQueries; i++ {
		partitionCount := partitionsPerQuery
		if i < remainingPartitions {
			partitionCount++
		}

		it.wg.Add(1)
		go it.partitionWorker(workerCtx, policy, partitionStart, partitionCount)

		partitionStart += partitionCount
	}

	go func() {
		it.wg.Wait()
		close(it.resultChan)
		close(it.errorChan)
	}()

	return it, nil
}

// partitionScanPolicy derives the policy for the partition-parallel scans from
// the configured query policy. TotalTimeout is dropped: a full scan of a large
// unmined set runs for hours, and a total cap aborts it at the same point on
// every attempt. Dead connections are still caught by SocketTimeout and by the
// iterator's idle watchdog.
func partitionScanPolicy(base *as.QueryPolicy) *as.QueryPolicy {
	policy := *base
	policy.TotalTimeout = 0
	policy.IncludeBinData = true
	policy.RecordQueueSize = 512

	return &policy
}

// partitionWorker processes a range of Aerospike partitions and writes batches directly to resultChan
func (it *unminedTxIterator) partitionWorker(ctx context.Context, policy *as.QueryPolicy, partitionStart, partitionCount int) {
	defer it.wg.Done()

	stmt := as.NewStatement(it.store.namespace, it.store.setName)

	switch {
	case it.prunerMode:
		// Pruner mode: tight server-side filter for only old unmined transactions
		if err := stmt.SetFilter(as.NewRangeFilter(fields.UnminedSince.String(), 1, int64(it.prunerCutoff))); err != nil {
			select {
			case it.errorChan <- err:
			default:
			}
			return
		}

		// Minimal bins: only what the pruner needs for parent preservation
		stmt.BinNames = []string{
			fields.TxID.String(),
			fields.UnminedSince.String(),
			fields.External.String(),
			fields.Inputs.String(),
		}
	case it.conflictingMode:
		// Conflicting mode: full scan, no server-side index filter. Client-side
		// filter in processRecordset keeps only conflicting=true records.
		stmt.BinNames = []string{
			fields.TxID.String(),
			fields.Fee.String(),
			fields.SizeInBytes.String(),
			fields.CreatedAt.String(),
			fields.Conflicting.String(),
			fields.Locked.String(),
			fields.BlockIDs.String(),
			fields.UnminedSince.String(),
			fields.IsCoinbase.String(),
			fields.External.String(),
			fields.Inputs.String(),
		}
	default:
		// Non-pruner mode: always apply unmined filter (index-based query)
		if err := stmt.SetFilter(as.NewRangeFilter(fields.UnminedSince.String(), 1, int64(math.MaxUint32))); err != nil {
			select {
			case it.errorChan <- err:
			default:
			}
			return
		}

		// Full bin set for block assembly
		stmt.BinNames = []string{
			fields.TxID.String(),
			fields.Fee.String(),
			fields.SizeInBytes.String(),
			fields.CreatedAt.String(),
			fields.Conflicting.String(),
			fields.Locked.String(),
			fields.BlockIDs.String(),
			fields.UnminedSince.String(),
			fields.IsCoinbase.String(),
		}

		if it.store.settings.BlockAssembly.StoreTxInpointsForSubtreeMeta {
			stmt.BinNames = append(stmt.BinNames, fields.External.String())
			stmt.BinNames = append(stmt.BinNames, fields.Inputs.String())
		}
	}

	// Create partition filter for this range
	partitionFilter := as.NewPartitionFilterByRange(partitionStart, partitionCount)

	if !it.prunerMode && !it.conflictingMode {
		it.queryRaw(ctx, policy, stmt, partitionFilter, partitionStart, partitionCount)
		return
	}

	recordset, err := it.store.client.QueryPartitions(policy, stmt, partitionFilter)
	if err != nil {
		it.store.logger.Errorf("[partitionWorker] Aerospike partition query failed (partitions %d-%d): %v", partitionStart, partitionStart+partitionCount-1, err)
		select {
		case it.errorChan <- err:
		default:
		}
		return
	}
	defer recordset.Close()

	// Process records directly into batches
	it.processRecordset(ctx, recordset.Results())
}

// queryRaw runs the block assembly scan of one partition range with
// QueryPartitionsRawContext: records are decoded and batched inline in each
// node command's goroutine, with no per-record channel handoff or BinMap.
//
// SocketTimeout is left as configured: the server also uses it as its send
// timeout, and a handler blocked on a slow consumer isn't reading its socket.
// Dead connections are caught by the watchdog instead, which counts only time
// spent waiting on Aerospike and reports the stall to the iterator as soon as
// it fires, even if the query stays blocked in a read until SocketTimeout.
func (it *unminedTxIterator) queryRaw(ctx context.Context, policy *as.QueryPolicy, stmt *as.Statement, partitionFilter *as.PartitionFilter, partitionStart, partitionCount int) {
	queryCtx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)

	handlers := &rawHandlerSet{it: it, ctx: queryCtx}

	stopWatch := func() {}
	if it.queryIdleTimeout > 0 {
		stopWatch = handlers.startWatch(queryCtx, cancel, it.queryIdleTimeout)
	}

	qErr := it.store.client.QueryPartitionsRawContext(queryCtx, policy, stmt, partitionFilter, handlers.newHandler)

	// Stop the watchdog before the final flush, which may wait on the consumer.
	stopWatch()

	if handlers.stallReported.Load() || ctx.Err() != nil {
		return
	}

	var err error
	if qErr != nil {
		err = qErr
	} else {
		err = handlers.flushAll()
	}

	if err == nil || ctx.Err() != nil {
		return
	}

	it.store.logger.Errorf("[partitionWorker] Aerospike partition query failed (partitions %d-%d): %v", partitionStart, partitionStart+partitionCount-1, err)

	select {
	case it.errorChan <- err:
	default:
	}
}

// processRecordset processes records from a recordset and writes batches to resultChan
func (it *unminedTxIterator) processRecordset(ctx context.Context, results <-chan *as.Result) {
	const (
		batchSize = 16 * 1024
		// The fast path does not watch ctx, so this bounds how many records a
		// worker decodes after cancellation.
		contextCheckPeriod = 1024
	)

	// Pre-compute field names to avoid repeated String() calls
	conflictingField := fields.Conflicting.String()
	coinbaseField := fields.IsCoinbase.String()

	// Local buffer to batch sends and reduce channel contention
	localBuffer := make([]*utxo.UnminedTransaction, 0, batchSize)
	itemsProcessed := 0

	// Flush function sends the entire batch as a single slice
	flush := func() error {
		if len(localBuffer) == 0 {
			return nil
		}

		// Create a copy of the slice to send
		batchToSend := make([]*utxo.UnminedTransaction, len(localBuffer))
		copy(batchToSend, localBuffer)

		select {
		case <-ctx.Done():
			return ctx.Err()
		case it.resultChan <- batchToSend:
		}

		localBuffer = localBuffer[:0]
		return nil
	}

	defer func() {
		_ = flush() // Best effort flush on exit
	}()

	// Create a reusable timer for idle timeout detection to avoid allocating a new
	// timer on every iteration (time.After would create GC pressure during high-throughput scans).
	var idleTimer *time.Timer
	if it.queryIdleTimeout > 0 {
		idleTimer = time.NewTimer(it.queryIdleTimeout)
		defer idleTimer.Stop()
	}

	for {
		// Only check context every contextCheckPeriod iterations for performance
		if itemsProcessed%contextCheckPeriod == 0 {
			select {
			case <-ctx.Done():
				return
			default:
			}
		}

		// Fast path: take an already-buffered record with a single-channel
		// non-blocking receive. A multi-case select locks every channel it names,
		// including the ctx.Done channel shared by all partition workers, and the
		// idle timer had to be reset and stopped per record; together that was the
		// scan's largest CPU cost. The waiting path runs only when the client has
		// nothing buffered for this worker.
		var rec *as.Result
		var ok bool
		select {
		case rec, ok = <-results:
		default:
			var abort bool
			if rec, ok, abort = it.waitForRecord(ctx, results, idleTimer); abort {
				return
			}
		}

		if !ok || rec == nil {
			return
		}

		if rec.Err != nil {
			select {
			case it.errorChan <- rec.Err:
			default:
			}
			return
		}

		itemsProcessed++

		// bufferTx appends to localBuffer and flushes when full
		bufferTx := func(tx *utxo.UnminedTransaction) bool {
			localBuffer = append(localBuffer, tx)
			if len(localBuffer) >= batchSize {
				if err := flush(); err != nil {
					return false
				}
			}
			return true
		}

		switch {
		case it.prunerMode:
			// Pruner mode: skip checks for bins we didn't fetch (createdAt, conflicting, coinbase)
			// The server-side filter already ensures unminedSince is in [1, cutoff]
			unminedTx, err := it.processPrunerRecord(ctx, rec.Record.Bins)
			if err != nil {
				select {
				case it.errorChan <- err:
				default:
				}
				return
			}

			if unminedTx != nil {
				if !bufferTx(unminedTx) {
					return
				}
			}
			continue
		case it.conflictingMode:
			// Conflicting mode: only emit records where conflicting=true.
			// Skip paginated split records (no createdAt on split records).
			if rec.Record.Bins[fields.CreatedAt.String()] == nil {
				continue
			}

			conflictingVal := rec.Record.Bins[conflictingField]
			if conflictingVal == nil {
				continue
			}
			conflictingBool, ok := conflictingVal.(bool)
			if !ok || !conflictingBool {
				continue
			}

			// Skip coinbase — coinbases are never conflicting in practice, but defend.
			if coinbaseBool, ok := rec.Record.Bins[coinbaseField].(bool); ok && coinbaseBool {
				continue
			}

			unminedTx, err := it.processRecord(ctx, rec.Record.Bins)
			if err != nil {
				select {
				case it.errorChan <- err:
				default:
				}
				return
			}
			if unminedTx != nil {
				if !bufferTx(unminedTx) {
					return
				}
			}
			continue
		}

		{
			// Default (unmined) mode.
			// check whether this is a main record (split records are loaded when full is set)
			// createAt is not set for split records
			if rec.Record.Bins[fields.CreatedAt.String()] == nil {
				continue
			}

			// Quick inline filtering checks
			if conflictingVal := rec.Record.Bins[conflictingField]; conflictingVal != nil {
				if conflicting, ok := conflictingVal.(bool); ok && conflicting {
					continue
				}
			}

			if coinbaseBool, ok := rec.Record.Bins[coinbaseField].(bool); ok && coinbaseBool {
				if !bufferTx(&utxo.UnminedTransaction{Skip: true}) {
					return
				}
				continue
			}

			// Process the record
			unminedTx, err := it.processRecord(ctx, rec.Record.Bins)
			if err != nil {
				select {
				case it.errorChan <- err:
				default:
				}
				return
			}

			if unminedTx != nil {
				if !bufferTx(unminedTx) {
					return
				}
			}
		}
	}
}

// waitForRecord blocks until the next record arrives, the context is cancelled,
// or the idle timeout fires. A hard timeout would be wrong since large scans can
// take hours, but if no record arrives within the idle timeout the connection is
// likely dead (e.g. Aerospike node restart). abort is true when the worker must
// stop; a stall is also reported on errorChan.
func (it *unminedTxIterator) waitForRecord(ctx context.Context, results <-chan *as.Result, idleTimer *time.Timer) (rec *as.Result, ok bool, abort bool) {
	if idleTimer == nil {
		select {
		case rec, ok = <-results:
			return rec, ok, false
		case <-ctx.Done():
			return nil, false, true
		}
	}

	// Since Go 1.23 Reset discards any expiry that fired while the worker was on
	// the fast path, so the timer needs no Stop/drain between waits.
	idleTimer.Reset(it.queryIdleTimeout)

	select {
	case rec, ok = <-results:
		return rec, ok, false
	case <-ctx.Done():
		return nil, false, true
	case <-idleTimer.C:
		it.store.logger.Errorf("[processRecordset] no records received from Aerospike partition query in %v — connection may be stalled, aborting worker", it.queryIdleTimeout)
		select {
		case it.errorChan <- errors.NewProcessingError("Aerospike partition query stalled: no records received in %v", it.queryIdleTimeout):
		default:
		}

		return nil, false, true
	}
}

// processRecord processes a single Aerospike record and returns the unmined transaction
func (it *unminedTxIterator) processRecord(ctx context.Context, bins map[string]interface{}) (*utxo.UnminedTransaction, error) {
	// Extract transaction data from the record
	txData, err := it.extractTransactionData(bins)
	if err != nil {
		return nil, errors.NewProcessingError("invalid transaction data", err)
	}

	blockIDs, err := processBlockIDs(bins)
	if err != nil {
		return nil, errors.NewProcessingError("invalid block IDs for %s", txData.hash.String(), err)
	}

	// Inpoints get their own allocation: the subtree processor keeps the
	// pointer in its tx map for as long as the tx is held, and an interior
	// pointer into a combined allocation would retain the whole record.
	txInpoints := &subtree.TxInpoints{}

	// Process external transaction if needed; otherwise the inpoints stay empty
	if it.store.settings.BlockAssembly.StoreTxInpointsForSubtreeMeta {
		*txInpoints, err = it.processTransactionInpoints(ctx, &txData, bins)
		if err != nil {
			return nil, errors.NewProcessingError("failed to process transaction inpoints for %s", txData.hash.String(), err)
		}
	}

	// Extract createdAt timestamp
	createdAt, err := it.extractCreatedAt(bins)
	if err != nil {
		return nil, errors.NewProcessingError("invalid createdAt for %s", txData.hash.String(), err)
	}

	locked, err := it.extractLocked(bins)
	if err != nil {
		return nil, errors.NewProcessingError("invalid locked status for %s", txData.hash.String(), err)
	}

	// The transaction and its node share one allocation; the node is copied
	// into the subtree, so neither outlives the load.
	rec := &unminedRecord{}
	rec.node = subtree.Node{
		Hash:        txData.hash,
		Fee:         txData.fee,
		SizeInBytes: txData.size,
	}
	rec.tx = utxo.UnminedTransaction{
		Node:         &rec.node,
		UnminedSince: txData.unminedSince,
		TxInpoints:   txInpoints,
		CreatedAt:    createdAt,
		Locked:       locked,
		BlockIDs:     blockIDs,
	}

	return &rec.tx, nil
}

// unminedRecord backs one yielded UnminedTransaction together with the Node it
// points at, so the pair costs a single heap allocation.
type unminedRecord struct {
	tx   utxo.UnminedTransaction
	node subtree.Node
}

// processPrunerRecord processes a single Aerospike record in pruner mode.
// Only extracts the minimal data needed for parent preservation: txID, unminedSince, and TxInpoints.
func (it *unminedTxIterator) processPrunerRecord(ctx context.Context, bins map[string]interface{}) (*utxo.UnminedTransaction, error) {
	hash, unminedSince, err := extractTxIDAndUnminedSince(bins)
	if err != nil {
		return nil, err
	}

	txInpoints, err := it.processTransactionInpoints(ctx, &transactionData{hash: hash, unminedSince: unminedSince}, bins)
	if err != nil {
		// In pruner mode, skip records with inpoint errors rather than failing
		return nil, nil
	}

	return &utxo.UnminedTransaction{
		Node: &subtree.Node{
			Hash: hash,
		},
		UnminedSince: unminedSince,
		TxInpoints:   &txInpoints,
	}, nil
}

// Next advances the iterator and returns a batch of unmined transactions.
// This method reads multiple transactions from the result channel populated by worker goroutines
// for improved performance by amortizing overhead across multiple transactions.
//
// Parameters:
//   - ctx: Context for cancellation
//
// Returns:
//   - []*utxo.UnminedTransaction: A batch of unmined transactions, or empty slice if iteration is complete
//   - error: Any error encountered during iteration
func (it *unminedTxIterator) Next(ctx context.Context) ([]*utxo.UnminedTransaction, error) {
	if it.done || it.err != nil {
		return nil, it.err
	}

	// If resultChan is nil, no workers were started (e.g., nil recordset)
	if it.resultChan == nil {
		it.closeWithLogging()
		return nil, nil
	}

	// Check for errors from workers
	select {
	case <-ctx.Done():
		it.err = ctx.Err()
		it.closeWithLogging()
		return nil, it.err
	case err := <-it.errorChan:
		if err != nil {
			it.err = err
			it.closeWithLogging()
			return nil, err
		}
	default:
	}

	// Watch errorChan too: a worker can fail while others are still running
	// (or blocked in a socket read), and that must surface now rather than
	// once every worker is done. Once errorChan is closed it is no longer
	// selected (nil channel), so its closed-channel zero values can't spin.
	errCh := it.errorChan

	for {
		select {
		case <-ctx.Done():
			it.err = ctx.Err()
			it.closeWithLogging()
			return nil, it.err
		case err, ok := <-errCh:
			if !ok {
				errCh = nil
				continue
			}

			if err != nil {
				it.err = err
				it.closeWithLogging()
				return nil, err
			}
		case batch, ok := <-it.resultChan:
			if !ok {
				// resultChan closed — all workers finished. Check if any worker reported an error
				// that arrived after our earlier non-blocking check (e.g. idle timeout error).
				select {
				case err := <-it.errorChan:
					if err != nil {
						it.err = err
						it.closeWithLogging()
						return nil, err
					}
				default:
				}
				it.closeWithLogging()
				return nil, nil
			}
			return batch, nil
		}
	}
}

// transactionData holds the basic transaction data extracted from a record
type transactionData struct {
	hash         chainhash.Hash
	fee          uint64
	size         uint64
	unminedSince int
}

// closeWithLogging closes the iterator with error logging
func (it *unminedTxIterator) closeWithLogging() {
	if err := it.Close(); err != nil {
		it.store.logger.Warnf("failed to close iterator: %v", err)
	}
}

// extractTxIDAndUnminedSince extracts the txID hash and unminedSince from record bins.
// Shared by both the full record processor and the pruner record processor.
func extractTxIDAndUnminedSince(bins map[string]interface{}) (chainhash.Hash, int, error) {
	var hash chainhash.Hash

	txidVal := bins[fields.TxID.String()]
	if txidVal == nil {
		return hash, 0, errors.NewProcessingError("txid not found")
	}

	txidValBytes, ok := txidVal.([]byte)
	if !ok {
		return hash, 0, errors.NewProcessingError("txid not []byte")
	}

	if err := hash.SetBytes(txidValBytes); err != nil {
		return hash, 0, err
	}

	unminedSince, _ := bins[fields.UnminedSince.String()].(int)

	return hash, unminedSince, nil
}

// extractTransactionData extracts basic transaction data from Aerospike record bins
func (it *unminedTxIterator) extractTransactionData(bins map[string]interface{}) (transactionData, error) {
	hash, unminedSince, err := extractTxIDAndUnminedSince(bins)
	if err != nil {
		return transactionData{}, err
	}

	feeVal := bins[fields.Fee.String()]
	if feeVal == nil {
		return transactionData{}, errors.NewProcessingError("fee not found")
	}

	fee, err := toUint64(feeVal)
	if err != nil {
		return transactionData{}, errors.NewProcessingError("Failed to convert fee")
	}

	sizeVal := bins[fields.SizeInBytes.String()]
	if sizeVal == nil {
		return transactionData{}, errors.NewProcessingError("size not found")
	}

	size, _ := toUint64(sizeVal)

	return transactionData{
		hash:         hash,
		fee:          fee,
		size:         size,
		unminedSince: unminedSince,
	}, nil
}

// processTransactionInpoints processes transaction inputs based on whether it's external or internal
func (it *unminedTxIterator) processTransactionInpoints(ctx context.Context, txData *transactionData, bins map[string]interface{}) (subtree.TxInpoints, error) {
	external, ok := bins[fields.External.String()].(bool)
	if !ok || !external {
		return it.processInternalTransactionInpoints(bins)
	}

	return it.processExternalTransactionInpoints(ctx, &txData.hash)
}

// processExternalTransactionInpoints processes inputs for external transactions
// using optimized parsing that skips all scripts (90%+ memory savings)
func (it *unminedTxIterator) processExternalTransactionInpoints(ctx context.Context, hash *chainhash.Hash) (subtree.TxInpoints, error) {
	// Use optimized function that only parses input references, skipping all scripts
	return it.store.GetTxInpointsFromExternalStore(ctx, *hash)
}

// processInternalTransactionInpoints processes inputs for internal transactions
func (it *unminedTxIterator) processInternalTransactionInpoints(bins map[string]interface{}) (subtree.TxInpoints, error) {
	txInpoints, err := processInputsToTxInpoints(bins)
	if err != nil {
		return subtree.TxInpoints{}, errors.NewTxInvalidError("could not process input interfaces", err)
	}

	return txInpoints, nil
}

// extractCreatedAt extracts the created_at timestamp from record bins
func (it *unminedTxIterator) extractCreatedAt(bins map[string]interface{}) (int, error) {
	createdAtVal, ok := bins[fields.CreatedAt.String()]
	if !ok || createdAtVal == nil {
		return 0, errors.NewProcessingError("%s not found", fields.CreatedAt.String())
	}

	createdAt, ok := createdAtVal.(int)
	if !ok {
		return 0, errors.NewProcessingError("%s not int64", fields.CreatedAt.String())
	}

	return createdAt, nil
}

// extractLocked extracts the locked status from record bins
func (it *unminedTxIterator) extractLocked(bins map[string]interface{}) (bool, error) {
	// Locked status is optional, so we check if it exists
	lockedVal, ok := bins[fields.Locked.String()].(bool)
	if ok {
		return lockedVal, nil
	}

	return false, nil
}

// Err returns the first error encountered during iteration, if any.
// This should be called after Next returns nil to check if iteration
// completed successfully or due to an error.
//
// Returns:
//   - error: The error that caused iteration to stop, or nil if no error occurred
func (it *unminedTxIterator) Err() error {
	return it.err
}

// Close releases resources held by the iterator and marks it as done.
// It cancels all worker goroutines and closes the recordset.
// It's safe to call Close multiple times. After calling Close, subsequent
// calls to Next will return nil.
//
// Returns:
//   - error: Always returns nil (kept for interface compatibility)
func (it *unminedTxIterator) Close() error {
	if it.done {
		return nil
	}

	it.done = true

	// Cancel workers
	if it.cancelWorkers != nil {
		it.cancelWorkers()
	}

	// Close recordset
	if it.recordset != nil {
		return it.recordset.Close()
	}

	return nil
}

// toUint64 converts various numeric interface{} types to uint64.
// This utility function handles type assertions for Aerospike record values
// which can come in different numeric types depending on the data source.
//
// Parameters:
//   - val: The interface{} value to convert (should be a numeric type)
//
// Returns:
//   - uint64: The converted value
//   - error: Error if the value cannot be converted to uint64
//
// nolint: gosec
func toUint64(val interface{}) (uint64, error) {
	switch v := val.(type) {
	case int:
		return uint64(v), nil
	case int64:
		return uint64(v), nil
	case uint64:
		return v, nil
	case float64:
		return uint64(v), nil
	case uint32:
		return uint64(v), nil
	case float32:
		return uint64(v), nil
	case nil:
		return 0, nil
	default:
		return 0, errors.NewProcessingError("unknown type for uint64 conversion")
	}
}

// GetUnminedTxIterator implements utxo.Store for Aerospike.
// Uses the unmined_since index to efficiently query only unmined transactions.
func (s *Store) GetUnminedTxIterator() (utxo.UnminedTxIterator, error) {
	if s.client == nil {
		return nil, errors.NewProcessingError(errAerospikeClientNotInit)
	}

	return newUnminedTxIterator(s)
}

// GetPrunableUnminedTxIterator returns a lightweight iterator optimized for pruner use.
// It filters server-side for unminedSince in [1, cutoffBlockHeight] and fetches only
// the bins needed by the pruner (txID, unminedSince, external, inputs).
func (s *Store) GetPrunableUnminedTxIterator(cutoffBlockHeight uint32) (utxo.UnminedTxIterator, error) {
	if s.client == nil {
		return nil, errors.NewProcessingError(errAerospikeClientNotInit)
	}

	return newPrunableUnminedTxIterator(s, cutoffBlockHeight)
}

// newPrunableUnminedTxIterator creates a pruner-specific iterator that:
// - Filters server-side: unminedSince in [1, cutoffBlockHeight] (only old unmined txs)
// - Fetches minimal bins: txID, unminedSince, external, inputs (4 bins vs 9-11)
func newPrunableUnminedTxIterator(store *Store, cutoffBlockHeight uint32) (*unminedTxIterator, error) {
	numPartitionQueries, err := calculatePartitionQueries(store)
	if err != nil {
		return nil, err
	}

	store.logger.Infof("[newPrunableUnminedTxIterator] Using %d parallel partition queries (cutoff=%d)", numPartitionQueries, cutoffBlockHeight)

	return launchPartitionIterator(store, numPartitionQueries, true, cutoffBlockHeight)
}

// GetConflictingTxIterator returns an iterator that scans all records and
// emits those with conflicting=true. Used by repair tooling to purge the
// losing side of double-spends during a blockchain rewind.
func (s *Store) GetConflictingTxIterator() (utxo.UnminedTxIterator, error) {
	if s.client == nil {
		return nil, errors.NewProcessingError(errAerospikeClientNotInit)
	}

	numPartitionQueries, err := calculatePartitionQueries(s)
	if err != nil {
		return nil, err
	}

	s.logger.Infof("[GetConflictingTxIterator] Using %d parallel partition queries", numPartitionQueries)

	return launchPartitionIteratorWithMode(s, numPartitionQueries, false, 0, true)
}
