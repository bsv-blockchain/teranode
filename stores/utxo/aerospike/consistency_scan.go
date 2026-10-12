package aerospike

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	as "github.com/bsv-blockchain/aerospike-client-go/v8"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/util"
)

// consistencyScanIterator is a lightweight iterator that scans all records
// but only fetches txid, block_ids, and unmined_since — the minimum data
// needed to detect and fix unmined_since inconsistencies.
// No TxInpoints, no file I/O, no heavy deserialization.
type consistencyScanIterator struct {
	store         *Store
	resultChan    chan []*utxo.InconsistentTxRecord
	errorChan     chan error
	cancelWorkers context.CancelFunc
	wg            sync.WaitGroup
	done          bool
	err           error
	totalScanned  atomic.Int64
}

// ScanInconsistentUnminedTxs returns a lightweight iterator that scans
// all records to find unmined_since inconsistencies.
func (s *Store) ScanInconsistentUnminedTxs() (utxo.ConsistencyScanIterator, error) {
	if s.client == nil {
		return nil, errors.NewProcessingError(errAerospikeClientNotInit)
	}

	numPartitionQueries, err := calculatePartitionQueries(s)
	if err != nil {
		return nil, err
	}

	s.logger.Infof("[ScanInconsistentUnminedTxs] using %d parallel partition queries for consistency scan", numPartitionQueries)

	return newConsistencyScanIterator(s, numPartitionQueries)
}

func newConsistencyScanIterator(store *Store, numPartitionQueries int) (*consistencyScanIterator, error) {
	policy := util.GetAerospikeQueryPolicy(store.settings)
	policy.IncludeBinData = true
	policy.RecordQueueSize = 1024

	workerFunc := func(ctx context.Context, it *consistencyScanIterator, partitionStart, partitionCount int) {
		it.partitionWorker(ctx, policy, partitionStart, partitionCount)
	}

	return launchConsistencyScan(store, numPartitionQueries, workerFunc)
}

// launchConsistencyScan sets up the partition-parallel scan infrastructure and launches workers.
// The workerFunc parameter allows testing without a live Aerospike connection.
func launchConsistencyScan(store *Store, numPartitionQueries int, workerFunc func(ctx context.Context, it *consistencyScanIterator, partitionStart, partitionCount int)) (*consistencyScanIterator, error) {
	const totalPartitions = 4096

	if numPartitionQueries < 1 {
		numPartitionQueries = 1
	}
	if numPartitionQueries > totalPartitions {
		numPartitionQueries = totalPartitions
	}

	partitionsPerQuery := totalPartitions / numPartitionQueries
	remainingPartitions := totalPartitions % numPartitionQueries

	workerCtx, cancel := context.WithCancel(context.Background())

	it := &consistencyScanIterator{
		store:         store,
		resultChan:    make(chan []*utxo.InconsistentTxRecord, numPartitionQueries*2),
		errorChan:     make(chan error, numPartitionQueries),
		cancelWorkers: cancel,
	}

	partitionStart := 0
	for i := 0; i < numPartitionQueries; i++ {
		partitionCount := partitionsPerQuery
		if i < remainingPartitions {
			partitionCount++
		}

		it.wg.Add(1)
		go func(ps, pc int) {
			defer it.wg.Done()
			workerFunc(workerCtx, it, ps, pc)
		}(partitionStart, partitionCount)

		partitionStart += partitionCount
	}

	go func() {
		it.wg.Wait()
		close(it.resultChan)
		close(it.errorChan)
	}()

	return it, nil
}

func (it *consistencyScanIterator) partitionWorker(ctx context.Context, policy *as.QueryPolicy, partitionStart, partitionCount int) {
	stmt := as.NewStatement(it.store.namespace, it.store.setName)

	// No filter — scan all records. Only fetch 3 lightweight bins.
	stmt.BinNames = []string{
		fields.TxID.String(),
		fields.BlockIDs.String(),
		fields.UnminedSince.String(),
	}

	partitionFilter := as.NewPartitionFilterByRange(partitionStart, partitionCount)

	recordset, err := it.store.client.QueryPartitions(policy, stmt, partitionFilter)
	if err != nil {
		it.store.logger.Errorf("[consistencyScan] partition query failed (partitions %d-%d): %v", partitionStart, partitionStart+partitionCount-1, err)
		select {
		case it.errorChan <- err:
		default:
		}
		return
	}
	defer recordset.Close()

	it.processResults(ctx, recordset.Results())
}

// processResults reads Aerospike results from a channel, parses consistency records,
// batches them, and sends batches to resultChan. Extracted from partitionWorker to
// enable testing without a live Aerospike connection.
func (it *consistencyScanIterator) processResults(ctx context.Context, results <-chan *as.Result) {
	const batchSize = 16 * 1024
	localBuffer := make([]*utxo.InconsistentTxRecord, 0, batchSize)

	flush := func() error {
		if len(localBuffer) == 0 {
			return nil
		}
		batchToSend := make([]*utxo.InconsistentTxRecord, len(localBuffer))
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
		_ = flush()
	}()

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		rec, ok := <-results
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

		it.totalScanned.Add(1)

		record, ok := parseConsistencyRecord(rec.Record.Bins)
		if !ok {
			continue
		}

		localBuffer = append(localBuffer, record)

		if len(localBuffer) >= batchSize {
			if err := flush(); err != nil {
				return
			}
		}
	}
}

// parseConsistencyRecord extracts txid, block_ids, and unmined_since from an Aerospike
// bin map. Returns the record and true if parsing succeeded, or nil and false if the
// record should be skipped (missing/invalid txid, bad block_ids, etc.).
func parseConsistencyRecord(bins map[string]interface{}) (*utxo.InconsistentTxRecord, bool) {
	txidVal, ok := bins[fields.TxID.String()].([]byte)
	if !ok {
		return nil, false
	}
	hash, err := chainhash.NewHash(txidVal)
	if err != nil {
		return nil, false
	}

	unminedSince, _ := bins[fields.UnminedSince.String()].(int)

	blockIDs, err := processBlockIDs(bins)
	if err != nil {
		return nil, false
	}

	return &utxo.InconsistentTxRecord{
		Hash:         *hash,
		BlockIDs:     blockIDs,
		UnminedSince: unminedSince,
	}, true
}

func (it *consistencyScanIterator) Next(ctx context.Context) ([]*utxo.InconsistentTxRecord, error) {
	if it.done || it.err != nil {
		return nil, it.err
	}

	// Check for worker errors
	select {
	case err := <-it.errorChan:
		if err != nil {
			it.err = err
			it.Close()
			return nil, err
		}
	default:
	}

	select {
	case <-ctx.Done():
		it.err = ctx.Err()
		it.Close()
		return nil, it.err
	case batch, ok := <-it.resultChan:
		if !ok {
			select {
			case err := <-it.errorChan:
				if err != nil {
					it.err = err
					it.Close()
					return nil, err
				}
			default:
			}
			it.Close()
			return nil, nil
		}
		return batch, nil
	}
}

func (it *consistencyScanIterator) TotalScanned() int64 {
	return it.totalScanned.Load()
}

func (it *consistencyScanIterator) Close() error {
	if it.done {
		return nil
	}
	it.done = true
	if it.cancelWorkers != nil {
		it.cancelWorkers()
	}
	return nil
}

func (it *consistencyScanIterator) Err() error {
	return it.err
}

// ProgressReporter starts a goroutine that logs scan progress at regular intervals.
// Returns a stop function that should be called when the scan is complete.
func (it *consistencyScanIterator) ProgressReporter(interval time.Duration) func() {
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		start := time.Now()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				scanned := it.totalScanned.Load()
				elapsed := time.Since(start)
				rate := float64(scanned) / elapsed.Seconds()
				it.store.logger.Infof("[consistencyScan] progress: %d records scanned, %.0f records/sec, elapsed %s",
					scanned, rate, elapsed.Truncate(time.Second))
			}
		}
	}()
	return cancel
}
