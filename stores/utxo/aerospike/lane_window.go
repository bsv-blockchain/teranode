package aerospike

import (
	"context"
	"sync"
	"time"

	as "github.com/bsv-blockchain/aerospike-client-go/v8"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/lane"
)

const (
	// laneWindowMaxQueries bounds the partition queries of a window read. The read runs beside the
	// node's own work, after block assembly's reload, and shares the server's query threads.
	laneWindowMaxQueries = 4

	// laneWindowBatch is how many records one batch to the caller holds.
	laneWindowBatch = 4_096
)

// laneWindowBins are the bins a reload from the window would read: the ones block assembly's
// reload reads, with the lane position.
var laneWindowBins = []string{
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
	fields.LanePosition.String(),
}

var _ utxo.LaneWindowReader = (*Store)(nil)

// ReadLaneWindow reads block assembly's restart window for its dry run at utxostore_laneMigration
// shadow: every record of the set whose last-update time is at or after w.From - w.Margin. It walks
// the primary index with the last-update time as a filter expression, which the server answers
// from the index, so only records inside the window are read from storage. Records the margin
// added are counted with the rest: the read returns no last-update time to tell them by. A record
// with no createdAt bin is a pagination record (NotMaster). The inputs of an external record are
// not fetched; such records are counted in External.
func (s *Store) ReadLaneWindow(ctx context.Context, w utxo.LaneWindow, fn func([]utxo.LaneWindowRecord) error) (utxo.LaneWindowStats, error) {
	start := time.Now()

	var st utxo.LaneWindowStats

	if s.client == nil {
		return st, errors.NewProcessingError(errAerospikeClientNotInit)
	}

	queries, err := calculatePartitionQueries(s)
	if err != nil {
		return st, err
	}

	queries = min(max(queries, 1), laneWindowMaxQueries)

	since := w.From.Add(-w.Margin).UnixNano()

	policy := partitionScanPolicy(util.GetAerospikeQueryPolicy(s.settings))
	policy.FilterExpression = as.ExpGreaterEq(as.ExpLastUpdate(), as.ExpIntVal(since))

	readCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	type result struct {
		batch    []utxo.LaneWindowRecord
		bytes    uint64
		external uint64
	}

	results := make(chan result, queries)
	errs := make(chan error, queries)

	var wg sync.WaitGroup

	const totalPartitions = 4096

	per, extra := totalPartitions/queries, totalPartitions%queries

	for i, first := 0, 0; i < queries; i++ {
		count := per
		if i < extra {
			count++
		}

		wg.Add(1)

		go func(first, count int) {
			defer wg.Done()

			stmt := as.NewStatement(s.namespace, s.setName, laneWindowBins...)

			rs, qErr := s.client.QueryPartitions(policy, stmt, as.NewPartitionFilterByRange(first, count))
			if qErr != nil {
				errs <- qErr

				return
			}

			defer func() { _ = rs.Close() }()

			// A cancel closes the recordset at once: the server may walk a long stretch of the
			// index with nothing inside the window, and send no record to notice the cancel by.
			stopClose := context.AfterFunc(readCtx, func() { _ = rs.Close() })
			defer stopClose()

			cur := result{batch: make([]utxo.LaneWindowRecord, 0, laneWindowBatch)}

			send := func() bool {
				select {
				case results <- cur:
					cur = result{batch: make([]utxo.LaneWindowRecord, 0, laneWindowBatch)}

					return true
				case <-readCtx.Done():
					return false
				}
			}

			for res := range rs.Results() {
				if readCtx.Err() != nil {
					return
				}

				if res.Err != nil {
					errs <- res.Err

					return
				}

				rec, n, external := laneWindowRecord(res.Record.Bins)
				cur.batch = append(cur.batch, rec)
				cur.bytes += n

				if external {
					cur.external++
				}

				if len(cur.batch) >= laneWindowBatch && !send() {
					return
				}
			}

			if len(cur.batch) > 0 {
				send()
			}
		}(first, count)

		first += count
	}

	go func() {
		wg.Wait()
		close(results)
	}()

	var fnErr error

	for r := range results {
		if fnErr != nil {
			continue
		}

		st.Records += uint64(len(r.batch))
		st.Bytes += r.bytes
		st.External += r.external

		if fnErr = fn(r.batch); fnErr != nil {
			cancel()
		}
	}

	st.Duration = time.Since(start)

	if fnErr != nil {
		return st, fnErr
	}

	select {
	case err = <-errs:
		return st, errors.NewStorageError("[aerospike] read the restart window", err)
	default:
	}

	if err = ctx.Err(); err != nil {
		return st, err
	}

	return st, nil
}

var _ utxo.LaneLastUpdateReader = (*Store)(nil)

// LaneLastUpdate reads a transaction's record's last-update time, which the restart window
// filters on.
func (s *Store) LaneLastUpdate(_ context.Context, hash chainhash.Hash) (time.Time, error) {
	if s.client == nil {
		return time.Time{}, errors.NewProcessingError(errAerospikeClientNotInit)
	}

	key, err := as.NewKey(s.namespace, s.setName, hash[:])
	if err != nil {
		return time.Time{}, errors.NewProcessingError("[aerospike] key for %s", hash.String(), err)
	}

	rec, aErr := s.client.Operate(nil, key, as.ExpReadOp("lut", as.ExpLastUpdate(), as.ExpReadFlagDefault))
	if aErr != nil {
		return time.Time{}, errors.NewStorageError("[aerospike] read the last-update time of %s", hash.String(), aErr)
	}

	lut, ok := rec.Bins["lut"].(int)
	if !ok {
		return time.Time{}, errors.NewProcessingError("[aerospike] last-update time of %s is %T", hash.String(), rec.Bins["lut"])
	}

	return time.Unix(0, int64(lut)), nil
}

// laneWindowRecord turns one record's bins into a window record, with the bytes of the values read
// and whether its inputs are in the external store.
func laneWindowRecord(bins as.BinMap) (utxo.LaneWindowRecord, uint64, bool) {
	var (
		rec   utxo.LaneWindowRecord
		bytes uint64
	)

	for _, v := range bins {
		bytes += laneWindowValueBytes(v)
	}

	if txid, ok := bins[fields.TxID.String()].([]byte); ok && len(txid) == len(rec.Hash) {
		copy(rec.Hash[:], txid)
	}

	createdAt, hasCreatedAt := bins[fields.CreatedAt.String()].(int)
	rec.NotMaster = !hasCreatedAt
	rec.CreatedAt = int64(createdAt)

	if p, ok := bins[fields.LanePosition.String()].(int); ok {
		rec.LanePosition = lane.FromSigned(int64(p))
	}

	rec.Conflicting, _ = bins[fields.Conflicting.String()].(bool)
	external, _ := bins[fields.External.String()].(bool)

	return rec, bytes, external
}

// laneWindowValueBytes is the size of one decoded bin value: its bytes for a blob or string, eight
// for a number or flag, the sum of its items for a list.
func laneWindowValueBytes(v any) uint64 {
	switch x := v.(type) {
	case []byte:
		return uint64(len(x))
	case string:
		return uint64(len(x))
	case []any:
		var n uint64
		for _, item := range x {
			n += laneWindowValueBytes(item)
		}

		return n
	case nil:
		return 0
	default:
		return 8
	}
}
