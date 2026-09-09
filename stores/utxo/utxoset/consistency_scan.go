package utxoset

import (
	"context"
	"sync/atomic"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/jackc/pgx/v5"
)

// consistencyBatchSize is how many rows one Next call returns.
//
// Same value and same reasoning as the waiting-transaction iterator's: the caller streams, so
// this bounds peak memory rather than total work. Materialising the whole answer instead is
// what this iterator exists to avoid.
const consistencyBatchSize = 1_000

// inconsistentUnminedSQL asks the one question the caller acts on: which transactions carry
// block membership AND are still marked as waiting to be mined.
//
// The store cannot decide whether those blocks are on the main chain, and has never been able
// to see the chain, which is why the mempool marker is a cached answer written down by the
// paths that learn it rather than anything derivable here. So the filter stops at "has
// membership and is still waiting", and block assembly intersects the block ids against its
// own header set.
//
// This is deliberately NOT the physical whole-table scan the aerospike store does, and the
// difference is not a corner cut. Aerospike reads every record because its secondary index is
// built asynchronously and can be incomplete, which is why block assembly waits for that index
// to be ready before trusting it. A PostgreSQL index cannot lag its heap, because it is updated
// inside the same transaction as the row it describes, so restricting the scan to the indexed
// set returns the identical answer. Every inconsistent row has the marker set by definition, so
// nothing is missed by starting from the index instead of from the table.
//
// The marker predicate is byte-identical to the predicate of the partial index on that column,
// so the planner can always prove the index applies. Without it this reads the whole identity
// table, which is hundreds of millions of rows on the mainnet box, to return the same answer.
//
// The membership length test is octet_length >= 12 rather than IS NOT NULL. The check
// constraint admits a ZERO-length membership, since zero is a multiple of twelve, and such a
// row names no block and can never be repaired, so it is excluded here rather than sent over
// the wire for the caller to drop. octet_length of NULL is NULL and NULL >= 12 is not true, so
// the same clause excludes an absent membership without a second test.
//
// Conflicting rows are deliberately NOT excluded. They are the one class this finds that the
// ordinary waiting-transaction iterator cannot, because that one masks them out, and a
// consistency scan trusting the same predicate as the thing it exists to double-check would be
// decoration.
//
// There is deliberately no ORDER BY. Nothing downstream depends on the order, so a sort here
// would be work thrown away.
const inconsistentUnminedSQL = `
SELECT txid, membership, off_chain_since
  FROM tx_ident
 WHERE off_chain_since IS NOT NULL
   AND octet_length(membership) >= 12`

// consistencyScanIterator streams the answer rather than materialising it.
//
// The query is issued on the FIRST Next rather than up front, and with that call's context.
// Issuing it early with a background context means pgx never arms a cancellation watcher, so a
// caller that abandons the scan blocks in Close while the server finishes streaming the whole
// result. Deferring it is what makes an abandoned reset actually stop.
type consistencyScanIterator struct {
	store     *Store
	rows      pgx.Rows
	started   bool
	done      bool
	err       error
	batchSize int
	scanned   atomic.Int64
}

// ScanInconsistentUnminedTxs returns the transactions that carry block membership while still
// marked as waiting to be mined.
//
// Block assembly runs this on an operator-requested full reset, then intersects each record's
// block ids against its own header set and repairs whatever it finds on the main chain.
//
// It cannot fail, because it does no work until the first Next. The caller tolerates a nil
// iterator, meaning "this store needs no scan", so returning an error here would be the one
// answer that is neither.
//
// One operational caveat. The scan streams inside a single snapshot held open for the whole
// drain, which defers the journal pruner's concurrent detach and holds back the vacuum horizon
// for as long as it runs. That is acceptable on an operator-triggered reset and would not be on
// a timer.
func (s *Store) ScanInconsistentUnminedTxs() (utxo.ConsistencyScanIterator, error) {
	return &consistencyScanIterator{store: s, batchSize: consistencyBatchSize}, nil
}

// Next returns nil, never a non-nil empty slice, once the rows are exhausted.
//
// That is load-bearing rather than tidy. This caller breaks on a nil batch and tests nothing
// else, so a non-nil empty batch is an infinite loop that never reaches the repair.
//
// The context is honoured, and it is also what the query is issued under, so a cancellation
// stops the server streaming rather than merely stopping this loop reading them. Reporting the
// cancellation as an error rather than as exhaustion is what stops the caller logging that it
// found nothing wrong after a scan that never finished.
func (it *consistencyScanIterator) Next(ctx context.Context) ([]*utxo.InconsistentTxRecord, error) {
	if it.err != nil || it.done {
		return nil, it.err
	}

	if err := ctx.Err(); err != nil {
		it.err = errors.NewProcessingError("[utxoset][ScanInconsistentUnminedTxs] cancelled", err)
		return nil, it.err
	}

	if !it.started {
		it.started = true

		rows, err := it.store.pool.Query(ctx, inconsistentUnminedSQL)
		if err != nil {
			it.err = errors.NewStorageError("[utxoset][ScanInconsistentUnminedTxs] scan", err)
			return nil, it.err
		}

		it.rows = rows
	}

	size := it.batchSize
	if size <= 0 {
		size = consistencyBatchSize
	}

	batch := make([]*utxo.InconsistentTxRecord, 0, size)

	for len(batch) < size && it.rows.Next() {
		var (
			txid          []byte
			membership    []byte
			offChainSince int32
		)

		if err := it.rows.Scan(&txid, &membership, &offChainSince); err != nil {
			it.err = errors.NewStorageError("[utxoset][ScanInconsistentUnminedTxs] row", err)
			return nil, it.err
		}

		var hash chainhash.Hash

		copy(hash[:], txid)

		// Every block the transaction claims, not the first one. The caller walks the whole
		// slice looking for a main-chain hit, so dropping any would make it miss a repair.
		blockIDs, _, _ := unpackMembership(membership)

		batch = append(batch, &utxo.InconsistentTxRecord{
			Hash:     hash,
			BlockIDs: blockIDs,
			// A marker of zero is a real height here, not an absent value. The interface has
			// no way to say "absent", and the caller reads zero as absent and drops the row.
			// The store still reports it, so that loss is the caller's visible decision rather
			// than this scan quietly deciding some inconsistencies do not count.
			UnminedSince: int(offChainSince),
		})

		it.scanned.Add(1)
	}

	if err := it.rows.Err(); err != nil {
		it.err = errors.NewStorageError("[utxoset][ScanInconsistentUnminedTxs] rows", err)
		return nil, it.err
	}

	if len(batch) == 0 {
		it.done = true
		return nil, nil
	}

	return batch, nil
}

// TotalScanned counts records YIELDED, which in this store is the candidate set rather than the
// whole table.
//
// Atomic because the caller polls it from a separate goroutine while the main one is inside
// Next. The number it feeds is therefore the size of the mempool rather than the size of the
// store, which is a different quantity from the one the aerospike store reports under this
// name.
func (it *consistencyScanIterator) TotalScanned() int64 { return it.scanned.Load() }

func (it *consistencyScanIterator) Err() error { return it.err }

func (it *consistencyScanIterator) Close() error {
	if it.rows != nil {
		it.rows.Close()
		it.rows = nil
	}

	it.done = true

	return nil
}
