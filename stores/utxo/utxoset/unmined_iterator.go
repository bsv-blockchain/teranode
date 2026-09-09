package utxoset

import (
	"context"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/jackc/pgx/v5"
)

// unminedBatchSize is how many rows one Next call returns.
//
// The caller streams, so this only bounds peak memory rather than total work. 1,000 keeps a
// batch small enough to stay comfortably inside a message while making the per-round-trip
// cost negligible against a mempool of tens of thousands.
const unminedBatchSize = 1_000

// unminedSQL lists every transaction that is waiting to be mined.
//
// "Waiting" means the marker is set, and the marker means nobody has told us a MAIN-CHAIN
// block contains this transaction. It is deliberately NOT "has no block membership": a
// transaction mined only into a block that lost carries membership and is still waiting.
// Measured on a 20 million row table, the weaker membership test returned 25,000 of 25,499
// waiting transactions, and the 499 it dropped were exactly the fork-mined ones.
//
// Served by the partial index on the marker, which only carries entries for waiting
// transactions and is therefore tiny: 524,288 bytes against a 43 million row table, which is
// 0.0122 bytes per row.
//
// There is deliberately no ORDER BY. The consumer sorts on creation time in Go, so ordering
// here would be work thrown away.
const unminedSQL = `
SELECT txid, fee, size_in_bytes, tx_inpoints, created_at, off_chain_since, membership, flags
  FROM tx_ident
 WHERE off_chain_since IS NOT NULL
   AND (flags & $1::smallint) = 0`

// unminedBelowSQL is the same question with an age bound, for the preservation pass: which
// transactions have been waiting longer than the retention window, so their parents need
// their lifetime extended.
const unminedBelowSQL = `
SELECT txid, fee, size_in_bytes, tx_inpoints, created_at, off_chain_since, membership, flags
  FROM tx_ident
 WHERE off_chain_since IS NOT NULL
   AND off_chain_since <= $2
   AND (flags & $1::smallint) = 0`

// unminedIterator streams the answer rather than materialising it.
//
// Block assembly rebuilds its entire mempool from this at startup and after every reorg, so
// the result set is the whole waiting population and holding it in one slice would be a
// needless peak.
type unminedIterator struct {
	rows pgx.Rows
	err  error
}

func (it *unminedIterator) Next(_ context.Context) ([]*utxo.UnminedTransaction, error) {
	if it.err != nil || it.rows == nil {
		return nil, it.err
	}

	batch := make([]*utxo.UnminedTransaction, 0, unminedBatchSize)

	for len(batch) < unminedBatchSize && it.rows.Next() {
		var (
			txid          []byte
			fee           *int64
			sizeInBytes   *int32
			inpoints      []byte
			createdAt     *int64
			offChainSince *int32
			membership    []byte
			flags         int16
		)

		if err := it.rows.Scan(&txid, &fee, &sizeInBytes, &inpoints, &createdAt,
			&offChainSince, &membership, &flags); err != nil {
			it.err = errors.NewStorageError("[utxoset][unmined] scan", err)
			return nil, it.err
		}

		var hash chainhash.Hash
		copy(hash[:], txid)

		u := &utxo.UnminedTransaction{
			Node:   &subtree.Node{Hash: hash},
			Locked: flags&FlagLocked != 0,
		}

		if fee != nil {
			u.Node.Fee = uint64(*fee) //nolint:gosec // a fee is never negative
		}

		if sizeInBytes != nil {
			u.Node.SizeInBytes = uint64(*sizeInBytes) //nolint:gosec // a size is never negative
		}

		if createdAt != nil {
			u.CreatedAt = int(*createdAt)
		}

		if offChainSince != nil {
			u.UnminedSince = int(*offChainSince)
		}

		u.BlockIDs, _, _ = unpackMembership(membership)

		// A transaction with no stored inputs is a coinbase, which has none to store. Any
		// other empty value would be a store defect, and handing back a nil here would make
		// block assembly dereference it, so give it an empty set either way.
		ip := subtree.TxInpoints{}

		if len(inpoints) > 0 {
			parsed, perr := subtree.NewTxInpointsFromBytes(inpoints)
			if perr != nil {
				it.err = errors.NewStorageError("[utxoset][unmined] inpoints %s", hash.String(), perr)
				return nil, it.err
			}

			ip = parsed
		}

		u.TxInpoints = &ip

		batch = append(batch, u)
	}

	if err := it.rows.Err(); err != nil {
		it.err = errors.NewStorageError("[utxoset][unmined] rows", err)
		return nil, it.err
	}

	// NIL when exhausted, never an empty slice, and this is the whole termination contract
	// rather than tidiness. Every caller breaks its loop on a nil batch and tests nothing else,
	// so handing back an empty but non-nil slice does not end the loop, it spins.
	//
	// It spun. On the mainnet box one call ran for 67 minutes without returning, burning about
	// 15% of the machine's CPU on allocating and zeroing this same slice, and because it sits in
	// the pruner's first phase it held the reclaim in the second phase behind it. Nothing was
	// ever reclaimed, transaction bodies and the undo journal grew without bound, and the shape
	// of it was invisible: no error, no log, a live process doing nothing.
	//
	// The interface documents nil as the terminator, the sql store returns nil, and the
	// consistency scan in this package returns nil. This was the one that did not.
	if len(batch) == 0 {
		return nil, nil
	}

	return batch, nil
}

func (it *unminedIterator) Err() error { return it.err }

func (it *unminedIterator) Close() error {
	if it.rows != nil {
		it.rows.Close()
	}

	return nil
}

// GetUnminedTxIterator lists every transaction waiting to be mined.
//
// This is what block assembly rebuilds its whole mempool from, at startup and on every reset,
// and a reorg triggers a reset. A transaction missing from the answer never gets mined, and
// on a delete-on-spend store that is unrecoverable: the coin rows its inputs pointed at were
// deleted when it was first accepted, and an absent coin row reads as already spent.
func (s *Store) GetUnminedTxIterator() (utxo.UnminedTxIterator, error) {
	rows, err := s.pool.Query(context.Background(), unminedSQL, FlagConflicting)
	if err != nil {
		return nil, errors.NewStorageError("[utxoset][GetUnminedTxIterator]", err)
	}

	return &unminedIterator{rows: rows}, nil
}

// GetPrunableUnminedTxIterator lists transactions that have been waiting since at or before
// cutoffHeight, which is the preservation pass's narrower question.
//
// The pass never deletes anything. It extends the lifetime of these transactions' parents, so
// that a transaction still waiting after a long time does not lose the coins it intends to
// spend.
func (s *Store) GetPrunableUnminedTxIterator(cutoffHeight uint32) (utxo.UnminedTxIterator, error) {
	rows, err := s.pool.Query(context.Background(), unminedBelowSQL,
		FlagConflicting, int32(cutoffHeight)) //nolint:gosec // a chain height fits int32
	if err != nil {
		return nil, errors.NewStorageError("[utxoset][GetPrunableUnminedTxIterator]", err)
	}

	return &unminedIterator{rows: rows}, nil
}

// conflictingSQL lists every transaction recorded as having lost a double-spend race.
//
// The projection MUST match unminedSQL column for column and in the same order, because the
// iterator's Next scans these positionally and is shared by all three constructors. A column
// added to one statement and not the others is a runtime scan failure rather than a compile
// error, which is why this lives beside them.
//
// There is deliberately no test on the mempool marker. A transaction that lost a race can have
// been mined into a block that later lost, so it carries membership and no marker, and it is
// still conflicting. Filtering on the marker would hide exactly the transactions a rewind
// exists to purge, and neither reference store filters on it either.
//
// Coinbases are excluded here rather than emitted and skipped later. A coinbase spends nothing,
// so it can never lose a race for a coin, and excluding it in the statement keeps the shared
// Next free of a branch only this caller would use.
//
// No index serves this predicate and none is added. It is a sequential scan of every partition,
// which is what the aerospike store does too, and the only caller is an offline repair tool run
// by hand with the node stopped.
const conflictingSQL = `
SELECT txid, fee, size_in_bytes, tx_inpoints, created_at, off_chain_since, membership, flags
  FROM tx_ident
 WHERE (flags & $1::smallint) <> 0
   AND (flags & $2::smallint) = 0`

// GetConflictingTxIterator lists the transactions recorded as having lost a double-spend race.
//
// The offline rewind tool reads this to decide what to purge. It holds one connection and one
// snapshot open for the length of the scan, which would defer the journal pruner's concurrent
// detach on a running node; that is acceptable because the tool runs with the node stopped.
func (s *Store) GetConflictingTxIterator() (utxo.UnminedTxIterator, error) {
	rows, err := s.pool.Query(context.Background(), conflictingSQL, FlagConflicting, FlagCoinbase)
	if err != nil {
		return nil, errors.NewStorageError("[utxoset][GetConflictingTxIterator]", err)
	}

	return &unminedIterator{rows: rows}, nil
}
