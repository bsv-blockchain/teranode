package utxoset

import (
	"context"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/jackc/pgx/v5"
)

// claimBlockSQL takes the ledger row for a block, or reports that someone already has it.
//
// ON CONFLICT DO NOTHING is genuinely idempotent here, unlike on the UTXO table: this
// table has a real PRIMARY KEY for it to act on. It is also the concurrency gate. A
// second caller offering the same block blocks on the conflicting row until the first
// transaction resolves, then inserts nothing if that transaction committed, or takes the
// claim if it rolled back. So a caller can never skip a block whose application is still
// in flight, and a rolled-back application never leaves the block marked as done.
const claimBlockSQL = `
INSERT INTO applied_block (height, block_hash, chunk_size, chunk_count, completed)
VALUES ($1, $2, $3, $4, TRUE)
ON CONFLICT (block_hash) DO NOTHING`

// ApplyBlock runs fn as one atomic, replay-safe unit of block application.
//
// It returns (true, nil) when fn ran and committed, and (false, nil) when the block had
// already been applied and was therefore skipped.
//
// This gate is not an optimisation, it is what keeps the store from inflating the money
// supply. The UTXO table's ukey is a 96-bit prefix and deliberately NON-UNIQUE, so
// the coin insert in createPlanSQL has no ON CONFLICT that could make it idempotent, and re-applying a
// block would insert every output a second time as independently spendable rows. Replay
// is routine rather than exotic: catchup, a restart mid-window, and the documented
// post-restart unrequested-block storm all re-offer blocks. The ledger is therefore
// ground truth written in the SAME transaction as the work it describes, so the claim
// and the rows it authorises commit or roll back together.
//
// Running fn inside that one transaction is the second half of the guard, and it is what
// makes the coinbase safe. A replayed ordinary transaction would fail its spends anyway,
// because the parents it consumes are gone, and take the chunk down with it. A coinbase
// has no inputs and so has no failing spend to trigger that rollback; only the ledger
// stops it.
func (s *Store) ApplyBlock(ctx context.Context, blockHash *chainhash.Hash, height uint32,
	fn func(dbTx pgx.Tx) error) (bool, error) {
	if blockHash == nil {
		return false, errors.NewProcessingError("[utxoset][ApplyBlock] nil block hash")
	}

	// BEFORE the transaction, never inside it. The callback writes transaction bodies, and
	// the window they land in is created by DDL that needs its own pool connection. Taking
	// one while holding a transaction borrowed from the same pool is a nested acquire, and
	// at pool_max_conns concurrent appliers every connection ends up held by a transaction
	// waiting for a connection. That deadlock has no timeout.
	if err := s.ensureTxBodyPartition(ctx, height); err != nil {
		return false, err
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return false, errors.NewStorageError("[utxoset][ApplyBlock] begin %s", blockHash.String(), err)
	}

	defer func() { _ = tx.Rollback(ctx) }()

	tag, err := tx.Exec(ctx, claimBlockSQL, int32(height), blockHash[:], 0, 1)
	if err != nil {
		return false, errors.NewStorageError("[utxoset][ApplyBlock] claim %s", blockHash.String(), err)
	}

	if tag.RowsAffected() == 0 {
		return false, nil // already applied by us or by a committed peer
	}

	if err = fn(tx); err != nil {
		return false, err
	}

	if err = tx.Commit(ctx); err != nil {
		return false, errors.NewStorageError("[utxoset][ApplyBlock] commit %s", blockHash.String(), err)
	}

	return true, nil
}

// claimForApplySQL takes the ledger row without marking it done, and reports whether a
// PREVIOUS attempt already finished.
//
// The RETURNING/coalesce shape gives one round trip for three outcomes: no row existed
// and we now own it (false), a row existed and was completed (true), a row existed and
// was not completed (false, meaning re-apply). ON CONFLICT DO UPDATE with a no-op SET is
// what makes the existing row visible to RETURNING at all; a bare DO NOTHING returns no
// row and cannot distinguish "already done" from "we just claimed it".
const claimForApplySQL = `
INSERT INTO applied_block (height, block_hash, chunk_size, chunk_count, completed)
VALUES ($1, $2, 0, 1, FALSE)
ON CONFLICT (block_hash) DO UPDATE SET height = applied_block.height
RETURNING completed`

const completeApplySQL = `UPDATE applied_block SET completed = TRUE WHERE block_hash = $1`

// BeginBlockApply claims a block for application and reports whether it can be skipped.
//
// It returns true only when a previous attempt ran to completion. That distinction is the
// whole value of the call, and getting it backwards is worse than not having it:
//
//   - Completed, so skip. Re-applying would insert every output a second time as an
//     independently spendable row, because the coin insert in createPlanSQL has no ON CONFLICT to
//     protect it.
//     That is money-supply inflation.
//   - Claimed but never completed, so RE-APPLY. The previous attempt died part-way. If we
//     skipped it, its outputs would never exist, every later block spending them would
//     fail forever, and nothing downstream would flag it. Silent loss is worse than the
//     duplication it avoids.
//
// Re-applying an incomplete block can still duplicate whatever the dead attempt managed
// to write. This does not fix that, and does not claim to: making it safe needs create
// and spend for a chunk in one transaction with applied_chunk tracking which rows landed.
// What this closes is replay of a FINISHED block, which is the routine case (catchup, a
// restart, the post-restart unrequested-block storm).
func (s *Store) BeginBlockApply(ctx context.Context, blockHash *chainhash.Hash, height uint32) (bool, error) {
	if blockHash == nil {
		return false, errors.NewProcessingError("[utxoset][BeginBlockApply] nil block hash")
	}

	var completed bool

	if err := s.pool.QueryRow(ctx, claimForApplySQL, int32(height), blockHash[:]).Scan(&completed); err != nil {
		return false, errors.NewStorageError("[utxoset][BeginBlockApply] claim %s", blockHash.String(), err)
	}

	return completed, nil
}

// CompleteBlockApply marks a block finished, so a later offer of it is skipped.
//
// Call this only after every output the block creates and every input it spends has been
// durably committed. Marking it early converts a crash into permanent silent loss,
// because the next offer will skip a block that never finished.
func (s *Store) CompleteBlockApply(ctx context.Context, blockHash *chainhash.Hash) error {
	if blockHash == nil {
		return errors.NewProcessingError("[utxoset][CompleteBlockApply] nil block hash")
	}

	if _, err := s.pool.Exec(ctx, completeApplySQL, blockHash[:]); err != nil {
		return errors.NewStorageError("[utxoset][CompleteBlockApply] %s", blockHash.String(), err)
	}

	return nil
}
