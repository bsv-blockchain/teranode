package sql

import (
	"context"
	"time"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
)

// rollbackChunkSize bounds each Unspend call made by rollbackSpendChunks.
//
// Unspend wraps its whole loop in one db.Begin()/Commit() (see its doc
// comment — that atomicity is relied on elsewhere and must not change), so a
// single failing spend inside a call — a non-ErrNoRows error, a genuinely
// missing output row, a setDAH failure, or ctx.Done() — rolls back every
// spend in that call, not just the one that failed. Left as one call over the
// whole batch, a failure at spend k of a wide transaction costs all N
// spends' worth of rollback progress rather than k's. Chunking bounds that
// blast radius to one chunk's worth, at the cost of atomicity across the
// rollback as a whole (which nothing needs: each chunk that commits is a
// self-contained set of reverted spends).
const rollbackChunkSize = 200

// maxRollbackChunkRetries bounds the retry applied only to isDeadlock(err)
// failures: a deadlock is the one failure class where the exact same chunk
// can plausibly succeed a moment later, unlike a missing row or a cancelled
// context.
const maxRollbackChunkRetries = 3

// rollbackRetryBackoff is the base delay between deadlock retries of one chunk.
// A Postgres deadlock victim or a SQLite BUSY clears only once the other writer
// commits, so retrying the same chunk immediately mostly burns the attempt budget.
const rollbackRetryBackoff = 25 * time.Millisecond

// rollbackSpendChunks reverts spentSpends via s.Unspend in bounded chunks
// rather than one call over the whole slice, so that a failure inside one
// chunk costs that chunk's spends rather than every spend in the batch (see
// rollbackChunkSize). A chunk whose failure is isDeadlock is retried a
// bounded number of times; any other failure is not retried, and the loop
// moves on to the next chunk regardless — every remaining chunk is still
// attempted, because a spend past a failed chunk is exactly the dangling
// #1214 reference this rollback exists to unwind.
//
// Returns the number of spends actually reverted (i.e. belonging to chunks
// that committed), so the caller can log and count a reference count rather
// than a single pass/fail per call, plus the first error hit, if any chunk
// never committed.
func (s *Store) rollbackSpendChunks(ctx context.Context, spentSpends []*utxo.Spend) (reverted int, firstErr error) {
	for start := 0; start < len(spentSpends); start += rollbackChunkSize {
		end := start + rollbackChunkSize
		if end > len(spentSpends) {
			end = len(spentSpends)
		}

		chunk := spentSpends[start:end]

		var chunkErr error

		for attempt := 0; attempt <= maxRollbackChunkRetries; attempt++ {
			chunkErr = s.Unspend(ctx, chunk)
			if chunkErr == nil || !isDeadlock(chunkErr) {
				break
			}

			// Back off before retrying: a Postgres deadlock victim or a SQLite BUSY
			// clears only once the other writer commits, so an immediate retry of the
			// same chunk mostly just burns the attempt budget.
			select {
			case <-ctx.Done():
				return reverted, chunkErr
			case <-time.After(time.Duration(attempt+1) * rollbackRetryBackoff):
			}
		}

		if chunkErr != nil {
			if firstErr == nil {
				firstErr = chunkErr
			}

			continue
		}

		reverted += len(chunk)
	}

	return reverted, firstErr
}

// hasTransientLockFailure reports whether any spend failed with ErrTxLocked.
//
// Mirrors the aerospike store's helper of the same name. Deliberately duplicated
// rather than shared: the two stores' Spend implementations are independent, and
// this predicate is the only thing they would have in common. Keep them in step.
//
// ErrTxLocked is the one failure class that means "try again in a moment": the
// parent's record is locked only for the two-phase-commit window between its own
// create and its block-assembly ack, which is milliseconds. A concurrent attempt
// at the SAME txid (duplicate relay, or a second validator pod behind the same
// store) can therefore be spending those inputs successfully while this attempt
// sees the lock — and because spending_data is just {TxID, Vin} with no attempt
// identity, rolling back here can clear the winner's slots, leaving a live or
// mined transaction whose input reads unspent. That is the inverse of #1214 and
// worse: the node would go on to accept and mine a double-spend.
//
// So the ErrTxLocked flavour of the #1214 dangling ref is NOT fixed by this
// rollback on its own — it needs hasUnwinnableFailure below to know when the
// exclusion must not apply. Covering the ErrTxLocked case unconditionally would
// need ordering that makes the record exist before any spend (create-first,
// #1355) or attempt identity in the stored spending data (#1291); neither
// belongs in an error-path rollback.
func hasTransientLockFailure(spends []*utxo.Spend) bool {
	for _, spend := range spends {
		if spend != nil && spend.Err != nil && errors.Is(spend.Err, errors.ErrTxLocked) {
			return true
		}
	}

	return false
}

// hasTransientCreatingFailure reports whether any spend failed with
// ErrTxCreating.
//
// Mirrors the aerospike store's sawTransientCreating flag, which gates that
// store's rollback the same way hasTransientLockFailure gates this one:
// ErrTxCreating is the aerospike store's OTHER two-phase-commit window on a
// parent record (set in create phase 1, cleared in phase 2 by
// ensureCreatingBin), so a concurrent creator can legitimately be racing this
// attempt while it sees ErrTxCreating, for the same reason ErrTxLocked can.
//
// This SQL store has no "creating" bin or equivalent 2PC window on a parent
// record — outputs.spending_data is set atomically with the row it belongs
// to, so there is nothing for ErrTxCreating to ever match here, and this
// predicate is inert on this backend today. It exists anyway, and is wired
// into the same guard as hasTransientLockFailure below, because the two
// stores' partial-spend-rollback doc comments explicitly require them to
// stay in step (see hasTransientLockFailure's doc comment) — if this store
// ever grows an equivalent creation window, the suppression is already
// correct without anyone having to remember to add it under time pressure.
func hasTransientCreatingFailure(spends []*utxo.Spend) bool {
	for _, spend := range spends {
		if spend != nil && spend.Err != nil && errors.Is(spend.Err, errors.ErrTxCreating) {
			return true
		}
	}

	return false
}

// hasUnwinnableFailure reports whether any spend failed with an error class
// meaning this txid can never win: ErrSpent, ErrTxConflicting, ErrFrozen, or
// ErrUtxoHashMismatch.
//
// Mirrors the aerospike store's sawUnwinnable flag. It exists to override
// hasTransientLockFailure's suppression of the partial-spend rollback: a
// concurrent attempt at the SAME txid sees the same outpoints and hits the
// same unwinnable failure, so it can never reach Create either. That means
// nobody can be a legitimate owner of the partial spends in this batch, and
// leaving them in place would be the exact #1214 shape — a parent naming a
// record-less spender — so the rollback is both safe and required.
func hasUnwinnableFailure(spends []*utxo.Spend) bool {
	for _, spend := range spends {
		if spend == nil || spend.Err == nil {
			continue
		}

		if errors.Is(spend.Err, errors.ErrSpent) ||
			errors.Is(spend.Err, errors.ErrTxConflicting) ||
			errors.Is(spend.Err, errors.ErrFrozen) ||
			errors.Is(spend.Err, errors.ErrUtxoHashMismatch) {
			return true
		}
	}

	return false
}
