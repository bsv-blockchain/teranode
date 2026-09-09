package utxo

import (
	"context"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/ulogger"
	"golang.org/x/sync/errgroup"
)

// PrunedReplayGhosts returns the records a failed spend phase leaves behind
// when the store rejected transactions of a block as pruned replays.
//
// Both below-checkpoint block paths create every transaction in the block first
// and spend the inputs afterwards (services/blockvalidation/quick_validate.go
// and services/legacy/netsync/handle_block.go). Create does not consult the
// pruner's replay markers - putting a parent-record read on the hottest write
// path in the node is the cost that ordering exists to avoid - so a block that
// replays pruned transactions gets through the create phase and is rejected in
// the spend phase, with the records it wrote still in the store. Each is stored
// mined with no delete_at_height, so neither the unmined reaper nor the DAH
// scan ever reclaims it, and on the legacy path it is unlocked and spendable
// even though a mined descendant already consumed those outputs.
//
// Two kinds of record qualify.
//
// The rejected transactions themselves are ghosts whether or not this attempt
// created them. The store rejects a spend on a replay marker only for a
// transaction this store already pruned, and pruning happens only once every
// output is spent and the transaction is buried below retention, so there is no
// route by which it legitimately comes back. A record for it can only be what a
// replay left behind: this attempt's create phase, or an earlier attempt whose
// compensating delete failed. Excluding "already existed" here is what made a
// transient delete failure permanent: the next attempt saw ErrTxExists, filed
// the leftover as pre-existing and skipped it forever.
//
// A transaction in the same block that spends an output of a ghost is a ghost
// too, but only when this attempt created it. Its own markers cannot catch it:
// they lived on the pruned parent's record and went with it, and the parent it
// spends was just recreated with unspent outputs, so its spend succeeds and it
// would stay behind with spendable outputs after the parent is removed. A
// dependent the store already held is different: a pruned parent says nothing
// about whether a still-unpruned child is legitimate, so that record belongs to
// whoever put it there and is left alone. The walk is transitive through ghosts
// only.
//
// "This attempt created it" includes records an EARLIER attempt at the same
// block created and never finished with: see LeftoversAmong. Without that, a
// crash or cancellation between the create phase and this compensation, a
// delete that failed past its retries, or a create batch the store client
// re-sent and got KEY_EXISTS back for, left a record that the next attempt
// filed as pre-existing, and a replay of a chain the pruner removed end to end
// was then blessed on the strength of it. The callers pass createdHere
// accordingly.
//
// txs need not be in dependency order; the walk repeats until it adds nothing.
// Every hash in rejected must name a transaction in txs, which holds at both
// call sites because the rejections came from spending exactly that list.
func PrunedReplayGhosts(txs []*bt.Tx, rejected []*chainhash.Hash, createdHere func(*chainhash.Hash) bool) []*bt.Tx {
	if len(rejected) == 0 {
		return nil
	}

	// Hash once per transaction: the walk below may visit each several times.
	hashes := make([]*chainhash.Hash, len(txs))
	byHash := make(map[chainhash.Hash]*bt.Tx, len(txs))

	for i, tx := range txs {
		hashes[i] = tx.TxIDChainHash()
		byHash[*hashes[i]] = tx
	}

	ghosts := make(map[chainhash.Hash]struct{}, len(rejected))
	result := make([]*bt.Tx, 0, len(rejected))

	for _, hash := range rejected {
		if _, ok := ghosts[*hash]; ok {
			continue
		}

		tx, ok := byHash[*hash]
		if !ok {
			continue
		}

		ghosts[*hash] = struct{}{}
		result = append(result, tx)
	}

	for added := true; added; {
		added = false

		for i, tx := range txs {
			txHash := hashes[i]
			if _, ok := ghosts[*txHash]; ok {
				continue
			}

			if !spendsAny(tx, ghosts) || !createdHere(txHash) {
				continue
			}

			ghosts[*txHash] = struct{}{}
			result = append(result, tx)
			added = true
		}
	}

	return result
}

// LeftoversAmong returns, out of the transactions whose create answered
// ErrTxExists, the ones an earlier attempt at this same block wrote and never
// finished with. They must be treated exactly like the records this attempt
// wrote: their presence proves nothing about prior validation, and if the
// spend phase rejects them they are ghosts to delete.
//
// The durable mark is the lock. Both below-checkpoint block paths create every
// transaction of a block locked and clear the lock only once the block is
// committed, so a record that already exists AND is locked at create time is a
// two-phase write that never completed: this block's own earlier attempt, or a
// sibling validation of the same block, which comes to the same thing. A
// legitimately pre-existing mined record is never locked: a mempool
// transaction is created locked but SetMined clears it when its block is
// processed, and a block-created record is unlocked by that block's post-commit
// pass. Read BEFORE the caller's SetMinedMulti for existing transactions, which
// clears the lock; the caller then leaves leftovers out of that call, which is
// safe because AssignBlockID is idempotent per block hash, so a leftover already
// carries this block's id.
//
// Two limits, both stated rather than hidden. With the catch-up lock switched
// off (blockvalidation_quick_validate_skip_utxo_lock) nothing writes the mark,
// and a leftover is filed as pre-existing again. And conflict resolution locks
// the parents of a conflicting transaction for the few store round trips of
// ProcessConflicting; a replay of such a parent's block in exactly that window
// would misfile it as a leftover, and if its own parent is pruned too it would
// be deleted. That fails towards a missing record this node will notice when it
// next validates a spend of it, not towards a double-spendable output.
func LeftoversAmong(ctx context.Context, store Store, existing []*chainhash.Hash) (map[chainhash.Hash]struct{}, error) {
	if len(existing) == 0 {
		return nil, nil
	}

	unresolved := make([]*UnresolvedMetaData, len(existing))
	for i, hash := range existing {
		unresolved[i] = &UnresolvedMetaData{Hash: *hash, Idx: i}
	}

	if err := store.BatchDecorate(ctx, unresolved, fields.Locked); err != nil {
		return nil, errors.NewStorageError("[LeftoversAmong] could not read the lock state of %d existing transactions", len(existing), err)
	}

	leftovers := make(map[chainhash.Hash]struct{})

	for _, item := range unresolved {
		if item.Data != nil && item.Data.Locked {
			leftovers[item.Hash] = struct{}{}
		}
	}

	return leftovers, nil
}

// IsPrunedReplayRejection reports whether a spend-phase error identifies the
// spending transaction as a replay of one the pruner removed.
//
// Two answers qualify. The marker rejection, ErrUtxoSpendingTxPruned, names the
// replay directly. A missing parent, ErrTxNotFound, does so only for a
// transaction this attempt created: below the checkpoint a parent record is
// absent only because it was fully spent and buried, so a transaction that had
// to be created in order to spend it is a replay of a chain the pruner removed
// end to end, marker and all. A pre-existing transaction with a missing parent
// is the case the stores' "already blessed" fallback exists for and is not a
// replay.
func IsPrunedReplayRejection(err error, createdHere bool) bool {
	if errors.Is(err, errors.ErrUtxoSpendingTxPruned) {
		return true
	}

	return createdHere && errors.Is(err, errors.ErrTxNotFound)
}

// spendsAny reports whether tx spends an output of any transaction in set.
func spendsAny(tx *bt.Tx, set map[chainhash.Hash]struct{}) bool {
	for _, input := range tx.Inputs {
		if _, ok := set[*input.PreviousTxIDChainHash()]; ok {
			return true
		}
	}

	return false
}

// deleteCreatedAttempts and deleteCreatedBackoff bound the retry of one
// compensating delete. A record left behind here is the ghost this package
// exists to remove, and for a dependent of a pruned replay the next attempt
// cannot tell the leftover from a legitimately pre-existing record (see
// PrunedReplayGhosts), so the in-band retry is the only chance to finish the job.
const (
	deleteCreatedAttempts = 3
	deleteCreatedBackoff  = 100 * time.Millisecond
)

// DeleteCreated removes the ghosts PrunedReplayGhosts found. It deletes their
// records and nothing else.
//
// In particular it does NOT reverse the ghosts' input spends, and that is the
// point rather than an omission. A replayed transaction carries the same
// identity as its original, so a parent output that records the ghost as its
// spender cannot say whether that spend is the confirmed, historical one or one
// this attempt just made, and the stores' spender-matched Unspend cannot tell
// them apart either. Every spend a ghost holds on a surviving output is the
// historical one:
//
//   - A rejected transaction's spend of the markered output never committed,
//     and the store rolls back whatever fresh sibling spends it made in the
//     same call (needsSpendRollback). What its inputs still record is the
//     original, confirmed spend, which the marker protects; clearing it would
//     hand a confirmed output to any new spender, and the marker would not
//     object because it names only the original child.
//   - A dependent ghost that spends a surviving output either hit that
//     output's marker for itself, in which case it was rejected and is a root
//     above, or the output has no marker for it, which means it was pruned by
//     code that wrote none and the output has recorded its spend all along, so
//     the re-spend was idempotent and the record holds the historical spend.
//
// Deleting the record and leaving those spends in place is exactly the state
// a normal prune leaves: a spent parent output naming a child that is gone.
//
// A missing record is success: it is gone, which is the outcome asked for. Any
// other failure is retried, then logged per transaction and reported in
// aggregate, because a record left behind here is the ghost this function exists
// to prevent and the operator has to see it.
func DeleteCreated(ctx context.Context, logger ulogger.Logger, store Store, ghosts []*bt.Tx, maxWorkers int) error {
	if len(ghosts) == 0 {
		return nil
	}

	logger.Warnf("[DeleteCreated] removing %d transaction(s) created by a block phase that then failed", len(ghosts))

	if maxWorkers < 1 {
		maxWorkers = 1
	}

	deleteG := new(errgroup.Group)
	deleteG.SetLimit(maxWorkers)

	for _, tx := range ghosts {
		hash := tx.TxIDChainHash()

		deleteG.Go(func() error {
			err := retryStoreCall(ctx, func() error {
				err := store.DeleteComplete(ctx, hash)
				if err != nil && (errors.Is(err, errors.ErrTxNotFound) || errors.Is(err, errors.ErrNotFound)) {
					return nil
				}

				return err
			})
			if err != nil {
				logger.Errorf("[DeleteCreated] failed to remove %s after a failed block phase (%d attempts); it remains in the store: %v", hash.String(), deleteCreatedAttempts, err)
			}

			return err
		})
	}

	if err := deleteG.Wait(); err != nil {
		return errors.NewStorageError("[DeleteCreated] could not remove all %d created transaction(s) after a failed block phase", len(ghosts), err)
	}

	return nil
}

// retryStoreCall runs fn up to deleteCreatedAttempts times with a linear
// backoff, giving up early when ctx ends.
func retryStoreCall(ctx context.Context, fn func() error) error {
	var err error

	for attempt := 0; attempt < deleteCreatedAttempts; attempt++ {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return errors.NewContextCanceledError("[DeleteCreated] cancelled while retrying", ctx.Err())
			case <-time.After(deleteCreatedBackoff * time.Duration(attempt)):
			}
		}

		if err = fn(); err == nil {
			return nil
		}
	}

	return err
}
