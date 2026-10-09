package utxo

import (
	"context"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-subtree"
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
// Two kinds of record qualify, and both only when this attempt created them.
//
// The rejected transactions themselves are ghosts when createdHere says so. A
// marker hit is not, on its own, proof that a record is a replay's leftover:
// the pruner writes a parent's marker before it deletes the child and holds the
// child back when a sibling parent's marker fails, so a live, mined child can
// sit behind a marker until a later cycle removes it. Deleting "whatever the
// store rejected" removed that live transaction, and every dependent this
// attempt created with it. The leftover of an earlier attempt whose
// compensating delete failed is still caught, because it is still locked and
// carries this block's id, which is exactly what createdHere recognises (see
// LeftoversAmong). With the catch-up lock switched off nothing writes that mark
// and such a leftover survives; that limit is the lock setting's, stated there.
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
// The walk starts from every rejected transaction, including one that is not
// createdHere and so is not itself deleted. Such a root is a record this
// attempt filed as pre-existing: a live child the pruner is holding behind its
// marker, or a leftover of an earlier attempt that could not be recognised
// (the catch-up lock switched off, or the restart/id-reuse limit described at
// LeftoversAmong). The root stays, as above, but a dependent THIS attempt wrote
// spending it is still this attempt's own write in a block that has already
// failed, and left alone it would sit stored mined with no delete_at_height and
// spendable outputs. Deleting it is safe for the same reason as every other
// deletion here: it is this attempt's write, and a retry of the block writes it
// again.
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
// txs need not be in dependency order. The walk indexes each transaction under
// the parents its inputs name, once, and follows that index breadth first, so
// it costs O(transactions + inputs) whatever the depth of the ghost chain.
// Every hash in rejected must name a transaction in txs, which holds at both
// call sites because the rejections came from spending exactly that list.
func PrunedReplayGhosts(txs []*bt.Tx, rejected []*chainhash.Hash, createdHere func(*chainhash.Hash) bool) []*bt.Tx {
	if len(rejected) == 0 || createdHere == nil {
		// Without a createdHere answer nothing can be shown to be this attempt's
		// own write, and deleting a record this attempt did not write is the one
		// thing this function must never do.
		return nil
	}

	hashes := make([]*chainhash.Hash, len(txs))
	index := make(map[chainhash.Hash]int, len(txs))

	for i, tx := range txs {
		hashes[i] = tx.TxIDChainHash()
		index[*hashes[i]] = i
	}

	// spenders maps a transaction of this list to the indices of the
	// transactions in it that spend one of its outputs.
	spenders := make(map[chainhash.Hash][]int)

	for i, tx := range txs {
		for _, input := range tx.Inputs {
			parent := *input.PreviousTxIDChainHash()
			if _, inList := index[parent]; !inList {
				continue
			}

			if list := spenders[parent]; len(list) > 0 && list[len(list)-1] == i {
				continue // several inputs of one transaction on the same parent
			}

			spenders[parent] = append(spenders[parent], i)
		}
	}

	// tainted holds every transaction the walk has reached: the rejected roots,
	// deleted or not, and the ghosts found from them.
	tainted := make(map[chainhash.Hash]struct{}, len(rejected))
	queue := make([]chainhash.Hash, 0, len(rejected))
	result := make([]*bt.Tx, 0, len(rejected))

	for _, hash := range rejected {
		if _, ok := tainted[*hash]; ok {
			continue
		}

		i, ok := index[*hash]
		if !ok {
			continue
		}

		tainted[*hash] = struct{}{}
		queue = append(queue, *hash)

		if createdHere(hash) {
			result = append(result, txs[i])
		}
	}

	for len(queue) > 0 {
		parent := queue[0]
		queue = queue[1:]

		for _, i := range spenders[parent] {
			txHash := hashes[i]
			if _, ok := tainted[*txHash]; ok {
				continue
			}

			if !createdHere(txHash) {
				continue
			}

			tainted[*txHash] = struct{}{}
			queue = append(queue, *txHash)
			result = append(result, txs[i])
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
// The durable mark is the lock together with this block's id. Both
// below-checkpoint block paths create every transaction of a block locked, with
// that block's id, and clear the lock only once the block is committed, so a
// record that already exists, is locked AND already carries blockID is a
// two-phase write of this very block that never completed: its own earlier
// attempt, or a sibling validation of the same block, which comes to the same
// thing. A retry of the same block gets the same id: both paths reuse the id
// recorded on the block's first non-coinbase record, and AssignBlockID is
// idempotent per block hash while the blockchain service holds the reservation.
// The reuse can also pick up another block's id from a shared transaction, so
// callers pass the result through ConfirmLeftovers before trusting it.
//
// The lock alone is not enough, because other writers leave a record locked: a
// different block validating concurrently that shares the transaction (its
// create wrote it locked with ITS id), a post-commit unlock pass that failed,
// conflict resolution locking a conflicting transaction's parents, or a mempool
// two-phase create. None of those carries this block's id, so each is filed as
// pre-existing. That keeps it in the caller's SetMinedMulti, so it gains this
// block's id instead of failing a descendant block with "has no block IDs", and
// it keeps it out of the compensation, so it is never deleted as a ghost. Read BEFORE the caller's SetMinedMulti for existing transactions, which
// clears the lock; the caller then leaves leftovers out of that call, which is
// safe because AssignBlockID is idempotent per block hash, so a leftover already
// carries this block's id.
//
// A store read that fails for any one record fails the whole call. BatchDecorate
// reports a per-record failure (a timeout, DEVICE_OVERLOAD, a replica read
// error) in that item's Err and returns nil overall, and reading such an item as
// "not locked" filed an unknown record as pre-existing, which switched the
// "already blessed" fallback back on for it. An unreadable record is not an
// answer; the block is retried instead. A record that is gone is an answer: it
// cannot be a leftover, so it is skipped rather than failing the block, which
// matters because the pruner may delete a record between its create answering
// ErrTxExists and this read.
//
// Two limits, stated rather than hidden. With the catch-up lock switched off
// (blockvalidation_quick_validate_skip_utxo_lock) nothing writes the mark, and a
// leftover is filed as pre-existing again. And the id match assumes the retry
// gets the id the earlier attempt stamped; if the process restarted (losing the
// blockchain service's reservation) AND the block's first non-coinbase record is
// gone (the id-reuse lookup reads it), the retry takes a new id and the other
// leftovers of the earlier attempt are filed as pre-existing.
func LeftoversAmong(ctx context.Context, store Store, existing []*chainhash.Hash, blockID uint32) (map[chainhash.Hash]struct{}, error) {
	if len(existing) == 0 {
		return nil, nil
	}

	unresolved := make([]*UnresolvedMetaData, len(existing))
	for i, hash := range existing {
		unresolved[i] = &UnresolvedMetaData{Hash: *hash, Idx: i}
	}

	if err := store.BatchDecorate(ctx, unresolved, fields.Locked, fields.BlockIDs); err != nil {
		return nil, errors.NewStorageError("[LeftoversAmong] could not read the lock state of %d existing transactions", len(existing), err)
	}

	leftovers := make(map[chainhash.Hash]struct{})

	for _, item := range unresolved {
		// The coinbase placeholder is never a stored record, and the Aerospike
		// store answers it with neither data nor an error.
		if item.Hash.Equal(subtree.CoinbasePlaceholderHashValue) {
			continue
		}

		// A record that is gone is unambiguously not a leftover. The concurrent
		// pruner can delete a record between its create answering ErrTxExists
		// and this read; failing the block on that turned a benign race into a
		// block failure that only a retry cleared.
		if item.Err != nil && errors.Is(item.Err, errors.ErrTxNotFound) {
			continue
		}

		if item.Err != nil {
			return nil, errors.NewStorageError("[LeftoversAmong] could not read the lock state of existing transaction %s", item.Hash.String(), item.Err)
		}

		if item.Data == nil {
			return nil, errors.NewStorageError("[LeftoversAmong] store returned no data for existing transaction %s", item.Hash.String())
		}

		if item.Data.Locked && carriesBlockID(item.Data.BlockIDs, blockID) {
			leftovers[item.Hash] = struct{}{}
		}
	}

	return leftovers, nil
}

// CommittedBlockHashFunc answers which block, if any, is committed under a
// block id: its hash, or nil when no committed block holds the id. It must be
// read-only. Callers build it from the blockchain service's GetBlockByID; it is
// a function rather than that interface so this package need not import model.
type CommittedBlockHashFunc func(ctx context.Context, id uint64) (*chainhash.Hash, error)

// ConfirmLeftovers keeps the leftovers LeftoversAmong found unless a different
// block is committed under blockID, in which case it returns nil with mismatch
// set.
//
// LeftoversAmong's whole argument is that no other writer carries THIS block's
// id. Both block paths may take that id from the mined-in ids already recorded
// on the block's first non-coinbase transaction, and a transaction shared with
// another block carries the OTHER block's id. When that other block is
// committed and its post-commit unlock failed, its records are still locked
// under its id, so adopting the id made them look like this block's own
// leftovers: dropped from this block's SetMinedMulti and eligible for this
// block's compensating delete. A committed block naming a different hash under
// the id settles that the records are its, and filing them as pre-existing is
// correct.
//
// The question is asked read-only on purpose. AssignBlockID is not usable here:
// the blockchain store sweeps reservations older than an hour, and a block
// retried after that reads its id back from its own records with no
// reservation left. AssignBlockID would then reserve a fresh id as a side
// effect, the real leftovers would be filed as pre-existing (re-opening the
// bless of a replay by its own earlier write), and AddBlock would refuse the
// block's own id against the new reservation on every retry.
//
// The limit, stated: a sibling that is still in flight holds the id only as a
// reservation, which this lookup does not see, so that case is not caught here.
// Closing it needs a read-only "who holds this id, committed or reserved"
// lookup on the blockchain service.
//
// An error is returned rather than guessed at. It asks only when there are
// leftovers, so a first attempt never pays for it.
func ConfirmLeftovers(ctx context.Context, committedHash CommittedBlockHashFunc, blockHash *chainhash.Hash, blockID uint32,
	leftovers map[chainhash.Hash]struct{}) (confirmed map[chainhash.Hash]struct{}, mismatch bool, err error) {
	if len(leftovers) == 0 {
		return leftovers, false, nil
	}

	if committedHash == nil {
		return nil, false, errors.NewProcessingError("[ConfirmLeftovers] no block lookup to confirm %d leftovers of block %s", len(leftovers), blockHash.String())
	}

	holder, err := committedHash(ctx, uint64(blockID))
	if err != nil {
		return nil, false, errors.NewProcessingError("[ConfirmLeftovers] could not look up which block holds id %d (block %s)", blockID, blockHash.String(), err)
	}

	if holder != nil && !holder.IsEqual(blockHash) {
		return nil, true, nil
	}

	return leftovers, false, nil
}

// carriesBlockID reports whether blockIDs contains blockID.
func carriesBlockID(blockIDs []uint32, blockID uint32) bool {
	for _, id := range blockIDs {
		if id == blockID {
			return true
		}
	}

	return false
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
//     same call (needsSpendRollback), on the marker answer and on the missing
//     parent answer alike. What its inputs still record is the
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

// RollbackSet decides which of a failed Spend's successful inputs must be
// reversed: everything this call actually wrote, plus, in most cases, the
// inputs whose output already recorded exactly this spend.
//
// An idempotent match is ambiguous on the record. "This output already records
// this spender" is byte-identical whether the spend was committed by a mined
// transaction long ago or by an earlier failed attempt at this very call, so
// the record cannot tell the two apart and the answer has to come from why the
// call is failing now.
//
// Two rejections make the match historical, the two a pruned replay gets
// (IsReplayAnswer). The marker fires only for a transaction this store pruned,
// which happens only once that transaction was mined, fully spent and buried,
// so any output recording it as spender is recording a confirmed spend. A
// missing parent is the same answer for a chain the pruner removed end to end,
// marker and all. Reversing either would hand a confirmed output to the next
// spender, which is the double-spend this exclusion was added to prevent.
//
// Every other rejection leaves the match reversible, and reversing it is what
// keeps the store self-healing. The store leaves partial spends committed when
// a call fails on a transient error, so an attempt that wrote one input and
// then failed on another leaves that input spent by a transaction it never
// created. On the next attempt that input reads as an idempotent match. Holding
// it back unconditionally made the orphan permanent: the output stayed spent by
// a transaction the store does not hold, and its next legitimate spender was
// refused with a txid this node has never seen.
//
// historical is the caller's answer to "might this call have been rejected as a
// pruned replay?". It must be true when any input was, and also whenever
// the caller does not know every input's answer, as on a spend call aborted
// while some inputs were still in flight: an input not yet answered may be the
// marker hit, and reading an in-flight slot is a data race besides. Holding an
// idempotent match back fails towards an output left spent by a transaction the
// store does not hold, which the next attempt heals; reversing it wrongly hands
// a confirmed output to anyone.
func RollbackSet(written, idempotent []*Spend, historical bool) []*Spend {
	if len(idempotent) == 0 || historical {
		return written
	}

	// A fresh slice, not append(written, ...): with spare capacity, append would
	// write the idempotent matches into the caller's backing array.
	rollback := make([]*Spend, 0, len(written)+len(idempotent))
	rollback = append(rollback, written...)

	return append(rollback, idempotent...)
}

// AnyPrunedReplay reports whether any spend failed with an answer that can mean
// a pruned replay (IsReplayAnswer). Only for spends whose Err slot is safe to
// read.
func AnyPrunedReplay(spends []*Spend) bool {
	for _, spend := range spends {
		if spend != nil && IsReplayAnswer(spend.Err) {
			return true
		}
	}

	return false
}

// IsReplayAnswer reports whether a per-input spend error is one of the two
// answers a replay of a pruned transaction gets: the marker rejection, or a
// parent record that is gone. The stores use it to decide that an idempotent
// match in the same call is historical and must not be reversed (RollbackSet).
// Whether the call really was a replay is the block paths' question, answered
// with IsPrunedReplayRejection.
func IsReplayAnswer(err error) bool {
	return err != nil && (errors.Is(err, errors.ErrUtxoSpendingTxPruned) || errors.Is(err, errors.ErrTxNotFound))
}

// ReplayRejectionsFirst reorders per-input spend errors so the ones that
// identify a pruned replay come first: the marker rejection, then a missing
// parent. Stores and the validator aggregate these with errors.JoinCapped, which
// keeps only the first few links, so on a wide transaction whose marker hit sat
// past the cap the rejection vanished from the error and the block paths could
// not recognise the replay. The order within each group is preserved.
func ReplayRejectionsFirst(errs []error) []error {
	ordered := make([]error, 0, len(errs))

	for _, rank := range []func(error) bool{
		func(err error) bool { return errors.Is(err, errors.ErrUtxoSpendingTxPruned) },
		func(err error) bool {
			return !errors.Is(err, errors.ErrUtxoSpendingTxPruned) && errors.Is(err, errors.ErrTxNotFound)
		},
		func(err error) bool {
			return !errors.Is(err, errors.ErrUtxoSpendingTxPruned) && !errors.Is(err, errors.ErrTxNotFound)
		},
	} {
		for _, err := range errs {
			if err != nil && rank(err) {
				ordered = append(ordered, err)
			}
		}
	}

	return ordered
}
