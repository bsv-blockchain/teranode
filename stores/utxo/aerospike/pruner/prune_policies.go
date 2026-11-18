package pruner

import (
	"github.com/bsv-blockchain/aerospike-client-go/v8"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
)

// The pruner only removes records that are already provably safe to drop, and
// every removal is idempotent: a replica that misses the write is caught by the
// next partition scan and re-pruned. Blocking on a full-replication ACK
// therefore buys no correctness, so removals default to COMMIT_MASTER.
// pruner_relaxRemovalCommitLevel=false puts them back on COMMIT_ALL without a
// code change, for when the relaxation has to be ruled out in the field.
//
// This applies to the pruner's own record removals ONLY. Parent updates (the
// deletedChildren marker writes, each native sub-op 15 or a map write filtered on the parent output)
// mutate records that survive the prune, so they keep COMMIT_ALL, as does every write outside the
// pruner — see util.GetAerospikeWritePolicy.

// removalCommitLevel maps pruner_relaxRemovalCommitLevel onto the commit level
// used for the pruner's own record removals.
func removalCommitLevel(relax bool) aerospike.CommitLevel {
	if relax {
		return aerospike.COMMIT_MASTER
	}

	return aerospike.COMMIT_ALL
}

// newPrunerDeletePolicy builds the per-record policy for hard-deleting a pruned
// UTXO record.
func newPrunerDeletePolicy(commitLevel aerospike.CommitLevel) *aerospike.BatchDeletePolicy {
	policy := aerospike.NewBatchDeletePolicy()
	policy.CommitLevel = commitLevel

	return policy
}

// newPrunerTTLTouchPolicy builds the per-record policy used when
// pruner_utxoSetTTL is enabled: instead of deleting, the record's TTL is set to
// one second and Aerospike's nsup thread expires it. UPDATE_ONLY keeps the
// touch from resurrecting an already-deleted record as an empty stub.
func newPrunerTTLTouchPolicy(commitLevel aerospike.CommitLevel) *aerospike.BatchWritePolicy {
	policy := aerospike.NewBatchWritePolicy()
	policy.RecordExistsAction = aerospike.UPDATE_ONLY
	policy.Expiration = 1
	policy.CommitLevel = commitLevel

	return policy
}

// carriedMasterGuard is the server-side condition a carried master delete
// must meet, the same rules partitionWorker's scan and dropUnminedRecords apply
// when the pruner selects a record: it still has a deleteAtHeight at or below
// blockHeight, it is not locked, and it has no unmined_since bin unless it is
// conflicting. The pruner selects a child while it is mined and fully spent, or
// conflicting, and deletes it a batch later, or at the worker's final drain. In between, a
// block invalidation can strip its block ids and stamp unmined_since, a reorg's
// move-back can lock it while it saves it for restoration, and a conflict
// resolution that releases one of its outputs clears its deleteAtHeight (Lua
// setDeleteAtHeight clears it once the record is no longer all spent). Deleting
// it then removes a live transaction, or a live output. A record that fails the
// guard answers FILTERED_OUT, which classifyDeletionResults reads as a refusal:
// the child is held back and its markers are withdrawn.
//
// Locking a record strips its deleteAtHeight (setLocked in teranode.lua), so a
// locked record that kept the guard's height leg is one locked after selection.
func carriedMasterGuard(blockHeight uint32) *aerospike.Expression {
	locked := fields.Locked.String()
	conflicting := fields.Conflicting.String()
	deleteAtHeight := fields.DeleteAtHeight.String()

	return aerospike.ExpAnd(
		aerospike.ExpBinExists(deleteAtHeight),
		aerospike.ExpLessEq(aerospike.ExpIntBin(deleteAtHeight), aerospike.ExpIntVal(int64(blockHeight))),
		aerospike.ExpOr(
			aerospike.ExpNot(aerospike.ExpBinExists(locked)),
			aerospike.ExpNot(aerospike.ExpBoolBin(locked)),
		),
		aerospike.ExpOr(
			aerospike.ExpNot(aerospike.ExpBinExists(fields.UnminedSince.String())),
			aerospike.ExpAnd(
				aerospike.ExpBinExists(conflicting),
				aerospike.ExpBoolBin(conflicting),
			),
		),
	)
}

// buildDeletionBatchRecords turns the pruned keys into the batch records that
// remove them: a hard delete per key, or a 1-second TTL touch when
// pruner_utxoSetTTL is enabled. Used by buildCarriedDeletes, which is the one
// path that removes a pruned record: a delete rides the batch after the one
// that wrote its parent markers. The keys at the indices in masters are master
// records and carry carriedMasterGuard; pagination records have no locked or
// unmined_since bin and go unguarded. blockHeight is the height the masters
// were selected at; see carriedMasterGuard.
func buildDeletionBatchRecords(keys []*aerospike.Key, masters map[int]*chainhash.Hash, blockHeight uint32, utxoSetTTL bool, commitLevel aerospike.CommitLevel) []aerospike.BatchRecordIfc {
	batchRecords := make([]aerospike.BatchRecordIfc, len(keys))

	if utxoSetTTL {
		ttlWritePolicy := newPrunerTTLTouchPolicy(commitLevel)
		guardedTTLPolicy := newPrunerTTLTouchPolicy(commitLevel)
		guardedTTLPolicy.FilterExpression = carriedMasterGuard(blockHeight)

		for i, key := range keys {
			policy := ttlWritePolicy
			if _, isMaster := masters[i]; isMaster {
				policy = guardedTTLPolicy
			}

			batchRecords[i] = aerospike.NewBatchWrite(policy, key, aerospike.TouchOp())
		}

		return batchRecords
	}

	batchDeletePolicy := newPrunerDeletePolicy(commitLevel)
	guardedDeletePolicy := newPrunerDeletePolicy(commitLevel)
	guardedDeletePolicy.FilterExpression = carriedMasterGuard(blockHeight)

	for i, key := range keys {
		policy := batchDeletePolicy
		if _, isMaster := masters[i]; isMaster {
			policy = guardedDeletePolicy
		}

		batchRecords[i] = aerospike.NewBatchDelete(policy, key)
	}

	return batchRecords
}
